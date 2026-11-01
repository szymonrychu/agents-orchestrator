"""Idle discard: an idle session flips to deleting through the discard path, guarded."""

from __future__ import annotations

import asyncio

import pytest
from factories import insert_agent, insert_message, insert_session, set_agent_status
from pydantic import ValidationError

from agents_orchestrator.api import autoclose
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue, sessions
from agents_orchestrator.db.queue import SessionBeingDeleted
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

IDLE = {"discard_after_idle_seconds": 60}


async def _age(pool, session_id, seconds: int = 120) -> None:
    """Move every idleness signal of the session into the past."""
    ago = f"now() - make_interval(secs => {int(seconds)})"
    async with pool.connection() as conn:
        await conn.execute(f"UPDATE sessions SET created_at = {ago} WHERE id = %s", (session_id,))
        await conn.execute(
            f"UPDATE agent_status SET free_since = {ago}, updated_at = {ago}"
            " WHERE agent_id IN (SELECT id FROM agents WHERE session_id = %s)",
            (session_id,),
        )
        await conn.execute(
            f"UPDATE messages SET created_at = {ago}, dispatched_at = {ago} WHERE session_id = %s",
            (session_id,),
        )


async def _idle_session(pool, spec=None, status=None):
    s = await insert_session(pool, spec=spec if spec is not None else IDLE, status=status)
    a = await insert_agent(pool, s["id"], ready=True)
    await set_agent_status(pool, a["id"], state="idle")
    await _age(pool, s["id"])
    return s, a


async def _desired(pool, session_id) -> str:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT desired FROM sessions WHERE id = %s", (session_id,))
        return (await cur.fetchone())[0]


async def _run(pool) -> list:
    async with pool.connection() as conn:
        return await sessions.discard_idle(conn)


def test_spec_field_minimum_60_and_default_unset():
    assert SessionSpec().discard_after_idle_seconds is None
    assert SessionSpec(discard_after_idle_seconds=60).discard_after_idle_seconds == 60
    with pytest.raises(ValidationError):
        SessionSpec(discard_after_idle_seconds=59)


async def test_idle_session_is_discarded_with_generation_bump(pool):
    s, _ = await _idle_session(pool)
    assert await _run(pool) == [s["id"]]
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT desired, generation FROM sessions WHERE id=%s", (s["id"],))
        assert await cur.fetchone() == ("deleting", s["generation"] + 1)


async def test_off_when_unset_or_null(pool):
    a, _ = await _idle_session(pool, spec={})
    b, _ = await _idle_session(pool, spec={"discard_after_idle_seconds": None})
    assert await _run(pool) == []
    assert await _desired(pool, a["id"]) == await _desired(pool, b["id"]) == "active"


async def test_not_idle_long_enough(pool):
    s, a = await _idle_session(pool)
    await set_agent_status(pool, a["id"], state="idle")  # free_since = now
    assert await _run(pool) == []


async def test_busy_agent_blocks(pool):
    s, a = await _idle_session(pool)
    b = await insert_agent(pool, s["id"], ready=True)
    await set_agent_status(pool, b["id"], state="busy")
    await _age(pool, s["id"])
    assert await _run(pool) == []


async def test_running_turn_blocks(pool):
    s, a = await _idle_session(pool)
    await set_agent_status(pool, a["id"], state="idle", current_turn_id="t1")
    await _age(pool, s["id"])
    assert await _run(pool) == []


@pytest.mark.parametrize("status", ["queued", "dispatching", "verifying"])
async def test_pending_message_blocks(pool, status):
    s, a = await _idle_session(pool)
    await insert_message(pool, s["id"], status=status)
    await _age(pool, s["id"])
    assert await _run(pool) == []


async def test_sent_message_does_not_block_but_restarts_clock(pool):
    s, a = await _idle_session(pool)
    m = await insert_message(pool, s["id"], status="sent")
    await _age(pool, s["id"])
    assert await _run(pool) == [s["id"]]
    s2, _ = await _idle_session(pool)
    await insert_message(pool, s2["id"], status="sent")  # created now: clock restarts
    assert await _run(pool) == []
    assert m["status"] == "sent"


async def test_attention_blocks(pool):
    s, a = await _idle_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET waiting_reason='permission',"
            " waiting_since=now()-interval '5 min'"
            " WHERE agent_id=%s",
            (a["id"],),
        )
    assert await _run(pool) == []


async def test_init_in_progress_blocks(pool):
    s, _ = await _idle_session(
        pool, status={"init": {"state": "running", "started_at": "2000-01-01T00:00:00+00:00"}}
    )
    assert await _run(pool) == []


async def test_recent_init_finish_restarts_clock(pool):
    s, _ = await _idle_session(
        pool, status={"init": {"state": "succeeded", "finished_at": "2999-01-01T00:00:00+00:00"}}
    )
    assert await _run(pool) == []


async def test_no_agents_or_gone_only_does_not_discard(pool):
    s = await insert_session(pool, spec=IDLE)
    await _age(pool, s["id"])
    assert await _run(pool) == []


async def test_already_deleting_untouched_and_idempotent(pool):
    s, _ = await _idle_session(pool)
    one, two = await asyncio.gather(_run(pool), _run(pool))
    assert sorted(one + two) == [s["id"]]
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT generation FROM sessions WHERE id=%s", (s["id"],))
        assert (await cur.fetchone())[0] == s["generation"] + 1


async def test_message_wins_race_against_pending_discard(pool):
    """A message committed first blocks the discard; one racing it either wins or is refused."""
    s, _ = await _idle_session(pool)
    async with pool.connection() as conn:
        await queue.enqueue_message(conn, s["id"], "follow-up")
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_discard_holding_lock_makes_enqueue_refuse_not_lose_message(pool):
    s, _ = await _idle_session(pool)

    async def discard():
        return await _run(pool)

    async def enqueue():
        async with pool.connection() as conn:
            try:
                await queue.enqueue_message(conn, s["id"], "late")
            except SessionBeingDeleted:
                return "refused"
            return "queued"

    d, e = await asyncio.gather(discard(), enqueue())
    desired = await _desired(pool, s["id"])
    if e == "queued":
        # the message won: the discard must have been cancelled
        assert d == [] and desired == "active"
    else:
        assert d == [s["id"]] and desired == "deleting"


async def test_discard_blocked_between_check_and_flip_by_interleaved_message(pool):
    """Deterministic interleave: the discard's session lock is held while a message waits."""
    s, _ = await _idle_session(pool)
    async with pool.connection() as holder:
        async with holder.transaction():
            await holder.execute("SELECT 1 FROM sessions WHERE id=%s FOR UPDATE", (s["id"],))
            task = asyncio.create_task(_enqueue_outcome(pool, s["id"]))
            await asyncio.sleep(0.3)
            assert not task.done()  # the message waits on the session lock
            await holder.execute(
                "UPDATE sessions SET desired='deleting', generation=generation+1 WHERE id=%s",
                (s["id"],),
            )
        assert await task == "refused"


async def _enqueue_outcome(pool, session_id) -> str:
    async with pool.connection() as conn:
        try:
            await queue.enqueue_message(conn, session_id, "late")
        except SessionBeingDeleted:
            return "refused"
    return "queued"


async def test_sweeper_discards_idle_session(dsn, pool, monkeypatch):
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.1)
    s, _ = await _idle_session(pool)
    app = create_app(Settings(database_url=dsn, agent_image="img:1"))
    async with app.router.lifespan_context(app):
        for _ in range(50):
            if await _desired(pool, s["id"]) == "deleting":
                break
            await asyncio.sleep(0.1)
    assert await _desired(pool, s["id"]) == "deleting"
