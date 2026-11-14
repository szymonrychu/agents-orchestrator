"""Idle discard: an idle session flips to deleting through the discard path, guarded."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

import pytest
from factories import (
    insert_agent,
    insert_message,
    insert_session,
    insert_turn,
    set_agent_status,
)

from agents_orchestrator.api import autoclose
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue, sessions
from agents_orchestrator.db.queue import SessionBeingDeleted
from agents_orchestrator.settings import Settings


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
    s = await insert_session(pool, spec=spec if spec is not None else {}, status=status)
    a = await insert_agent(pool, s["id"], ready=True)
    await set_agent_status(pool, a["id"], state="idle")
    await _age(pool, s["id"])
    return s, a


async def _desired(pool, session_id) -> str:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT desired FROM sessions WHERE id = %s", (session_id,))
        return (await cur.fetchone())[0]


async def _run(pool, grace: int = 30) -> list:
    async with pool.connection() as conn:
        return await sessions.discard_idle(conn, grace)


async def _age_close(pool, session_id, seconds: int) -> None:
    """Move the session's close request `seconds` into the past."""
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET close_requested_at = now() - make_interval(secs => %s)"
            " WHERE id = %s",
            (seconds, session_id),
        )


async def _request(pool, session_id, reason="done") -> None:
    async with pool.connection() as conn:
        await sessions.request_close(conn, session_id, reason)


async def _age_all(pool, session_id, seconds: int) -> None:
    await _age(pool, session_id, seconds)
    await _age_close(pool, session_id, seconds)


async def _closing_session(pool, status=None):
    """An idle session whose close request is older than the default grace (30 s)."""
    s, a = await _idle_session(pool, status=status)
    await _request(pool, s["id"])
    await _age_all(pool, s["id"], 120)
    return s, a


async def _lock_waiters(pool, at_least: int, timeout: float = 10.0) -> None:
    """Wait until `at_least` other backends of this database wait on a lock.

    Makes the arrival order at a row lock observed instead of assumed from sleeps.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    async with pool.connection() as conn:
        while True:
            cur = await conn.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()"
                " AND pid <> pg_backend_pid() AND wait_event_type = 'Lock'"
            )
            waiting = (await cur.fetchone())[0]
            await conn.rollback()  # pg_stat_activity is a per-transaction snapshot
            if waiting >= at_least:
                return
            assert asyncio.get_running_loop().time() < deadline, "lock waiters never showed up"
            await asyncio.sleep(0.02)


async def test_idle_session_is_discarded_with_generation_bump(pool):
    s, _ = await _closing_session(pool)
    assert await _run(pool) == [s["id"]]
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT desired, generation FROM sessions WHERE id=%s", (s["id"],))
        assert await cur.fetchone() == ("deleting", s["generation"] + 1)


async def test_idle_session_without_close_request_is_never_discarded(pool):
    s, _ = await _idle_session(pool)
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_not_idle_long_enough(pool):
    s, a = await _closing_session(pool)
    await set_agent_status(pool, a["id"], state="idle")  # free_since = now
    assert await _run(pool) == []


async def test_busy_agent_blocks(pool):
    s, a = await _closing_session(pool)
    b = await insert_agent(pool, s["id"], ready=True)
    await set_agent_status(pool, b["id"], state="busy")
    await _age(pool, s["id"])
    assert await _run(pool) == []


async def test_running_turn_blocks(pool):
    s, a = await _closing_session(pool)
    await set_agent_status(pool, a["id"], state="idle", current_turn_id="t1")
    await _age(pool, s["id"])
    assert await _run(pool) == []


@pytest.mark.parametrize("status", ["queued", "dispatching", "verifying"])
async def test_pending_message_blocks(pool, status):
    s, a = await _closing_session(pool)
    await insert_message(pool, s["id"], status=status)
    await _age(pool, s["id"])
    assert await _run(pool) == []


async def test_sent_message_does_not_block_but_restarts_clock(pool):
    s, a = await _closing_session(pool)
    m = await insert_message(pool, s["id"], status="sent")
    await _age(pool, s["id"])
    assert await _run(pool) == [s["id"]]
    s2, _ = await _closing_session(pool)
    await insert_message(pool, s2["id"], status="sent")  # created now: clock restarts
    assert await _run(pool) == []
    assert m["status"] == "sent"


async def test_attention_blocks(pool):
    s, a = await _closing_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET waiting_reason='permission',"
            " waiting_since=now()-interval '5 min'"
            " WHERE agent_id=%s",
            (a["id"],),
        )
    assert await _run(pool) == []


async def test_init_in_progress_blocks(pool):
    s, _ = await _closing_session(
        pool, status={"init": {"state": "running", "started_at": "2000-01-01T00:00:00+00:00"}}
    )
    assert await _run(pool) == []


async def test_recent_init_finish_restarts_clock(pool):
    s, _ = await _closing_session(
        pool, status={"init": {"state": "succeeded", "finished_at": "2999-01-01T00:00:00+00:00"}}
    )
    assert await _run(pool) == []


async def test_no_agents_or_gone_only_does_not_discard(pool):
    s = await insert_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_all(pool, s["id"], 120)
    assert await _run(pool) == []


async def test_already_deleting_untouched_and_idempotent(pool):
    s, _ = await _closing_session(pool)
    one, two = await asyncio.gather(_run(pool), _run(pool))
    assert sorted(one + two) == [s["id"]]
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT generation FROM sessions WHERE id=%s", (s["id"],))
        assert (await cur.fetchone())[0] == s["generation"] + 1


async def test_message_wins_race_against_pending_discard(pool):
    """A message committed first blocks the discard; one racing it either wins or is refused."""
    s, _ = await _closing_session(pool)
    async with pool.connection() as conn:
        await queue.enqueue_message(conn, s["id"], "follow-up")
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_discard_holding_lock_makes_enqueue_refuse_not_lose_message(pool):
    s, _ = await _closing_session(pool)

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
    s, _ = await _closing_session(pool)
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
    s, _ = await _closing_session(pool)
    app = create_app(Settings(database_url=dsn, agent_image="img:1"))
    async with app.router.lifespan_context(app):
        for _ in range(50):
            if await _desired(pool, s["id"]) == "deleting":
                break
            await asyncio.sleep(0.1)
    assert await _desired(pool, s["id"]) == "deleting"


# Self-close (hive-self-close): a requested close is discarded once idle plus the grace.


async def test_requested_close_flips_after_grace(pool, caplog):
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_close(pool, s["id"], 40)
    with caplog.at_level(logging.INFO, logger="agents_orchestrator.db.sessions"):
        assert await _run(pool, grace=30) == [s["id"]]
    assert await _desired(pool, s["id"]) == "deleting"
    [line] = [m for m in caplog.messages if m.startswith("session close: ")][-1:]
    assert line.startswith(f"session close: {s['id']} close_kind=self step=discard reason=")


async def test_requested_close_waits_for_grace(pool):
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])  # close_requested_at = now: the grace restarts the clock
    assert await _run(pool, grace=30) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_requested_close_waits_for_running_turn(pool):
    s, a = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    await _age_all(pool, s["id"], 120)
    assert await _run(pool) == []
    await set_agent_status(pool, a["id"], state="idle", current_turn_id="t1")
    await _age_all(pool, s["id"], 120)
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_requested_close_counts_grace_from_turn_end(pool):
    """An old request meets a freshly ended calling turn: the grace restarts from the turn end."""
    s, a = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_all(pool, s["id"], 120)
    turn = await insert_turn(pool, a["id"], status="completed", completed_at=datetime.now(UTC))
    assert await _run(pool) == []
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE turns SET started_at = now() - interval '2 min',"
            " completed_at = now() - interval '2 min' WHERE turn_id = %s",
            (turn["turn_id"],),
        )
    assert await _run(pool) == [s["id"]]


async def test_requested_close_waits_for_queued_message(pool):
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    m = await insert_message(pool, s["id"], status="queued")
    await _age_all(pool, s["id"], 120)
    assert await _run(pool) == []
    async with pool.connection() as conn:
        await conn.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (m["id"],))
    await _age_all(pool, s["id"], 120)
    assert await _run(pool) == [s["id"]]


async def test_requested_close_waits_for_attention_and_init(pool):
    s, a = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET waiting_reason = 'permission',"
            " waiting_since = now() - interval '5 min' WHERE agent_id = %s",
            (a["id"],),
        )
    await _age_all(pool, s["id"], 120)
    assert await _run(pool) == []

    init = {"init": {"state": "running", "started_at": "2000-01-01T00:00:00+00:00"}}
    s2, _ = await _idle_session(pool, spec={}, status=init)
    await _request(pool, s2["id"])
    await _age_all(pool, s2["id"], 120)
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == await _desired(pool, s2["id"]) == "active"


async def test_grace_zero_flips_on_next_pass_when_idle(pool):
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_close(pool, s["id"], 1)
    assert await _run(pool, grace=0) == [s["id"]]


async def test_cancel_before_flip_keeps_session(pool):
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_all(pool, s["id"], 120)
    async with pool.connection() as conn:
        await sessions.cancel_close(conn, s["id"])
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_cancel_racing_flip_has_one_outcome(pool):
    """A cancel committed while the discard waits on the row lock wins; a discard that took the
    lock first wins and the cancel is refused."""
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_all(pool, s["id"], 120)
    async with pool.connection() as holder:
        async with holder.transaction():
            await holder.execute("SELECT 1 FROM sessions WHERE id = %s FOR UPDATE", (s["id"],))
            task = asyncio.create_task(_run(pool))
            await _lock_waiters(pool, 1)
            assert not task.done()  # the discard waits on the session lock
            await sessions.cancel_close(holder, s["id"])
        assert await task == []
    assert await _desired(pool, s["id"]) == "active"

    # mirror: the discard holds the lock first, so the cancel queued behind it is refused
    m, _ = await _idle_session(pool, spec={})
    await _request(pool, m["id"])
    await _age_all(pool, m["id"], 120)

    async def cancel() -> str:
        async with pool.connection() as conn:
            try:
                await sessions.cancel_close(conn, m["id"])
            except SessionBeingDeleted:
                return "refused"
        return "cancelled"

    async with pool.connection() as holder, holder.transaction():
        await holder.execute("SELECT 1 FROM sessions WHERE id = %s FOR UPDATE", (m["id"],))
        flip = asyncio.create_task(_run(pool))
        await _lock_waiters(pool, 1)  # the flip is queued on the row lock ...
        cancelled = asyncio.create_task(cancel())
        await _lock_waiters(pool, 2)  # ... before the cancel joins the queue behind it
        assert not flip.done() and not cancelled.done()
    assert await flip == [m["id"]]
    assert await cancelled == "refused"
    assert await _desired(pool, m["id"]) == "deleting"


async def test_sweep_passes_grace(monkeypatch, pool):
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.05)
    seen: list[int] = []

    async def fake_discard_idle(conn, close_grace_seconds):
        seen.append(close_grace_seconds)
        return []

    monkeypatch.setattr(sessions, "discard_idle", fake_discard_idle)
    settings = Settings(
        database_url="postgresql://x/y", agent_image="img:1", close_grace_seconds=17
    )
    task = asyncio.create_task(autoclose.sweep(pool, settings))
    for _ in range(100):
        if seen:
            break
        await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert seen and set(seen) == {17}


async def test_sweeper_uses_close_grace_setting(dsn, pool, monkeypatch):
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.1)
    s, _ = await _idle_session(pool, spec={})
    await _request(pool, s["id"])
    await _age_close(pool, s["id"], 15)
    app = create_app(Settings(database_url=dsn, agent_image="img:1", close_grace_seconds=10))
    async with app.router.lifespan_context(app):
        for _ in range(50):
            if await _desired(pool, s["id"]) == "deleting":
                break
            await asyncio.sleep(0.1)
    assert await _desired(pool, s["id"]) == "deleting"


async def test_running_background_subagent_blocks_discard(pool):
    s, a = await _closing_session(pool)
    started = datetime.now(UTC).isoformat()
    await set_agent_status(pool, a["id"], state="idle", background_subagents={"sa1": started})
    await _age(pool, s["id"])
    assert await _run(pool) == []
    assert await _desired(pool, s["id"]) == "active"


async def test_stale_background_subagent_does_not_block_discard(pool):
    s, a = await _closing_session(pool)
    stale = datetime(2020, 1, 1, tzinfo=UTC).isoformat()
    await set_agent_status(pool, a["id"], state="idle", background_subagents={"sa1": stale})
    await _age(pool, s["id"])
    assert await _run(pool) == [s["id"]]
