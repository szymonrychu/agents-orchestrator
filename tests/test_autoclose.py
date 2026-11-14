"""Autoclose sweeper: due sessions flip to deleting through the discard path."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import psycopg
import pytest
from factories import insert_agent, insert_session, set_agent_status

from agents_orchestrator import inactivity
from agents_orchestrator.api import autoclose
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import handover, sessions
from agents_orchestrator.db.queue import enqueue_message
from agents_orchestrator.settings import Settings


async def _backdate(pool, session_id, seconds: int) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET created_at = now() - make_interval(secs => %s) WHERE id = %s",
            (seconds, session_id),
        )


async def _row(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM sessions WHERE id = %s", (session_id,))
        cols = [d.name for d in cur.description]
        return dict(zip(cols, await cur.fetchone(), strict=True))


class _Compose:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def __call__(self, session, deadline) -> str:
        self.calls.append((session, deadline))
        return "ask"


async def _ask(pool, compose=None, handoff: int = 300) -> list:
    async with pool.connection() as conn:
        return await sessions.ask_autoclose(conn, handoff, compose or _Compose())


async def _discard(pool) -> list:
    async with pool.connection() as conn:
        return await sessions.discard_idle(conn, 30)


async def _shift(pool, session_id, seconds: int) -> None:
    """Let `seconds` pass for the session: move every stored time of it into the past."""
    iv = "make_interval(secs => %(secs)s)"
    params = {"secs": seconds, "id": session_id}
    async with pool.connection() as conn:
        await conn.execute(
            f"UPDATE sessions SET created_at = created_at - {iv},"
            f" last_update_at = last_update_at - {iv},"
            f" autoclose_asked_at = autoclose_asked_at - {iv},"
            f" close_requested_at = close_requested_at - {iv},"
            f" close_deadline_at = close_deadline_at - {iv} WHERE id = %(id)s",
            params,
        )
        await conn.execute(
            f"UPDATE messages SET created_at = created_at - {iv},"
            f" dispatched_at = dispatched_at - {iv} WHERE session_id = %(id)s",
            params,
        )
        await conn.execute(
            f"UPDATE turns SET started_at = started_at - {iv},"
            f" completed_at = completed_at - {iv}"
            " WHERE agent_id IN (SELECT id FROM agents WHERE session_id = %(id)s)",
            params,
        )
        await conn.execute(
            f"UPDATE agent_status SET free_since = free_since - {iv}"
            " WHERE agent_id IN (SELECT id FROM agents WHERE session_id = %(id)s)",
            params,
        )


async def _idle_session(pool, spec):
    c = await insert_session(pool, spec=spec)
    a = await insert_agent(pool, c["id"], ready=True)
    await set_agent_status(pool, a["id"], state="idle")
    return c, a


async def _notices(pool, session_id) -> list[tuple]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT notice, lane, text FROM messages"
            " WHERE session_id = %s AND notice IS NOT NULL ORDER BY id",
            (session_id,),
        )
        return await cur.fetchall()


async def _delivered(pool, session_id) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET status = 'sent' WHERE session_id = %s", (session_id,)
        )


async def test_due_session_is_asked_not_flipped(pool, dsn):
    c, _ = await _idle_session(pool, {"autoclose_after_seconds": 3600})
    await _shift(pool, c["id"], 3601)
    compose = _Compose()
    notified: list[dict] = []
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as lconn:
        await lconn.execute("LISTEN session_changed")
        assert await _ask(pool, compose) == [c["id"]]
        async for n in lconn.notifies(timeout=2, stop_after=1):
            notified.append(json.loads(n.payload))
    assert notified == [{"session_id": str(c["id"])}]
    row = await _row(pool, c["id"])
    assert row["close_kind"] == "autoclose"
    assert row["close_reason"] == "Lifetime of 1 h reached (autoclose_after_seconds 3600)"
    assert (row["close_deadline_at"] - row["close_requested_at"]).total_seconds() == 300
    assert row["autoclose_asked_at"] == row["close_requested_at"]
    assert (row["desired"], row["generation"]) == ("active", c["generation"])
    assert await _notices(pool, c["id"]) == [("autoclose", "regular", "ask")]
    [(session, deadline)] = compose.calls
    assert session.id == c["id"] and deadline == row["close_deadline_at"]
    assert await _ask(pool) == []  # asked once


async def test_not_due_and_unset_not_asked(pool):
    young, _ = await _idle_session(pool, {"autoclose_after_seconds": 300})
    none, _ = await _idle_session(pool, {})
    gone = await insert_session(pool, spec={"autoclose_after_seconds": 300}, desired="deleting")
    await _shift(pool, young["id"], 10)
    await _shift(pool, none["id"], 99999)
    await _shift(pool, gone["id"], 99999)
    assert await _ask(pool) == []
    for c in (young, none, gone):
        assert (await _row(pool, c["id"]))["close_requested_at"] is None
        assert await _notices(pool, c["id"]) == []


async def test_autoclose_busy_never_flipped(pool):
    c, a = await _idle_session(pool, {"autoclose_after_seconds": 3600})
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    await _shift(pool, c["id"], 3601)
    assert await _ask(pool) == [c["id"]]
    await _delivered(pool, c["id"])
    await _shift(pool, c["id"], 24 * 3600)
    assert await _discard(pool) == []
    assert (await _row(pool, c["id"]))["desired"] == "active"


async def test_autoclose_idle_fallback(pool, caplog):
    c, _ = await _idle_session(pool, {"autoclose_after_seconds": 3600})
    await _shift(pool, c["id"], 3601)
    assert await _ask(pool) == [c["id"]]
    await _delivered(pool, c["id"])
    await _shift(pool, c["id"], 300 + 31)
    with caplog.at_level(logging.INFO, logger="agents_orchestrator.db.sessions"):
        assert await _discard(pool) == [c["id"]]
    assert (await _row(pool, c["id"]))["desired"] == "deleting"
    [record] = [r for r in caplog.records if r.getMessage().startswith("session close: ")]
    assert record.fields["close_kind"] == "autoclose"
    assert record.fields["step"] == "discard"


async def test_autoclose_not_released_by_message_or_update(pool):
    c, _ = await _idle_session(pool, {"autoclose_after_seconds": 3600})
    await _shift(pool, c["id"], 3601)
    assert await _ask(pool) == [c["id"]]
    async with pool.connection() as conn:
        await enqueue_message(conn, c["id"], "hello")
        await sessions.touch_update(conn, c["id"])
        assert await sessions.release_inactive_close(conn) == []
    assert (await _row(pool, c["id"]))["close_kind"] == "autoclose"


async def test_keep_session_defers_next_ask(pool):
    c, _ = await _idle_session(pool, {"autoclose_after_seconds": 3600})
    await _shift(pool, c["id"], 3601)
    assert await _ask(pool) == [c["id"]]
    async with pool.connection() as conn:
        await sessions.cancel_close(conn, c["id"])
    row = await _row(pool, c["id"])
    assert row["close_requested_at"] is None and row["autoclose_asked_at"] is not None
    assert await _ask(pool) == []  # the next poll does not re-ask
    await _shift(pool, c["id"], 3601)
    assert await _ask(pool) == [c["id"]]  # one more lifetime later it does
    assert len(await _notices(pool, c["id"])) == 2


async def test_superleader_autoclose_notice(pool, dsn):
    c, _ = await _idle_session(pool, {"autoclose_after_seconds": 3600, "session_messaging": True})
    await _shift(pool, c["id"], 3601)
    settings = Settings(database_url=dsn, agent_image="img:1")
    async with pool.connection() as conn:
        assert await sessions.ask_autoclose(conn, 300, autoclose._autoclose_notice(settings)) == [
            c["id"]
        ]
    [(kind, _, text)] = await _notices(pool, c["id"])
    assert kind == "autoclose" and "orchestrator_self_close" not in text
    assert text.startswith(inactivity.AUTOCLOSE_PREFIX)
    await _delivered(pool, c["id"])
    await _shift(pool, c["id"], 300 + 31)
    assert await _discard(pool) == [c["id"]]  # the fallback still closes it


async def test_two_replicas_ask_once(pool):
    for _ in range(5):
        c, _ = await _idle_session(pool, {"autoclose_after_seconds": 300})
        await _shift(pool, c["id"], 400)
        a, b = await asyncio.gather(_ask(pool), _ask(pool))
        assert sorted(a + b) == [c["id"]]
        assert len(await _notices(pool, c["id"])) == 1
        async with pool.connection() as conn:  # keep the next iteration's due sessions apart
            await sessions.cancel_close(conn, c["id"])


async def test_autoclose_before_inactivity(pool):
    c, _ = await _idle_session(pool, {"autoclose_after_seconds": 3600})
    await _shift(pool, c["id"], 20000)
    async with pool.connection() as conn:
        assert await sessions.ask_autoclose(conn, 300, _Compose()) == [c["id"]]
        assert await sessions.close_inactive(conn, 14400, 300, lambda *a: "close") == []
    assert [n[0] for n in await _notices(pool, c["id"])] == ["autoclose"]


async def test_sweeper_asks_in_api_lifespan(dsn, pool, monkeypatch):
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.1)
    due = await insert_session(pool, spec={"autoclose_after_seconds": 300})
    await _backdate(pool, due["id"], 400)
    settings = Settings(database_url=dsn, agent_image="img:1")
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        task = app.state.autoclose_task
        for _ in range(50):
            if (await _row(pool, due["id"]))["close_kind"] == "autoclose":
                break
            await asyncio.sleep(0.1)
        row = await _row(pool, due["id"])
        assert row["close_kind"] == "autoclose" and row["desired"] == "active"
        assert [n[0] for n in await _notices(pool, due["id"])] == ["autoclose"]
        assert not task.done()
    assert task.cancelled() or task.done()


async def test_sweep_step_order(monkeypatch, pool):
    """One poll runs the steps in this order: handover delivered, autoclose asked, released,
    closed, reminded, discarded idle."""
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.05)
    calls: list[str] = []

    def record(name):
        async def step(conn, *args, **kwargs):
            calls.append(name)
            return []

        return step

    names = [
        "deliver_handover",
        "ask_autoclose",
        "release_inactive_close",
        "close_inactive",
        "remind_inactive",
        "discard_idle",
    ]
    monkeypatch.setattr(handover, "deliver_handover", record("deliver_handover"))
    for name in names[1:]:
        monkeypatch.setattr(sessions, name, record(name))
    task = asyncio.create_task(autoclose.sweep(pool, Settings(database_url="x", agent_image="i")))
    for _ in range(100):
        if len(calls) >= len(names):
            break
        await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert calls[: len(names)] == names


async def _quiet_for(pool, session_id, seconds: int) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET created_at = now() - make_interval(secs => %s),"
            " last_update_at = now() - make_interval(secs => %s) WHERE id = %s",
            (seconds, seconds, session_id),
        )


async def test_sweeper_sends_inactivity_notices(dsn, pool, monkeypatch):
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.1)
    quiet = await insert_session(pool, spec={})
    silent = await insert_session(pool, spec={})
    await _quiet_for(pool, quiet["id"], 3700)
    await _quiet_for(pool, silent["id"], 14405)
    settings = Settings(
        database_url=dsn,
        agent_image="img:1",
        inactivity_reminder_seconds=3600,
        inactivity_close_seconds=14400,
    )
    app = create_app(settings)

    async def notices(session_id) -> list[tuple]:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT notice, text FROM messages WHERE session_id = %s AND notice IS NOT NULL",
                (session_id,),
            )
            return await cur.fetchall()

    async with app.router.lifespan_context(app):
        for _ in range(50):
            if await notices(quiet["id"]) and await notices(silent["id"]):
                break
            await asyncio.sleep(0.1)
    [(kind, text)] = await notices(quiet["id"])
    assert kind == inactivity.NOTICE_REMINDER
    assert text.startswith(inactivity.REMINDER_PREFIX)
    assert "for 1 h (last activity" in text and "holding 0 agent pods" in text
    assert "keep posting at least every 1 h" not in text  # no update tool configured
    assert "never mid-turn" in text
    [(kind, text)] = await notices(silent["id"])
    assert kind == inactivity.NOTICE_CLOSE
    assert text.startswith(inactivity.CLOSE_PREFIX)
    assert "for 4 h, so the orchestrator asks you to close it" in text
    assert "orchestrator_self_close" in text
    assert "it is discarded 30 s later" in text
    row = await _row(pool, silent["id"])
    assert row["close_kind"] == "inactivity" and row["desired"] == "active"


@pytest.mark.parametrize(
    "broken",
    [
        "deliver_handover",
        "ask_autoclose",
        "release_inactive_close",
        "close_inactive",
        "remind_inactive",
        "discard_idle",
    ],
)
async def test_failing_step_does_not_skip_the_others(monkeypatch, pool, caplog, broken):
    """A step that raises on every poll is logged and the other steps still run each poll."""
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.05)
    calls: list[str] = []

    def step(name):
        async def run(conn, *args, **kwargs):
            calls.append(name)
            if name == broken:
                raise RuntimeError(f"{name} is broken")
            return []

        return run

    names = [
        "deliver_handover",
        "ask_autoclose",
        "release_inactive_close",
        "close_inactive",
        "remind_inactive",
        "discard_idle",
    ]
    monkeypatch.setattr(handover, "deliver_handover", step("deliver_handover"))
    for name in names[1:]:
        monkeypatch.setattr(sessions, name, step(name))
    caplog.set_level(logging.ERROR, logger=autoclose.__name__)
    task = asyncio.create_task(autoclose.sweep(pool, Settings(database_url="x", agent_image="i")))
    for _ in range(100):
        if len(calls) >= 2 * len(names):  # two full polls
            break
        await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert calls[: 2 * len(names)] == names * 2
    failures = [r for r in caplog.records if broken in r.getMessage()]
    assert failures and all(r.exc_info for r in failures)
