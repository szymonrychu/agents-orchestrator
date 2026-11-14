"""Autoclose sweeper: due sessions flip to deleting through the discard path."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging

import psycopg
import pytest
from factories import insert_session

from agents_orchestrator import inactivity
from agents_orchestrator.api import autoclose
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import handover, sessions
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


async def test_close_expired_flips_only_due_sessions(pool, dsn):
    due = await insert_session(pool, spec={"autoclose_after_seconds": 300})
    young = await insert_session(pool, spec={"autoclose_after_seconds": 300})
    none = await insert_session(pool, spec={})
    gone = await insert_session(pool, spec={"autoclose_after_seconds": 300}, desired="deleting")
    for c in (due, young, none, gone):
        await _backdate(pool, c["id"], 301 if c is not young else 10)
    before = {c["id"]: await _row(pool, c["id"]) for c in (due, young, none, gone)}

    notified: list[dict] = []
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as lconn:
        await lconn.execute("LISTEN session_changed")
        async with pool.connection() as conn:
            closed = await sessions.close_expired(conn)
        async for n in lconn.notifies(timeout=2, stop_after=1):
            notified.append(json.loads(n.payload))

    assert closed == [due["id"]]
    assert notified == [{"session_id": str(due["id"])}]
    after = {c["id"]: await _row(pool, c["id"]) for c in (due, young, none, gone)}
    for cid in (young["id"], none["id"], gone["id"]):
        assert after[cid] == before[cid]
    d_before, d_after = before[due["id"]], after[due["id"]]
    assert (d_after["desired"], d_after["generation"]) == ("deleting", d_before["generation"] + 1)
    changed = {k for k in d_before if d_before[k] != d_after[k]}
    assert changed == {"desired", "generation"}


async def test_close_expired_is_idempotent(pool):
    ids = []
    for _ in range(5):
        c = await insert_session(pool, spec={"autoclose_after_seconds": 300})
        await _backdate(pool, c["id"], 400)
        ids.append(c["id"])

    async def one():
        async with pool.connection() as conn:
            return await sessions.close_expired(conn)

    a, b = await asyncio.gather(one(), one())
    assert sorted(a + b) == sorted(ids)
    for cid in ids:
        assert (await _row(pool, cid))["generation"] == 2


async def test_sweeper_runs_in_api_lifespan(dsn, pool, monkeypatch):
    monkeypatch.setattr(autoclose, "AUTOCLOSE_POLL_SECONDS", 0.1)
    due = await insert_session(pool, spec={"autoclose_after_seconds": 300})
    await _backdate(pool, due["id"], 400)
    settings = Settings(database_url=dsn, agent_image="img:1")
    app = create_app(settings)
    async with app.router.lifespan_context(app):
        task = app.state.autoclose_task
        for _ in range(50):
            if (await _row(pool, due["id"]))["desired"] == "deleting":
                break
            await asyncio.sleep(0.1)
        assert (await _row(pool, due["id"]))["desired"] == "deleting"
        assert not task.done()
    assert task.cancelled() or task.done()


async def test_sweep_step_order(monkeypatch, pool):
    """One poll runs the steps in this order: handover delivered, expired, released,
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
        "close_expired",
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
        "close_expired",
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
        "close_expired",
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
