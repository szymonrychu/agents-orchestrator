"""Runaway park sweep (T-224): park_breaching decides per session under a row lock; the autoclose
steps park, interrupt and notify."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from datetime import datetime

from factories import insert_agent, insert_session, set_agent_status
from psycopg.rows import dict_row

from agents_orchestrator import runaway
from agents_orchestrator.api import autoclose
from agents_orchestrator.db import handover
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

SETTINGS = Settings(database_url="x", agent_image="i")
WINDOW = datetime.fromisoformat("2026-01-01T00:00:00+00:00")


async def rows(pool, sql: str, params: tuple = ()) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchall()


async def session_row(pool, session_id) -> dict:
    (row,) = await rows(pool, "SELECT * FROM sessions WHERE id = %s", (session_id,))
    return row


async def guarded_agent(pool, session_id, *, turns=0, tokens=0, stop=False, busy=False) -> dict:
    a = await insert_agent(pool, session_id, pod_name="agent-x-0", ready=True)
    await set_agent_status(
        pool,
        a["id"],
        state="busy" if busy else "idle",
        current_turn_id="t1" if busy else None,
    )
    await rows(
        pool,
        "UPDATE agent_status SET guard_turns = %s, guard_tokens = %s, guard_window_at = %s,"
        " guard_stop_loop_at = CASE WHEN %s THEN %s ELSE NULL END WHERE agent_id = %s"
        " RETURNING agent_id",
        (turns, tokens, WINDOW, stop, WINDOW, a["id"]),
    )
    return a


async def sweep_park(pool, settings=SETTINGS) -> list[uuid.UUID]:
    async with pool.connection() as conn:
        return await sessions_repo.park_breaching(conn, autoclose._decide(settings))


async def notices(pool, session_id) -> list[dict]:
    return await rows(
        pool,
        "SELECT * FROM messages WHERE session_id = %s AND notice = 'runaway_parked'",
        (session_id,),
    )


async def test_park_runaway_parks_on_turns(pool):
    s = await insert_session(pool)
    a = await guarded_agent(pool, s["id"], turns=301, busy=True)
    assert await sweep_park(pool) == [s["id"]]
    row = await session_row(pool, s["id"])
    assert row["parked_reason"] == "turns"
    assert row["parked_detail"] == {
        "agent_id": str(a["id"]),
        "pod": "agent-x-0",
        "value": 301,
        "limit": 300,
        "window_at": WINDOW.isoformat(),
    }
    (msg,) = await notices(pool, s["id"])
    assert msg["text"].startswith("Orchestrator notice (runaway guard):")
    cmds = await rows(pool, "SELECT * FROM commands WHERE kind = 'interrupt'")
    assert [c["agent_id"] for c in cmds] == [a["id"]]


async def test_turns_at_limit_not_parked(pool):
    s = await insert_session(pool)
    await guarded_agent(pool, s["id"], turns=300)
    assert await sweep_park(pool) == []
    assert (await session_row(pool, s["id"]))["parked_at"] is None


async def test_preset_zero_never_parks(pool):
    s = await insert_session(pool, spec={"runaway": {"max_turns_per_task": 0}})
    await guarded_agent(pool, s["id"], turns=10000)
    assert await sweep_park(pool) == []


async def test_park_on_tokens_and_stop_loop(pool):
    t = await insert_session(pool)
    await guarded_agent(pool, t["id"], tokens=60000001)
    s = await insert_session(pool)
    await guarded_agent(pool, s["id"], stop=True)
    assert sorted(await sweep_park(pool)) == sorted([t["id"], s["id"]])
    assert (await session_row(pool, t["id"]))["parked_reason"] == "tokens"
    row = await session_row(pool, s["id"])
    assert row["parked_reason"] == "stop_loop"
    assert (row["parked_detail"]["value"], row["parked_detail"]["limit"]) == (25, 25)


async def test_resume_resets_window_lazily_and_ignores_old_stop_loop_trip(pool):
    s = await insert_session(pool)
    await guarded_agent(pool, s["id"], turns=301, stop=True)
    assert await sweep_park(pool) == [s["id"]]
    async with pool.connection() as conn:
        await sessions_repo.resume(conn, s["id"])
    # no ingester write has restarted the window yet: the old counters and trip are ignored
    assert await sweep_park(pool) == []
    assert (await session_row(pool, s["id"]))["parked_at"] is None
    async with pool.connection() as conn:
        (w,) = await sessions_repo.guard_windows(conn, s["id"])
    assert (w.turns, w.tokens, w.window_at, w.stop_loop_at) == (0, 0, None, None)


async def test_park_runaway_runs_once_concurrently(pool):
    s = await insert_session(pool)
    await guarded_agent(pool, s["id"], turns=301)
    results = await asyncio.gather(sweep_park(pool), sweep_park(pool))
    assert sorted(len(r) for r in results) == [0, 1]
    assert len(await notices(pool, s["id"])) == 1


async def test_dead_agents_do_not_count(pool):
    s = await insert_session(pool)
    a = await guarded_agent(pool, s["id"], turns=999)
    await rows(pool, "UPDATE agents SET gone_at = now() WHERE id = %s RETURNING id", (a["id"],))
    assert await sweep_park(pool) == []


async def test_parked_session_is_not_a_candidate(pool):
    s = await insert_session(pool)
    await guarded_agent(pool, s["id"], turns=301)
    assert await sweep_park(pool) == [s["id"]]
    assert await sweep_park(pool) == []
    assert len(await notices(pool, s["id"])) == 1


async def test_interrupt_step_catches_a_claimed_message(pool):
    """park itself misses a message claimed but not yet running; the sweep step interrupts it."""
    s = await insert_session(pool)
    a = await guarded_agent(pool, s["id"], turns=301)
    assert await sweep_park(pool) == [s["id"]]
    assert await rows(pool, "SELECT * FROM commands WHERE kind = 'interrupt'") == []
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="late")
    async with pool.connection() as conn:
        assert await sessions_repo.interrupt_parked(conn) == [a["id"]]


async def test_sweep_step_order(monkeypatch, pool):
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
        "park_breaching",
        "interrupt_parked",
        "notify_parked",
        "discard_idle",
    ]
    monkeypatch.setattr(handover, "deliver_handover", record("deliver_handover"))
    monkeypatch.setattr(autoclose, "_notify_parked", record("notify_parked"))
    for name in names:
        if name not in ("deliver_handover", "notify_parked"):
            monkeypatch.setattr(sessions_repo, name, record(name))
    task = asyncio.create_task(autoclose.sweep(pool, SETTINGS))
    for _ in range(100):
        if len(calls) >= len(names):
            break
        await asyncio.sleep(0.05)
    task.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await task
    assert calls[: len(names)] == names


async def test_decide_composes_notice():
    class S:
        spec = __import__("agents_orchestrator.spec", fromlist=["SessionSpec"]).SessionSpec()

    w = runaway.AgentWindow(uuid.uuid4(), "agent-x-0", WINDOW, 301, 0, None)
    decided = autoclose._decide(SETTINGS)(S(), [w])
    assert decided is not None
    b, text = decided
    assert b.reason == "turns" and text == runaway.parked_notice(b)
    ok = runaway.AgentWindow(uuid.uuid4(), "agent-x-0", WINDOW, 1, 0, None)
    assert autoclose._decide(SETTINGS)(S(), [ok]) is None
