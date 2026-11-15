"""Runaway guard window counters (T-224): ingester projection into agent_status guard_*."""

from __future__ import annotations

from datetime import datetime

import psycopg
import pytest
from test_ingester import events_rows, fetch, harness, status_row, wait_until  # noqa: F401
from test_migrations import insert_session_sync

from agents_orchestrator.db import status
from agents_orchestrator.db.migrate import run_migrations


def ts(n: int) -> str:
    return f"2026-01-01T00:00:{n:02d}+00:00"


async def turn(h, n: int, origin: str) -> None:
    h.emit_at("turn_started", ts(n), {"turn_id": f"t{n}", "origin": origin})
    await h.stored_through(h.last_fake_seq())


async def guard(pool, h):
    row = await status_row(pool, h.agent_id)
    return (
        row["guard_turns"],
        row["guard_tokens"],
        row["guard_window_at"],
        row["guard_stop_loop_at"],
    )


def test_billable_tokens_rules() -> None:
    assert (
        status.billable_tokens(
            {
                "input_tokens": 100,
                "output_tokens": 50,
                "cache_creation_input_tokens": 25,
                "cache_read_input_tokens": 9,
            }
        )
        == 175
    )
    assert (
        status.billable_tokens(
            {"input_tokens": "9", "output_tokens": -4, "cache_creation_input_tokens": 1.5}
        )
        == 0
    )
    assert status.billable_tokens({}) == 0


async def test_delivered_turn_starts_window(pool, harness) -> None:  # noqa: F811
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    origins = ["client", "agent", "agent", "peer", "agent", "initial"]
    want = [1, 2, 3, 1, 2, 1]
    for i, (origin, turns) in enumerate(zip(origins, want, strict=True), start=1):
        await turn(h, i, origin)
        got_turns, _, window, _ = await guard(pool, h)
        assert got_turns == turns
        if origin in status.DELIVERED_ORIGINS:
            assert window == datetime.fromisoformat(ts(i))


USAGE = {
    "input_tokens": 100,
    "output_tokens": 50,
    "cache_creation_input_tokens": 25,
    "cache_read_input_tokens": 1000000,
}


async def test_usage_tokens_skip_cache_reads(pool, harness) -> None:  # noqa: F811
    h = await harness()
    await h.start()
    await turn(h, 1, "client")
    h.emit_at("usage", ts(2), dict(USAGE))
    await h.stored_through(h.last_fake_seq())
    assert (await guard(pool, h))[1] == 175
    h.emit_at("usage", ts(3), {**USAGE, "agent_id": "sub-1"})
    await h.stored_through(h.last_fake_seq())
    assert (await guard(pool, h))[1] == 350


async def test_replayed_usage_not_double_counted(pool, harness) -> None:  # noqa: F811
    h = await harness()
    h.replay_all = True
    await h.start()
    await turn(h, 1, "client")
    h.emit_at("usage", ts(2), dict(USAGE))
    seq = h.last_fake_seq()
    await h.stored_through(seq)
    n = len(h.opened)
    h.fake.drop_streams()
    await wait_until(lambda: len(h.opened) > n)
    h.emit_at("assistant_text", ts(3), {"text": "x"})
    await h.stored_through(h.last_fake_seq())
    assert (await guard(pool, h))[1] == 175
    assert (await guard(pool, h))[0] == 1


async def test_window_reset_after_resume_is_lazy(pool, harness) -> None:  # noqa: F811
    h = await harness()
    await h.start()
    await turn(h, 1, "client")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET guard_turns = 301, guard_tokens = 7 WHERE agent_id = %s",
            (h.agent_id,),
        )
        await conn.execute(
            "UPDATE sessions SET guard_reset_at = %s::timestamptz WHERE id = %s",
            ("2026-01-01T00:00:01.5+00:00", h.session_id),
        )
    await turn(h, 2, "agent")
    got_turns, got_tokens, window, _ = await guard(pool, h)
    assert (got_turns, got_tokens) == (1, 0)
    assert window == datetime.fromisoformat(ts(2))


async def set_reset(pool, h, stamp: str) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET guard_reset_at = %s::timestamptz WHERE id = %s",
            (stamp, h.session_id),
        )


async def test_event_earlier_than_reset_is_clamped_and_accumulates(pool, harness) -> None:  # noqa: F811
    """A wrapper clock behind the api clock must not keep the window stale forever."""
    h = await harness()
    await h.start()
    await set_reset(pool, h, "2026-01-01T00:10:00+00:00")
    await turn(h, 1, "client")
    await turn(h, 2, "agent")
    await turn(h, 3, "agent")
    got_turns, _, window, _ = await guard(pool, h)
    assert got_turns == 3
    assert window == datetime.fromisoformat("2026-01-01T00:10:00+00:00")


async def test_usage_restarts_stale_window(pool, harness) -> None:  # noqa: F811
    h = await harness()
    await h.start()
    await turn(h, 1, "client")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET guard_turns = 301, guard_tokens = 7,"
            " guard_stop_loop_at = %s::timestamptz WHERE agent_id = %s",
            (ts(1), h.agent_id),
        )
    await set_reset(pool, h, ts(5))
    h.emit_at("usage", ts(10), dict(USAGE))
    await h.stored_through(h.last_fake_seq())
    got_turns, got_tokens, window, stop = await guard(pool, h)
    assert (got_turns, got_tokens, stop) == (0, 175, None)
    assert window == datetime.fromisoformat(ts(10))


async def test_stop_loop_on_stale_window_restarts_empty(pool, harness) -> None:  # noqa: F811
    h = await harness()
    await h.start()
    await turn(h, 1, "client")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET guard_turns = 301, guard_tokens = 7 WHERE agent_id = %s",
            (h.agent_id,),
        )
    await set_reset(pool, h, ts(5))
    h.emit_at("guard_tripped", ts(10), {"kind": "stop_loop"})
    await h.stored_through(h.last_fake_seq())
    got_turns, got_tokens, window, stop = await guard(pool, h)
    assert (got_turns, got_tokens) == (0, 0)
    assert window == datetime.fromisoformat(ts(10))
    assert stop == datetime.fromisoformat(ts(10))


async def test_stop_loop_trip_marks_agent(pool, harness) -> None:  # noqa: F811
    h = await harness()
    await h.start()
    await turn(h, 1, "client")
    for n, kind in ((2, "agent_spawns"), (3, "tool_breaker")):
        h.emit_at("guard_tripped", ts(n), {"kind": kind})
        await h.stored_through(h.last_fake_seq())
    assert (await guard(pool, h))[3] is None
    h.emit_at("guard_tripped", ts(4), {"kind": "stop_loop"})
    await h.stored_through(h.last_fake_seq())
    assert (await guard(pool, h))[3] == datetime.fromisoformat(ts(4))
    types = [r["type"] for r in await events_rows(pool, h.agent_id)]
    assert types.count("guard_tripped") == 3


def test_runaway_parked_notice_kind_allowed(empty_dsn) -> None:
    run_migrations(empty_dsn)
    with psycopg.connect(empty_dsn, autocommit=True) as c:
        session = insert_session_sync(c)
        insert = "INSERT INTO messages (session_id, text, notice) VALUES (%s, 't', %s)"
        c.execute(insert, (session, "runaway_parked"))
        with pytest.raises(psycopg.errors.CheckViolation):
            c.execute(insert, (session, "bogus"))


async def test_pre_reset_event_after_resume_counts_in_new_window(pool, harness) -> None:  # noqa: F811
    """Accepted trade-off: a late-ingested pre-resume event cannot be told from clock skew, so it
    counts in the fresh window at the reset time (the guard errs toward parking)."""
    h = await harness()
    await h.start()
    await set_reset(pool, h, ts(5))
    await turn(h, 1, "client")
    await turn(h, 2, "agent")
    got_turns, _, window, _ = await guard(pool, h)
    assert got_turns == 2
    assert window == datetime.fromisoformat(ts(5))
