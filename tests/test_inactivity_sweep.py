"""Inactivity limits in the sweeper: the quiet clock, the reminder, the hard limit with its
handoff deadline, and what cancels an inactivity close (spec 3.2 to 3.4)."""

from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from factories import (
    insert_agent,
    insert_event,
    insert_message,
    insert_session,
    insert_turn,
    set_agent_status,
)

from agents_orchestrator.db import queue, sessions
from agents_orchestrator.db.queue import SessionBeingDeleted

REMINDER = 3600
CLOSE = 14400
HANDOFF = 300


class _Compose:
    """A notice composer that records its arguments."""

    def __init__(self, text: str) -> None:
        self.text = text
        self.calls: list[tuple] = []

    def __call__(self, session, a, b) -> str:
        self.calls.append((session, a, b))
        return self.text


async def _session(pool, spec=None):
    s = await insert_session(pool, spec=spec if spec is not None else {})
    a = await insert_agent(pool, s["id"], ready=True)
    await set_agent_status(pool, a["id"], state="idle")
    return s, a


async def _shift(pool, session_id, seconds: int) -> None:
    """Let `seconds` pass for the session: move every stored time of it into the past."""
    iv = "make_interval(secs => %(secs)s)"
    params = {"secs": seconds, "id": session_id}
    async with pool.connection() as conn:
        await conn.execute(
            f"UPDATE sessions SET created_at = created_at - {iv},"
            f" last_update_at = last_update_at - {iv},"
            f" inactivity_reminded_at = inactivity_reminded_at - {iv},"
            f" close_requested_at = close_requested_at - {iv},"
            f" close_deadline_at = close_deadline_at - {iv} WHERE id = %(id)s",
            params,
        )
        await conn.execute(
            f"UPDATE messages SET created_at = created_at - {iv},"
            f" dispatched_at = dispatched_at - {iv}"
            " WHERE session_id = %(id)s OR sender_session_id = %(id)s",
            params,
        )
        await conn.execute(
            f"UPDATE turns SET started_at = started_at - {iv},"
            f" completed_at = completed_at - {iv}"
            " WHERE agent_id IN (SELECT id FROM agents WHERE session_id = %(id)s)",
            params,
        )
        await conn.execute(
            f"UPDATE agent_status SET free_since = free_since - {iv},"
            f" waiting_since = waiting_since - {iv}"
            " WHERE agent_id IN (SELECT id FROM agents WHERE session_id = %(id)s)",
            params,
        )


async def _quiet(pool, session_id) -> datetime:
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {sessions.quiet_clock_sql('s')} FROM sessions s WHERE s.id = %s",
            (session_id,),
        )
        return (await cur.fetchone())[0]


async def _row(pool, session_id) -> dict:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT * FROM sessions WHERE id = %s", (session_id,))
        cols = [d.name for d in cur.description]
        return dict(zip(cols, await cur.fetchone(), strict=True))


async def _notices(pool, session_id) -> list[tuple]:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT notice, lane, target_agent_id, text FROM messages"
            " WHERE session_id = %s AND notice IS NOT NULL ORDER BY id",
            (session_id,),
        )
        return await cur.fetchall()


async def _now(pool) -> datetime:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT now()")
        return (await cur.fetchone())[0]


async def _remind(pool, reminder: int = REMINDER, compose=None) -> list:
    async with pool.connection() as conn:
        return await sessions.remind_inactive(conn, reminder, compose or _Compose("remind"))


async def _close(pool, close: int = CLOSE, handoff: int = HANDOFF, compose=None) -> list:
    async with pool.connection() as conn:
        return await sessions.close_inactive(conn, close, handoff, compose or _Compose("close"))


async def _release(pool) -> list:
    async with pool.connection() as conn:
        return await sessions.release_inactive_close(conn)


async def _overdue(pool) -> list:
    async with pool.connection() as conn:
        return await sessions.close_overdue(conn)


async def _discard(pool, grace: int = 30) -> list:
    async with pool.connection() as conn:
        return await sessions.discard_idle(conn, grace)


async def _enqueue(pool, session_id, text="are you there?") -> int:
    async with pool.connection() as conn:
        return await queue.enqueue_message(conn, session_id, text)


async def _race(i: int, first, second) -> tuple:
    """Run two steps together; run `i` gives one of them a head start of a few ms, alternating
    sides, so both arrival orders at the row lock occur over the runs."""

    async def after(seconds: float, step):
        await asyncio.sleep(seconds)
        return await step

    lag = (i // 2 % 5) * 0.002
    return await asyncio.gather(after(lag * (i % 2), first), after(lag * (1 - i % 2), second))


def _close_fields(row: dict) -> tuple:
    return (
        row["close_requested_at"],
        row["close_reason"],
        row["close_kind"],
        row["close_deadline_at"],
    )


async def test_reminder_fires_once_per_quiet_period(pool):
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 3700)
    quiet = await _quiet(pool, s["id"])
    compose = _Compose("please post an update")
    assert await _remind(pool, compose=compose) == [s["id"]]
    assert [(c[0].id, c[1], c[2]) for c in compose.calls] == [(s["id"], quiet, 1)]
    assert await _notices(pool, s["id"]) == [
        ("inactivity_reminder", "regular", None, "please post an update")
    ]
    assert (await _row(pool, s["id"]))["inactivity_reminded_at"] is not None

    assert await _remind(pool) == []
    assert len(await _notices(pool, s["id"])) == 1

    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET last_update_at = now() WHERE id = %s", (s["id"],))
    await _shift(pool, s["id"], 3700)
    assert await _remind(pool) == [s["id"]]
    assert len(await _notices(pool, s["id"])) == 2


async def test_notice_does_not_reset_quiet_clock(pool):
    s, a = await _session(pool)
    await _shift(pool, s["id"], 3700)
    before = await _quiet(pool, s["id"])
    assert await _remind(pool) == [s["id"]]
    turn = await insert_turn(pool, a["id"], status="completed", completed_at=datetime.now(UTC))
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET status = 'sent', turn_id = %s WHERE session_id = %s",
            (turn["turn_id"], s["id"]),
        )
    assert await _quiet(pool, s["id"]) == before


async def test_counted_updates(pool):
    s, a = await _session(pool)
    other, _ = await _session(pool)
    await _shift(pool, s["id"], 3700)
    base = await _quiet(pool, s["id"])

    # not counted: tool activity and a turn that carried only a notice
    await insert_event(pool, s["id"], a["id"], type="tool_call")
    notice = await insert_message(pool, s["id"], status="sent", notice="inactivity_reminder")
    turn = await insert_turn(pool, a["id"], status="completed", completed_at=datetime.now(UTC))
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET turn_id = %s WHERE id = %s", (turn["turn_id"], notice["id"])
        )
    assert await _quiet(pool, s["id"]) == base

    # an inbound user message
    inbound = await insert_message(pool, s["id"], status="sent")
    assert await _quiet(pool, s["id"]) == inbound["created_at"] > base

    # the completed turn that answered it
    await _shift(pool, s["id"], 3700)
    base = await _quiet(pool, s["id"])
    answer = await insert_turn(pool, a["id"], status="completed", completed_at=datetime.now(UTC))
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET turn_id = %s WHERE id = %s", (answer["turn_id"], inbound["id"])
        )
    assert await _quiet(pool, s["id"]) == answer["completed_at"] > base

    # a message this session sent to another session
    await _shift(pool, s["id"], 3700)
    base = await _quiet(pool, s["id"])
    async with pool.connection() as conn:
        await queue.enqueue_message(
            conn, other["id"], "status", sender_session_id=s["id"], sender_slug=s["slug"]
        )
    assert await _quiet(pool, s["id"]) > base

    # an api-written touch of last_update_at
    await _shift(pool, s["id"], 3700)
    base = await _quiet(pool, s["id"])
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET last_update_at = now() WHERE id = %s", (s["id"],))
    assert await _quiet(pool, s["id"]) > base


async def test_busy_silent_session_is_reminded(pool):
    s, a = await _session(pool)
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    await _shift(pool, s["id"], 3700)
    await insert_event(pool, s["id"], a["id"], type="tool_call", turn_id="t1")
    assert await _remind(pool) == [s["id"]]


async def test_attention_blocks_reminder_not_close(pool):
    s, a = await _session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET waiting_reason = 'permission', waiting_since = now()"
            " WHERE agent_id = %s",
            (a["id"],),
        )
    await _shift(pool, s["id"], 3700)
    assert await _remind(pool) == []
    await _shift(pool, s["id"], CLOSE)
    assert await _close(pool) == [s["id"]]
    assert (await _row(pool, s["id"]))["close_kind"] == "inactivity"


async def test_superleader_exempt(pool):
    s, _ = await _session(pool, spec={"session_messaging": True})
    await _shift(pool, s["id"], CLOSE + 100)
    assert await _remind(pool) == []
    assert await _close(pool) == []
    assert await _notices(pool, s["id"]) == []


async def test_off_when_zero(pool):
    off, _ = await _session(pool, spec={"inactivity": {"reminder_seconds": 0, "close_seconds": 0}})
    unset, _ = await _session(pool)
    for s in (off, unset):
        await _shift(pool, s["id"], CLOSE + 100)
    # no spec value and the settings off: nothing
    assert await _remind(pool, reminder=0) == []
    assert await _close(pool, close=0) == []
    # the spec turns both off although the settings are on
    assert await _remind(pool) == [unset["id"]]
    assert await _close(pool) == [unset["id"]]
    assert await _notices(pool, off["id"]) == []


async def test_spec_value_overrides_setting(pool):
    s, _ = await _session(
        pool, spec={"inactivity": {"reminder_seconds": 600, "close_seconds": 900}}
    )
    await _shift(pool, s["id"], 700)
    assert await _remind(pool, reminder=0) == [s["id"]]
    assert await _close(pool, close=0) == []
    await _shift(pool, s["id"], 300)
    assert await _close(pool, close=0) == [s["id"]]
    assert (await _row(pool, s["id"]))["close_reason"] == (
        "No update for 16 min (inactivity limit 900 s)"
    )


async def test_hard_limit_requests_close(pool):
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 14405)
    quiet = await _quiet(pool, s["id"])
    compose = _Compose("closing")
    assert await _close(pool, compose=compose) == [s["id"]]
    row = await _row(pool, s["id"])
    assert row["close_kind"] == "inactivity"
    assert row["close_reason"] == "No update for 4 h (inactivity limit 14400 s)"
    assert (row["close_deadline_at"] - row["close_requested_at"]).total_seconds() == HANDOFF
    assert abs((await _now(pool) - row["close_requested_at"]).total_seconds()) < 2
    assert row["desired"] == "active"
    assert [(c[0].id, c[1], c[2]) for c in compose.calls] == [
        (s["id"], quiet, row["close_deadline_at"])
    ]
    assert await _notices(pool, s["id"]) == [("inactivity_close", "regular", None, "closing")]
    # the same poll's later steps: no reminder, and the close notice cancels nothing
    assert await _remind(pool) == []
    assert await _release(pool) == []
    assert await _close(pool) == []
    assert len(await _notices(pool, s["id"])) == 1


async def test_overdue_discards_mid_turn(pool):
    s, a = await _session(pool)
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    await _shift(pool, s["id"], 14405)
    assert await _close(pool) == [s["id"]]
    assert await _overdue(pool) == []  # inside the handoff window
    await _shift(pool, s["id"], HANDOFF + 1)
    assert await _discard(pool) == []  # busy: the idle path waits
    assert await _overdue(pool) == [s["id"]]
    row = await _row(pool, s["id"])
    assert (row["desired"], row["generation"]) == ("deleting", s["generation"] + 1)
    assert await _overdue(pool) == []


async def test_overdue_logs(pool, caplog):
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 14405)
    with caplog.at_level(logging.INFO, logger="agents_orchestrator.db.sessions"):
        assert await _close(pool) == [s["id"]]
        await _shift(pool, s["id"], HANDOFF + 1)
        assert await _overdue(pool) == [s["id"]]
    deadline = caplog.messages[0]
    assert deadline.startswith(f"inactivity: session {s['id']} closing, deadline ")
    assert f"inactivity: session {s['id']} deadline passed, discarding" in caplog.messages


async def test_reminder_logs(pool, caplog):
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 3700)
    with caplog.at_level(logging.INFO, logger="agents_orchestrator.db.sessions"):
        assert await _remind(pool) == [s["id"]]
    assert f"inactivity: session {s['id']} reminded" in caplog.messages


async def test_inactivity_close_discards_at_turn_end(pool, caplog):
    """The idle path discards an inactivity close after the grace, logged by its kind."""
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 14405)
    assert await _close(pool) == [s["id"]]
    async with pool.connection() as conn:
        await conn.execute("UPDATE messages SET status = 'sent' WHERE session_id = %s", (s["id"],))
    await _shift(pool, s["id"], 60)
    with caplog.at_level(logging.INFO, logger="agents_orchestrator.db.sessions"):
        assert await _discard(pool) == [s["id"]]
    assert f"inactivity: session {s['id']} is idle, discarding" in caplog.messages
    assert not any(m.startswith("self-close:") for m in caplog.messages)


async def test_inbound_message_cancels_inactivity_close(pool):
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 14405)
    assert await _close(pool) == [s["id"]]
    await _enqueue(pool, s["id"])
    async with pool.connection() as conn:
        await conn.execute("UPDATE messages SET status = 'sent' WHERE session_id = %s", (s["id"],))
    await _shift(pool, s["id"], HANDOFF + 60)  # past the deadline and idle past the grace
    assert await _overdue(pool) == []
    assert await _discard(pool) == []
    assert await _release(pool) == [s["id"]]
    row = await _row(pool, s["id"])
    assert _close_fields(row) == (None, None, None, None)
    assert row["desired"] == "active"
    assert abs((await _now(pool) - row["last_update_at"]).total_seconds()) < 2
    assert await _close(pool) == []
    assert await _release(pool) == []


async def test_message_begun_before_the_close_still_cancels_it(pool):
    """A message whose transaction began before the close request (it waited on the row lock)
    carries an earlier created_at, but comes after the close notice: it cancels the close."""
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 14405)
    async with pool.connection() as early:
        await early.execute("SELECT now()")  # its transaction, and its now(), start here
        await asyncio.sleep(0.01)
        assert await _close(pool) == [s["id"]]
        await queue.enqueue_message(early, s["id"], "still there?")
        await early.commit()
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT m.created_at < s.close_requested_at FROM messages m"
            " JOIN sessions s ON s.id = m.session_id WHERE m.session_id = %s AND m.notice IS NULL",
            (s["id"],),
        )
        assert await cur.fetchone() == (True,)
    assert await _release(pool) == [s["id"]]
    assert _close_fields(await _row(pool, s["id"])) == (None, None, None, None)


async def test_inbound_message_keeps_self_close(pool):
    s, _ = await _session(pool)
    async with pool.connection() as conn:
        await sessions.request_close(conn, s["id"], "done")
    await _enqueue(pool, s["id"])
    assert await _release(pool) == []
    row = await _row(pool, s["id"])
    assert row["close_kind"] == "self" and row["close_reason"] == "done"
    assert row["close_requested_at"] is not None


async def test_self_close_during_handoff_keeps_first(pool):
    s, _ = await _session(pool)
    await _shift(pool, s["id"], 14405)
    assert await _close(pool) == [s["id"]]
    before = await _row(pool, s["id"])
    async with pool.connection() as conn:
        out = await sessions.request_close(conn, s["id"], "done")
    assert out.already is True
    after = await _row(pool, s["id"])
    assert after["close_kind"] == "inactivity"
    assert _close_fields(after) == _close_fields(before)


async def test_keep_session_races_deadline_flip(pool):
    for i in range(20):
        s, _ = await _session(pool)
        await _shift(pool, s["id"], 14405)
        assert await _close(pool) == [s["id"]]
        await _shift(pool, s["id"], HANDOFF + 1)

        async def keep(session_id=s["id"]) -> str:
            async with pool.connection() as conn:
                try:
                    await sessions.cancel_close(conn, session_id)
                except SessionBeingDeleted:
                    return "refused"
            return "kept"

        kept, flipped = await _race(i, keep(), _overdue(pool))
        row = await _row(pool, s["id"])
        if kept == "kept":
            assert flipped == [] and row["desired"] == "active"
            assert _close_fields(row) == (None, None, None, None)
            assert abs((await _now(pool) - row["last_update_at"]).total_seconds()) < 2
        else:
            assert flipped == [s["id"]] and row["desired"] == "deleting"
            assert row["close_kind"] == "inactivity" and row["close_requested_at"] is not None


async def test_message_races_close_inactive(pool):
    for i in range(20):
        s, _ = await _session(pool)
        await _shift(pool, s["id"], 14405)
        _, closed = await _race(i, _enqueue(pool, s["id"]), _close(pool))
        released = await _release(pool)
        if closed:
            assert released == [s["id"]]  # the close came first: the message cancels it
        else:
            assert released == []  # the message came first: no close
        row = await _row(pool, s["id"])
        assert row["desired"] == "active"
        assert _close_fields(row) == (None, None, None, None)


async def test_no_reminder_right_after_migration_backfill(pool):
    s, _ = await _session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET created_at = now() - interval '7 days', last_update_at = now()"
            " WHERE id = %s",
            (s["id"],),
        )
    assert await _remind(pool) == []
    assert await _close(pool) == []
    assert await _notices(pool, s["id"]) == []
