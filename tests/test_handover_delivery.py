"""Handover delivery (voluntary-handover, spec 3.4): the api sweeper renders the predecessor's
transcript and queues it as an attached `handover` notice to the new leader, once per change."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from factories import insert_agent, insert_event, insert_session
from psycopg.rows import dict_row

from agents_orchestrator.db import handover
from agents_orchestrator.db.queue import message_attachments

BUDGET = 65536
OUTPUT = 500


async def _run(pool, sql: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(sql, params)


async def _rows(pool, sql: str, params: tuple = ()) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchall()


async def _hive(pool, n: int = 3):
    s = await insert_session(pool, spec={"replicas": n, "max_replicas": n})
    agents = [
        await insert_agent(pool, s["id"], ordinal=i, ready=True, wrapper_session_id=f"w{i}")
        for i in range(n)
    ]
    return s, agents


async def _lead(pool, session_id, agent_id, epoch, reason, previous=None, since=None) -> None:
    """Write the leader row as the dispatcher pass does (upsert on the session)."""
    await _run(
        pool,
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason,"
        " previous_agent_id) VALUES (%s, %s, %s, COALESCE(%s, now()), %s, %s)"
        " ON CONFLICT (session_id) DO UPDATE SET agent_id = EXCLUDED.agent_id,"
        " epoch = EXCLUDED.epoch, since = EXCLUDED.since, reason = EXCLUDED.reason,"
        " previous_agent_id = EXCLUDED.previous_agent_id",
        (session_id, agent_id, epoch, since, reason, previous),
    )


async def _deliver(pool) -> list:
    async with pool.connection() as conn:
        return await handover.deliver_handover(conn, budget_bytes=BUDGET, output_chars=OUTPUT)


async def _messages(pool, session_id) -> list[dict]:
    return await _rows(
        pool, "SELECT * FROM messages WHERE session_id = %s ORDER BY id", (session_id,)
    )


async def _attachments(pool, message_id):
    async with pool.connection() as conn:
        return await message_attachments(conn, message_id)


async def _notice_epoch(pool, session_id) -> int:
    [row] = await _rows(
        pool, "SELECT handover_notice_epoch FROM sessions WHERE id = %s", (session_id,)
    )
    return row["handover_notice_epoch"]


async def test_notice_enqueued_with_attachment(pool, caplog):
    s, (a0, a1, _) = await _hive(pool)
    await _lead(pool, s["id"], a0["id"], 1, "initial")
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="user_message",
        data={"text": "build the parser"},
    )
    await _lead(pool, s["id"], a1["id"], 2, "handover: parser half done", previous=a0["id"])

    caplog.set_level("INFO", logger="agents_orchestrator.db.sessions")
    assert await _deliver(pool) == [s["id"]]

    [msg] = await _messages(pool, s["id"])
    assert msg["notice"] == "handover"
    assert msg["target_agent_id"] == a1["id"]
    assert msg["lane"] == "regular"
    assert msg["status"] == "queued"
    assert "parser half done" in msg["text"]
    assert a0["pod_name"] in msg["text"]
    [att] = await _attachments(pool, msg["id"])
    assert (att.name, att.mime) == ("handover-2.md", "text/markdown")
    text = att.data.decode()
    assert "parser half done" in text
    assert "build the parser" in text
    assert f"hive {s['slug']}" in text
    assert await _notice_epoch(pool, s["id"]) == 2
    assert f"handover: session {s['id']} epoch 2 notice queued" in caplog.text


async def test_forced_move_gets_notice(pool):
    s, (a0, a1, _) = await _hive(pool)
    await _lead(pool, s["id"], a0["id"], 1, "initial")
    await _lead(pool, s["id"], a1["id"], 2, "leader_lost", previous=a0["id"])

    assert await _deliver(pool) == [s["id"]]
    [msg] = await _messages(pool, s["id"])
    assert msg["target_agent_id"] == a1["id"]
    assert "Their summary: none (forced move)." in msg["text"]
    [att] = await _attachments(pool, msg["id"])
    assert att.name == "handover-2.md"
    assert "leader_lost" in att.data.decode()


async def test_initial_gets_no_notice(pool):
    s, (a0, _, _) = await _hive(pool)
    await _lead(pool, s["id"], a0["id"], 1, "initial")

    assert await _deliver(pool) == []
    assert await _messages(pool, s["id"]) == []
    assert await _notice_epoch(pool, s["id"]) == 0


async def test_already_noticed_epoch_and_deleting_session_skipped(pool):
    s, (a0, a1, _) = await _hive(pool)
    await _lead(pool, s["id"], a1["id"], 2, "leader_lost", previous=a0["id"])
    await _run(pool, "UPDATE sessions SET handover_notice_epoch = 2 WHERE id = %s", (s["id"],))
    d, (b0, b1, _) = await _hive(pool)
    await _lead(pool, d["id"], b1["id"], 2, "leader_lost", previous=b0["id"])
    await _run(pool, "UPDATE sessions SET desired = 'deleting' WHERE id = %s", (d["id"],))

    assert await _deliver(pool) == []
    assert await _messages(pool, s["id"]) == []
    assert await _messages(pool, d["id"]) == []


async def test_idempotent_two_sweepers(pool):
    s, (a0, a1, _) = await _hive(pool)
    await _lead(pool, s["id"], a1["id"], 2, "handover: done", previous=a0["id"])

    a, b = await asyncio.gather(_deliver(pool), _deliver(pool))
    assert a + b == [s["id"]]
    assert len(await _messages(pool, s["id"])) == 1
    assert await _deliver(pool) == []
    assert len(await _messages(pool, s["id"])) == 1


async def test_two_changes_one_poll_one_notice(pool):
    """A voluntary move then leader_lost before the sweeper polls: one notice, for the current
    epoch, from the current row's predecessor."""
    s, (a0, a1, a2) = await _hive(pool)
    await _lead(pool, s["id"], a0["id"], 1, "initial")
    await _lead(pool, s["id"], a1["id"], 2, "handover: first", previous=a0["id"])
    await _lead(pool, s["id"], a2["id"], 3, "leader_lost", previous=a1["id"])

    assert await _deliver(pool) == [s["id"]]
    [msg] = await _messages(pool, s["id"])
    assert msg["target_agent_id"] == a2["id"]
    assert f"taking over from {a1['pod_name']}" in msg["text"]
    assert "none (forced move)" in msg["text"]
    [att] = await _attachments(pool, msg["id"])
    assert att.name == "handover-3.md"
    assert "epoch 2 to 3" in att.data.decode()
    assert await _notice_epoch(pool, s["id"]) == 3


async def test_in_flight_counts_queued_messages_and_locks(pool):
    s, (a0, a1, _) = await _hive(pool)
    await _lead(pool, s["id"], a1["id"], 2, "handover: x", previous=a0["id"])
    await _run(pool, "INSERT INTO messages (session_id, text) VALUES (%s, 'hi')", (s["id"],))
    await _run(
        pool,
        "INSERT INTO file_locks (session_id, path, agent_id) VALUES (%s, 'src/a.py', %s)",
        (s["id"], a0["id"]),
    )

    await _deliver(pool)
    [msg] = [m for m in await _messages(pool, s["id"]) if m["notice"] == "handover"]
    [att] = await _attachments(pool, msg["id"])
    text = att.data.decode()
    assert "Queued messages for the session: 1" in text
    assert "src/a.py" in text


async def test_rows_bounded_by_change_time(pool):
    s, (a0, a1, _) = await _hive(pool)
    since = datetime.now(UTC) - timedelta(minutes=5)
    before = since - timedelta(minutes=1)
    after = since + timedelta(minutes=1)
    for ts, text in ((before, "before the change"), (after, "after the change")):
        await insert_event(
            pool,
            s["id"],
            a0["id"],
            wrapper_session_id="w0",
            type="user_message",
            data={"text": text},
            ts=ts,
        )
    await _lead(pool, s["id"], a1["id"], 2, "leader_lost", previous=a0["id"], since=since)

    async with pool.connection() as conn:
        got = await handover.transcript_rows(conn, a0["id"], since)
    assert [r["data"]["text"] for r in got] == ["before the change"]

    await _deliver(pool)
    [msg] = await _messages(pool, s["id"])
    [att] = await _attachments(pool, msg["id"])
    assert "before the change" in att.data.decode()
    assert "after the change" not in att.data.decode()


async def test_transcript_rows_latest_wrapper_session_main_chain(pool):
    s, (a0, _, _) = await _hive(pool)
    now = datetime.now(UTC)
    t = now - timedelta(minutes=10)

    async def ev(wrapper, type_, data, minutes):
        return await insert_event(
            pool,
            s["id"],
            a0["id"],
            wrapper_session_id=wrapper,
            type=type_,
            data=data,
            ts=t + timedelta(minutes=minutes),
        )

    await ev("old", "user_message", {"text": "old session"}, 0)
    main = await ev("new", "user_message", {"text": "main"}, 1)
    await ev("new", "assistant_text", {"text": "side", "agent_id": "sub1"}, 2)
    started = await ev("new", "subagent_started", {"agent_id": "sub1"}, 3)
    linked = await ev("new", "subagent_linked", {"agent_id": "sub1", "parent_tool_use_id": "t"}, 4)
    stopped = await ev("new", "subagent_stopped", {"agent_id": "sub1"}, 5)
    # A wrapper session that starts after the change does not count.
    await ev("later", "user_message", {"text": "later session"}, 20)

    async with pool.connection() as conn:
        got = await handover.transcript_rows(conn, a0["id"], now)
    assert [r["id"] for r in got] == [main["id"], started["id"], linked["id"], stopped["id"]]
    assert set(got[0]) >= {"id", "ts", "type", "data"}


async def test_transcript_rows_empty_without_events(pool):
    s, (a0, _, _) = await _hive(pool)
    async with pool.connection() as conn:
        assert await handover.transcript_rows(conn, a0["id"], datetime.now(UTC)) == []


async def test_transcript_rows_start_after_latest_clear(pool):
    # Q13: an ex-leader is cleared after a voluntary move and keeps its wrapper session; when it
    # leads again, its next successor gets only the post-clear history.
    s, (a0, _, _) = await _hive(pool)
    now = datetime.now(UTC)
    t = now - timedelta(minutes=10)

    async def ev(type_, data, minutes, wrapper="w0"):
        return await insert_event(
            pool,
            s["id"],
            a0["id"],
            wrapper_session_id=wrapper,
            type=type_,
            data=data,
            ts=t + timedelta(minutes=minutes),
            created_at=t + timedelta(minutes=minutes),
        )

    # a clear in an older wrapper session, or after the change, does not move the start
    await ev("session_cleared", {}, 0, wrapper="older")
    await ev("user_message", {"text": "first lead"}, 0)
    await ev("session_cleared", {}, 1)
    await ev("user_message", {"text": "second lead"}, 2)
    await ev("session_cleared", {}, 3)
    third = await ev("user_message", {"text": "third lead"}, 4)
    await ev("session_cleared", {}, 20)

    async with pool.connection() as conn:
        got = await handover.transcript_rows(conn, a0["id"], now)
    assert [r["id"] for r in got] == [third["id"]]


async def test_transcript_rows_cleared_with_nothing_after(pool):
    s, (a0, _, _) = await _hive(pool)
    t = datetime.now(UTC) - timedelta(minutes=10)
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="user_message",
        data={"text": "old"},
        ts=t,
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="session_cleared",
        data={},
        ts=t + timedelta(minutes=1),
        created_at=t + timedelta(minutes=1),
    )
    async with pool.connection() as conn:
        assert await handover.transcript_rows(conn, a0["id"], datetime.now(UTC)) == []


async def test_transcript_unaffected_by_reset(pool):
    # the ex-leader's clear lands after the change: the notice still carries its pre-change
    # history, and the clear marker never enters the body
    s, (a0, a1, _) = await _hive(pool)
    since = datetime.now(UTC) - timedelta(minutes=5)
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="user_message",
        data={"text": "led before the change"},
        ts=since - timedelta(minutes=1),
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="session_cleared",
        data={},
        ts=since + timedelta(seconds=10),
        created_at=since + timedelta(seconds=10),
    )
    await _lead(pool, s["id"], a1["id"], 2, "handover: done", previous=a0["id"], since=since)
    await _deliver(pool)
    [msg] = await _messages(pool, s["id"])
    [att] = await _attachments(pool, msg["id"])
    body = att.data.decode()
    assert "led before the change" in body
    assert "session_cleared" not in body


async def test_transcript_keeps_body_when_pod_clock_lags_db_clock(pool):
    # the clear this handover triggers is ingested after the change (DB clock) but carries a pod
    # `ts` slightly before `since` (lagging pod clock): it must not count, the body is kept
    s, (a0, a1, _) = await _hive(pool)
    since = datetime.now(UTC) - timedelta(minutes=5)
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="user_message",
        data={"text": "led before the change"},
        ts=since - timedelta(seconds=30),
        created_at=since - timedelta(seconds=30),
    )
    await insert_event(
        pool,
        s["id"],
        a0["id"],
        wrapper_session_id="w0",
        type="session_cleared",
        data={},
        ts=since - timedelta(seconds=3),
        created_at=since + timedelta(seconds=2),
    )
    await _lead(pool, s["id"], a1["id"], 2, "handover: done", previous=a0["id"], since=since)
    await _deliver(pool)
    [msg] = await _messages(pool, s["id"])
    [att] = await _attachments(pool, msg["id"])
    assert "led before the change" in att.data.decode()


async def test_ex_leader_leading_again_omits_pre_clear_history(pool):
    s, (a0, a1, _) = await _hive(pool)
    since = datetime.now(UTC) - timedelta(minutes=1)
    t = since - timedelta(minutes=30)
    for minutes, type_, data in (
        (0, "user_message", {"text": "pre-clear work"}),
        (1, "session_cleared", {}),
        (2, "user_message", {"text": "post-clear work"}),
    ):
        await insert_event(
            pool,
            s["id"],
            a0["id"],
            wrapper_session_id="w0",
            type=type_,
            data=data,
            ts=t + timedelta(minutes=minutes),
            created_at=t + timedelta(minutes=minutes),
        )
    await _lead(pool, s["id"], a1["id"], 5, "handover: done", previous=a0["id"], since=since)
    await _deliver(pool)
    [msg] = await _messages(pool, s["id"])
    [att] = await _attachments(pool, msg["id"])
    body = att.data.decode()
    assert "post-clear work" in body
    assert "pre-clear work" not in body
