"""Relay marks and the recent-send lookup (migration 0036)."""

from __future__ import annotations

import pytest
from factories import insert_message, insert_session

from agents_orchestrator.db import queue, session_messaging
from agents_orchestrator.db.session_messaging import RelayMark


async def _sent(pool, sender, receiver, age: str) -> None:
    row = await insert_message(pool, receiver["id"])
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET sender_session_id = %s, created_at = now() - %s::interval"
            " WHERE id = %s",
            (sender["id"], age, row["id"]),
        )


async def test_sent_to_within_window_and_direction(pool):
    a, b = await insert_session(pool), await insert_session(pool)
    await _sent(pool, a, b, "10 seconds")
    async with pool.connection() as conn:
        sent = session_messaging.sent_to_within
        assert await sent(conn, sender=a["id"], receiver=b["id"], seconds=60)
        assert not await sent(conn, sender=b["id"], receiver=a["id"], seconds=60)
        assert not await sent(conn, sender=a["id"], receiver=b["id"], seconds=0)
    c, d = await insert_session(pool), await insert_session(pool)
    await _sent(pool, c, d, "2 hours")
    async with pool.connection() as conn:
        assert not await sent(conn, sender=c["id"], receiver=d["id"], seconds=3600)


async def test_enqueue_relay_notice_marks_row(pool):
    a, b = await insert_session(pool), await insert_session(pool)
    async with pool.connection() as conn:
        mid = await session_messaging.enqueue_relay_notice(
            conn,
            b["id"],
            "hi",
            sender_session_id=a["id"],
            sender_slug="aa",
            hop=2,
            send_id="s5",
            room_id="!r:x",
        )
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT sender_session_id, sender_slug, hop, lane, notice, chat_relay_send,"
                " chat_relay_room FROM messages WHERE id = %s",
                (mid,),
            )
            row = await cur.fetchone()
        assert row == (a["id"], "aa", 2, "regular", None, "s5", "!r:x")
        assert await session_messaging.relay_mark(conn, mid) == RelayMark("s5", "!r:x")
        plain = await queue.enqueue_message(conn, b["id"], "plain")
        assert await session_messaging.relay_mark(conn, plain) is None


async def test_enqueue_relay_notice_refuses_deleting(pool):
    a = await insert_session(pool)
    b = await insert_session(pool, desired="deleting")
    async with pool.connection() as conn:
        with pytest.raises(queue.SessionBeingDeleted):
            await session_messaging.enqueue_relay_notice(
                conn,
                b["id"],
                "hi",
                sender_session_id=a["id"],
                sender_slug="aa",
                hop=0,
                send_id="s1",
                room_id="!r:x",
            )
        async with conn.cursor() as cur:
            await cur.execute("SELECT count(*) FROM messages WHERE session_id = %s", (b["id"],))
            assert (await cur.fetchone())[0] == 0


async def test_relay_counts_as_session_message(pool):
    a, b = await insert_session(pool), await insert_session(pool)
    async with pool.connection() as conn:
        await session_messaging.enqueue_relay_notice(
            conn,
            b["id"],
            "hi",
            sender_session_id=a["id"],
            sender_slug="aa",
            hop=0,
            send_id="s1",
            room_id="!r:x",
        )
        assert await queue.count_between(conn, a["id"], b["id"], 3600) == 1
        assert await queue.count_sent_by(conn, a["id"], 60) == 1
