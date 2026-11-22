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


async def test_fanout_rows_not_in_count_sent_by_or_between(pool):
    from agents_orchestrator.db import project_posts

    async with pool.connection() as conn:
        await conn.execute("INSERT INTO projects (slug, name) VALUES ('a', 'A')")
    sender = await insert_session(pool, project="a")
    r1, r2, r3 = [await insert_session(pool, project="a") for _ in range(3)]
    async with pool.connection() as conn:
        post_id = await project_posts.insert_post(
            conn,
            project="a",
            sender_kind="leader",
            sender_session_id=sender["id"],
            sender_slug=sender["slug"],
            text="hi",
            reply_to=None,
            addressed=[],
        )
        for r in (r1, r2, r3):
            await project_posts.enqueue_fanout(
                conn,
                r["id"],
                "hi",
                sender_session_id=sender["id"],
                sender_slug=sender["slug"],
                hop=0,
                post_id=post_id,
            )
        assert await queue.count_sent_by(conn, sender["id"], 60) == 0
        assert await queue.count_between(conn, sender["id"], r1["id"], 3600) == 0
        await queue.enqueue_message(
            conn, r1["id"], "direct", sender_session_id=sender["id"], sender_slug=sender["slug"]
        )
        assert await queue.count_sent_by(conn, sender["id"], 60) == 1
        assert await queue.count_between(conn, sender["id"], r1["id"], 3600) == 1


async def test_agent_channel_rows_are_not_session_messages(pool):
    from agents_orchestrator.db import leader_state

    a, b = await insert_session(pool), await insert_session(pool)
    async with pool.connection() as conn:
        first = await queue.enqueue_message(
            conn, b["id"], "s", sender_session_id=a["id"], sender_slug="a"
        )
        await queue.enqueue_message(
            conn,
            b["id"],
            "x",
            sender_session_id=a["id"],
            sender_slug="a",
            channel="agent",
            notify="idle",
            envelope_ref="e1",
        )
        assert await queue.count_queued_session_messages(conn, b["id"]) == 1
        assert await queue.count_sent_by(conn, a["id"], 60) == 1
        assert await queue.count_between(conn, a["id"], b["id"], 60) == 1
        state = await leader_state.collect(conn, b["id"])
    assert [m.id for m in state.messages] == [first]


async def test_turn_hop_for_reads_a_given_past_turn(pool):
    from factories import insert_agent, set_agent_status

    a, b = await insert_session(pool), await insert_session(pool)
    agent = await insert_agent(pool, b["id"])
    async with pool.connection() as conn:
        for turn, hop in (("t1", 3), ("t2", 5)):
            mid = await queue.enqueue_message(
                conn, b["id"], "m", sender_session_id=a["id"], sender_slug="a", hop=hop
            )
            await conn.execute("UPDATE messages SET turn_id = %s WHERE id = %s", (turn, mid))
    await set_agent_status(pool, agent["id"], current_turn_id="t2")
    async with pool.connection() as conn:
        assert await queue.turn_hop_for(conn, agent["id"], "t1") == 3
        assert await queue.turn_hop_for(conn, agent["id"], "nope") == 0
        assert await queue.turn_hop(conn, agent["id"]) == 5


async def test_turn_hop_is_the_max_over_the_turn_messages(pool):
    from factories import insert_agent

    a, b = await insert_session(pool), await insert_session(pool)
    agent = await insert_agent(pool, b["id"])
    async with pool.connection() as conn:
        high = await queue.enqueue_message(
            conn, b["id"], "m", sender_session_id=a["id"], sender_slug="a", hop=7
        )
        low = await queue.enqueue_message(
            conn, b["id"], "member", sender_agent_id=agent["id"], channel="agent"
        )
        await conn.execute("UPDATE messages SET turn_id = 't1' WHERE id = ANY(%s)", ([high, low],))
        assert await queue.turn_hop_for(conn, agent["id"], "t1") == 7
