"""POST /agent/v1/envelope/stop: the onStop webhook for chat envelopes (S-70)."""

from __future__ import annotations

import logging
from typing import Any

from test_api_agent import client_for, headers, make_settings
from test_api_leadership import hive

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import envelope_outbox as outbox
from agents_orchestrator.db.envelope_outbox import Row

IP0 = "10.9.0.10"
OK = '<chat-send reply_to="m1">hello</chat-send>'
BAD = '<chat-send reply_to="m1">never closed'
BAD_ROOM = '<chat-send room="!r:x">never closed'
URL = "/agent/v1/envelope/stop"


def texts(*blocks: str, start: int = 1) -> list[dict[str, str]]:
    return [{"message_id": f"msg{start + i}", "text": b} for i, b in enumerate(blocks)]


def payload(turn_id: str | None, assistant_texts: list[dict[str, str]] | None) -> dict[str, Any]:
    data: dict[str, Any] = {"turn_id": turn_id, "stop_hook_active": False}
    if assistant_texts is not None:
        data["assistant_texts"] = assistant_texts
        data["assistant_texts_truncated"] = False
    return {"event": "onStop", "turn_id": turn_id, "data": data}


async def stop(dsn, pool, c, body, ip=IP0):
    async with client_for(create_app(make_settings(dsn), pool), ip) as http:
        r = await http.post(URL, headers=headers(c["id"]), json=body)
    assert r.status_code == 200, r.text
    return r.json()


async def db_rows(pool, sql, *args):
    async with pool.connection() as conn:
        cur = await conn.execute(sql, args)
        return await cur.fetchall()


async def checks(pool):
    return await db_rows(
        pool,
        "SELECT turn_id, blocked_at IS NOT NULL, allowed_at IS NOT NULL, blocks_seen"
        " FROM envelope_stop_checks ORDER BY turn_id",
    )


async def notices(pool):
    return await db_rows(
        pool, "SELECT room, status, turn_id FROM envelope_outbox WHERE kind = 'notice'"
    )


async def test_requires_agent_credentials(dsn, pool):
    c, _a0, _a1 = await hive(pool)
    async with client_for(create_app(make_settings(dsn), pool), IP0) as http:
        r = await http.post(URL, json=payload("t1", texts(OK)))
    assert r.status_code == 401


async def test_allows_without_texts_or_turn_id(dsn, pool):
    c, a0, _ = await hive(pool)
    assert (await stop(dsn, pool, c, payload("t1", None)))["decision"] == "allow"
    assert (await stop(dsn, pool, c, payload(None, texts(BAD))))["decision"] == "allow"
    assert (await stop(dsn, pool, c, payload("t2", [])))["decision"] == "allow"
    # only a Stop with a turn id is recorded as allowed
    assert await checks(pool) == [("t1", False, True, 1), ("t2", False, True, 1)]


async def test_unreadable_body_allows(dsn, pool):
    c, _, _ = await hive(pool)
    async with client_for(create_app(make_settings(dsn), pool), IP0) as http:
        r = await http.post(URL, headers=headers(c["id"]), content=b"not json")
        r2 = await http.post(URL, headers=headers(c["id"]), json=[1, 2])
    assert r.json() == {"decision": "allow"} and r2.json() == {"decision": "allow"}


async def test_valid_envelopes_allow_and_record_allowed(dsn, pool):
    c, a0, _ = await hive(pool)
    out = await stop(dsn, pool, c, payload("t1", texts("plain text", OK)))
    assert out == {"decision": "allow"}
    assert await checks(pool) == [("t1", False, True, 1)]
    assert await notices(pool) == []


async def test_non_leader_is_allowed(dsn, pool):
    c, a0, a1 = await hive(pool)
    out = await stop(dsn, pool, c, payload("t1", texts(BAD)), ip="10.9.0.11")
    assert out == {"decision": "allow"}


async def test_first_stop_with_errors_blocks_with_numbered_reason(dsn, pool):
    c, a0, _ = await hive(pool)
    out = await stop(dsn, pool, c, payload("t1", texts(OK, BAD)))
    assert out["decision"] == "block"
    reason = out["reason"]
    assert reason.startswith("Some chat envelopes were not delivered: 1) envelope ")
    assert "of message msg2" in reason
    assert reason.endswith(
        "The others were delivered; do not resend them. Resend only the corrected ones now."
    )
    assert await checks(pool) == [("t1", True, False, 2)]
    assert await notices(pool) == []


async def test_first_stop_lists_refused_rows_of_the_turn(dsn, pool):
    c, a0, _ = await hive(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(
            conn,
            [
                Row(
                    session_id=c["id"],
                    agent_id=a0["id"],
                    wrapper_session_id="w",
                    event_seq=1,
                    envelope_index=3,
                    turn_id="t1",
                    kind="send",
                    body="x",
                    status="refused",
                    error_code="room_not_allowed",
                ),
                Row(
                    session_id=c["id"],
                    agent_id=a0["id"],
                    wrapper_session_id="w",
                    event_seq=2,
                    envelope_index=1,
                    turn_id="other",
                    kind="send",
                    body="y",
                    status="refused",
                    error_code="elsewhere",
                ),
            ],
        )
    out = await stop(dsn, pool, c, payload("t1", texts(OK)))
    assert out["decision"] == "block"
    assert "1) envelope 3: refused by the chat (room_not_allowed)" in out["reason"]
    assert "elsewhere" not in out["reason"]


async def test_second_stop_always_allows_and_a_repaired_retry_is_silent(dsn, pool):
    c, a0, _ = await hive(pool)
    first = texts(BAD)
    assert (await stop(dsn, pool, c, payload("t1", first)))["decision"] == "block"
    second = first + texts(OK, start=2)
    assert (await stop(dsn, pool, c, payload("t1", second))) == {"decision": "allow"}
    assert await checks(pool) == [("t1", True, True, 1)]
    assert await notices(pool) == []


async def test_retry_with_new_errors_queues_one_notice_to_the_named_room(dsn, pool, caplog):
    c, a0, _ = await hive(pool)
    await stop(dsn, pool, c, payload("t1", texts(BAD_ROOM)))
    again = texts(BAD_ROOM) + texts(BAD_ROOM, start=2)
    with caplog.at_level(logging.WARNING):
        assert (await stop(dsn, pool, c, payload("t1", again))) == {"decision": "allow"}
    assert await notices(pool) == [("!r:x", "queued", "t1")]
    assert any("chat_protocol_failed" in r.getMessage() for r in caplog.records)
    # a third Stop of the same turn does not queue a second notice
    await stop(dsn, pool, c, payload("t1", again + texts(BAD_ROOM, start=3)))
    assert len(await notices(pool)) == 1


async def test_retry_without_any_valid_envelope_is_a_protocol_failure(dsn, pool):
    c, a0, _ = await hive(pool)
    await stop(dsn, pool, c, payload("t1", texts(BAD)))
    # the agent answered the block with plain prose, no envelope at all
    await stop(dsn, pool, c, payload("t1", texts(BAD) + texts("sorry, fixing", start=2)))
    assert await notices(pool) == [(None, "queued", "t1")]


async def test_no_notice_when_something_is_delivered_to_the_target_room(dsn, pool):
    c, a0, _ = await hive(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(
            conn,
            [
                Row(
                    session_id=c["id"],
                    agent_id=a0["id"],
                    wrapper_session_id="w",
                    event_seq=1,
                    envelope_index=1,
                    turn_id="t1",
                    kind="send",
                    body="ack",
                    status="accepted",
                ),
            ],
        )
    await stop(dsn, pool, c, payload("t1", texts(BAD)))
    await stop(dsn, pool, c, payload("t1", texts(BAD) + texts("prose", start=2)))
    assert await notices(pool) == []


async def test_notice_when_only_another_room_was_delivered(dsn, pool):
    c, a0, _ = await hive(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(
            conn,
            [
                Row(
                    session_id=c["id"],
                    agent_id=a0["id"],
                    wrapper_session_id="w",
                    event_seq=1,
                    envelope_index=1,
                    turn_id="t1",
                    kind="send",
                    body="ack",
                    room="!other:x",
                    status="accepted",
                ),
            ],
        )
    await stop(dsn, pool, c, payload("t1", texts(BAD_ROOM)))
    await stop(dsn, pool, c, payload("t1", texts(BAD_ROOM) + texts("prose", start=2)))
    assert await notices(pool) == [("!r:x", "queued", "t1")]
