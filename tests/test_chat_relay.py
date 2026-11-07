"""The chat relay (T-56): a post refused with 409 `room_bound` naming an occupier goes out in the
occupier's name, the occupier gets a relay notice, the poster gets 202 `relayed`.

The first cases are pure; the DB cases drive the real app over Postgres with `FakeChat` as the
chat upstream (it refuses the cron's session and accepts the occupier's).
"""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

import httpx
from factories import insert_message, set_agent_status
from fakes.fake_chat import FakeChat
from test_agent_chat import CHAT_TOKEN, CHAT_URL
from test_agent_session_messaging import Sess, _delivered, _exec, _rows, alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api import chat_relay
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue, session_messaging

ROOM = "!room:hs"
MESSAGES = "POST /chat/v1/messages"


def _conflict(occupier: Any, code: str = "room_bound") -> bytes:
    error: dict[str, Any] = {"code": code, "message": "m"}
    if occupier is not None:
        error["occupier"] = occupier
    return json.dumps({"error": error}).encode()


def test_occupier_of():
    o = uuid.uuid4()
    assert chat_relay.occupier_of(409, _conflict(str(o))) == o
    assert chat_relay.occupier_of(409, _conflict(None)) is None
    assert chat_relay.occupier_of(409, _conflict("not-a-uuid")) is None
    assert chat_relay.occupier_of(409, _conflict(5)) is None
    assert chat_relay.occupier_of(409, _conflict(str(o), "other")) is None
    assert chat_relay.occupier_of(403, _conflict(str(o))) is None
    assert chat_relay.occupier_of(409, b"not json") is None
    assert chat_relay.occupier_of(409, b"[1]") is None


def test_without_occupier_keeps_bytes_when_absent():
    raw = b'{"error": {"code": "room_bound",   "message": "m"}}'
    assert chat_relay.without_occupier(raw) is raw
    assert chat_relay.without_occupier(b"not json") == b"not json"


def test_without_occupier_strips_the_field():
    out = json.loads(chat_relay.without_occupier(_conflict(str(uuid.uuid4()))))
    assert out == {"error": {"code": "room_bound", "message": "m"}}


# ------------------------------------------------------------------------------- DB cases


class Rig:
    def __init__(self, pool: Any, dsn: str, **overrides: Any) -> None:
        self.pool = pool
        self.fake = FakeChat(CHAT_TOKEN)
        settings = make_settings(dsn).model_copy(
            update={"chat_url": CHAT_URL, "chat_token": CHAT_TOKEN, **overrides}
        )
        self.app = create_app(settings, pool)
        self.app.state.chat_http = httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.fake.app), base_url="http://chat"
        )

    def refuse_for(self, cron: Sess, occupier: Any, **kw: Any) -> None:
        extra = {"occupier": str(occupier)} if occupier is not None else None
        self.fake.refuse(MESSAGES, 409, "room_bound", extra=extra, session=str(cron.id), **kw)

    async def post(self, caller: Sess, **body: Any) -> httpx.Response:
        body = {"text": "hello", "room": ROOM, **body}
        async with client_for(self.app, caller.leader_ip) as http:
            return await http.post("/agent/v1/chat/messages", json=body, headers=headers(caller.id))


async def _pair(rig: Rig, **kw: Any) -> tuple[Sess, Sess]:
    cron = await alone(rig.pool, "cron-x")
    occ = await alone(rig.pool, "occ-x", **kw)
    rig.refuse_for(cron, occ.id)
    return cron, occ


async def _last_update(pool: Any, session_id: Any) -> Any:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT last_update_at FROM sessions WHERE id = %s", (session_id,))
        return (await cur.fetchone())[0]


async def test_relay_posts_as_occupier_and_queues_notice(pool, dsn):
    rig = Rig(pool, dsn)
    cron, occ = await _pair(rig)
    await _exec(pool, "UPDATE sessions SET last_update_at = now() - interval '5 h'")
    old = await _last_update(pool, occ.id)
    r = await rig.post(cron)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "relayed" and body["via"] == occ.slug and body["send_id"] == "s1"
    first, second = rig.fake.calls
    assert first.headers["x-orch-session"] == str(cron.id)
    assert second.headers["x-orch-session"] == str(occ.id)
    assert second.headers["x-orch-slug"] == occ.slug
    assert second.json() == {"text": "[cron-x] hello", "room": ROOM}
    (row,) = await _rows(pool)
    assert row["id"] == body["message_id"] and row["session_id"] == occ.id
    assert row["sender_session_id"] == cron.id and row["text"] == "hello"
    assert row["chat_relay_send"] == "s1" and row["chat_relay_room"] == ROOM
    assert row["lane"] == "regular"
    assert await _last_update(pool, cron.id) > old
    assert await _last_update(pool, occ.id) == old
    assert "occupier" not in r.text


async def test_room_bound_without_occupier_passes_through(pool, dsn):
    rig = Rig(pool, dsn)
    cron = await alone(pool, "cron-x")
    rig.refuse_for(cron, None)
    r = await rig.post(cron)
    (call,) = rig.fake.calls
    assert r.status_code == 409
    assert r.json()["error"]["code"] == "room_bound"
    assert r.content == json.dumps({"error": {"code": "room_bound", "message": "refused"}}).encode()
    assert call.path == "/chat/v1/messages"
    assert await _rows(pool) == []


async def test_unknown_occupier_passes_plain_room_bound(pool, dsn):
    rig = Rig(pool, dsn)
    cron = await alone(pool, "cron-x")
    rig.refuse_for(cron, uuid.uuid4())
    r = await rig.post(cron)
    assert r.status_code == 409 and r.json()["error"]["code"] == "room_bound"
    assert "occupier" not in r.text
    assert len(rig.fake.calls) == 1
    assert await _rows(pool) == []


async def test_occupier_closing(pool, dsn):
    rig = Rig(pool, dsn)
    cron, _ = await _pair(rig, desired="deleting")
    r = await rig.post(cron)
    assert_error(r, 409, "occupier_closing")
    assert r.json()["error"]["retry_after"] == 60
    assert r.headers["Retry-After"] == "60"
    assert len(rig.fake.calls) == 1
    assert await _rows(pool) == []


async def _refused_before_post(pool: Any, dsn: str, code: str, status: int, prepare: Any) -> None:
    overrides, setup = prepare
    rig = Rig(pool, dsn, **overrides)
    cron, occ = await _pair(rig)
    third = await alone(pool, "third-x")
    await setup(pool, cron, occ, third)
    before = len(await _rows(pool))
    assert_error(await rig.post(cron), status, code)
    assert len(rig.fake.calls) == 1
    assert len(await _rows(pool)) == before


async def _said(pool: Any, sender: Sess, receiver: Sess) -> None:
    """A queued session message sender -> receiver."""
    row = await insert_message(pool, receiver.id)
    await _exec(
        pool,
        "UPDATE messages SET sender_session_id = %s, sender_slug = %s, hop = 1 WHERE id = %s",
        (sender.id, sender.slug, row["id"]),
    )


async def _hop_setup(pool: Any, cron: Sess, occ: Sess, third: Sess) -> None:
    await _delivered(pool, cron, third, hop=3, turn="t1")
    await set_agent_status(pool, cron.leader_id, state="busy", current_turn_id="t1")


async def _rate_setup(pool: Any, cron: Sess, occ: Sess, third: Sess) -> None:
    await _said(pool, cron, third)


async def _pair_setup(pool: Any, cron: Sess, occ: Sess, third: Sess) -> None:
    await _said(pool, cron, occ)


async def _queue_setup(pool: Any, cron: Sess, occ: Sess, third: Sess) -> None:
    await _said(pool, third, occ)


async def test_relay_limits_refuse_before_post(pool, dsn):
    cases = {
        "hop_limit": (409, {"session_message_max_hops": 3}, _hop_setup),
        "rate_limited": (429, {"session_message_rate_per_minute": 1}, _rate_setup),
        "pair_budget": (429, {"session_message_pair_max_per_hour": 1}, _pair_setup),
        "target_queue_full": (409, {"session_message_queued_max": 1}, _queue_setup),
    }
    for code, (status, overrides, setup) in cases.items():
        await _exec(pool, "TRUNCATE sessions CASCADE")
        await _refused_before_post(pool, dsn, code, status, (overrides, setup))


async def test_relay_text_cap_counts_prefix(pool, dsn):
    rig = Rig(pool, dsn)
    cron, _ = await _pair(rig)
    over = "x" * (16000 - len("[cron-x] ") + 1)
    assert_error(await rig.post(cron, text=over), 422, "invalid_request")
    assert len(rig.fake.calls) == 1
    assert await _rows(pool) == []
    fits = "x" * (16000 - len("[cron-x] "))
    assert (await rig.post(cron, text=fits)).status_code == 202


async def test_occupier_post_refusal_passes_back(pool, dsn):
    rig = Rig(pool, dsn)
    cron, occ = await _pair(rig)
    rig.fake.refuse(MESSAGES, 422, "pseudonym_disclosed", session=str(occ.id))
    r = await rig.post(cron)
    assert_error(r, 422, "pseudonym_disclosed")
    assert len(rig.fake.calls) == 2
    assert await _rows(pool) == []


async def test_reply_to_is_never_relayed(pool, dsn):
    rig = Rig(pool, dsn)
    cron, _ = await _pair(rig)
    r = await rig.post(cron, reply_to="m1")
    assert r.status_code == 409 and r.json()["error"]["code"] == "room_bound"
    assert "occupier" not in r.text
    assert len(rig.fake.calls) == 1
    assert await _rows(pool) == []


async def test_notice_lost_after_post_still_answers_202(pool, dsn, caplog, monkeypatch):
    rig = Rig(pool, dsn)
    cron, occ = await _pair(rig)

    async def broken(*_a: Any, **_kw: Any) -> int:
        raise queue.SessionBeingDeleted("gone")

    monkeypatch.setattr(session_messaging, "enqueue_relay_notice", broken)
    with caplog.at_level(logging.WARNING):
        r = await rig.post(cron)
    assert r.status_code == 202 and r.json()["message_id"] is None
    assert r.json()["via"] == occ.slug and r.json()["send_id"] == "s1"
    lines = [rec.getMessage() for rec in caplog.records]
    assert "chat relay notice lost from=cron-x to=occ-x send=s1" in lines
    assert await _rows(pool) == []


async def test_relay_logs_no_text(pool, dsn, caplog):
    rig = Rig(pool, dsn)
    cron, occ = await _pair(rig)
    marker = "relay-body-marker"
    with caplog.at_level(logging.INFO):
        assert (await rig.post(cron, text=marker)).status_code == 202
    lines = [r.getMessage() for r in caplog.records]
    assert f"chat relay from=cron-x to=occ-x room={ROOM} send=s1 status=202" in lines
    assert not any(marker in line for line in lines)
