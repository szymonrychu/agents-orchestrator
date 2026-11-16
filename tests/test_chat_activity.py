"""db.chat_activity and its two recorders: the inbound connector post and the outbound relay."""

from datetime import UTC, datetime, timedelta

from factories import insert_agent, insert_session
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers
from test_api_sessions import make_settings
from test_chat_relay import MESSAGES, Rig
from test_service_port import TOKEN, client, sha

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import chat_activity

CONNECTOR_TOKEN = "connector-token-value"


async def _rows(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT direction, agent_id FROM chat_activity WHERE session_id = %s ORDER BY id",
            (session_id,),
        )
        return await cur.fetchall()


async def test_record_and_seen_since(pool):
    session = await insert_session(pool)
    a = await insert_agent(pool, session["id"])
    b = await insert_agent(pool, session["id"])
    before = datetime.now(UTC) - timedelta(seconds=5)
    after = datetime.now(UTC) + timedelta(seconds=5)
    async with pool.connection() as conn:
        assert not await chat_activity.seen_since(conn, session["id"], a["id"], before)
        assert await chat_activity.last_out(conn, session["id"]) is None
        await chat_activity.record(conn, session["id"], b["id"], "out")
        assert not await chat_activity.seen_since(conn, session["id"], a["id"], before)
        assert await chat_activity.seen_since(conn, session["id"], b["id"], before)
        assert not await chat_activity.seen_since(conn, session["id"], b["id"], after)
        await chat_activity.record(conn, session["id"], None, "in")
        assert await chat_activity.seen_since(conn, session["id"], a["id"], before)
        assert await chat_activity.last_out(conn, session["id"]) is not None


async def test_inbound_from_connector_recorded(pool, dsn):
    session = await insert_session(pool)
    tokens = f"agents-matrix-connector:{sha(CONNECTOR_TOKEN)},agents-webhook:{sha(TOKEN)}"
    app = create_app(make_settings(dsn, service_tokens=tokens), pool)
    path = f"/api/v1/sessions/{session['id']}/messages"
    async with client(app, 8002, token=CONNECTOR_TOKEN) as c:
        assert (await c.post(path, json={"text": "hi"})).status_code == 202
    async with client(app, 8002, token=TOKEN) as c:
        assert (await c.post(path, json={"text": "from the webhook"})).status_code == 202
    assert await _rows(pool, session["id"]) == [("in", None)]
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT text, origin_service FROM messages WHERE session_id = %s ORDER BY id",
            (session["id"],),
        )
        assert await cur.fetchall() == [
            ("hi", "agents-matrix-connector"),
            ("from the webhook", "agents-webhook"),
        ]


async def test_outbound_chat_post_recorded(pool, dsn):
    rig = Rig(pool, dsn)
    sess = await alone(pool, "chatty")
    assert (await rig.post(sess)).status_code == 202
    async with client_for(rig.app, sess.leader_ip) as http:
        r = await http.post(
            "/agent/v1/chat/reactions", json={"to": "m1", "key": "ok"}, headers=headers(sess.id)
        )
    assert r.status_code == 202, r.text
    assert await _rows(pool, sess.id) == [("out", sess.leader_id)] * 2
    async with pool.connection() as conn:
        assert await chat_activity.last_out(conn, sess.id) is not None


async def test_non_2xx_not_recorded(pool, dsn):
    rig = Rig(pool, dsn)
    sess = await alone(pool, "refused")
    rig.fake.refuse(MESSAGES, 409, "room_bound")
    assert (await rig.post(sess)).status_code == 409
    assert await _rows(pool, sess.id) == []
