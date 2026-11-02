"""Agent self-close route: POST /agent/v1/close (leader only; a superleader session is refused)."""

from __future__ import annotations

import httpx
import pytest
from factories import insert_agent, insert_session
from test_agent_scale import hive
from test_api_agent import IP0, IP1, client_for, headers, make_settings, set_ip
from test_api_sessions import assert_error

from agents_orchestrator.api.app import create_app


async def close_columns(pool, session_id) -> tuple:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT close_requested_at, close_reason, desired FROM sessions WHERE id = %s",
            (session_id,),
        )
        return await cur.fetchone()


async def post_close(pool, dsn, session_id, body, ip=IP0):
    async with client_for(create_app(make_settings(dsn), pool), ip) as http:
        return await http.post("/agent/v1/close", json=body, headers=headers(session_id))


async def test_leader_requests_close_202(pool, dsn):
    c, _, _ = await hive(pool)
    r = await post_close(pool, dsn, c["id"], {"reason": " done "})
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["close_reason"] == "done" and body["already_requested"] is False
    assert body["grace_seconds"] == 30 and body["session_id"] == str(c["id"])
    requested_at, reason, desired = await close_columns(pool, c["id"])
    assert requested_at is not None and reason == "done" and desired == "active"
    assert body["close_requested_at"] == requested_at.isoformat().replace("+00:00", "Z")


async def test_member_refused_403_not_leader(pool, dsn):
    c, _, _ = await hive(pool)
    r = await post_close(pool, dsn, c["id"], {"reason": "done"}, ip=IP1)
    assert_error(r, 403, "not_leader")
    assert (await close_columns(pool, c["id"]))[:2] == (None, None)


async def test_alone_session_agent_may_close(pool, dsn):
    c = await insert_session(pool, spec={"replicas": 1})
    a0 = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    await set_ip(pool, a0["id"], IP0)
    r = await post_close(pool, dsn, c["id"], {"reason": "done"})
    assert r.status_code == 202, r.text
    assert (await close_columns(pool, c["id"]))[1] == "done"


async def test_superleader_refused_own_session(pool, dsn):
    c = await insert_session(pool, spec={"replicas": 1, "session_messaging": True})
    a0 = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    await set_ip(pool, a0["id"], IP0)
    r = await post_close(pool, dsn, c["id"], {"reason": "done"})
    assert_error(r, 422, "own_session")
    assert (await close_columns(pool, c["id"]))[:2] == (None, None)


async def test_deleting_session_refused_409(pool, dsn):
    c = await insert_session(pool, spec={"replicas": 1}, desired="deleting")
    a0 = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    await set_ip(pool, a0["id"], IP0)
    r = await post_close(pool, dsn, c["id"], {"reason": "done"})
    assert_error(r, 409, "session_deleting")
    assert (await close_columns(pool, c["id"]))[:2] == (None, None)


async def test_repeat_keeps_first_request(pool, dsn):
    c, _, _ = await hive(pool)
    first = await post_close(pool, dsn, c["id"], {"reason": "done"})
    second = await post_close(pool, dsn, c["id"], {"reason": "x"})
    assert second.status_code == 202, second.text
    body = second.json()
    assert body["already_requested"] is True and body["close_reason"] == "done"
    assert body["close_requested_at"] == first.json()["close_requested_at"]
    assert (await close_columns(pool, c["id"]))[1] == "done"


@pytest.mark.parametrize(
    "body",
    [{"reason": ""}, {"reason": "   "}, {"reason": "a" * 501}, {"reason": "x", "now": True}, {}],
)
async def test_reason_validation(pool, dsn, body):
    c, _, _ = await hive(pool)
    r = await post_close(pool, dsn, c["id"], body)
    assert_error(r, 422, "invalid_request")
    assert (await close_columns(pool, c["id"]))[:2] == (None, None)


async def test_reason_of_500_characters_accepted(pool, dsn):
    c, _, _ = await hive(pool)
    r = await post_close(pool, dsn, c["id"], {"reason": "a" * 500})
    assert r.status_code == 202, r.text


async def test_not_on_main_port(pool, dsn):
    c, _, _ = await hive(pool)
    app = create_app(make_settings(dsn), pool)
    transport = httpx.ASGITransport(app=app, client=(IP0, 4321))
    async with httpx.AsyncClient(transport=transport, base_url="http://orch:8000") as pub:
        r = await pub.post("/agent/v1/close", json={"reason": "done"}, headers=headers(c["id"]))
    assert r.status_code == 404, r.text
    assert (await close_columns(pool, c["id"]))[:2] == (None, None)
