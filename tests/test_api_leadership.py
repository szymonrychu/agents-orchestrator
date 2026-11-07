"""Leadership requests: the agent handover route, the public make-leader route, current_leader."""

from __future__ import annotations

import asyncio
import json
import uuid

import psycopg
import pytest
from factories import insert_agent, insert_session
from test_api_agent import IP0, IP1, client_for, headers, make_settings, set_ip
from test_api_sessions import PEER
from test_api_sessions import client_for as public_client
from test_api_sessions import make_settings as public_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import leadership


async def hive(pool, replicas: int = 2, leader: int | None = 0):
    c = await insert_session(pool, spec={"replicas": replicas, "max_replicas": 4})
    a0 = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    a1 = await insert_agent(pool, c["id"], ordinal=1, ready=True)
    await set_ip(pool, a0["id"], IP0)
    await set_ip(pool, a1["id"], IP1)
    if leader is not None:
        led = (a0, a1)[leader]
        async with pool.connection() as conn:
            await conn.execute(
                "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
                " VALUES (%s, %s, 3, now(), 'start')",
                (c["id"], led["id"]),
            )
    return c, a0, a1


async def requests(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT kind, from_agent_id, to_agent_id, reason, epoch, status"
            " FROM leadership_requests WHERE session_id = %s ORDER BY id",
            (session_id,),
        )
        return await cur.fetchall()


async def post_handover(dsn, pool, c, ip=IP0, **body):
    body.setdefault("reason", "context is full")
    body.setdefault("epoch", 3)
    async with client_for(create_app(make_settings(dsn), pool), ip) as http:
        return await http.post("/agent/v1/leadership/handover", json=body, headers=headers(c["id"]))


async def test_handover_queued(pool, dsn):
    c, a0, a1 = await hive(pool)
    got: list[dict] = []
    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as lc:
        await lc.execute("LISTEN leadership_requested")

        async def hear():
            async for n in lc.notifies(timeout=5):
                got.append(json.loads(n.payload))
                return

        task = asyncio.create_task(hear())
        r = await post_handover(dsn, pool, c, to=a1["pod_name"])
        await task
    assert r.status_code == 202, r.text
    request_id = r.json()["request_id"]
    assert isinstance(request_id, int)
    assert got == [{"session_id": str(c["id"])}]
    assert await requests(pool, c["id"]) == [
        ("handover", a0["id"], a1["id"], "context is full", 3, "queued")
    ]
    # `to` is optional: any agent
    r2 = await post_handover(dsn, pool, c)
    assert r2.status_code == 202
    assert (await requests(pool, c["id"]))[1][2] is None


async def test_handover_prechecks(pool, dsn):
    c, a0, a1 = await hive(pool)
    r = await post_handover(dsn, pool, c, ip=IP1)  # a1 is not the leader
    assert (r.status_code, r.json()["error"]["code"]) == (409, "not_leader")
    r = await post_handover(dsn, pool, c, epoch=2)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "stale_epoch")
    r = await post_handover(dsn, pool, c, to="nope-9")
    assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")
    r = await post_handover(dsn, pool, c, reason="")
    assert r.status_code == 422
    r = await post_handover(dsn, pool, c, reason="x" * 2001)
    assert r.status_code == 422
    # no leader row at all
    c2, _, _ = await hive(pool, leader=None)
    r = await post_handover(dsn, pool, c2)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "not_leader")
    # alone: a non-hive session
    solo = await insert_session(pool, spec={"replicas": 1})
    s0 = await insert_agent(pool, solo["id"], ordinal=0, ready=True)
    await set_ip(pool, s0["id"], "10.9.0.50")
    r = await post_handover(dsn, pool, solo, ip="10.9.0.50")
    assert (r.status_code, r.json()["error"]["code"]) == (409, "alone")
    for cid in (c["id"], c2["id"], solo["id"]):
        assert await requests(pool, cid) == []


async def post_leader(dsn, pool, session_id, agent_id):
    async with public_client(create_app(public_settings(dsn), pool), PEER) as http:
        return await http.post(
            f"/api/v1/sessions/{session_id}/leader", json={"agent_id": str(agent_id)}
        )


async def test_user_make_leader(pool, dsn):
    c, a0, a1 = await hive(pool)
    r = await post_leader(dsn, pool, c["id"], a1["id"])
    assert r.status_code == 202, r.text
    assert isinstance(r.json()["request_id"], int)
    assert await requests(pool, c["id"]) == [("user", None, a1["id"], "user", None, "queued")]
    other, o0, _ = await hive(pool)
    r = await post_leader(dsn, pool, c["id"], o0["id"])
    assert (r.status_code, r.json()["error"]["code"]) == (404, "not_found")
    r = await post_leader(dsn, pool, uuid.uuid4(), a1["id"])
    assert r.status_code == 404
    r = await post_leader(dsn, pool, c["id"], uuid.uuid4())
    assert r.status_code == 404
    solo = await insert_session(pool, spec={"replicas": 1})
    s0 = await insert_agent(pool, solo["id"], ordinal=0, ready=True)
    r = await post_leader(dsn, pool, solo["id"], s0["id"])
    assert (r.status_code, r.json()["error"]["code"]) == (409, "alone")
    assert len(await requests(pool, c["id"])) == 1


async def test_current_leader(pool):
    c, a0, a1 = await hive(pool, leader=1)
    async with pool.connection() as conn:
        got = await leadership.current_leader(conn, c["id"])
        assert got == leadership.Leader(agent_id=a1["id"], pod_name=a1["pod_name"], epoch=3)
        c2, _, _ = await hive(pool, leader=None)
        assert await leadership.current_leader(conn, c2["id"]) is None
        rid = await leadership.insert_request(conn, c["id"], "user", None, a0["id"], "r", None)
        assert isinstance(rid, int)
    with pytest.raises(psycopg.errors.CheckViolation):
        async with pool.connection() as conn:
            await leadership.insert_request(conn, c["id"], "bogus", None, None, "r", None)


async def test_handover_answer_pending(pool, dsn):
    c, _, _ = await hive(pool)
    r = await post_handover(dsn, pool, c)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["status"] == "pending"
    assert body["applies"] == "when your turn has ended and your background agents have reported"
    assert isinstance(body["request_id"], int)


async def test_handover_reason_2000_ok_2001_rejected(pool, dsn):
    c, _, _ = await hive(pool)
    assert (await post_handover(dsn, pool, c, reason="x" * 2000)).status_code == 202
    assert (await post_handover(dsn, pool, c, reason="x" * 2001)).status_code == 422
