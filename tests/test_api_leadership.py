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
from agents_orchestrator.db import handoffs, leadership


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
    for cid in (c["id"], c2["id"]):
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


# ---------------------------------------------------------------- T-256 step 8


SOLO_IP = "10.9.0.50"


async def solo_session(pool, ip: str = SOLO_IP):
    solo = await insert_session(pool, spec={"replicas": 1})
    s0 = await insert_agent(pool, solo["id"], ordinal=0, ready=True)
    await set_ip(pool, s0["id"], ip)
    return solo, s0


async def open_handoff(pool, session_id, agent_id, phase="notified", trigger="strong", secs=300):
    async with pool.connection() as conn:
        return await handoffs.insert(
            conn,
            session_id,
            agent_id,
            trigger,
            phase,
            deadline_at=None if phase in ("handed", "requested") else _later(secs),
        )


def _later(secs: int):
    from datetime import UTC, datetime, timedelta

    return datetime.now(UTC) + timedelta(seconds=secs)


async def handoff_rows(pool, agent_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT trigger, phase, close_reason, handoff_text, deadline_at FROM agent_handoffs"
            " WHERE agent_id = %s ORDER BY id",
            (agent_id,),
        )
        return await cur.fetchall()


async def test_extend_moves_deadline_once(pool, dsn):
    c, a0 = await solo_session(pool)
    row = await open_handoff(pool, c["id"], a0["id"])
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, extend=True, reason=None, epoch=None)
    assert r.status_code == 202, r.text
    body = r.json()
    assert body["request_id"] is None and body["deadline_at"] is not None
    ((_, phase, _, _, deadline),) = await handoff_rows(pool, a0["id"])
    assert phase == "extended"
    assert deadline.timestamp() == pytest.approx(row.deadline_at.timestamp() + 300, abs=2)
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, extend=True, reason=None, epoch=None)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "extension_used")


async def test_extend_without_countdown_409(pool, dsn):
    c, a0 = await solo_session(pool)
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, extend=True, reason=None, epoch=None)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "no_countdown")
    await open_handoff(pool, c["id"], a0["id"], phase="requested")
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, extend=True, reason=None, epoch=None)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "no_countdown")


async def test_extend_marks_handled_and_hive_leader_may_extend(pool, dsn):
    c, a0, _ = await hive(pool)
    await open_handoff(pool, c["id"], a0["id"])
    r = await post_handover(dsn, pool, c, extend=True, reason=None, epoch=None, handled=[])
    assert r.status_code == 202, r.text
    assert r.json()["deadline_at"] is not None
    assert await requests(pool, c["id"]) == []


async def test_member_extend_409_not_leader(pool, dsn):
    c, a0, a1 = await hive(pool)
    await open_handoff(pool, c["id"], a1["id"])
    r = await post_handover(dsn, pool, c, ip=IP1, extend=True, reason=None, epoch=None)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "not_leader")
    assert [row[1] for row in await handoff_rows(pool, a1["id"])] == ["notified"]


async def test_solo_voluntary_handoff_opens_handed_row(pool, dsn):
    c, a0 = await solo_session(pool)
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, reason="my handoff", epoch=None)
    assert r.status_code == 202, r.text
    assert r.json()["request_id"] is None
    assert [row[:4] for row in await handoff_rows(pool, a0["id"])] == [
        ("voluntary", "handed", None, "my handoff")
    ]
    assert await requests(pool, c["id"]) == []
    # a second one while the first is past `requested`
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, reason="again", epoch=None)
    assert (r.status_code, r.json()["error"]["code"]) == (409, "handoff_open")


async def test_solo_voluntary_cancels_countdown_row(pool, dsn):
    c, a0 = await solo_session(pool)
    await open_handoff(pool, c["id"], a0["id"])
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, reason="now", epoch=None)
    assert r.status_code == 202, r.text
    assert [row[:4] for row in await handoff_rows(pool, a0["id"])] == [
        ("strong", "cancelled", "voluntary", None),
        ("voluntary", "handed", None, "now"),
    ]


async def test_solo_voluntary_requires_reason_422(pool, dsn):
    c, a0 = await solo_session(pool)
    r = await post_handover(dsn, pool, c, ip=SOLO_IP, reason=None, epoch=None)
    assert r.status_code == 422
    assert await handoff_rows(pool, a0["id"]) == []


async def test_solo_handoff_open_409(pool, dsn):
    c, a0 = await solo_session(pool)
    for phase in ("discussing", "clearing", "returning"):
        async with pool.connection() as conn:
            await conn.execute("DELETE FROM agent_handoffs WHERE agent_id = %s", (a0["id"],))
        await open_handoff(pool, c["id"], a0["id"], phase=phase)
        r = await post_handover(dsn, pool, c, ip=SOLO_IP, reason="x", epoch=None)
        assert (r.status_code, r.json()["error"]["code"]) == (409, "handoff_open"), phase


async def test_hive_leader_without_epoch_422(pool, dsn):
    c, _, _ = await hive(pool)
    r = await post_handover(dsn, pool, c, epoch=None)
    assert (r.status_code, r.json()["error"]["code"]) == (422, "epoch_required")
    assert await requests(pool, c["id"]) == []


async def test_hive_leader_handover_unchanged(pool, dsn):
    c, a0, a1 = await hive(pool)
    r = await post_handover(dsn, pool, c, to=a1["pod_name"])
    assert r.status_code == 202, r.text
    body = r.json()
    assert isinstance(body["request_id"], int) and body["deadline_at"] is None
