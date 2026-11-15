"""Runaway park (T-224): park a session, hold its queue, interrupt its turns, the resume route."""

from __future__ import annotations

import hashlib
import uuid
from datetime import timedelta

import httpx
import pytest
from factories import insert_agent, insert_command, insert_session, set_agent_status
from psycopg.rows import dict_row
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.runaway import NOTICE_PARKED

SERVICE_TOKEN = "s3cret-park-service-token"


async def rows(pool, sql: str, params: tuple = ()) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return await cur.fetchall()


async def session_row(pool, session_id) -> dict:
    (row,) = await rows(pool, "SELECT * FROM sessions WHERE id = %s", (session_id,))
    return row


async def live_agent(pool, session_id, turn: str | None = None) -> dict:
    a = await insert_agent(
        pool,
        session_id,
        ready=True,
        wrapper_session_id=f"w-{uuid.uuid4().hex[:6]}",
        url=f"http://agent-{uuid.uuid4().hex[:6]}:8080",
    )
    await set_agent_status(pool, a["id"], state="busy" if turn else "idle", current_turn_id=turn)
    return a


async def park(pool, session_id, reason="turns", detail=None, notice="N"):
    async with pool.connection() as conn:
        return await sessions_repo.park(conn, session_id, reason, detail or {}, notice)


async def resume(pool, session_id):
    async with pool.connection() as conn:
        return await sessions_repo.resume(conn, session_id)


async def interrupt_parked(pool) -> list[uuid.UUID]:
    async with pool.connection() as conn:
        return await sessions_repo.interrupt_parked(conn)


async def interrupts(pool, agent_id=None) -> list[dict]:
    return await rows(
        pool,
        "SELECT * FROM commands WHERE kind = 'interrupt'"
        " AND (%s::uuid IS NULL OR agent_id = %s) ORDER BY id",
        (agent_id, agent_id),
    )


async def test_park_sets_columns_queues_notice_and_interrupts_busy_agents(pool):
    s = await insert_session(pool)
    a = await live_agent(pool, s["id"], turn="t1")
    b = await live_agent(pool, s["id"])
    detail = {
        "agent_id": str(a["id"]),
        "pod": "agent-x-0",
        "value": 301,
        "limit": 300,
        "window_at": None,
    }
    out = await park(pool, s["id"], "turns", detail, "N")
    assert out.parked and out.interrupts == 1 and out.message_id is not None
    row = await session_row(pool, s["id"])
    assert row["parked_at"] is not None and row["park_notified_at"] is None
    assert row["parked_reason"] == "turns" and row["parked_detail"] == detail
    (msg,) = await rows(pool, "SELECT * FROM messages WHERE session_id = %s", (s["id"],))
    assert msg["id"] == out.message_id
    assert (msg["notice"], msg["status"], msg["text"], msg["lane"]) == (
        NOTICE_PARKED,
        "queued",
        "N",
        "regular",
    )
    cmds = await interrupts(pool)
    assert [c["agent_id"] for c in cmds] == [a["id"]]
    assert await interrupts(pool, b["id"]) == []


async def test_park_twice_is_noop(pool):
    s = await insert_session(pool)
    await live_agent(pool, s["id"], turn="t1")
    assert (await park(pool, s["id"])).parked
    first = (await session_row(pool, s["id"]))["parked_at"]
    out = await park(pool, s["id"], "tokens")
    assert out == sessions_repo.ParkOutcome(False, 0, None)
    row = await session_row(pool, s["id"])
    assert row["parked_at"] == first and row["parked_reason"] == "turns"
    assert len(await rows(pool, "SELECT id FROM messages WHERE session_id = %s", (s["id"],))) == 1
    assert len(await interrupts(pool)) == 1


async def test_park_of_deleting_session_is_noop(pool):
    s = await insert_session(pool, desired="deleting")
    out = await park(pool, s["id"])
    assert out == sessions_repo.ParkOutcome(False, 0, None)
    assert (await session_row(pool, s["id"]))["parked_at"] is None


async def test_park_inside_caller_transaction(pool):
    s = await insert_session(pool)
    async with pool.connection() as conn, conn.transaction():
        out = await sessions_repo.park(conn, s["id"], "stop_loop", {}, "N")
    assert out.parked
    assert (await session_row(pool, s["id"]))["parked_reason"] == "stop_loop"


async def test_claim_skips_parked_session(pool):
    s = await insert_session(pool)
    await live_agent(pool, s["id"])
    async with pool.connection() as conn:
        mid = await queue.enqueue_message(conn, s["id"], "work")
    await park(pool, s["id"])
    async with pool.connection() as conn:
        assert await queue.claim_and_pick(conn) is None
    await resume(pool, s["id"])
    async with pool.connection() as conn:
        claim = await queue.claim_and_pick(conn)
    assert claim is not None and claim.message_id == mid


async def test_parked_session_interrupts_peer_and_agent_turns(pool):
    s = await insert_session(pool)
    a = await live_agent(pool, s["id"])
    out = await park(pool, s["id"])
    assert out.interrupts == 0
    assert await interrupt_parked(pool) == []
    # a peer-origin turn starts in the parked session
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="peer-1")
    # a busy agent of a session that is not parked is never touched
    other = await insert_session(pool)
    busy = await live_agent(pool, other["id"], turn="t9")
    # a gone agent with a stale turn is not interrupted
    gone = await live_agent(pool, s["id"], turn="t-old")
    await rows(pool, "UPDATE agents SET gone_at = now() WHERE id = %s RETURNING id", (gone["id"],))
    assert await interrupt_parked(pool) == [a["id"]]
    assert await interrupt_parked(pool) == []  # the interrupt is still queued
    (cmd,) = await interrupts(pool, a["id"])
    await rows(
        pool, "UPDATE commands SET status = 'running' WHERE id = %s RETURNING id", (cmd["id"],)
    )
    assert await interrupt_parked(pool) == []  # running counts as not finished
    await rows(
        pool,
        "UPDATE commands SET status = 'done', finished_at = now() WHERE id = %s RETURNING id",
        (cmd["id"],),
    )
    assert await interrupt_parked(pool) == [a["id"]]  # the turn still runs
    assert await interrupts(pool, busy["id"]) == []
    assert await interrupts(pool, gone["id"]) == []


async def test_interrupt_parked_ignores_other_command_kinds(pool):
    s = await insert_session(pool)
    a = await live_agent(pool, s["id"])
    await park(pool, s["id"])
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="agent-1")
    await insert_command(pool, a["id"], kind="compact")
    assert await interrupt_parked(pool) == [a["id"]]


async def test_resume_clears_park_and_sets_reset(pool):
    s = await insert_session(pool)
    plain = await insert_session(pool)
    await park(pool, s["id"], "tokens", {"value": 1})
    async with pool.connection() as conn:
        parked_at = (await session_row(pool, s["id"]))["parked_at"]
        assert await sessions_repo.mark_park_notified(conn, s["id"], parked_at)
    got = await resume(pool, s["id"])
    assert got.parked_at is None and got.parked_reason is None and got.parked_detail is None
    assert got.park_notified_at is None and got.guard_reset_at is not None
    row = await session_row(pool, s["id"])
    assert row["parked_at"] is None and row["guard_reset_at"] == got.guard_reset_at
    # not parked: no write
    again = await resume(pool, s["id"])
    assert again.guard_reset_at == got.guard_reset_at
    untouched = await resume(pool, plain["id"])
    assert untouched.guard_reset_at is None
    assert (await session_row(pool, plain["id"]))["guard_reset_at"] is None


async def test_resume_unknown_and_deleting(pool):
    with pytest.raises(sessions_repo.SessionNotFound):
        await resume(pool, uuid.uuid4())
    s = await insert_session(pool, desired="deleting")
    with pytest.raises(sessions_repo.SessionBeingDeleted):
        await resume(pool, s["id"])


async def test_mark_park_notified_guarded_by_parked_at(pool):
    s = await insert_session(pool)
    await park(pool, s["id"])
    parked_at = (await session_row(pool, s["id"]))["parked_at"]
    async with pool.connection() as conn:
        assert await sessions_repo.unnotified_parks(conn) == [
            await sessions_repo.get(conn, s["id"])
        ]
        wrong = parked_at - timedelta(seconds=1)
        assert not await sessions_repo.mark_park_notified(conn, s["id"], wrong)
        assert await sessions_repo.mark_park_notified(conn, s["id"], parked_at)
        assert not await sessions_repo.mark_park_notified(conn, s["id"], parked_at)
        assert await sessions_repo.unnotified_parks(conn) == []
    assert (await session_row(pool, s["id"]))["park_notified_at"] is not None


async def test_unnotified_parks_skips_deleting_and_unparked(pool):
    s = await insert_session(pool)
    await insert_session(pool)
    await park(pool, s["id"])
    await rows(
        pool, "UPDATE sessions SET desired = 'deleting' WHERE id = %s RETURNING id", (s["id"],)
    )
    async with pool.connection() as conn:
        assert await sessions_repo.unnotified_parks(conn) == []


async def test_delete_park_route(pool, dsn):
    s = await insert_session(pool)
    await park(pool, s["id"], "stop_loop")
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.delete(f"/api/v1/sessions/{s['id']}/park")
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["id"] == str(s["id"])
        assert body["parked_at"] is None and body["parked_reason"] is None
        assert body["parked_detail"] is None
        # idempotent when not parked
        r = await http.delete(f"/api/v1/sessions/{s['id']}/park")
        assert r.status_code == 200, r.text
        assert_error(await http.delete(f"/api/v1/sessions/{uuid.uuid4()}/park"), 404, "not_found")
        gone = await insert_session(pool, desired="deleting")
        assert_error(
            await http.delete(f"/api/v1/sessions/{gone['id']}/park"), 409, "session_deleting"
        )
    assert (await session_row(pool, s["id"]))["guard_reset_at"] is not None


async def test_park_route_not_on_service_or_agent_port(pool, dsn):
    s = await insert_session(pool)
    await park(pool, s["id"])
    digest = hashlib.sha256(SERVICE_TOKEN.encode()).hexdigest()
    app = create_app(make_settings(dsn, service_tokens=f"agents-webhook:{digest}"), pool)
    path = f"/api/v1/sessions/{s['id']}/park"
    transport = httpx.ASGITransport(app=app, client=("203.0.113.9", 4444))
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://orch:8002",
        headers={"Authorization": f"Bearer {SERVICE_TOKEN}"},
    ) as service:
        assert_error(await service.delete(path), 403, "forbidden")
    agent_transport = httpx.ASGITransport(app=app, client=("10.1.2.3", 5555))
    async with httpx.AsyncClient(transport=agent_transport, base_url="http://orch:8001") as agent:
        assert_error(await agent.delete(path), 404, "not_found")
        assert_error(await agent.delete(f"/agent/v1/sessions/{s['id']}/park"), 404, "not_found")
    assert (await session_row(pool, s["id"]))["parked_at"] is not None


async def test_session_view_parked_fields(pool, dsn):
    s = await insert_session(pool)
    plain = await insert_session(pool)
    detail = {"agent_id": None, "pod": "agent-0", "value": 3, "limit": 3, "window_at": None}
    await park(pool, s["id"], "stop_loop", detail)
    stored = (await session_row(pool, s["id"]))["parked_at"]
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        view = (await http.get(f"/api/v1/sessions/{s['id']}")).json()
        listed = {x["id"]: x for x in (await http.get("/api/v1/sessions")).json()}
        plain_view = (await http.get(f"/api/v1/sessions/{plain['id']}")).json()
    for v in (view, listed[str(s["id"])]):
        assert v["parked_at"] == stored.isoformat().replace("+00:00", "Z")
        assert v["parked_reason"] == "stop_loop" and v["parked_detail"] == detail
    assert plain_view["parked_at"] is None and plain_view["parked_reason"] is None
    assert plain_view["parked_detail"] is None
