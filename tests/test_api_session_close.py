"""Pending self-close in the session view and stream, and the cancel route (DELETE /close)."""

from __future__ import annotations

import hashlib
import uuid

import httpx
from factories import insert_session
from test_api_sessions import assert_error, client_for, make_settings
from test_api_streams import open_sse, srv  # noqa: F401  (srv is a fixture)

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import sessions as sessions_repo

SERVICE_TOKEN = "s3cret-close-service-token"


async def request_close(pool, session_id, reason="done"):
    async with pool.connection() as conn:
        await sessions_repo.request_close(conn, session_id, reason)


async def close_columns(pool, session_id) -> tuple:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT close_requested_at, close_reason, desired FROM sessions WHERE id = %s",
            (session_id,),
        )
        return await cur.fetchone()


async def test_view_carries_close_fields(pool, dsn):
    asked = await insert_session(pool)
    plain = await insert_session(pool)
    await request_close(pool, asked["id"], "done")
    stored_at = (await close_columns(pool, asked["id"]))[0]
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        detail = (await http.get(f"/api/v1/sessions/{asked['id']}")).json()
        listed = {s["id"]: s for s in (await http.get("/api/v1/sessions")).json()}
        plain_detail = (await http.get(f"/api/v1/sessions/{plain['id']}")).json()
    iso = stored_at.isoformat().replace("+00:00", "Z")
    for view in (detail, listed[str(asked["id"])]):
        assert view["close_reason"] == "done" and view["close_requested_at"] == iso
    for view in (plain_detail, listed[str(plain["id"])]):
        assert view["close_reason"] is None and view["close_requested_at"] is None


async def test_stream_snapshot_carries_close_fields(srv, pool):  # noqa: F811
    asked = await insert_session(pool)
    await request_close(pool, asked["id"], "all done")
    async with open_sse(srv, "/api/v1/stream") as sse:
        snap = await sse.next()
    assert snap["event"] == "snapshot"
    by_id = {x["session"]["id"]: x["session"] for x in snap["data"]["sessions"]}
    assert by_id[str(asked["id"])]["close_reason"] == "all done"
    assert by_id[str(asked["id"])]["close_requested_at"] is not None


async def test_cancel_clears_200(pool, dsn):
    s = await insert_session(pool)
    await request_close(pool, s["id"])
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.delete(f"/api/v1/sessions/{s['id']}/close")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["id"] == str(s["id"])
    assert body["close_requested_at"] is None and body["close_reason"] is None
    assert (await close_columns(pool, s["id"]))[:2] == (None, None)


async def test_cancel_without_request_is_200(pool, dsn):
    s = await insert_session(pool)
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.delete(f"/api/v1/sessions/{s['id']}/close")
    assert r.status_code == 200, r.text
    assert r.json()["close_requested_at"] is None
    assert (await close_columns(pool, s["id"]))[:2] == (None, None)


async def test_cancel_unknown_404(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.delete(f"/api/v1/sessions/{uuid.uuid4()}/close")
    assert_error(r, 404, "not_found")


async def test_cancel_after_flip_409(pool, dsn):
    s = await insert_session(pool)
    await request_close(pool, s["id"], "done")
    async with pool.connection() as conn:
        await sessions_repo.request_delete(conn, s["id"])
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.delete(f"/api/v1/sessions/{s['id']}/close")
    assert_error(r, 409, "session_deleting")
    requested_at, reason, desired = await close_columns(pool, s["id"])
    assert requested_at is not None and reason == "done" and desired == "deleting"


async def test_cancel_not_on_service_or_agent_port(pool, dsn):
    s = await insert_session(pool)
    await request_close(pool, s["id"])
    digest = hashlib.sha256(SERVICE_TOKEN.encode()).hexdigest()
    app = create_app(make_settings(dsn, service_tokens=f"agents-webhook:{digest}"), pool)
    path = f"/api/v1/sessions/{s['id']}/close"
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
    assert (await close_columns(pool, s["id"]))[1] == "done"
