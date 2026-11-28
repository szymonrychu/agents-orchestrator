"""Per agent auth state (T-225): migration, snapshot model, views and the gone-agent rule."""

from __future__ import annotations

import pytest
from factories import insert_agent, insert_session, set_agent_status
from psycopg.errors import CheckViolation
from test_api_sessions import client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import status
from agents_orchestrator.wrapper import Snapshot

pytestmark = pytest.mark.critical


async def _fail(pool, agent_id, reason="revoked") -> None:
    async with pool.connection() as conn:
        await status.set_auth_failed(conn, agent_id, reason)


async def test_migration_adds_auth_columns(pool):
    c = await insert_session(pool, slug="auth-cols")
    a = await insert_agent(pool, c["id"], ready=True)
    await set_agent_status(pool, a["id"], state="idle")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='agent_status'"
            " AND column_name IN ('auth_state', 'auth_reason', 'auth_failed_at')"
        )
        assert {r[0] for r in await cur.fetchall()} == {
            "auth_state",
            "auth_reason",
            "auth_failed_at",
        }
        with pytest.raises(CheckViolation):
            await conn.execute(
                "UPDATE agent_status SET auth_state='bogus' WHERE agent_id=%s", (a["id"],)
            )


def test_older_wrapper_snapshot_without_auth_fields_parses():
    snap = Snapshot.model_validate(
        {"session_id": "w0", "state": "idle", "turns_completed": 0, "bootstrap": {"steps": []}}
    )
    assert snap.auth_state is None and snap.auth_reason is None


async def test_agent_view_auth_fields(pool, dsn):
    c = await insert_session(pool, slug="auth-view")
    a0 = await insert_agent(pool, c["id"], ready=True)
    a1 = await insert_agent(pool, c["id"], ready=True)
    gone = await insert_agent(pool, c["id"], ready=True)
    for a in (a0, a1, gone):
        await set_agent_status(pool, a["id"], state="idle")
    await _fail(pool, a0["id"], "revoked")
    await _fail(pool, gone["id"], "revoked")
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id=%s", (gone["id"],))
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        agents = (await http.get(f"/api/v1/sessions/{c['id']}/agents")).json()
        by_id = {x["id"]: x for x in agents}
        assert by_id[str(a0["id"])]["auth_state"] == "failed"
        assert by_id[str(a0["id"])]["auth_reason"] == "revoked"
        assert by_id[str(a1["id"])]["auth_state"] is None
        assert by_id[str(a1["id"])]["auth_reason"] is None
        summary = (await http.get(f"/api/v1/sessions/{c['id']}")).json()["summary"]
        assert summary["auth_failed_agents"] == 1


async def test_any_auth_failed_ignores_gone_agents(pool):
    c = await insert_session(pool, slug="auth-any")
    live = await insert_agent(pool, c["id"], ready=True)
    gone = await insert_agent(pool, c["id"], ready=True)
    for a in (live, gone):
        await set_agent_status(pool, a["id"], state="idle")
    async with pool.connection() as conn:
        assert await status.any_auth_failed(conn) is False
        await status.set_auth_failed(conn, gone["id"], None)
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id=%s", (gone["id"],))
        assert await status.any_auth_failed(conn) is False
        await status.set_auth_failed(conn, live["id"], None)
        assert await status.any_auth_failed(conn) is True
