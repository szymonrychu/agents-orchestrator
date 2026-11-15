"""The credential gate: starts and scale-ups refused from expires_at - refuse_hours (409
`credential_expiring`); restarts, scale-downs and the warn phase are never refused."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from factories import attach_preset, insert_session
from test_agent_scale import hive as scale_hive
from test_agent_scale import replicas_of
from test_agent_session_messaging import alone
from test_agent_session_messaging import hive as admin_hive
from test_api_agent import client_for as agent_client
from test_api_agent import headers
from test_api_agent import make_settings as agent_settings
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

C = "/api/v1/sessions"


def ends(**delta) -> datetime:
    return (datetime.now(UTC) + timedelta(**delta)).replace(microsecond=0)


async def session_count(pool) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM sessions")
        return (await cur.fetchone())[0]


async def test_create_refused_inside_refuse_margin(pool, dsn):
    at = ends(hours=12)
    app = create_app(make_settings(dsn, claude_credential_expires_at=at), pool)
    before = await session_count(pool)
    async with client_for(app) as http:
        r = await http.post(C, json={"slug": "late", "spec": {}})
    assert_error(r, 409, "credential_expiring")
    assert r.json()["error"]["credential"] == {"expires_at": at.isoformat(), "phase": "refuse"}
    assert await session_count(pool) == before


async def test_create_refused_after_expiry(pool, dsn):
    at = ends(hours=-1)
    app = create_app(make_settings(dsn, claude_credential_expires_at=at), pool)
    async with client_for(app) as http:
        r = await http.post(C, json={"slug": "gone", "spec": {}})
    assert_error(r, 409, "credential_expiring")
    assert r.json()["error"]["credential"]["phase"] == "expired"


async def test_create_allowed_in_warn(pool, dsn):
    app = create_app(make_settings(dsn, claude_credential_expires_at=ends(days=5)), pool)
    async with client_for(app) as http:
        r = await http.post(C, json={"slug": "warned", "spec": {}})
    assert r.status_code == 201, r.text


async def test_create_allowed_without_date(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        r = await http.post(C, json={"slug": "undated", "spec": {}})
    assert r.status_code == 201, r.text


async def test_scale_up_refused_with_usage_gate_off(pool, dsn):
    c = await insert_session(pool, spec={"replicas": 1, "max_replicas": 3})
    app = create_app(make_settings(dsn, claude_credential_expires_at=ends(hours=1)), pool)
    assert app.state.usage is None
    async with client_for(app) as http:
        r = await http.post(f"{C}/{c['id']}/scale", json={"replicas": 2})
        assert_error(r, 409, "credential_expiring")
        assert await replicas_of(pool, c["id"]) == 1
        r = await http.post(f"{C}/{c['id']}/scale", json={"replicas": 1})
        assert r.status_code == 202, r.text


async def test_agent_and_admin_scale_up_refused(pool, dsn):
    at = ends(hours=1)
    c, _, _ = await scale_hive(pool, replicas=1, max_replicas=3)
    app = create_app(
        agent_settings(dsn).model_copy(update={"claude_credential_expires_at": at}), pool
    )
    async with agent_client(app) as http:
        r = await http.post("/agent/v1/scale", json={"replicas": 2}, headers=headers(c["id"]))
    assert_error(r, 409, "credential_expiring")
    assert await replicas_of(pool, c["id"]) == 1

    boss = await alone(pool, "cred-boss", superleader=True)
    t = await insert_session(pool, slug="cred-target", spec={"replicas": 1, "max_replicas": 2})
    await attach_preset(pool, t["id"], manageable=True)
    app = create_app(
        agent_settings(dsn).model_copy(update={"claude_credential_expires_at": at}), pool
    )
    async with agent_client(app, boss.leader_ip) as http:
        r = await http.post(
            f"{ADMIN_PREFIX}/sessions/cred-target/scale",
            headers=headers(boss.id),
            json={"replicas": 2},
        )
    assert_error(r, 409, "credential_expiring")
    assert await replicas_of(pool, t["id"]) == 1


async def test_restart_not_refused(pool, dsn):
    boss = await admin_hive(pool, "cr-boss", superleader=True)
    other = await admin_hive(pool, "cr-other")
    await attach_preset(pool, other.id)
    app = create_app(
        agent_settings(dsn).model_copy(update={"claude_credential_expires_at": ends(hours=1)}),
        pool,
    )
    async with agent_client(app, boss.leader_ip) as http:
        r = await http.post(
            f"{ADMIN_PREFIX}/sessions/{other.slug}/restart", headers=headers(boss.id)
        )
    assert r.status_code == 202, r.text


async def test_meta_credential():
    at = ends(days=5)
    app = create_app(make_settings(claude_credential_expires_at=at), None)
    async with client_for(app) as http:
        meta = (await http.get("/api/v1/meta")).json()
    # the model serialises UTC as Z; the same instant
    assert datetime.fromisoformat(meta["credential"].pop("expires_at")) == at
    assert meta["credential"] == {
        "phase": "warn",
        "warn_days": 10,
        "refuse_hours": 24,
    }
    app = create_app(make_settings(), None)
    async with client_for(app) as http:
        meta = (await http.get("/api/v1/meta")).json()
    assert meta["credential"] == {
        "expires_at": None,
        "phase": "unknown",
        "warn_days": 10,
        "refuse_hours": 24,
    }
