"""O6: every /agent/v1/admin route sits behind require_agent and the superleader gate (D3, D10)."""

from __future__ import annotations

import uuid

import pytest
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api import admin
from agents_orchestrator.api.admin import ADMIN_PREFIX, require_superleader
from agents_orchestrator.api.app import create_app

GET_PRESETS = f"{ADMIN_PREFIX}/presets"


def admin_routes(app):
    """The admin router's routes, checked against what the app really serves (its OpenAPI
    paths), because FastAPI keeps included routers lazy in `app.routes`."""
    routes = list(admin.router.routes)
    served = {p for p in app.openapi()["paths"] if p.startswith(ADMIN_PREFIX)}
    assert served == {r.path for r in routes}
    return routes


def concrete(path: str) -> str:
    out = path
    for part in [p for p in path.split("/") if p.startswith("{")]:
        out = out.replace(part, str(uuid.uuid4()) if "id" in part else "x")
    return out


async def mark_trigger(pool, session_id, service="agents-webhook"):
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET created_by_service = %s WHERE id = %s", (service, session_id)
        )


async def test_every_admin_route_carries_the_gate(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    routes = admin_routes(app)
    assert len(routes) >= 7
    for route in routes:
        calls = [d.call for d in route.dependant.dependencies]
        assert require_superleader in calls, route.path


async def test_gate_table_over_all_admin_routes(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    plain = await alone(pool, "g-plain")
    trigger = await alone(pool, "g-trigger", superleader=True)
    await mark_trigger(pool, trigger.id)
    boss = await alone(pool, "g-boss", superleader=True)
    for route in admin_routes(app):
        for method in route.methods - {"HEAD", "OPTIONS"}:
            path = concrete(route.path)
            async with client_for(app, plain.leader_ip) as http:
                no_auth = await http.request(method, path, json={})
                bad = await http.request(
                    method, path, json={}, headers=headers(plain.id, "wrong-token")
                )
                non_super = await http.request(method, path, json={}, headers=headers(plain.id))
            assert_error(no_auth, 401, "unauthorized")
            assert_error(bad, 401, "unauthorized")
            assert_error(non_super, 403, "not_superleader")
            async with client_for(app, trigger.leader_ip) as http:
                trig = await http.request(method, path, json={}, headers=headers(trigger.id))
            assert trig.status_code not in (401, 403), (method, route.path, trig.text)
            async with client_for(app, boss.leader_ip) as http:
                ok = await http.request(method, path, json={}, headers=headers(boss.id))
            assert ok.status_code not in (401, 403), (method, route.path, ok.text)


@pytest.mark.parametrize(
    "service", ["agents-webhook", "agents-schedule", "agents-matrix-connector"]
)
async def test_flagged_session_is_admitted_whoever_created_it(pool, dsn, service):
    app = create_app(make_settings(dsn), pool)
    s = await hive(pool, f"g-t-{service[-4:]}", superleader=True)
    await mark_trigger(pool, s.id, service)
    async with client_for(app, s.leader_ip) as http:
        r = await http.get(GET_PRESETS, headers=headers(s.id))
    assert r.status_code == 200, r.text


async def test_superleader_reads_presets(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    boss = await alone(pool, "g-ok", superleader=True)
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(GET_PRESETS, headers=headers(boss.id))
    assert r.status_code == 200, r.text
    assert isinstance(r.json(), list)


async def test_flag_does_not_bypass_gate(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    plain = await alone(pool, "g-flag-plain")
    async with client_for(app, plain.leader_ip) as http:
        r = await http.get(GET_PRESETS, headers=headers(plain.id))
    assert_error(r, 403, "not_superleader")
