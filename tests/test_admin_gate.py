"""O6: every /agent/v1/admin route sits behind require_agent and a gate (D3, D10): the
superleader gate, or for the two session spec routes (GET and PATCH /sessions/{slug}) the
session editor gate, which admits a superleader on any slug and any other session's leader on
its own slug only; the proposal routes carry the proposer gate, which admits a superleader and
any other session's leader (the handlers keep a regular leader on its own session)."""

from __future__ import annotations

import uuid

import pytest
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api import admin, proposal_routes
from agents_orchestrator.api.admin import (
    ADMIN_PREFIX,
    require_session_editor,
    require_superleader,
)
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.proposal_routes import require_proposer

GET_PRESETS = f"{ADMIN_PREFIX}/presets"


GATES = {
    id(admin.router): require_superleader,
    id(admin.session_router): require_session_editor,
    id(proposal_routes.proposal_router): require_proposer,
}
ROUTERS = (admin.router, admin.session_router, proposal_routes.proposal_router)
EDITOR_ROUTES = {
    ("GET", f"{ADMIN_PREFIX}/sessions/{{slug}}"),
    ("PATCH", f"{ADMIN_PREFIX}/sessions/{{slug}}"),
}
PROPOSER_ROUTES = {
    ("POST", f"{ADMIN_PREFIX}/proposals"),
    ("GET", f"{ADMIN_PREFIX}/proposals"),
    ("GET", f"{ADMIN_PREFIX}/proposals/{{proposal_id}}"),
    ("POST", f"{ADMIN_PREFIX}/proposals/{{proposal_id}}/revise"),
    ("POST", f"{ADMIN_PREFIX}/proposals/{{proposal_id}}/withdraw"),
}


def admin_routes(app):
    """(router, route) of every admin-prefix router, checked against what the app really serves
    (its OpenAPI operations), because FastAPI keeps included routers lazy in `app.routes`."""
    routes = [(r, route) for r in ROUTERS for route in r.routes]
    served = {
        (method.upper(), path)
        for path, ops in app.openapi()["paths"].items()
        if path.startswith(ADMIN_PREFIX)
        for method in ops
    }
    assert served == {
        (m, route.path) for _, route in routes for m in route.methods - {"HEAD", "OPTIONS"}
    }
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


async def test_every_admin_route_carries_its_gate(pool, dsn):
    """Each route carries exactly the gate of its router: the session editor gate on the two
    session spec routes, the proposer gate on the proposal routes, the superleader gate on every
    other."""
    app = create_app(make_settings(dsn), pool)
    routes = admin_routes(app)
    assert len(routes) >= 7
    editor = set()
    for router, route in routes:
        calls = [d.call for d in route.dependant.dependencies]
        gate = GATES[id(router)]
        others = set(GATES.values()) - {gate}
        assert gate in calls and not others & set(calls), route.path
        if gate is require_session_editor:
            editor.update((m, route.path) for m in route.methods - {"HEAD", "OPTIONS"})
    assert editor == EDITOR_ROUTES
    proposer = {
        (m, route.path)
        for router, route in routes
        if router is proposal_routes.proposal_router
        for m in route.methods
    }
    assert proposer == PROPOSER_ROUTES


async def test_editor_gate_is_per_route_and_slug_stays_a_path_parameter(pool, dsn):
    """The editor gate reads `slug`: it sits on the routes that have {slug}, never on the router,
    so a route added without one cannot turn it into a required query parameter."""
    assert admin.session_router.dependencies == []
    for route in admin.session_router.routes:
        assert "{slug}" in route.path, route.path
    app = create_app(make_settings(dsn), pool)
    for path, ops in app.openapi()["paths"].items():
        if not path.startswith(ADMIN_PREFIX):
            continue
        for method, op in ops.items():
            where = {p["name"]: p["in"] for p in op.get("parameters", [])}
            assert where.get("slug", "path") == "path", (method, path)
            assert ("slug" in where) == ("{slug}" in path), (method, path)


async def test_gate_table_over_all_admin_routes(pool, dsn):
    """A regular leader passes only the proposer gate; another agent of a regular hive is
    refused there 403 not_leader."""
    app = create_app(make_settings(dsn), pool)
    plain = await alone(pool, "g-plain")
    member = await hive(pool, "g-member")
    trigger = await alone(pool, "g-trigger", superleader=True)
    await mark_trigger(pool, trigger.id)
    boss = await alone(pool, "g-boss", superleader=True)
    for router, route in admin_routes(app):
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
            if router is proposal_routes.proposal_router:
                assert non_super.status_code not in (401, 403), (method, route.path)
                async with client_for(app, member.member_ip) as http:
                    r = await http.request(method, path, json={}, headers=headers(member.id))
                assert_error(r, 403, "not_leader")
            else:
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
