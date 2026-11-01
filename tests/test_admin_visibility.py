"""Every admin route is classified by what it hides from a superleader, and every scoped route
answers 404 for a target the superleader may not manage."""

from __future__ import annotations

import pytest
from factories import attach_preset
from fakes.fake_kube import FakeKube
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api import admin, admin_relay
from agents_orchestrator.api.admin import ADMIN_VISIBILITY
from agents_orchestrator.api.app import create_app

PRESET_BODY = {"name": "hidden", "spec": {}}
BODIES = {
    ("PUT", "/agent/v1/admin/presets/{ref}"): PRESET_BODY,
    ("PUT", "/agent/v1/admin/presets/{ref}/secrets/{name}"): {"value": "v"},
}


def own_routes() -> list[tuple[str, str]]:
    prefixes = tuple(admin.ADMIN_PREFIX + v[0] for v in admin_relay.SERVICES.values())
    return sorted(
        (m, r.path)
        for r in admin.router.routes
        if not r.path.startswith(prefixes)
        for m in r.methods - {"HEAD", "OPTIONS"}
    )


def test_every_admin_route_is_classified():
    assert set(ADMIN_VISIBILITY) == set(own_routes())
    assert set(ADMIN_VISIBILITY.values()) <= {"preset", "session", "list", "create", "none"}


@pytest.mark.parametrize(
    ("method", "path"), [k for k, v in ADMIN_VISIBILITY.items() if v in ("preset", "session")]
)
async def test_every_scoped_admin_route_404s_for_hidden_target(pool, dsn, method, path):
    app = create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))
    boss = await alone(pool, "vis-boss", superleader=True)
    other = await hive(pool, "vis-other")
    await attach_preset(pool, other.id, manageable=False)
    async with main_client(app) as main:
        hidden = (await main.post("/api/v1/presets", json=PRESET_BODY)).json()
    pod = "x"
    if "/agents/" in path:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT pod_name FROM agents WHERE session_id = %s", (other.id,)
            )
            pod = (await cur.fetchone())[0]
    url = (
        path.replace("{ref}", hidden["id"])
        .replace("{slug}", other.slug)
        .replace("{pod}", pod)
        .replace("{name}", "API_KEY")
    )
    kw = {"json": BODIES[(method, path)]} if (method, path) in BODIES else {}
    async with client_for(app, boss.leader_ip) as http:
        r = await http.request(method, url, headers=headers(boss.id), **kw)
    assert_error(r, 404, "not_found")
