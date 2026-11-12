"""Every admin route is classified by what it withholds from a superleader. Decision 388: a scoped
route reads any target (200) and its writes answer 403 `not_manageable` for one the superleader
may not manage; the proposal reads (list, get) show every proposal, revise and withdraw answer 404
for another session's proposal and for one whose preset the superleader may not manage, and a
create on such a target is 403."""

from __future__ import annotations

import pytest
from factories import attach_preset
from fakes.fake_kube import FakeKube
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api import admin, admin_relay, proposal_routes
from agents_orchestrator.api.admin import ADMIN_VISIBILITY
from agents_orchestrator.api.app import create_app

PRESET_BODY = {"name": "hidden", "spec": {}}
BODIES = {
    ("PUT", "/agent/v1/admin/presets/{ref}"): PRESET_BODY,
    ("PATCH", "/agent/v1/admin/presets/{ref}"): {},
    ("PATCH", "/agent/v1/admin/sessions/{slug}"): {"spec": {}},
    ("POST", "/agent/v1/admin/sessions/{slug}/leadership/withdraw"): {},
    ("PUT", "/agent/v1/admin/presets/{ref}/vault/{entry_id}"): {},
    ("POST", "/agent/v1/admin/sessions/{slug}/projects"): {"project": "default", "reason": "r"},
    ("POST", "/agent/v1/admin/presets/{ref}/projects"): {"project": "default", "reason": "r"},
}


def own_routes() -> list[tuple[str, str]]:
    prefixes = tuple(admin.ADMIN_PREFIX + v[0] + "/" for v in admin_relay.SERVICES.values())
    return sorted(
        (m, r.path)
        for r in [
            *admin.router.routes,
            *admin.session_router.routes,
            *proposal_routes.proposal_router.routes,
        ]
        if not r.path.startswith(prefixes)
        for m in r.methods - {"HEAD", "OPTIONS"}
    )


def test_every_admin_route_is_classified():
    assert set(ADMIN_VISIBILITY) == set(own_routes())
    assert set(ADMIN_VISIBILITY.values()) <= {
        "preset",
        "session",
        "list",
        "create",
        "none",
        "proposal",
    }
    proposal = {k for k, v in ADMIN_VISIBILITY.items() if v == "proposal"}
    assert proposal == {
        (m, r.path) for r in proposal_routes.proposal_router.routes for m in r.methods
    }


@pytest.mark.parametrize(
    ("method", "path"), [k for k, v in ADMIN_VISIBILITY.items() if v in ("preset", "session")]
)
async def test_every_scoped_admin_route_reads_open_and_refuses_writes_on_unmanageable_target(
    pool, dsn, method, path
):
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
        .replace("{entry_id}", "11111111-1111-4111-8111-111111111111")
    )
    kw = {"json": BODIES[(method, path)]} if (method, path) in BODIES else {}
    async with client_for(app, boss.leader_ip) as http:
        r = await http.request(method, url, headers=headers(boss.id), **kw)
    if method == "GET":
        assert r.status_code == 200, r.text
    else:
        assert_error(r, 403, "not_manageable")


PROPOSAL_BODIES = {
    ("POST", "/agent/v1/admin/proposals"): {
        "kind": "preset",
        "target": "{ref}",
        "patch": {"env": {"PLAIN": {"value": "changed"}}},
        "reason": "r",
    },
    ("POST", "/agent/v1/admin/proposals/{proposal_id}/revise"): {
        "patch": {"env": {"PLAIN": {"value": "again"}}}
    },
}


@pytest.mark.parametrize("hidden_by", ["another_session", "unmanageable_target"])
@pytest.mark.parametrize(
    ("method", "path"), [k for k, v in ADMIN_VISIBILITY.items() if v == "proposal"]
)
async def test_every_proposal_route_hides_what_the_caller_may_not_see(
    pool, dsn, method, path, hidden_by
):
    """Another session's proposal, or one on a preset the superleader may not manage (any longer):
    reads (list, get) show it, revise and withdraw answer 404, create on it is 403."""
    app = create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))
    boss = await alone(pool, "vis-p-boss", superleader=True)
    filer = await alone(pool, "vis-p-filer", superleader=True)
    async with main_client(app) as main:
        target = (
            await main.post(
                "/api/v1/presets",
                json={
                    "name": "shared",
                    "spec": {"env": {"PLAIN": {"value": "plain"}}},
                    "superleader_manageable": True,
                },
            )
        ).json()
    who = filer if hidden_by == "another_session" else boss
    create = dict(PROPOSAL_BODIES[("POST", "/agent/v1/admin/proposals")], target=target["id"])
    async with client_for(app, who.leader_ip) as http:
        r = await http.post("/agent/v1/admin/proposals", json=create, headers=headers(who.id))
    assert r.status_code == 201, r.text
    proposal_id = r.json()["id"]
    if hidden_by == "unmanageable_target":
        async with main_client(app) as main:
            r = await main.put(
                f"/api/v1/presets/{target['id']}",
                json={
                    "name": "shared",
                    "spec": {"env": {"PLAIN": {"value": "plain"}}},
                    "superleader_manageable": False,
                },
            )
        assert r.status_code == 200, r.text
    url = path.replace("{proposal_id}", str(proposal_id))
    kw = {}
    if (method, path) in PROPOSAL_BODIES:
        kw["json"] = dict(PROPOSAL_BODIES[(method, path)])
        if "target" in kw["json"]:
            kw["json"]["target"] = target["id"]
    async with client_for(app, boss.leader_ip) as http:
        r = await http.request(method, url, headers=headers(boss.id), **kw)
    if (method, path) == ("GET", "/agent/v1/admin/proposals"):
        assert r.status_code == 200, r.text  # decision 388: a superleader reads every proposal
        assert [v["id"] for v in r.json()] == [proposal_id]
    elif (method, path) == ("GET", "/agent/v1/admin/proposals/{proposal_id}"):
        assert r.status_code == 200, r.text
        assert r.json()["id"] == proposal_id
    elif (method, path) == ("POST", "/agent/v1/admin/proposals") and hidden_by == "another_session":
        assert r.status_code == 201, r.text  # a manageable target: the caller files its own
    elif (method, path) == ("POST", "/agent/v1/admin/proposals"):
        assert_error(r, 403, "not_manageable")  # decision 388: a write on an unmanageable preset
    else:
        assert_error(r, 404, "not_found")
