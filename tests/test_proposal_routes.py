"""Change proposal routes over HTTP (plan 3.3, 4): the agent-port `proposal_router` (proposer
gate, own proposals on visible targets only, `AdminRoute` audit) and the human-only main-port
routes (list, read, review), which neither the agent port nor the service port serves.

Every call that sends a value checks the canary never reaches a response, a table row, an
`events` row or the log."""

from __future__ import annotations

import logging
import secrets as stdlib_secrets

import pytest
from fakes.fake_kube import FakeKube
from pydantic import SecretStr
from test_admin_session_patch import audits
from test_agent_session_messaging import Sess, alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client
from test_proposals_service import MCP_PATCH, TABLES, _rows

from agents_orchestrator.api import proposal_routes
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.k8s import on_start
from agents_orchestrator.k8s import secrets as store

NS = "agents"
P = f"{ADMIN_PREFIX}/proposals"
M = "/api/v1/proposals"
PLAIN_PATCH = {"env": {"PLAIN": {"value": "changed"}}}
AGENT_ROUTES = [
    ("POST", P),
    ("GET", P),
    ("GET", f"{P}/1"),
    ("POST", f"{P}/1/revise"),
    ("POST", f"{P}/1/withdraw"),
]


@pytest.fixture
def settings(dsn):
    return make_settings(dsn).model_copy(update={"docker_image": "docker:27-dind"})


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def app(pool, settings, kube):
    return create_app(settings, pool, kube=kube)


@pytest.fixture
def canary(caplog):
    caplog.set_level(logging.DEBUG)
    value = "canary-" + stdlib_secrets.token_hex(16)
    yield value
    assert value not in caplog.text


@pytest.fixture
async def boss(pool):
    return await alone(pool, "pr-boss", superleader=True)


@pytest.fixture
async def other_boss(pool):
    return await alone(pool, "pr-other", superleader=True)


@pytest.fixture
async def preset(pool, kube, canary):
    """A privileged manageable preset with a protected TOKEN whose value is set (version 1)."""
    spec = {"docker": True, "env": {"TOKEN": {"protected": True}, "PLAIN": {"value": "plain"}}}
    async with pool.connection() as conn:
        found = await presets_repo.create(conn, "infra", spec, superleader_manageable=True)
        await store.set_secret(conn, kube, NS, found.id, "TOKEN", SecretStr(canary + "-live"))
    return found


async def agent(app, who: Sess, method: str, path: str, **kw):
    async with client_for(app, who.leader_ip) as http:
        return await http.request(method, path, headers=headers(who.id), **kw)


async def human(app, method: str, path: str, **kw):
    async with main_client(app) as http:
        return await http.request(method, path, **kw)


def body(**over):
    return {"kind": "preset", "target": "infra", "patch": MCP_PATCH, "reason": "grafana", **over}


async def filed(app, who: Sess, **over) -> dict:
    r = await agent(app, who, "POST", P, json=body(**over))
    assert r.status_code == 201, r.text
    return r.json()


async def hide(pool, preset_id) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (preset_id,)
        )


async def assert_nowhere(pool, value: str, *texts: str) -> None:
    for text in texts:
        assert value not in text
    for table in (*TABLES, "events", "messages"):
        for (row,) in await _rows(pool, f"SELECT row_to_json(t)::text FROM {table} t"):  # noqa: S608
            assert value not in row, table


# ---------------------------------------------------------------- the gate


@pytest.mark.parametrize(("method", "path"), AGENT_ROUTES)
async def test_no_auth_401_and_not_audited(app, pool, boss, method, path):
    async with client_for(app, boss.leader_ip) as http:
        none = await http.request(method, path, json={})
        bad = await http.request(method, path, json={}, headers=headers(boss.id, "wrong"))
    assert_error(none, 401, "unauthorized")
    assert_error(bad, 401, "unauthorized")
    assert await audits(pool, boss.id) == []


@pytest.mark.parametrize(("method", "path"), AGENT_ROUTES)
async def test_non_superleader_403_on_every_agent_route(app, pool, preset, method, path):
    """Stage A admits superleaders only: a regular leader is refused before any lookup."""
    plain = await alone(pool, "pr-plain")
    r = await agent(app, plain, method, path, json=body())
    assert_error(r, 403, "not_superleader")
    assert await audits(pool, plain.id) == []
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


# ---------------------------------------------------------------- create


async def test_create_201_audited_with_fields_and_secret_names_only(
    app, pool, kube, boss, preset, canary, caplog
):
    caplog.set_level(logging.INFO)
    r = await agent(app, boss, "POST", P, json=body(secrets={"TOKEN": canary}))
    assert r.status_code == 201, r.text
    view = r.json()
    assert view["status"] == "open" and view["revision"] == 1
    assert view["proposer_slug"] == boss.slug and view["target_name"] == "infra"
    items = {i["path"]: i for i in view["revisions"][0]["items"]}
    assert set(items) == {"env.TOKEN", "wrapper.mcp_servers"}
    assert items["env.TOKEN"]["secret"] == "changed"
    rows = await audits(pool, boss.id)
    assert len(rows) == 1
    assert rows[0] == {
        "method": "POST",
        "path": P,
        "status": 201,
        "session": boss.slug,
        "secrets": ["TOKEN"],
        "fields": ["env.TOKEN", "wrapper.mcp_servers"],
    }
    lines = [x.getMessage() for x in caplog.records if x.getMessage().startswith("admin ")]
    assert lines == [
        f"admin POST {P} -> 201 session={boss.slug} secrets=['TOKEN']"
        " fields=['env.TOKEN', 'wrapper.mcp_servers']"
    ]
    await assert_nowhere(pool, canary, r.text, caplog.text)


async def test_replay_answers_200_with_the_first_proposal(app, pool, kube, boss, preset, canary):
    first = await filed(app, boss, client_key="k1", secrets={"TOKEN": canary})
    staged = kube.names("Secret")
    r = await agent(app, boss, "POST", P, json=body(client_key="k1", secrets={"TOKEN": canary}))
    assert r.status_code == 200, r.text
    assert r.json()["id"] == first["id"]
    assert kube.names("Secret") == staged  # nothing staged again
    assert [(a["status"], a["secrets"]) for a in await audits(pool, boss.id)] == [
        (201, ["TOKEN"]),
        (200, ["TOKEN"]),
    ]
    await assert_nowhere(pool, canary, r.text)


async def test_hidden_and_unknown_preset_answer_alike(app, pool, boss, preset):
    async with pool.connection() as conn:
        await presets_repo.create(conn, "secret-ops", {}, superleader_manageable=False)
    hidden = await agent(app, boss, "POST", P, json=body(target="secret-ops"))
    unknown = await agent(app, boss, "POST", P, json=body(target="no-such-preset"))
    assert_error(hidden, 404, "not_found")
    assert hidden.json() == unknown.json()
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    assert [a["status"] for a in await audits(pool, boss.id)] == [404, 404]


async def test_refused_patch_is_proposable_and_files_as_a_proposal(app, pool, boss, preset):
    """Q3 A: the PATCH keeps its 403 and marks it proposable; the same change then files."""
    r = await agent(app, boss, "PATCH", f"{ADMIN_PREFIX}/presets/infra", json={"spec": MCP_PATCH})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["proposable"] is True
    view = await filed(app, boss)
    assert [i["path"] for i in view["revisions"][0]["items"]] == ["wrapper.mcp_servers"]


# ---------------------------------------------------------------- own proposals only


async def test_list_and_get_show_own_proposals_only(app, pool, boss, other_boss, preset):
    mine = await filed(app, boss)
    theirs = await filed(app, other_boss, patch=PLAIN_PATCH)
    r = await agent(app, boss, "GET", P)
    assert r.status_code == 200, r.text
    assert [v["id"] for v in r.json()] == [mine["id"]]
    r = await agent(app, boss, "GET", f"{P}/{mine['id']}")
    assert r.status_code == 200 and r.json()["id"] == mine["id"]
    assert [a["status"] for a in await audits(pool, boss.id)] == [201, 200, 200]
    unknown = await agent(app, boss, "GET", f"{P}/999999")
    for method, suffix, kw in [
        ("GET", "", {}),
        ("POST", "/revise", {"json": {"patch": PLAIN_PATCH}}),
        ("POST", "/withdraw", {}),
    ]:
        r = await agent(app, boss, method, f"{P}/{theirs['id']}{suffix}", **kw)
        assert_error(r, 404, "not_found")
        assert r.json() == unknown.json(), suffix
    r = await human(app, "GET", f"{M}/{theirs['id']}")
    assert r.json()["status"] == "open" and r.json()["revision"] == 1


async def test_status_filter_and_bad_status(app, pool, boss, preset):
    view = await filed(app, boss)
    r = await agent(app, boss, "POST", f"{P}/{view['id']}/withdraw")
    assert r.status_code == 200, r.text
    assert (await agent(app, boss, "GET", P)).json() == []
    closed = (await agent(app, boss, "GET", P, params={"status": "closed"})).json()
    assert [v["id"] for v in closed] == [view["id"]]
    r = await agent(app, boss, "GET", P, params={"status": "bogus"})
    assert_error(r, 422, "invalid_request")
    assert (await audits(pool, boss.id))[-1]["status"] == 422


async def test_hidden_target_answers_404_like_unknown(app, pool, kube, boss, preset, canary):
    """A target the superleader may no longer manage hides its proposals on every agent route."""
    view = await filed(app, boss, secrets={"TOKEN": canary})
    await hide(pool, preset.id)
    unknown = await agent(app, boss, "GET", f"{P}/999999")
    for method, suffix, kw in [
        ("GET", "", {}),
        ("POST", "/revise", {"json": {"patch": PLAIN_PATCH}}),
        ("POST", "/withdraw", {}),
    ]:
        r = await agent(app, boss, method, f"{P}/{view['id']}{suffix}", **kw)
        assert_error(r, 404, "not_found")
        assert r.json() == unknown.json(), suffix
    assert (await agent(app, boss, "GET", P)).json() == []
    r = await human(app, "GET", f"{M}/{view['id']}")
    assert r.json()["status"] == "open" and r.json()["revision"] == 1
    reserved = await _rows(pool, "SELECT state FROM change_proposal_secrets")
    assert reserved == [("staged",)]


async def test_deleted_target_stays_readable(app, pool, boss, preset):
    view = await filed(app, boss)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset.id,))
    r = await agent(app, boss, "GET", f"{P}/{view['id']}")
    assert r.status_code == 200, r.text
    assert r.json()["stale"] is True
    assert [v["id"] for v in (await agent(app, boss, "GET", P)).json()] == [view["id"]]


# ---------------------------------------------------------------- revise, withdraw


async def test_revise_and_withdraw_audited(app, pool, kube, boss, preset, canary, caplog):
    caplog.set_level(logging.INFO)
    view = await filed(app, boss)
    r = await agent(
        app,
        boss,
        "POST",
        f"{P}/{view['id']}/revise",
        json={"patch": PLAIN_PATCH, "secrets": {"TOKEN": canary}},
    )
    assert r.status_code == 200, r.text
    assert r.json()["revision"] == 2 and r.json()["status"] == "open"
    w = await agent(app, boss, "POST", f"{P}/{view['id']}/withdraw")
    assert w.status_code == 200 and w.json()["status"] == "withdrawn"
    again = await agent(app, boss, "POST", f"{P}/{view['id']}/withdraw")
    assert_error(again, 409, "not_open")
    rows = await audits(pool, boss.id)
    assert [(a["status"], a["secrets"], a.get("fields")) for a in rows] == [
        (201, [], ["wrapper.mcp_servers"]),
        (200, ["TOKEN"], ["env.PLAIN", "env.TOKEN"]),
        (200, [], None),
        (409, [], None),
    ]
    assert rows[1]["path"] == f"{P}/{view['id']}/revise"
    assert await _rows(pool, "SELECT name FROM change_proposal_secrets") == []
    await assert_nowhere(pool, canary, r.text, caplog.text)


# ---------------------------------------------------------------- main port, human only


async def test_main_port_lists_reads_and_reviews_without_agent_auth(
    app, pool, kube, boss, other_boss, preset, canary
):
    mine = await filed(app, boss, secrets={"TOKEN": canary})
    theirs = await filed(app, other_boss, patch=PLAIN_PATCH)
    r = await human(app, "GET", M)
    assert r.status_code == 200, r.text
    assert [v["id"] for v in r.json()] == [theirs["id"], mine["id"]]
    r = await human(app, "GET", f"{M}/{mine['id']}")
    assert r.status_code == 200 and r.json()["proposer_slug"] == boss.slug
    items = [{"path": i["path"], "decision": "approve"} for i in r.json()["revisions"][0]["items"]]
    r = await human(app, "POST", f"{M}/{mine['id']}/review", json={"revision": 1, "items": items})
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "applied"
    async with pool.connection() as conn:
        found = await presets_repo.get(conn, preset.id)
    assert found is not None and "grafana" in found.spec["wrapper"]["mcp_servers"]
    r = await human(app, "POST", f"{M}/{mine['id']}/review", json={"revision": 1, "items": items})
    assert_error(r, 409, "not_open")
    assert_error(await human(app, "GET", f"{M}/999999"), 404, "not_found")
    stale = (await human(app, "GET", f"{M}/{theirs['id']}")).json()
    assert stale["status"] == "stale"  # the apply changed the target under it
    r = await human(app, "GET", M, params={"status": "bogus"})
    assert_error(r, 422, "invalid_request")
    await assert_nowhere(pool, canary, r.text)


async def test_main_port_routes_are_not_on_the_agent_port(app, pool, boss, preset):
    view = await filed(app, boss)
    review = {"revision": 1, "items": [{"path": "wrapper.mcp_servers", "decision": "approve"}]}
    for method, path, kw in [
        ("GET", M, {}),
        ("GET", f"{M}/{view['id']}", {}),
        ("POST", f"{M}/{view['id']}/review", {"json": review}),
    ]:
        r = await agent(app, boss, method, path, **kw)
        assert_error(r, 404, "not_found")
    assert (await human(app, "GET", f"{M}/{view['id']}")).json()["status"] == "open"


async def test_agent_port_has_no_review_route(app, pool, boss, preset):
    """Review is human only: no agent-port route, no admin relay path and no MCP tool."""
    view = await filed(app, boss)
    review = {"revision": 1, "items": [{"path": "wrapper.mcp_servers", "decision": "approve"}]}
    r = await agent(app, boss, "POST", f"{P}/{view['id']}/review", json=review)
    assert r.status_code in (404, 405), r.text
    assert (await human(app, "GET", f"{M}/{view['id']}")).json()["status"] == "open"
    agent_paths = [p for p in app.openapi()["paths"] if p.startswith("/agent/v1")]
    assert [p for p in agent_paths if "review" in p.split("/")] == []
    assert [p for p in agent_paths if "proposals" in p] == [
        P,
        f"{P}/{{proposal_id}}",
        f"{P}/{{proposal_id}}/revise",
        f"{P}/{{proposal_id}}/withdraw",
    ]
    tools = [*on_start.ADMIN_TOOLS, *on_start.SESSION_EDIT_TOOLS]
    assert [t for t in tools if "review" in t.split("_")] == []


def test_review_route_only_on_the_main_router():
    main = {(m, r.path) for r in proposal_routes.main_router.routes for m in r.methods}
    assert main == {
        ("GET", M),
        ("GET", f"{M}/{{proposal_id}}"),
        ("POST", f"{M}/{{proposal_id}}/review"),
    }
    agent_side = [r.path for r in proposal_routes.proposal_router.routes]
    assert not [p for p in agent_side if "review" in p.split("/")]
