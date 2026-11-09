"""Change proposal routes over HTTP (plan 3.3, 4): the agent-port `proposal_router` (proposer
gate, own proposals on visible targets only, `AdminRoute` audit) and the human-only main-port
routes (list, read, review), which neither the agent port nor the service port serves.

Every call that sends a value checks the canary never reaches a response, a table row, an
`events` row or the log."""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets
import uuid

import pytest
from fakes.fake_kube import FakeKube
from pydantic import SecretStr
from test_admin_session_patch import audits
from test_agent_session_messaging import Sess, alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client
from test_proposal_sweep import _advisory_held
from test_proposals_service import MCP_PATCH, TABLES, _rows

from agents_orchestrator.api import proposal_routes, proposals, secret_store
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
        await store.set_secret(
            conn, kube, NS, store.Owner.preset(found.id), "TOKEN", SecretStr(canary + "-live")
        )
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
async def test_non_leader_of_a_regular_hive_403_on_every_agent_route(
    app, pool, preset, method, path
):
    """Q10 B: a regular session passes the gate only through its leader; another agent of it is
    refused before any lookup and is not audited."""
    plain = await hive(pool, "pr-plain")
    async with client_for(app, plain.member_ip) as http:
        r = await http.request(method, path, json=body(), headers=headers(plain.id))
    assert_error(r, 403, "not_leader")
    assert await audits(pool, plain.id) == []
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_non_leader_agent_of_a_superleader_passes(app, pool, preset):
    boss = await hive(pool, "pr-boss-hive", superleader=True)
    async with client_for(app, boss.member_ip) as http:
        r = await http.request("POST", P, json=body(), headers=headers(boss.id))
    assert r.status_code == 201, r.text
    assert r.json()["proposer_superleader"] is True


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


async def _staged_without_secret(pool, kube) -> list:
    """Staged reservations whose Secret is gone: an approve would make such a version live."""
    rows = await _rows(
        pool,
        "SELECT preset_id, name, version FROM change_proposal_secrets WHERE state = 'staged'",
    )
    present = {k[2] for k in kube.objects if k[0] == "Secret"}
    return [r for r in rows if store.secret_name(store.Owner.preset(r[0]), *r[1:]) not in present]


def _release_after_commit(pool, preset_id, seen: list):
    """Wrap `release`: record what a second connection sees (the committed proposal state) and
    whether any session still holds the variable's lock when the release starts."""
    real = secret_store.release

    async def spy(conn, kube_, namespace, *, proposal_id, revision=None):
        [state] = await _rows(
            pool, "SELECT revision, status FROM change_proposals WHERE id = %s", (proposal_id,)
        )
        seen.append((*state, await _advisory_held(pool, preset_id, "TOKEN")))
        return await real(conn, kube_, namespace, proposal_id=proposal_id, revision=revision)

    return spy


async def _broken_view(*args, **kwargs):
    raise RuntimeError("connection lost")


async def test_revise_failing_after_the_release_leaves_a_consistent_proposal(
    app, pool, kube, boss, preset, canary, monkeypatch
):
    """Important 1 (final review A-O): the route reads first (`_own`), and the old revision's
    staged Secret is released only once the new revision has committed and its locks are gone.
    A request that then fails leaves r2 current with its own Secret, r1 released."""
    view = await filed(app, boss, secrets={"TOKEN": canary})
    r1, r2 = (store.secret_name(store.Owner.preset(preset.id), "TOKEN", v) for v in (2, 3))
    seen: list = []
    monkeypatch.setattr(secret_store, "release", _release_after_commit(pool, preset.id, seen))
    monkeypatch.setattr(proposals, "get_view", _broken_view)
    try:
        r = await agent(
            app,
            boss,
            "POST",
            f"{P}/{view['id']}/revise",
            json={"patch": MCP_PATCH, "secrets": {"TOKEN": canary + "2"}},
        )
        assert r.status_code == 500
    except RuntimeError:
        pass  # the transport re-raises the app error
    assert seen == [(2, "open", 0)]  # committed, no lock held across the release
    assert await _rows(
        pool, "SELECT revision, status FROM change_proposals WHERE id = %s", (view["id"],)
    ) == [(2, "open")]
    secrets = {k[2] for k in kube.objects if k[0] == "Secret"}
    assert r1 not in secrets and r2 in secrets
    assert await _staged_without_secret(pool, kube) == []
    await assert_nowhere(pool, canary)


async def test_revise_failing_before_the_commit_keeps_the_previous_staged_secret(
    app, pool, kube, boss, preset, canary, monkeypatch
):
    """A revise that fails inside its transaction (after staging) rolls back to r1 with r1's
    staged Secret intact; r2's staged Secret is deleted again and nothing is released."""
    view = await filed(app, boss, secrets={"TOKEN": canary})
    r1, r2 = (store.secret_name(store.Owner.preset(preset.id), "TOKEN", v) for v in (2, 3))
    real_stage = proposals._stage

    async def stage_then_fail(*args, **kwargs):
        await real_stage(*args, **kwargs)
        raise RuntimeError("connection lost")

    released: list = []
    monkeypatch.setattr(proposals, "_stage", stage_then_fail)
    monkeypatch.setattr(secret_store, "release", _release_after_commit(pool, preset.id, released))
    try:
        r = await agent(
            app,
            boss,
            "POST",
            f"{P}/{view['id']}/revise",
            json={"patch": MCP_PATCH, "secrets": {"TOKEN": canary + "2"}},
        )
        assert r.status_code == 500
    except RuntimeError:
        pass
    assert released == []
    assert await _rows(
        pool, "SELECT revision, status FROM change_proposals WHERE id = %s", (view["id"],)
    ) == [(1, "open")]
    secrets = {k[2] for k in kube.objects if k[0] == "Secret"}
    assert r1 in secrets and r2 not in secrets
    assert await _rows(pool, "SELECT version, revision, state FROM change_proposal_secrets") == [
        (2, 1, "staged")
    ]
    assert await _staged_without_secret(pool, kube) == []


async def test_withdraw_releases_only_after_the_commit(
    app, pool, kube, boss, preset, canary, monkeypatch
):
    """Withdraw commits `withdrawn` before any staged Secret goes: a request that fails after the
    release leaves a withdrawn proposal, never an open one whose staged Secret is gone."""
    view = await filed(app, boss, secrets={"TOKEN": canary})
    staged = store.secret_name(store.Owner.preset(preset.id), "TOKEN", 2)
    seen: list = []
    monkeypatch.setattr(secret_store, "release", _release_after_commit(pool, preset.id, seen))
    monkeypatch.setattr(proposals, "get_view", _broken_view)
    try:
        r = await agent(app, boss, "POST", f"{P}/{view['id']}/withdraw")
        assert r.status_code == 500
    except RuntimeError:
        pass
    assert seen == [(1, "withdrawn", 0)]
    assert staged not in {k[2] for k in kube.objects if k[0] == "Secret"}
    assert await _rows(pool, "SELECT name FROM change_proposal_secrets") == []


async def test_withdraw_that_fails_leaves_the_staged_secret(
    app, pool, kube, boss, preset, canary, monkeypatch
):
    """A withdraw whose transaction fails releases nothing: still open, Secret and row intact."""
    view = await filed(app, boss, secrets={"TOKEN": canary})
    staged = store.secret_name(store.Owner.preset(preset.id), "TOKEN", 2)

    async def broken_transition(*args, **kwargs):
        raise RuntimeError("connection lost")

    released: list = []
    monkeypatch.setattr(proposals.repo, "transition", broken_transition)
    monkeypatch.setattr(secret_store, "release", _release_after_commit(pool, preset.id, released))
    try:
        r = await agent(app, boss, "POST", f"{P}/{view['id']}/withdraw")
        assert r.status_code == 500
    except RuntimeError:
        pass
    assert released == []
    assert await _rows(
        pool, "SELECT revision, status FROM change_proposals WHERE id = %s", (view["id"],)
    ) == [(1, "open")]
    assert staged in {k[2] for k in kube.objects if k[0] == "Secret"}
    assert await _rows(pool, "SELECT version, state FROM change_proposal_secrets") == [
        (2, "staged")
    ]


async def _advisory_waiters(pool, count: int) -> None:
    """Return once `count` backends of this database wait on an advisory lock (no sleeps: each
    poll is a query)."""

    async def poll() -> None:
        async with pool.connection() as conn:
            while True:
                cur = await conn.execute(
                    "SELECT count(*) FROM pg_locks l JOIN pg_database d ON d.oid = l.database"
                    " WHERE d.datname = current_database() AND l.locktype = 'advisory'"
                    " AND NOT l.granted"
                )
                row = await cur.fetchone()
                await conn.rollback()
                if row is not None and row[0] >= count:
                    return

    await asyncio.wait_for(poll(), timeout=10)


async def test_concurrent_stagings_on_one_preset_do_not_deadlock(
    app, pool, kube, boss, other_boss, preset, canary
):
    """Important 1(b) (final review A-O): a revise staging B whose previous revision staged A,
    and a create staging A then B on the same preset. A blocker holds B so both queue on it
    (revise first, pg_locks says when) while the create holds A. Once B is free the revise stages
    B and commits before it releases A, so it never waits on A while holding B."""
    env = {"A": {"protected": True}, "B": {"protected": True}}
    first = await filed(app, boss, patch={"env": env}, secrets={"A": canary})
    old_a = store.secret_name(store.Owner.preset(preset.id), "A", 1)
    async with pool.connection() as blocker, blocker.transaction():
        await blocker.execute(
            "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))", (f"{preset.id}/B",)
        )
        revise = asyncio.create_task(
            agent(
                app,
                boss,
                "POST",
                f"{P}/{first['id']}/revise",
                json={"patch": {"env": env}, "secrets": {"B": canary}},
            )
        )
        await _advisory_waiters(pool, 1)
        create = asyncio.create_task(
            agent(
                app,
                other_boss,
                "POST",
                P,
                json=body(patch={"env": env}, secrets={"A": canary, "B": canary}),
            )
        )
        await _advisory_waiters(pool, 2)
    revised, created = await asyncio.wait_for(asyncio.gather(revise, create), timeout=30)
    assert revised.status_code == 200, revised.text
    assert created.status_code == 201, created.text
    assert await _rows(
        pool,
        "SELECT name, version, proposal_id, revision FROM change_proposal_secrets"
        " WHERE state = 'staged' ORDER BY name, version",
    ) == [
        ("A", 2, created.json()["id"], 1),
        ("B", 1, first["id"], 2),
        ("B", 2, created.json()["id"], 1),
    ]
    assert old_a not in {k[2] for k in kube.objects if k[0] == "Secret"}
    assert await _staged_without_secret(pool, kube) == []


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
    assert_error(r, 404, "not_found")
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


async def test_list_page_straddling_hidden_rows_is_full(app, pool, kube, boss, preset):
    async with pool.connection() as conn:
        other = await presets_repo.create(conn, "pr-other-target", {}, superleader_manageable=True)
    first = await filed(app, boss)
    second = await filed(app, boss, target="pr-other-target")
    await hide(pool, other.id)
    third = await filed(app, boss)
    r = await agent(app, boss, "GET", P, params={"limit": 2})
    assert [v["id"] for v in r.json()] == [third["id"], first["id"]]
    assert second["id"] not in [v["id"] for v in r.json()]


async def test_revise_on_target_hidden_mid_call_answers_proposal_404(
    app, pool, kube, boss, preset, monkeypatch
):
    """The target turns hidden after the route pre-check: still the unknown-proposal body."""
    view = await filed(app, boss)
    unknown = await agent(app, boss, "GET", f"{P}/999999")
    monkeypatch.setattr(proposal_routes, "_own", _hide_then_pass(pool, preset.id))
    r = await agent(app, boss, "POST", f"{P}/{view['id']}/revise", json={"patch": PLAIN_PATCH})
    assert_error(r, 404, "not_found")
    assert r.json() == unknown.json()


def _hide_then_pass(pool, preset_id):
    async def own(conn, proposal_id, caller):
        await hide(pool, preset_id)

    return own


# ---------------------------------------------------------------- session targets


async def _session_of(pool, slug: str, preset_id) -> Sess:
    sess = await alone(pool, slug)
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET preset_id = %s WHERE id = %s", (preset_id, sess.id))
    return sess


async def test_session_proposal_hidden_when_its_preset_turns_unmanageable(app, pool, boss, preset):
    """A foreign session's proposal follows `admin._target`: once the session's preset is no
    longer manageable, get, list, revise and withdraw answer like an unknown id."""
    target = await _session_of(pool, "pr-target", preset.id)
    view = await filed(app, boss, kind="session", target=target.slug, patch=PLAIN_PATCH)
    await hide(pool, preset.id)
    unknown = await agent(app, boss, "GET", f"{P}/999999")
    for method, suffix, kw in [
        ("GET", "", {}),
        ("POST", "/revise", {"json": {"patch": MCP_PATCH}}),
        ("POST", "/withdraw", {}),
    ]:
        r = await agent(app, boss, method, f"{P}/{view['id']}{suffix}", **kw)
        assert_error(r, 404, "not_found")
        assert r.json() == unknown.json(), suffix
    assert (await agent(app, boss, "GET", P)).json() == []
    r = await human(app, "GET", f"{M}/{view['id']}")
    assert r.json()["status"] == "open" and r.json()["revision"] == 1


async def test_session_proposal_hidden_when_its_preset_is_deleted(app, pool, boss, preset):
    """A deleted preset leaves the session with no preset: `admin._target` hides it, so does
    the listing (unlike a deleted preset target, which stays readable)."""
    target = await _session_of(pool, "pr-target", preset.id)
    view = await filed(app, boss, kind="session", target=target.slug, patch=PLAIN_PATCH)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset.id,))
    r = await agent(app, boss, "GET", f"{P}/{view['id']}")
    assert_error(r, 404, "not_found")
    assert (await agent(app, boss, "GET", P)).json() == []


async def test_own_session_proposal_stays_visible_on_an_unmanageable_preset(
    app, pool, boss, preset
):
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET preset_id = %s WHERE id = %s", (preset.id, boss.id))
    view = await filed(app, boss, kind="session", target=boss.slug, patch=MCP_PATCH)
    await hide(pool, preset.id)
    r = await agent(app, boss, "GET", f"{P}/{view['id']}")
    assert r.status_code == 200, r.text
    assert [v["id"] for v in (await agent(app, boss, "GET", P)).json()] == [view["id"]]
    r = await agent(app, boss, "POST", f"{P}/{view['id']}/revise", json={"patch": PLAIN_PATCH})
    assert r.status_code == 200, r.text


async def test_purged_session_proposal_stays_readable(app, pool, boss, preset):
    target = await _session_of(pool, "pr-target", preset.id)
    view = await filed(app, boss, kind="session", target=target.slug, patch=PLAIN_PATCH)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (target.id,))
    r = await agent(app, boss, "GET", f"{P}/{view['id']}")
    assert r.status_code == 200, r.text
    assert [v["id"] for v in (await agent(app, boss, "GET", P)).json()] == [view["id"]]


# ---------------------------------------------------------------- regular leaders (Q10 B)


async def _leader_of(pool, slug: str, preset_id, *, hive_session: bool = False) -> Sess:
    sess = await (hive(pool, slug) if hive_session else alone(pool, slug))
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET preset_id = %s WHERE id = %s", (preset_id, sess.id))
    return sess


def own(sess: Sess, **over) -> dict:
    return body(**{"kind": "session", "target": sess.slug, "patch": PLAIN_ENV, **over})


PLAIN_ENV = {"env": {"PLAIN": {"value": "x"}}}


async def test_regular_leader_proposes_a_grant_and_a_value_for_its_own_session(
    app, pool, kube, preset, canary, caplog
):
    caplog.set_level(logging.INFO)
    lead = await _leader_of(pool, "pr-lead", preset.id)
    patch = {"session_messaging": True, "env": {"TOKEN": {"protected": True}}}
    q = "SELECT spec, generation, secret_versions FROM sessions WHERE id = %s"
    before = await _rows(pool, q, (lead.id,))
    r = await agent(app, lead, "POST", P, json=own(lead, patch=patch, secrets={"TOKEN": canary}))
    assert r.status_code == 201, r.text
    view = r.json()
    assert (view["kind"], view["target_id"], view["target_name"]) == (
        "session",
        str(lead.id),
        lead.slug,
    )
    assert view["proposer_superleader"] is False
    items = {i["path"]: i for i in view["revisions"][0]["items"]}
    assert sorted(items) == ["env.TOKEN", "session_messaging"]
    assert (items["session_messaging"]["grant"], items["env.TOKEN"]["secret"]) == (True, "set")
    assert store.secret_name(store.Owner.preset(preset.id), "TOKEN", 2) in kube.names("Secret")
    after = await _rows(pool, q, (lead.id,))
    assert "session_messaging" not in after[0][0]  # filing applies nothing
    assert after == before
    [row] = await audits(pool, lead.id)
    assert (row["status"], row["secrets"], row["fields"]) == (
        201,
        ["TOKEN"],
        ["env.TOKEN", "session_messaging"],
    )
    await assert_nowhere(pool, canary, r.text, caplog.text)


async def test_regular_leader_off_its_own_session_is_403_alike(app, pool, kube, preset, canary):
    """Another session (hidden or visible), an unknown slug, a preset or a schedule: one 403
    body, before any lookup, nothing staged or stored (no existence leak)."""
    lead = await _leader_of(pool, "pr-lead", preset.id)
    other = await _leader_of(pool, "pr-other-lead", preset.id)
    staged = kube.names("Secret")
    bodies = [
        body(kind="session", target=other.slug, patch=PLAIN_ENV, secrets={"TOKEN": canary}),
        body(kind="session", target="no-such-session", patch=PLAIN_ENV),
        body(kind="preset", target="infra", patch=PLAIN_ENV, secrets={"TOKEN": canary}),
        body(kind="preset", target="no-such-preset", patch=PLAIN_ENV),
        body(kind="schedule", target=str(uuid.uuid4()), patch={"prompt": "x"}),
    ]
    answers = [await agent(app, lead, "POST", P, json=b) for b in bodies]
    for r in answers:
        assert_error(r, 403, "not_superleader")
        assert r.json() == answers[0].json()
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    assert kube.names("Secret") == staged
    await assert_nowhere(pool, canary, *(r.text for r in answers))


async def test_regular_leader_reads_revises_and_withdraws_only_its_own(
    app, pool, kube, boss, preset, canary
):
    lead = await _leader_of(pool, "pr-lead", preset.id)
    mine = (await agent(app, lead, "POST", P, json=own(lead))).json()
    theirs = await filed(app, boss)
    assert [v["id"] for v in (await agent(app, lead, "GET", P)).json()] == [mine["id"]]
    r = await agent(app, lead, "GET", f"{P}/{mine['id']}")
    assert r.status_code == 200 and r.json()["proposer_superleader"] is False
    unknown = await agent(app, lead, "GET", f"{P}/999999")
    for method, suffix, kw in [
        ("GET", "", {}),
        ("POST", "/revise", {"json": {"patch": PLAIN_PATCH}}),
        ("POST", "/withdraw", {}),
    ]:
        r = await agent(app, lead, method, f"{P}/{theirs['id']}{suffix}", **kw)
        assert_error(r, 404, "not_found")
        assert r.json() == unknown.json(), suffix
    r = await agent(
        app,
        lead,
        "POST",
        f"{P}/{mine['id']}/revise",
        json={"patch": {"inactivity": {"close_seconds": 7200}}},
    )
    assert r.status_code == 200, r.text
    assert r.json()["revision"] == 2
    w = await agent(app, lead, "POST", f"{P}/{mine['id']}/withdraw")
    assert w.status_code == 200 and w.json()["status"] == "withdrawn"


async def test_demoted_superleader_may_not_revise_its_preset_proposal(app, pool, kube, preset):
    """Revise follows create: a non-superleader revises only a proposal on its own session; its
    other proposals stay readable and withdrawable."""
    lead = await alone(pool, "pr-demoted", superleader=True)
    view = await filed(app, lead)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET spec = spec - 'session_messaging' WHERE id = %s", (lead.id,)
        )
    r = await agent(app, lead, "POST", f"{P}/{view['id']}/revise", json={"patch": PLAIN_PATCH})
    assert_error(r, 403, "not_superleader")
    async with client_for(app, lead.leader_ip) as http:
        other = await http.post(P, json=body(), headers=headers(lead.id))
    assert r.json() == other.json()
    assert (await agent(app, lead, "GET", f"{P}/{view['id']}")).json()["revision"] == 1
    w = await agent(app, lead, "POST", f"{P}/{view['id']}/withdraw")
    assert w.status_code == 200 and w.json()["status"] == "withdrawn"


async def test_new_leader_after_a_handover_sees_the_session_proposals(app, pool, kube, preset):
    lead = await _leader_of(pool, "pr-hive", preset.id, hive_session=True)
    view = (await agent(app, lead, "POST", P, json=own(lead))).json()
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE session_leaders SET agent_id = (SELECT id FROM agents WHERE session_id = %s"
            " AND ordinal = 1), epoch = epoch + 1 WHERE session_id = %s",
            (lead.id, lead.id),
        )
    async with client_for(app, lead.member_ip) as http:
        listed = await http.get(P, headers=headers(lead.id))
        got = await http.get(f"{P}/{view['id']}", headers=headers(lead.id))
    assert [v["id"] for v in listed.json()] == [view["id"]]
    assert got.status_code == 200 and got.json()["proposer_slug"] == lead.slug
    old = await agent(app, lead, "GET", P)
    assert_error(old, 403, "not_leader")
