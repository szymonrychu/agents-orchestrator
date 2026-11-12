"""T-120 slice 2 (decision 388): a superleader READS every preset and session, WRITES on a target
that is not superleader_manageable answer 403 `not_manageable`; unknown refs stay 404."""

from __future__ import annotations

import pytest
from factories import attach_preset
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

A = f"{ADMIN_PREFIX}/presets"
S = f"{ADMIN_PREFIX}/sessions"
M = "/api/v1/presets"
ENTRY = "11111111-1111-4111-8111-111111111111"


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))


@pytest.fixture
async def boss(pool):
    return await alone(pool, "split-boss", superleader=True)


@pytest.fixture
def call(app, boss):
    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    return go


@pytest.fixture
async def locked_preset(app):
    async with main_client(app) as main:
        r = await main.post(M, json={"name": "locked", "spec": {}})
    assert r.status_code == 201, r.text
    assert r.json()["superleader_manageable"] is False
    return r.json()


@pytest.fixture
async def locked_session(pool):
    other = await hive(pool, "split-other")
    await attach_preset(pool, other.id, manageable=False)
    return other


async def audit_statuses(pool):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT data FROM events WHERE type = 'admin_call' ORDER BY id")
        return [r["data"]["status"] for r in await cur.fetchall()]


async def test_list_presets_includes_unmanageable(call, locked_preset):
    r = await call("GET", A)
    assert r.status_code == 200, r.text
    got = {p["name"]: p for p in r.json()}
    assert got["locked"]["superleader_manageable"] is False


async def test_get_unmanageable_preset_200(call, locked_preset):
    for ref in (locked_preset["id"], locked_preset["name"]):
        r = await call("GET", f"{A}/{ref}")
        assert r.status_code == 200, r.text
        assert r.json()["superleader_manageable"] is False


PRESET_WRITES = [
    ("PATCH", "{ref}", {}),
    ("PUT", "{ref}", {"name": "locked", "spec": {}}),
    ("DELETE", "{ref}", None),
    ("POST", "{ref}/copy", None),
    ("PUT", "{ref}/vault/" + ENTRY, {}),
    ("DELETE", "{ref}/vault/" + ENTRY, None),
    ("POST", "{ref}/projects", {"project": "default", "reason": "r"}),
    ("DELETE", "{ref}/projects/default", None),
]


@pytest.mark.parametrize(("method", "tail", "body"), PRESET_WRITES)
async def test_patch_unmanageable_preset_403_not_manageable(
    pool, call, locked_preset, method, tail, body
):
    kw = {} if body is None else {"json": body}
    r = await call(method, f"{A}/" + tail.format(ref=locked_preset["id"]), **kw)
    assert_error(r, 403, "not_manageable")
    assert (await audit_statuses(pool))[-1] == 403


async def test_unknown_preset_stays_404(call):
    assert_error(await call("GET", f"{A}/no-such"), 404, "not_found")
    assert_error(await call("PATCH", f"{A}/no-such", json={}), 404, "not_found")


async def test_get_session_of_unmanageable_preset_200(call, locked_session):
    r = await call("GET", f"{S}/{locked_session.slug}")
    assert r.status_code == 200, r.text
    assert r.json()["slug"] == locked_session.slug


async def test_get_raw_spec_session_200(call, main_raw):
    r = await call("GET", f"{S}/{main_raw}")
    assert r.status_code == 200, r.text


@pytest.fixture
async def main_raw(app):
    async with main_client(app) as main:
        r = await main.post("/api/v1/sessions", json={"slug": "raw-split", "spec": {}})
    assert r.status_code == 201, r.text
    return "raw-split"


SESSION_WRITES = [
    ("PATCH", "", {"spec": {}}),
    ("DELETE", "", None),
    ("POST", "/restart", None),
    ("POST", "/agents/x/restart", None),
    ("POST", "/leadership/withdraw", {}),
    ("POST", "/projects", {"project": "default", "reason": "r"}),
    ("DELETE", "/projects/default", None),
]


@pytest.mark.parametrize(("method", "tail", "body"), SESSION_WRITES)
async def test_patch_session_of_unmanageable_preset_403(
    pool, call, locked_session, method, tail, body
):
    kw = {} if body is None else {"json": body}
    r = await call(method, f"{S}/{locked_session.slug}{tail}", **kw)
    assert_error(r, 403, "not_manageable")
    assert (await audit_statuses(pool))[-1] == 403


@pytest.mark.parametrize("method", ["PATCH", "DELETE", "POST"])
async def test_write_on_raw_spec_session_403(call, main_raw, method):
    path = f"{S}/{main_raw}" + ("/restart" if method == "POST" else "")
    kw = {"json": {"spec": {}}} if method == "PATCH" else {}
    assert_error(await call(method, path, **kw), 403, "not_manageable")


async def test_unknown_session_stays_404(call):
    assert_error(await call("GET", f"{S}/nope"), 404, "not_found")
    assert_error(await call("DELETE", f"{S}/nope"), 404, "not_found")


async def test_proposal_create_on_unmanageable_preset_403(pool, call, locked_preset):
    body = {
        "kind": "preset",
        "target": locked_preset["id"],
        "patch": {"env": {"PLAIN": {"value": "x"}}},
        "reason": "r",
    }
    r = await call("POST", f"{ADMIN_PREFIX}/proposals", json=body)
    assert_error(r, 403, "not_manageable")
    assert (await audit_statuses(pool))[-1] == 403


async def test_proposal_create_on_unmanageable_session_403(call, locked_session):
    body = {
        "kind": "session",
        "target": locked_session.slug,
        "patch": {"replicas": 2},
        "reason": "r",
    }
    assert_error(await call("POST", f"{ADMIN_PREFIX}/proposals", json=body), 403, "not_manageable")


async def test_regular_leader_get_other_session_still_own_only(pool, app):
    leader = await hive(pool, "split-lead")
    other = await alone(pool, "split-bystander")
    async with client_for(app, leader.leader_ip) as http:
        r = await http.get(f"{S}/{other.slug}", headers=headers(leader.id))
        assert_error(r, 403, "not_superleader")
        own = await http.get(f"{S}/{leader.slug}", headers=headers(leader.id))
        assert own.status_code == 200, own.text
