"""O7: admin session routes (get, create, discard) by slug, sharing the main port's logic."""

from __future__ import annotations

import secrets as stdlib_secrets
import uuid

import pytest
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions"
P = f"{ADMIN_PREFIX}/presets"


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


@pytest.fixture
async def boss(pool):
    return await alone(pool, "adm-boss", superleader=True)


@pytest.fixture
def call(pool, dsn, boss):
    app = create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))

    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    return go


async def row(pool, slug):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM sessions WHERE slug = %s", (slug,))
        return await cur.fetchone()


async def test_get_by_slug_and_missing(call, boss):
    r = await call("GET", f"{S}/{boss.slug}")
    assert r.status_code == 200, r.text
    assert r.json()["slug"] == "adm-boss" and r.json()["id"] == str(boss.id)
    assert_error(await call("GET", f"{S}/nope"), 404, "not_found")


async def test_create_from_preset_records_the_superleader_as_creator(call, pool, boss):
    preset = (await call("POST", P, json={"name": "worker", "spec": {}})).json()
    r = await call("POST", S, json={"preset_id": preset["id"], "slug": "child-one"})
    assert r.status_code == 201, r.text
    assert r.json()["slug"] == "child-one"
    stored = await row(pool, "child-one")
    assert stored["created_by_service"] is None
    assert stored["created_by_session"] == boss.id
    assert stored["preset_id"] == uuid.UUID(preset["id"])
    # a user-created session has no creator session
    other = await row(pool, "adm-boss")
    assert other["created_by_session"] is None


async def test_create_from_spec_with_slug(call, pool, boss):
    spec = {"env": {"PLAIN": {"value": "x"}}}
    r = await call("POST", S, json={"slug": "child-spec", "spec": spec})
    assert r.status_code == 201, r.text
    assert (await row(pool, "child-spec"))["created_by_session"] == boss.id
    assert_error(await call("POST", S, json={"spec": spec}), 422, "invalid_request")
    assert_error(await call("POST", S, json={"slug": "child-spec", "spec": spec}), 409, "conflict")


async def test_create_applies_the_secret_rules(call, pool, canary):
    unset = {"env": {"API_KEY": {"protected": True}}}
    preset = (await call("POST", P, json={"name": "needs-key", "spec": unset})).json()
    r = await call("POST", S, json={"preset_id": preset["id"], "slug": "child-bad"})
    assert_error(r, 422, "secret_unset")
    assert await row(pool, "child-bad") is None
    # a spec without a preset cannot carry a protected variable at all
    r = await call("POST", S, json={"slug": "child-bad2", "spec": unset})
    assert_error(r, 422, "secret_unset")
    # once the value is set, create works and shows no value
    await call("PUT", f"{P}/needs-key/secrets/API_KEY", json={"value": canary})
    r = await call("POST", S, json={"preset_id": preset["id"], "slug": "child-good"})
    assert r.status_code == 201, r.text
    assert canary not in r.text


async def test_discard_another_session(call, pool):
    await call("POST", S, json={"slug": "victim", "spec": {}})
    r = await call("DELETE", f"{S}/victim")
    assert r.status_code == 202, r.text
    assert r.json()["slug"] == "victim"
    assert (await row(pool, "victim"))["desired"] == "deleting"
    assert_error(await call("DELETE", f"{S}/nope"), 404, "not_found")


async def test_discarding_the_callers_own_session_is_refused(call, pool):
    assert_error(await call("DELETE", f"{S}/adm-boss"), 422, "own_session")
    assert (await row(pool, "adm-boss"))["desired"] == "active"
