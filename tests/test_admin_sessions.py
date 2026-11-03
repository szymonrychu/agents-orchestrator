"""O7: admin session routes (get, create, discard) by slug, sharing the main port's logic."""

from __future__ import annotations

import secrets as stdlib_secrets
import uuid

import pytest
from factories import attach_preset
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.preset_policy import privileged

S = f"{ADMIN_PREFIX}/sessions"
P = f"{ADMIN_PREFIX}/presets"
M = "/api/v1/presets"


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


@pytest.fixture
async def boss(pool):
    return await alone(pool, "adm-boss", superleader=True)


@pytest.fixture
def app(pool, dsn):
    settings = make_settings(dsn).model_copy(
        update={"kube_access_service_account": "kube-sa", "docker_image": "docker:27-dind"}
    )
    return create_app(settings, pool, kube=FakeKube(namespace="agents"))


@pytest.fixture
def call(app, boss):
    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    return go


@pytest.fixture
def main(app):
    async def go(method, path, **kw):
        async with main_client(app) as http:
            return await http.request(method, path, **kw)

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


async def test_admin_create_raw_spec_422(call, pool):
    spec = {"env": {"PLAIN": {"value": "x"}}}
    r = await call("POST", S, json={"slug": "child-spec", "spec": spec})
    assert_error(r, 422, "preset_required")
    assert await row(pool, "child-spec") is None


async def test_create_from_preset_with_slug_conflict(call):
    preset = (await call("POST", P, json={"name": "dup", "spec": {}})).json()
    body = {"preset_id": preset["id"], "slug": "child-dup"}
    assert (await call("POST", S, json=body)).status_code == 201
    assert_error(await call("POST", S, json=body), 409, "conflict")


async def test_admin_create_from_hidden_preset_404(call, main):
    hidden = (await main("POST", M, json={"name": "hid", "spec": {}})).json()
    r = await call("POST", S, json={"preset_id": hidden["id"], "slug": "child-h"})
    assert_error(r, 404, "not_found")


async def test_admin_create_from_manageable_preset_201(call, main):
    shown = (
        await main("POST", M, json={"name": "shw", "spec": {}, "superleader_manageable": True})
    ).json()
    r = await call("POST", S, json={"preset_id": shown["id"], "slug": "child-s"})
    assert r.status_code == 201, r.text


async def test_get_session_of_hidden_preset_404(call, pool):
    other = await alone(pool, "hid-get")
    await attach_preset(pool, other.id, manageable=False)
    assert_error(await call("GET", f"{S}/{other.slug}"), 404, "not_found")
    assert (await call("GET", f"{S}/nope")).json() == (
        await call("GET", f"{S}/{other.slug}")
    ).json()


async def test_discard_session_of_hidden_preset_404(call, pool):
    other = await alone(pool, "hid-del")
    await attach_preset(pool, other.id, manageable=False)
    assert_error(await call("DELETE", f"{S}/{other.slug}"), 404, "not_found")
    assert (await row(pool, other.slug))["desired"] == "active"


async def test_session_without_preset_404(call, pool, main):
    r = await main("POST", "/api/v1/sessions", json={"slug": "raw-one", "spec": {}})
    assert r.status_code == 201, r.text
    assert_error(await call("GET", f"{S}/raw-one"), 404, "not_found")


async def test_session_after_preset_deleted_404(call, pool, main):
    p = (
        await main("POST", M, json={"name": "gone", "spec": {}, "superleader_manageable": True})
    ).json()
    r = await main("POST", "/api/v1/sessions", json={"slug": "orphan", "preset_id": p["id"]})
    assert r.status_code == 201, r.text
    assert (await call("GET", f"{S}/orphan")).status_code == 200
    assert (await main("DELETE", f"{M}/{p['id']}")).status_code == 204
    assert (await row(pool, "orphan"))["preset_id"] is None
    assert_error(await call("GET", f"{S}/orphan"), 404, "not_found")


async def test_own_session_visible_with_hidden_preset(call, pool, boss):
    await attach_preset(pool, boss.id, manageable=False)
    assert (await call("GET", f"{S}/{boss.slug}")).status_code == 200


async def test_own_session_visible_without_preset(call, boss):
    assert (await call("GET", f"{S}/{boss.slug}")).status_code == 200


async def test_create_applies_the_secret_rules(call, pool, canary):
    unset = {"env": {"API_KEY": {"protected": True}}}
    preset = (await call("POST", P, json={"name": "needs-key", "spec": unset})).json()
    r = await call("POST", S, json={"preset_id": preset["id"], "slug": "child-bad"})
    assert_error(r, 422, "secret_unset")
    assert await row(pool, "child-bad") is None
    # a spec without a preset is refused outright
    r = await call("POST", S, json={"slug": "child-bad2", "spec": unset})
    assert_error(r, 422, "preset_required")
    # once the value is set, create works and shows no value
    await call("PUT", f"{P}/needs-key/secrets/API_KEY", json={"value": canary})
    r = await call("POST", S, json={"preset_id": preset["id"], "slug": "child-good"})
    assert r.status_code == 201, r.text
    assert canary not in r.text


async def test_discard_another_session(call, pool):
    preset = (await call("POST", P, json={"name": "vict", "spec": {}})).json()
    await call("POST", S, json={"slug": "victim", "preset_id": preset["id"]})
    r = await call("DELETE", f"{S}/victim")
    assert r.status_code == 202, r.text
    assert r.json()["slug"] == "victim"
    assert (await row(pool, "victim"))["desired"] == "deleting"
    assert_error(await call("DELETE", f"{S}/nope"), 404, "not_found")


async def test_discarding_the_callers_own_session_is_refused(call, pool):
    assert_error(await call("DELETE", f"{S}/adm-boss"), 422, "own_session")
    assert (await row(pool, "adm-boss"))["desired"] == "active"


async def last_admin_call(pool, session_id):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call' "
            "ORDER BY id DESC LIMIT 1",
            (session_id,),
        )
        return (await cur.fetchone())["data"]


@pytest.mark.parametrize(
    "flags",
    [
        {"kube_access": True},
        {"session_messaging": True},
        {"docker": True},
        {"kube_access": True, "docker": True},
    ],
)
async def test_admin_create_from_privileged_preset_403(call, main, pool, boss, flags):
    made = await main(
        "POST", M, json={"name": "priv", "spec": flags, "superleader_manageable": True}
    )
    assert made.status_code == 201, made.text
    r = await call("POST", S, json={"preset_id": made.json()["id"], "slug": "child-p"})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == privileged(flags)
    assert await row(pool, "child-p") is None
    audit = await last_admin_call(pool, boss.id)
    assert audit["status"] == 403 and audit["fields"] == privileged(flags)


async def test_admin_create_from_preset_with_flags_off_201(call, main):
    spec = {
        "kube_access": False,
        "session_messaging": None,
        "discard_hook_url": "https://h.example",
    }
    made = await main("POST", M, json={"name": "off", "spec": spec, "superleader_manageable": True})
    assert made.status_code == 201, made.text
    r = await call("POST", S, json={"preset_id": made.json()["id"], "slug": "child-off"})
    assert r.status_code == 201, r.text


async def test_admin_create_hidden_privileged_is_404(call, main):
    made = await main("POST", M, json={"name": "hidp", "spec": {"kube_access": True}})
    assert made.status_code == 201, made.text
    r = await call("POST", S, json={"preset_id": made.json()["id"], "slug": "child-hp"})
    assert_error(r, 404, "not_found")


async def test_main_create_from_privileged_preset_201(main):
    spec = {"kube_access": True, "session_messaging": True}
    made = await main("POST", M, json={"name": "mainp", "spec": spec})
    assert made.status_code == 201, made.text
    r = await main(
        "POST", "/api/v1/sessions", json={"preset_id": made.json()["id"], "slug": "human-p"}
    )
    assert r.status_code == 201, r.text
