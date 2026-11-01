"""O10: a credential agents-webhook issues once never reaches the superleader's transcript.

The relay stores it in the Secret `webhook-<id>-password` and answers with a reference. Only a
call that sends `X-Orch-Tool: local` also gets the value back once, for the local tool's file.
"""

from __future__ import annotations

import secrets as stdlib_secrets

import pytest
from fakes import fake_upstream as fu
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_admin_relay import (
    DOCS,
    UPSTREAM_PREFIX,
    _body_schema,
    relay_url,
    rig,  # noqa: F401
)
from test_api_sessions import assert_error

ENDPOINTS = UPSTREAM_PREFIX["webhook"] + "/endpoints"
ISSUING = {
    "create": ("POST", "/endpoints", 201),
    "rotate": ("POST", "/endpoints/{id}/rotate", 200),
    "clone": ("POST", "/endpoints/{id}/clone", 201),
    "patch": ("PATCH", "/endpoints/{id}", 200),
}


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


def issue(rig, name, canary, *, field="password", id_=7, mode="basic"):  # noqa: F811
    """Script the upstream to answer the `name` route with a one-time credential."""
    method, template, status = ISSUING[name]
    fake = rig.fakes["webhook"]
    key = (method, UPSTREAM_PREFIX["webhook"] + template)
    body = fu.sample(DOCS["webhook"], {"$ref": "#/components/schemas/EndpointCreated"})
    body.update({"id": id_, "auth_mode": mode, "password": None, "secret": None, field: canary})
    fake.answers[key] = (status, body)
    return method, relay_url("webhook", template), status


async def go(rig, name, canary, **kw):  # noqa: F811
    headers = kw.pop("headers", {})
    field = kw.pop("field", "password")
    method, url, status = issue(rig, name, canary, field=field, **kw)
    body = {} if method == "PATCH" or name != "create" else None
    if name == "create":
        fake = rig.fakes["webhook"]
        key = (method, UPSTREAM_PREFIX["webhook"] + "/endpoints")
        body = fu.sample(DOCS["webhook"], _body_schema(fake, key))
    r = await rig.call(method, url, json=body, headers=headers)
    return r, status


def secret_of(rig, id_=7):  # noqa: F811
    """The last apply of the credential Secret (the fake keeps no stringData)."""
    name = f"webhook-{id_}-password"
    return [a.obj for a in rig.kube.applied("Secret") if a.obj["metadata"]["name"] == name][-1]


@pytest.mark.parametrize("name", list(ISSUING))
async def test_credential_never_reaches_the_caller_and_goes_to_a_secret(rig, canary, name):  # noqa: F811
    r, status = await go(rig, name, canary)
    assert r.status_code == status, r.text
    assert canary not in r.text
    body = r.json()
    assert body["password_secret"] == "webhook-7-password" and body["key"] == "password"
    assert "password" not in body and "secret" not in body and "credential" not in body
    stored = secret_of(rig)
    assert stored["stringData"] == {"password": canary}
    assert stored["metadata"]["namespace"] == rig.kube.namespace
    assert "immutable" not in stored
    assert "webhook-7-password" in rig.kube.names("Secret")


async def test_header_secret_goes_under_the_secret_key(rig, canary):  # noqa: F811
    r, _ = await go(rig, "create", canary, field="secret", mode="hmac")
    assert canary not in r.text and r.json()["key"] == "secret"
    assert secret_of(rig)["stringData"] == {"secret": canary}


async def test_rotate_overwrites_and_drops_a_stale_key(rig, canary):  # noqa: F811
    await go(rig, "create", canary)
    again = "canary-" + stdlib_secrets.token_hex(16)
    r, _ = await go(rig, "rotate", again, field="secret", mode="bearer")
    assert canary not in r.text and again not in r.text
    assert secret_of(rig)["stringData"] == {"secret": again}


async def test_the_apply_response_is_dropped_and_nothing_logs_it(rig, canary, caplog):  # noqa: F811
    with caplog.at_level("DEBUG"):
        r, _ = await go(rig, "create", canary)
    assert canary not in caplog.text and canary not in r.text


async def test_audit_row_names_the_secret_never_the_value(rig, canary):  # noqa: F811
    await go(rig, "create", canary)
    async with rig.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call'", (rig.boss.id,)
        )
        rows = await cur.fetchall()
    assert rows and canary not in str(rows)
    assert any("webhook-7-password" in r["data"]["secrets"] for r in rows)


async def test_local_tool_channel_returns_the_value_once(rig, canary):  # noqa: F811
    r, _ = await go(rig, "create", canary, headers={"X-Orch-Tool": "local"})
    assert r.json()["credential"] == {"key": "password", "value": canary}
    assert secret_of(rig)["stringData"] == {"password": canary}
    other = "canary-" + stdlib_secrets.token_hex(16)
    r, _ = await go(rig, "rotate", other, headers={"X-Orch-Tool": "remote"})
    assert other not in r.text and "credential" not in r.json()


async def test_local_channel_is_behind_the_same_gate(pool, dsn, canary):
    from test_admin_relay import make_app
    from test_agent_session_messaging import alone
    from test_api_agent import client_for, headers

    plain = await alone(pool, "wp-plain")
    app, fakes, *_ = make_app(dsn, pool)
    async with client_for(app, plain.leader_ip) as http:
        r = await http.post(
            relay_url("webhook", "/endpoints"),
            json={},
            headers={**headers(plain.id), "X-Orch-Tool": "local"},
        )
    assert_error(r, 403, "not_superleader")
    assert not fakes["webhook"].calls


async def test_failure_to_store_fails_the_call_without_the_value(rig, canary):  # noqa: F811
    rig.kube.fail("Secret", "webhook-7-password", verbs={"patch"})
    r, _ = await go(rig, "create", canary, headers={"X-Orch-Tool": "local"})
    assert_error(r, 503, "credential_not_stored")
    assert canary not in r.text
    assert "webhook-7-password" not in rig.kube.names("Secret")


async def test_delete_removes_the_secret(rig, canary):  # noqa: F811
    await go(rig, "create", canary)
    assert "webhook-7-password" in rig.kube.names("Secret")
    fake = rig.fakes["webhook"]
    key = ("DELETE", ENDPOINTS + "/{id}")
    fake.answers[key] = (204, None)
    r = await rig.call("DELETE", relay_url("webhook", "/endpoints/{id}"))
    assert r.status_code == 204
    assert "webhook-7-password" not in rig.kube.names("Secret")


async def test_failed_delete_keeps_the_secret(rig, canary):  # noqa: F811
    await go(rig, "create", canary)
    fake = rig.fakes["webhook"]
    fake.answers[("DELETE", ENDPOINTS + "/{id}")] = (
        422,
        {"detail": [{"loc": ["path"], "msg": "x", "type": "x"}]},
    )
    await rig.call("DELETE", relay_url("webhook", "/endpoints/{id}"))
    assert "webhook-7-password" in rig.kube.names("Secret")


async def test_a_credential_on_a_non_issuing_route_is_refused(rig, canary):  # noqa: F811
    fake = rig.fakes["webhook"]
    key = ("GET", UPSTREAM_PREFIX["webhook"] + "/endpoints")
    view = fu.sample(DOCS["webhook"], {"$ref": "#/components/schemas/EndpointView"})
    fake.answers[key] = (200, [{**view, "secret": canary}])
    r = await rig.call("GET", relay_url("webhook", "/endpoints"))
    assert_error(r, 503, "upstream_unavailable")
    assert canary not in r.text
    assert not rig.kube.names("Secret")


async def test_other_services_are_not_touched(rig):  # noqa: F811
    fake = rig.fakes["schedule"]
    key = ("GET", UPSTREAM_PREFIX["schedule"] + "/schedules")
    fake.answers[key] = fu.success_sample(fake, key)
    assert (await rig.call("GET", relay_url("schedule", "/schedules"))).status_code == 200
    assert not rig.kube.applied("Secret")
    assert isinstance(rig.kube, FakeKube)
