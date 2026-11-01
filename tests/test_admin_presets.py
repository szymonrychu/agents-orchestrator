"""O6: admin preset routes behave as the main port through the shared service functions, and
every call leaves an audit log line and an events row that never carry values (D15)."""

from __future__ import annotations

import logging
import secrets as stdlib_secrets

import pytest
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

A = f"{ADMIN_PREFIX}/presets"
M = "/api/v1/presets"


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


@pytest.fixture
async def boss(pool):
    return await alone(pool, "adm-boss", superleader=True)


@pytest.fixture
def call(pool, dsn, boss):
    kube = FakeKube(namespace="agents")
    app = create_app(make_settings(dsn), pool, kube=kube)

    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    go.kube = kube
    go.app = app
    return go


def spec(value=None):
    entry = {"protected": True} if value is None else {"protected": True, "value": value}
    return {"env": {"API_KEY": entry, "PLAIN": {"value": "visible"}}}


async def events(pool, session_id):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT type, data FROM events WHERE session_id = %s ORDER BY id", (session_id,)
        )
        return list(await cur.fetchall())


async def test_crud_by_name_matches_the_main_port(call, canary):
    r = await call("POST", A, json={"name": "alpha", "spec": spec(canary)})
    assert r.status_code == 201, r.text
    assert canary not in r.text
    pid = r.json()["id"]
    entry = r.json()["spec"]["env"]["API_KEY"]
    assert entry["protected"] and entry["set"] and entry["version"] == 1 and "value" not in entry

    got = await call("GET", f"{A}/alpha")
    assert got.status_code == 200 and canary not in got.text
    async with main_client(call.app) as main:
        assert got.json() == (await main.get(f"{M}/{pid}")).json()
    assert (await call("GET", f"{A}/{pid}")).json() == got.json()
    listed = await call("GET", A)
    assert [p["name"] for p in listed.json()].count("alpha") == 1

    # keep-on-untouched: no value sent, version stays
    put = await call("PUT", f"{A}/alpha", json={"name": "alpha", "spec": spec()})
    assert put.status_code == 200, put.text
    assert put.json()["spec"]["env"]["API_KEY"]["version"] == 1
    # overwrite: new version
    put = await call("PUT", f"{A}/alpha", json={"name": "alpha", "spec": spec(canary + "2")})
    assert put.json()["spec"]["env"]["API_KEY"]["version"] == 2
    assert canary not in put.text

    copy = await call("POST", f"{A}/alpha/copy", json={"name": "beta"})
    assert copy.status_code == 201, copy.text
    assert copy.json()["spec"]["env"]["API_KEY"]["set"] is False

    assert (await call("DELETE", f"{A}/beta")).status_code == 204
    assert_error(await call("GET", f"{A}/beta"), 404, "not_found")


async def test_secret_set_returns_metadata_only(call, canary):
    await call("POST", A, json={"name": "gamma", "spec": spec()})
    value = canary + "-new"
    r = await call("PUT", f"{A}/gamma/secrets/API_KEY", json={"value": value})
    assert r.status_code == 200, r.text
    assert value not in r.text
    assert r.json()["set"] is True and r.json()["version"] == 1
    bad = await call("PUT", f"{A}/gamma/secrets/PLAIN", json={"value": value})
    assert_error(bad, 422, "invalid_request")
    assert value not in bad.text


async def test_audit_line_and_event_row_carry_no_values(call, pool, boss, canary, caplog):
    caplog.set_level(logging.INFO)
    await call("POST", A, json={"name": "delta", "spec": spec(canary)})
    await call("PUT", f"{A}/delta/secrets/API_KEY", json={"value": canary + "-2"})
    await call("GET", f"{A}/missing")
    rows = await events(pool, boss.id)
    assert [r["type"] for r in rows] == ["admin_call"] * 3
    first, second, third = (r["data"] for r in rows)
    assert (first["method"], first["status"]) == ("POST", 201)
    assert first["session"] == "adm-boss" and first["path"] == A
    assert first["secrets"] == ["API_KEY"]
    assert second["secrets"] == ["API_KEY"] and second["path"] == f"{A}/delta/secrets/API_KEY"
    assert third["status"] == 404 and third["secrets"] == []
    assert canary not in str(rows)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("admin ")]
    assert len(lines) == 3 and "adm-boss" in lines[0] and "API_KEY" in lines[0]
    assert canary not in " ".join(r.getMessage() for r in caplog.records)


async def test_refused_calls_are_not_audited(pool, dsn, boss):
    plain = await alone(pool, "adm-plain")
    app = create_app(make_settings(dsn), pool)
    async with client_for(app, plain.leader_ip) as http:
        r = await http.get(A, headers=headers(plain.id))
    assert r.status_code == 403
    assert await events(pool, plain.id) == []
