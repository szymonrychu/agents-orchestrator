"""O6: admin preset routes behave as the main port through the shared service functions, and
every call leaves an audit log line and an events row that never carry values (D15)."""

from __future__ import annotations

import logging
import secrets as stdlib_secrets
import uuid

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


def spec(value="visible"):
    return {"env": {"PLAIN": {"value": value}}}


async def events(pool, session_id):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT type, data FROM events WHERE session_id = %s ORDER BY id", (session_id,)
        )
        return list(await cur.fetchall())


async def test_crud_by_name_matches_the_main_port(call):
    r = await call("POST", A, json={"name": "alpha", "spec": spec()})
    assert r.status_code == 201, r.text
    pid = r.json()["id"]
    assert r.json()["spec"] == spec()

    got = await call("GET", f"{A}/alpha")
    assert got.status_code == 200
    async with main_client(call.app) as main:
        assert got.json() == (await main.get(f"{M}/{pid}")).json()
    assert (await call("GET", f"{A}/{pid}")).json() == got.json()
    listed = await call("GET", A)
    assert [p["name"] for p in listed.json()].count("alpha") == 1

    put = await call("PUT", f"{A}/alpha", json={"name": "alpha", "spec": spec("other")})
    assert put.status_code == 200, put.text
    assert put.json()["spec"] == spec("other")

    copy = await call("POST", f"{A}/alpha/copy", json={"name": "beta"})
    assert copy.status_code == 201, copy.text
    assert copy.json()["spec"] == spec("other")

    assert (await call("DELETE", f"{A}/beta")).status_code == 204
    assert_error(await call("GET", f"{A}/beta"), 404, "not_found")


async def test_audit_line_and_event_row_carry_no_values(call, pool, boss, canary, caplog):
    caplog.set_level(logging.INFO)
    await call("POST", A, json={"name": "delta", "spec": spec(canary)})
    await call("GET", f"{A}/missing")
    rows = await events(pool, boss.id)
    assert [r["type"] for r in rows] == ["admin_call"] * 2
    first, second = (r["data"] for r in rows)
    assert (first["method"], first["status"]) == ("POST", 201)
    assert first["session"] == "adm-boss" and first["path"] == A
    assert first["secrets"] == [] and first["fields"] == ["env.PLAIN", "name"]
    assert second["status"] == 404 and second["secrets"] == []
    assert canary not in str(rows)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("admin ")]
    assert len(lines) == 2 and "adm-boss" in lines[0]
    assert canary not in " ".join(r.getMessage() for r in caplog.records)


async def test_refused_calls_are_not_audited(pool, dsn, boss):
    plain = await alone(pool, "adm-plain")
    app = create_app(make_settings(dsn), pool)
    async with client_for(app, plain.leader_ip) as http:
        r = await http.get(A, headers=headers(plain.id))
    assert r.status_code == 403
    assert await events(pool, plain.id) == []


async def main_call(app, method, path, **kw):
    async with main_client(app) as http:
        return await http.request(method, path, **kw)


async def hidden_preset(call, name="hidden"):
    r = await main_call(call.app, "POST", M, json={"name": name, "spec": {}})
    assert r.status_code == 201, r.text
    assert r.json()["superleader_manageable"] is False
    return r.json()


async def test_list_only_manageable(call):
    await hidden_preset(call)
    await call("POST", A, json={"name": "shown", "spec": {}})
    names = [p["name"] for p in (await call("GET", A)).json()]
    assert "shown" in names and "hidden" not in names


HIDDEN_CALLS = [
    ("GET", "{ref}", None),
    ("PUT", "{ref}", {"name": "hidden", "spec": {}}),
    ("DELETE", "{ref}", None),
    ("POST", "{ref}/copy", None),
]


@pytest.mark.parametrize(("method", "tail", "body"), HIDDEN_CALLS)
async def test_hidden_preset_404(call, method, tail, body):
    hidden = await hidden_preset(call)
    kw = {} if body is None else {"json": body}
    got = await call(method, f"{A}/" + tail.format(ref=hidden["id"]), **kw)
    missing = await call(method, f"{A}/" + tail.format(ref=uuid.uuid4()), **kw)
    assert_error(got, 404, "not_found")
    assert got.json() == missing.json()
    async with main_client(call.app) as http:
        assert (await http.get(f"{M}/{hidden['id']}")).status_code == 200


async def test_hidden_preset_is_indistinguishable_from_missing(call):
    hidden = await hidden_preset(call)
    for ref, other in ((hidden["name"], "no-such-name"), (hidden["id"], str(uuid.uuid4()))):
        a = await call("GET", f"{A}/{ref}")
        b = await call("GET", f"{A}/{other}")
        assert a.status_code == b.status_code == 404
        assert a.json() == b.json()


async def test_admin_create_forced_manageable(call):
    r = await call("POST", A, json={"name": "forced", "spec": {}})
    assert r.status_code == 201 and r.json()["superleader_manageable"] is True
    got = await main_call(call.app, "GET", f"{M}/{r.json()['id']}")
    assert got.json()["superleader_manageable"] is True


async def test_admin_create_false_rejected(call):
    r = await call("POST", A, json={"name": "nope", "spec": {}, "superleader_manageable": False})
    assert_error(r, 422, "manageable_main_port_only")
    assert [p for p in (await main_call(call.app, "GET", M)).json() if p["name"] == "nope"] == []


async def test_admin_put_false_rejected(call):
    made = (await call("POST", A, json={"name": "keep", "spec": {}})).json()
    r = await call(
        "PUT",
        f"{A}/{made['id']}",
        json={"name": "keep", "spec": {}, "superleader_manageable": False},
    )
    assert_error(r, 422, "manageable_main_port_only")
    got = await main_call(call.app, "GET", f"{M}/{made['id']}")
    assert got.json()["superleader_manageable"] is True


@pytest.mark.parametrize("extra", [{}, {"superleader_manageable": True}])
async def test_admin_put_true_or_absent_accepted(call, extra):
    made = (await call("POST", A, json={"name": "keep2", "spec": {}})).json()
    r = await call("PUT", f"{A}/{made['id']}", json={"name": "keep2", "spec": {}, **extra})
    assert r.status_code == 200, r.text
    assert r.json()["superleader_manageable"] is True


async def test_admin_copy_is_manageable(call):
    made = (await call("POST", A, json={"name": "orig", "spec": {}})).json()
    r = await call("POST", f"{A}/{made['id']}/copy")
    assert r.status_code == 201 and r.json()["superleader_manageable"] is True


# ---------------------------------------------------------------- projects (T-19 P3)


async def _project(call, slug):
    async with main_client(call.app) as main:
        r = await main.post("/api/v1/projects", json={"slug": slug, "name": slug.title()})
    assert r.status_code == 201, r.text


async def _human_get(call, pid):
    async with main_client(call.app) as main:
        return (await main.get(f"{M}/{pid}")).json()


async def test_admin_create_in_project(call, pool, boss):
    await _project(call, "alpha")
    r = await call("POST", A, json={"name": "adm-alpha", "spec": {}, "project": "alpha"})
    assert r.status_code == 201, r.text
    assert r.json()["project"] == "alpha"
    assert (await events(pool, boss.id))[-1]["data"]["fields"] == ["name", "project"]
    plain = await call("POST", A, json={"name": "adm-plain", "spec": {}})
    assert plain.status_code == 201 and plain.json()["project"] == "default"
    assert_error(
        await call("POST", A, json={"name": "adm-lost", "project": "nope"}), 422, "unknown_project"
    )


async def test_admin_move_project_by_patch_and_put(call, pool, boss):
    await _project(call, "alpha")
    await _project(call, "beta")
    pid = (await call("POST", A, json={"name": "mover", "spec": {}})).json()["id"]
    r = await call("PATCH", f"{A}/{pid}", json={"project": "alpha"})
    assert r.status_code == 200, r.text
    assert r.json()["project"] == "alpha"
    assert (await _human_get(call, pid))["project"] == "alpha"
    last = (await events(pool, boss.id))[-1]
    assert last["type"] == "admin_call" and last["data"]["status"] == 200
    assert last["data"]["fields"] == ["project"]
    r = await call("PUT", f"{A}/{pid}", json={"name": "mover", "spec": {}, "project": "beta"})
    assert r.status_code == 200, r.text
    assert (await _human_get(call, pid))["project"] == "beta"
    assert (await events(pool, boss.id))[-1]["data"]["fields"] == ["project"]


async def test_admin_move_to_unknown_project_422(call):
    pid = (await call("POST", A, json={"name": "stay", "spec": {}})).json()["id"]
    before = await _human_get(call, pid)
    for method, body in (
        ("PATCH", {"project": "nope"}),
        ("PUT", {"name": "stay", "spec": {}, "project": "nope"}),
    ):
        assert_error(await call(method, f"{A}/{pid}", json=body), 422, "unknown_project")
    assert await _human_get(call, pid) == before


async def test_admin_move_to_archived_project_422(call):
    await _project(call, "attic")
    async with main_client(call.app) as main:
        r = await main.patch("/api/v1/projects/attic", json={"archived": True})
    assert r.status_code == 200, r.text
    pid = (await call("POST", A, json={"name": "stay2", "spec": {}})).json()["id"]
    before = await _human_get(call, pid)
    for method, body in (
        ("PATCH", {"project": "attic"}),
        ("PUT", {"name": "stay2", "spec": {}, "project": "attic"}),
    ):
        assert_error(await call(method, f"{A}/{pid}", json=body), 422, "project_archived")
    assert await _human_get(call, pid) == before


async def test_non_superleader_cannot_move_preset(call, pool, dsn):
    await _project(call, "alpha")
    pid = (await call("POST", A, json={"name": "guarded", "spec": {}})).json()["id"]
    plain = await alone(pool, "adm-plain-mover")
    app = create_app(make_settings(dsn), pool)
    async with client_for(app, plain.leader_ip) as http:
        for method, body in (
            ("PATCH", {"project": "alpha"}),
            ("PUT", {"name": "guarded", "spec": {}, "project": "alpha"}),
        ):
            r = await http.request(method, f"{A}/{pid}", headers=headers(plain.id), json=body)
            assert_error(r, 403, "not_superleader")
    assert (await _human_get(call, pid))["project"] == "default"


async def test_move_keeps_running_session_in_old_project(call, pool):
    await _project(call, "alpha")
    pid = (await call("POST", A, json={"name": "live", "spec": {}})).json()["id"]
    running = await alone(pool, "live-session")
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET preset_name = 'live' WHERE id = %s", (running.id,))
    r = await call("PATCH", f"{A}/{pid}", json={"project": "alpha"})
    assert r.status_code == 200, r.text
    assert (await _human_get(call, pid))["project"] == "alpha"
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT project, desired FROM sessions WHERE id = %s", (running.id,)
        )
        assert await cur.fetchone() == ("default", "active")


async def test_admin_put_same_project_ok(call):
    await _project(call, "alpha")
    r = await call("POST", A, json={"name": "same", "spec": {}, "project": "alpha"})
    pid = r.json()["id"]
    put = await call(
        "PUT", f"{A}/{pid}", json={"name": "same", "spec": {"replicas": 2}, "project": "alpha"}
    )
    assert put.status_code == 200, put.text
    assert put.json()["project"] == "alpha" and put.json()["spec"] == {"replicas": 2}
    patched = await call("PATCH", f"{A}/{pid}", json={"spec": {"replicas": 1}})
    assert patched.status_code == 200 and patched.json()["project"] == "alpha"
    same = await call("PATCH", f"{A}/{pid}", json={"project": "alpha"})
    assert same.status_code == 200 and same.json()["project"] == "alpha"


async def test_admin_put_without_project_keeps_it(call):
    await _project(call, "alpha")
    pid = (await call("POST", A, json={"name": "kept", "spec": {}, "project": "alpha"})).json()[
        "id"
    ]
    put = await call("PUT", f"{A}/{pid}", json={"name": "kept", "spec": {"replicas": 2}})
    assert put.status_code == 200, put.text
    assert put.json()["project"] == "alpha" and put.json()["spec"] == {"replicas": 2}
    assert (await _human_get(call, pid))["project"] == "alpha"


async def test_admin_copy_keeps_or_sets_project(call):
    await _project(call, "alpha")
    await _project(call, "beta")
    src = (await call("POST", A, json={"name": "csrc", "project": "alpha"})).json()
    kept = await call("POST", f"{A}/{src['id']}/copy", json={"name": "ckept"})
    assert kept.status_code == 201 and kept.json()["project"] == "alpha"
    moved = await call("POST", f"{A}/{src['id']}/copy", json={"name": "cmoved", "project": "beta"})
    assert moved.status_code == 201 and moved.json()["project"] == "beta"
    lost = await call("POST", f"{A}/{src['id']}/copy", json={"name": "clost", "project": "nope"})
    assert_error(lost, 422, "unknown_project")


async def test_admin_preset_writes_refuse_reserved_project_label(call):
    made = (await call("POST", A, json={"name": "lbl", "spec": {}, "labels": ["x"]})).json()
    one = f"{A}/{made['id']}"
    for method, path, body in (
        ("POST", A, {"name": "lbl2", "spec": {}, "labels": ["project:other"]}),
        ("PUT", one, {"name": "lbl", "spec": {}, "labels": ["x", "Project:other"]}),
        ("PATCH", one, {"labels": ["project:other"]}),
    ):
        r = await call(method, path, json=body)
        assert_error(r, 422, "invalid_request")
        assert "'project:' prefix is reserved" in r.text
    got = await main_call(call.app, "GET", f"{M}/{made['id']}")
    assert got.json()["labels"] == ["x"]
