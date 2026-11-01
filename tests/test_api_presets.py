"""Preset routes: CRUD, copy, validation against settings and the guards on the default."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as repo

DEFAULT_ID = "00000000-0000-0000-0000-000000000001"
P = "/api/v1/presets"


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn, max_replicas=4), pool)
    async with client_for(app) as c:
        yield c


async def make(http, name, spec=None):
    r = await http.post(P, json={"name": name, "spec": spec or {}})
    assert r.status_code == 201, r.text
    return r.json()


async def test_list_has_seeded_default(http):
    r = await http.get(P)
    assert r.status_code == 200
    rows = r.json()
    assert len(rows) == 1
    assert rows[0]["id"] == DEFAULT_ID
    assert rows[0]["name"] == "Default"
    assert rows[0]["is_default"] is True
    assert rows[0]["spec"] == {"replicas": 1}
    assert {"created_at", "updated_at"} <= set(rows[0])
    await make(http, "zeta")
    await make(http, "Alpha")
    assert [p["name"] for p in (await http.get(P)).json()] == ["Default", "Alpha", "zeta"]


async def test_create_stores_only_set_fields(http):
    spec = {
        "replicas": 2,
        "model": "claude-opus-5-5",
        "resources": {"requests": {"cpu": "1"}},
        "env": {"A": {"value": "b"}},
    }
    r = await http.post(P, json={"name": "big", "spec": spec})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["spec"] == spec
    assert body["is_default"] is False
    got = (await http.get(f"{P}/{body['id']}")).json()
    assert got == body
    empty = await make(http, "empty")
    assert empty["spec"] == {}
    no_spec = await http.post(P, json={"name": "nospec"})
    assert no_spec.status_code == 201 and no_spec.json()["spec"] == {}


async def test_name_rules_and_unique_409(http):
    for bad in ("", "x" * 41, " lead", "trail ", "bad/slash", "emoji!"):
        assert_error(await http.post(P, json={"name": bad}), 422, "invalid_request")
    assert_error(await http.post(P, json={"name": "ok", "nope": 1}), 422, "invalid_request")
    assert_error(await http.post(P, json={"spec": {}}), 422, "invalid_request")
    created = await make(http, "Taken")
    assert_error(await http.post(P, json={"name": "Taken"}), 409, "conflict")
    assert_error(await http.post(P, json={"name": "tAKEN"}), 409, "conflict")
    assert_error(await http.post(P, json={"name": "default"}), 409, "conflict")
    other = await make(http, "other")
    assert_error(await http.put(f"{P}/{other['id']}", json={"name": " x"}), 422, "invalid_request")
    assert_error(await http.put(f"{P}/{other['id']}", json={"name": "TAKEN"}), 409, "conflict")
    same = await http.put(f"{P}/{created['id']}", json={"name": "TAKEN", "spec": {}})
    assert same.status_code == 200 and same.json()["name"] == "TAKEN"


@pytest.mark.parametrize(
    "spec",
    [
        {"model": "gpt-9"},
        {"effort": "ludicrous"},
        {"replicas": 5},
        {"image": "evil:1"},
        {"env": {"POD_NAME": {"value": "x"}}},
        {"nope": 1},
    ],
)
async def test_save_validates_spec_against_settings(http, spec):
    assert_error(await http.post(P, json={"name": "bad", "spec": spec}), 422, "invalid_request")
    ok = await make(http, "ok")
    r = await http.put(f"{P}/{ok['id']}", json={"name": "ok", "spec": spec})
    assert_error(r, 422, "invalid_request")
    assert (await http.get(f"{P}/{ok['id']}")).json()["spec"] == {}


async def test_put_replaces_and_default_not_renamed(http):
    made = await make(http, "one", {"replicas": 2, "model": "claude-opus-5-5"})
    r = await http.put(f"{P}/{made['id']}", json={"name": "two", "spec": {"replicas": 3}})
    assert r.status_code == 200, r.text
    body = r.json()
    assert (body["id"], body["name"], body["spec"]) == (made["id"], "two", {"replicas": 3})
    assert body["created_at"] == made["created_at"]
    assert body["updated_at"] > made["updated_at"]
    assert (await http.get(f"{P}/{made['id']}")).json() == body

    edited = await http.put(f"{P}/{DEFAULT_ID}", json={"name": "Default", "spec": {"replicas": 2}})
    assert edited.status_code == 200
    assert edited.json()["spec"] == {"replicas": 2} and edited.json()["is_default"] is True
    assert_error(
        await http.put(f"{P}/{DEFAULT_ID}", json={"name": "Renamed", "spec": {}}),
        409,
        "preset_default",
    )
    missing = uuid.uuid4()
    assert_error(await http.put(f"{P}/{missing}", json={"name": "x"}), 404, "not_found")
    assert_error(await http.get(f"{P}/{missing}"), 404, "not_found")


async def test_delete_guards(http, pool):
    made = await make(http, "gone")
    r = await http.delete(f"{P}/{made['id']}")
    assert r.status_code == 204 and r.content == b""
    assert_error(await http.get(f"{P}/{made['id']}"), 404, "not_found")
    assert_error(await http.delete(f"{P}/{made['id']}"), 404, "not_found")
    assert_error(await http.delete(f"{P}/{DEFAULT_ID}"), 409, "preset_default")
    # remove the default flag's row companions: only the default is left now
    only = await make(http, "second")
    async with pool.connection() as conn:
        await conn.execute("UPDATE presets SET is_default = false WHERE is_default")
        await conn.execute("UPDATE presets SET is_default = true WHERE name = 'second'")
        await conn.execute("DELETE FROM presets WHERE name = 'Default'")
    assert_error(await http.delete(f"{P}/{only['id']}"), 409, "preset_default")
    async with pool.connection() as conn:
        await conn.execute("UPDATE presets SET is_default = false")
    assert_error(await http.delete(f"{P}/{only['id']}"), 409, "last_preset")


async def test_delete_keeps_sessions_spec(http, pool):
    made = await make(http, "src", {"replicas": 2})
    pid = uuid.UUID(made["id"])
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO sessions (id, slug, spec, preset_id, preset_name)"
            " VALUES (%s, 'c1', %s::jsonb, %s, 'src')",
            (uuid.uuid4(), '{"replicas": 2}', pid),
        )
    assert (await http.delete(f"{P}/{made['id']}")).status_code == 204
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT spec, preset_id, preset_name FROM sessions")
        spec, preset_id, preset_name = await cur.fetchone()
    assert spec == {"replicas": 2} and preset_id is None and preset_name == "src"


async def test_copy_default_name_and_verbatim_spec(http):
    spec = {"replicas": 2, "resources": {"limits": {"memory": "2Gi"}}}
    src = await make(http, "web", spec)
    c1 = await http.post(f"{P}/{src['id']}/copy")
    assert c1.status_code == 201, c1.text
    assert c1.json()["name"] == "web copy"
    assert c1.json()["spec"] == spec and c1.json()["is_default"] is False
    assert c1.json()["id"] != src["id"]
    c2 = await http.post(f"{P}/{src['id']}/copy", json={})
    assert c2.json()["name"] == "web copy 2"
    c3 = await http.post(f"{P}/{src['id']}/copy", json={"name": None})
    assert c3.json()["name"] == "web copy 3"
    named = await http.post(f"{P}/{src['id']}/copy", json={"name": "mine"})
    assert named.status_code == 201 and named.json()["name"] == "mine"
    assert_error(await http.post(f"{P}/{src['id']}/copy", json={"name": "MINE"}), 409, "conflict")
    assert_error(
        await http.post(f"{P}/{src['id']}/copy", json={"name": " x"}), 422, "invalid_request"
    )
    assert_error(await http.post(f"{P}/{src['id']}/copy", json={"x": 1}), 422, "invalid_request")
    assert_error(await http.post(f"{P}/{uuid.uuid4()}/copy"), 404, "not_found")

    dflt = await http.post(f"{P}/{DEFAULT_ID}/copy")
    assert dflt.json()["name"] == "Default copy"
    assert dflt.json()["spec"] == {"replicas": 1} and dflt.json()["is_default"] is False

    long = await make(http, "a" * 40)
    first = (await http.post(f"{P}/{long['id']}/copy")).json()["name"]
    second = (await http.post(f"{P}/{long['id']}/copy")).json()["name"]
    assert first == "a" * 35 + " copy" and second == "a" * 33 + " copy 2"
    spaced = await make(http, "b" * 34 + " zzzzz")  # cut lands right after a space
    assert (await http.post(f"{P}/{spaced['id']}/copy")).json()["name"] == "b" * 34 + " copy"


async def test_put_accepts_unchanged_out_of_list_value(pool, dsn):
    # a preset stored while the lists were wider
    async with pool.connection() as conn:
        old = await repo.create(
            conn, "old", {"model": "retired-model", "effort": "retired", "image": "gone:1"}
        )
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        stored = old.spec
        r = await http.put(f"{P}/{old.id}", json={"name": "old", "spec": {**stored, "replicas": 2}})
        assert r.status_code == 200, r.text
        assert r.json()["spec"]["model"] == "retired-model"
        for key, value in (("model", "other-retired"), ("effort", "other"), ("image", "x:2")):
            spec = {**stored, key: value}
            assert_error(
                await http.put(f"{P}/{old.id}", json={"name": "old", "spec": spec}),
                422,
                "invalid_request",
            )
        # POST has no stored preset, so the same values are refused there
        assert_error(
            await http.post(P, json={"name": "new", "spec": {"model": "retired-model"}}),
            422,
            "invalid_request",
        )
        assert_error(
            await http.post(P, json={"name": "new", "spec": {"effort": "retired"}}),
            422,
            "invalid_request",
        )


async def test_reuse_running_agent_round_trip(http):
    made = await make(http, "reuse", {"replicas": 2, "reuse_running_agent": True})
    assert made["spec"] == {"replicas": 2, "reuse_running_agent": True}
    got = (await http.get(f"{P}/{made['id']}")).json()
    assert got["spec"]["reuse_running_agent"] is True

    plain = await make(http, "plain", {"replicas": 2})
    assert "reuse_running_agent" not in (await http.get(f"{P}/{plain['id']}")).json()["spec"]

    c = await http.post("/api/v1/sessions", json={"preset_id": made["id"], "slug": "reuse-c"})
    assert c.status_code == 201, c.text
    assert c.json()["spec"]["reuse_running_agent"] is True
    p = await http.post("/api/v1/sessions", json={"preset_id": plain["id"], "slug": "plain-c"})
    assert p.status_code == 201, p.text
    assert p.json()["spec"]["reuse_running_agent"] is None
    assert {k: v for k, v in c.json()["spec"].items() if k != "reuse_running_agent"} == {
        k: v for k, v in p.json()["spec"].items() if k != "reuse_running_agent"
    }

    r = await http.put(f"{P}/{made['id']}", json={"name": "reuse", "spec": {"replicas": 2}})
    assert r.status_code == 200, r.text
    assert "reuse_running_agent" not in r.json()["spec"]
    assert "reuse_running_agent" not in (await http.get(f"{P}/{made['id']}")).json()["spec"]


async def test_create_default_not_manageable(http):
    body = await make(http, "plain")
    assert body["superleader_manageable"] is False


async def test_create_manageable(http):
    r = await http.post(P, json={"name": "m", "superleader_manageable": True})
    assert r.status_code == 201, r.text
    assert r.json()["superleader_manageable"] is True
    got = (await http.get(f"{P}/{r.json()['id']}")).json()
    assert got["superleader_manageable"] is True


async def test_put_without_flag_keeps_it(http):
    r = await http.post(P, json={"name": "m", "superleader_manageable": True})
    pid = r.json()["id"]
    r = await http.put(f"{P}/{pid}", json={"name": "m", "spec": {"replicas": 2}})
    assert r.status_code == 200, r.text
    assert r.json()["superleader_manageable"] is True


async def test_put_sets_flag(http):
    pid = (await make(http, "m"))["id"]
    for value in (False, True):
        r = await http.put(
            f"{P}/{pid}", json={"name": "m", "spec": {}, "superleader_manageable": value}
        )
        assert r.status_code == 200, r.text
        assert r.json()["superleader_manageable"] is value


async def test_copy_keeps_flag(http):
    managed = (await http.post(P, json={"name": "m", "superleader_manageable": True})).json()
    plain = await make(http, "p")
    c1 = await http.post(f"{P}/{managed['id']}/copy")
    c2 = await http.post(f"{P}/{plain['id']}/copy")
    assert c1.json()["superleader_manageable"] is True
    assert c2.json()["superleader_manageable"] is False


async def test_list_shows_flag(http):
    await make(http, "p")
    rows = (await http.get(P)).json()
    assert rows and all(r["superleader_manageable"] is False for r in rows)


async def test_put_flag_roundtrip_keeps_spec(pool, dsn):
    from fakes.fake_kube import FakeKube

    app = create_app(make_settings(dsn, max_replicas=4), pool, kube=FakeKube(namespace="agents"))
    spec = {"env": {"API_KEY": {"protected": True, "value": "s3cret"}}}
    async with client_for(app) as http:
        r = await http.post(P, json={"name": "p", "spec": spec})
        assert r.status_code == 201, r.text
        first = (await http.get(f"{P}/{r.json()['id']}")).json()
        # the read-only metadata of a protected variable is not accepted back as input
        sent = {
            "env": {
                k: {f: v for f, v in e.items() if f not in ("set", "version", "updated_at")}
                for k, e in first["spec"]["env"].items()
            }
        }
        r = await http.put(
            f"{P}/{first['id']}",
            json={"name": first["name"], "spec": sent, "superleader_manageable": True},
        )
        assert r.status_code == 200, r.text
        again = (await http.get(f"{P}/{first['id']}")).json()
    assert again["spec"] == first["spec"]
    assert again["spec"]["env"]["API_KEY"]["version"] == 1
    assert again["superleader_manageable"] is True
