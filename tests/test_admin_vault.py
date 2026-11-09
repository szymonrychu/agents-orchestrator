"""O7: a superleader attaches and detaches vault entries on manageable presets through the
entry-based admin routes; the save path is the shared one (vault check, row lock, events)."""

from __future__ import annotations

import json
import logging
import uuid

import httpx
import pytest
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for as agent_client
from test_api_agent import headers
from test_api_agent import make_settings as agent_settings
from test_api_presets_vault import ENTRY, GONE, OTHER, FakeVault, ref
from test_api_sessions import assert_error, client_for

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as repo

P = "/api/v1/presets"
A = f"{ADMIN_PREFIX}/presets"
HARBOR = "44444444-4444-4444-8444-444444444444"
CANARY = "canary-value-must-never-show"


class ListingVault(FakeVault):
    """The fake vault that also lists its entries (GET /vault/v1/entries)."""

    def __init__(self):
        super().__init__()
        self.entries[HARBOR] = ("harbor", {"HARBOR_PASSWORD", "HARBOR_USER"}, None)
        self.listed = 0

    def handler(self, req: httpx.Request) -> httpx.Response:
        if self.down:
            return httpx.Response(503, text="down")
        if req.method == "GET":
            self.listed += 1
            return httpx.Response(
                200,
                json=[
                    {
                        "id": eid,
                        "name": name,
                        "envs": [
                            {"env": e, "protected": True, "value": None} for e in sorted(envs)
                        ],
                    }
                    for eid, (name, envs, _) in self.entries.items()
                ],
            )
        return super().handler(req)


@pytest.fixture
def vault():
    return ListingVault()


@pytest.fixture
async def boss(pool):
    return await alone(pool, "o7-boss", superleader=True)


@pytest.fixture
def app(pool, dsn, vault):
    settings = agent_settings(dsn).model_copy(
        update={
            "vault_url": "http://vault",
            "vault_token": "t",
            "kube_cluster_access_enabled": True,
        }
    )
    return create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))


@pytest.fixture
def call(app, boss):
    async def go(method, path, **kw):
        async with agent_client(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    return go


async def make(app, name, env=None, manageable=True, labels=("team-a",), **spec):
    async with client_for(app) as main:
        r = await main.post(
            P,
            json={
                "name": name,
                "spec": {"env": env or {}, **spec},
                "superleader_manageable": manageable,
                "labels": list(labels),
            },
        )
    assert r.status_code == 201, r.text
    return r.json()


async def vault_events(pool, preset_id):
    async with pool.connection() as conn:
        return await repo.vault_events(conn, uuid.UUID(preset_id))


async def test_not_superleader_403(pool, app, vault):
    plain = await alone(pool, "o7-plain")
    await make(app, "a")
    async with agent_client(app, plain.leader_ip) as http:
        r = await http.put(f"{A}/a/vault/{ENTRY}", headers=headers(plain.id), json={})
    assert_error(r, 403, "not_superleader")
    assert vault.calls == []


async def test_not_manageable_404(app, call, vault):
    await make(app, "hidden", manageable=False)
    for method in ("PUT", "DELETE"):
        kw = {"json": {}} if method == "PUT" else {}
        assert_error(await call(method, f"{A}/hidden/vault/{ENTRY}", **kw), 404, "not_found")
    assert vault.calls == [] and vault.listed == 0


async def test_privileged_preset_200(app, call):
    made = await make(app, "priv", kube_cluster_access=True)
    r = await call("PUT", f"{A}/priv/vault/{ENTRY}", json={"envs": {"TOKEN": "GH"}})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["env"]["GH"] == ref()
    r = await call("DELETE", f"{A}/priv/vault/{ENTRY}")
    assert r.status_code == 200, r.text
    assert "GH" not in r.json()["spec"]["env"]
    assert r.json()["id"] == made["id"]


async def test_attach_without_envs_attaches_every_env_of_the_entry(app, call, pool, vault):
    made = await make(app, "all")
    r = await call("PUT", f"{A}/all/vault/{ENTRY}", json={})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["env"] == {"TOKEN": ref(ENTRY, "TOKEN"), "USER": ref(ENTRY, "USER")}
    rows = await vault_events(pool, made["id"])
    assert sorted((x["kind"], x["data"]["env"]) for x in rows) == [
        ("preset_vault_attached", "TOKEN"),
        ("preset_vault_attached", "USER"),
    ]
    assert vault.listed == 1


async def test_attach_is_one_save(app, call, pool, monkeypatch):
    made = await make(app, "one")
    saves = []
    real = repo.update

    async def counting(*a, **kw):
        saves.append(1)
        return await real(*a, **kw)

    monkeypatch.setattr(repo, "update", counting)
    r = await call("PUT", f"{A}/one/vault/{ENTRY}", json={})
    assert r.status_code == 200, r.text
    assert len(saves) == 1
    assert r.json()["updated_at"] > made["updated_at"]
    rows = await vault_events(pool, made["id"])
    assert len([x for x in rows if x["kind"] == "preset_vault_attached"]) == 2
    assert {x["actor"] for x in rows} == {"o7-boss"}


async def test_unknown_entry_422(app, call, vault):
    await make(app, "u")
    r = await call("PUT", f"{A}/u/vault/{GONE}", json={"envs": {"TOKEN": "X"}})
    assert_error(r, 422, "vault_entry_unknown")
    assert r.json()["error"]["envs"] == ["X"] and r.json()["error"]["entries"] == [GONE]
    r = await call("PUT", f"{A}/u/vault/{GONE}", json={})
    assert_error(r, 422, "vault_entry_unknown")
    assert r.json()["error"]["entries"] == [GONE]


async def test_unknown_entry_env_422(app, call):
    await make(app, "e")
    r = await call("PUT", f"{A}/e/vault/{ENTRY}", json={"envs": {"NOPE": "X"}})
    assert_error(r, 422, "vault_env_unknown")
    assert r.json()["error"]["envs"] == ["X"]


async def test_label_mismatch_422(app, call):
    await make(app, "m", labels=())
    r = await call("PUT", f"{A}/m/vault/{ENTRY}", json={"envs": {"TOKEN": "X"}})
    assert_error(r, 422, "vault_label_mismatch")
    assert r.json()["error"]["envs"] == ["X"]
    r = await call("GET", f"{A}/m")
    assert "X" not in r.json()["spec"]["env"]


async def test_vault_down_503(app, call, vault):
    await make(app, "d")
    vault.down = True
    for body in ({}, {"envs": {"TOKEN": "X"}}):
        assert_error(await call("PUT", f"{A}/d/vault/{ENTRY}", json=body), 503, "vault_unavailable")


async def test_existing_plain_env_409_then_replace_200(app, call, pool):
    made = await make(app, "c", {"TOKEN": {"value": "plain"}, "KEEP": {"value": "k"}})
    body = {"envs": {"TOKEN": "TOKEN"}}
    r = await call("PUT", f"{A}/c/vault/{ENTRY}", json=body)
    assert_error(r, 409, "env_exists")
    assert r.json()["error"]["envs"] == ["TOKEN"]
    assert (await call("GET", f"{A}/c")).json()["spec"]["env"]["TOKEN"] == {"value": "plain"}
    r = await call("PUT", f"{A}/c/vault/{ENTRY}", json={**body, "replace": True})
    assert r.status_code == 200, r.text
    env = r.json()["spec"]["env"]
    assert env["TOKEN"] == ref() and env["KEEP"] == {"value": "k"}
    rows = await vault_events(pool, made["id"])
    assert [x["kind"] for x in rows] == ["preset_vault_attached"]


async def test_attaching_the_same_ref_again_is_not_a_conflict(app, call):
    await make(app, "same", {"TOKEN": ref()})
    r = await call("PUT", f"{A}/same/vault/{ENTRY}", json={"envs": {"TOKEN": "TOKEN"}})
    assert r.status_code == 200, r.text


async def test_repoint_to_another_entry_200_with_replace(app, call, pool):
    made = await make(app, "r", {"TOKEN": ref(ENTRY)})
    body = {"envs": {"TOKEN": "TOKEN"}}
    assert_error(await call("PUT", f"{A}/r/vault/{OTHER}", json=body), 409, "env_exists")
    r = await call("PUT", f"{A}/r/vault/{OTHER}", json={**body, "replace": True})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["env"]["TOKEN"] == ref(OTHER)
    rows = (await vault_events(pool, made["id"]))[1:]  # the first is the attach at create
    assert sorted((x["kind"], x["data"]["entry"]) for x in rows) == [
        ("preset_vault_attached", OTHER),
        ("preset_vault_detached", ENTRY),
    ]


async def test_one_entry_env_backs_several_preset_envs_across_calls(app, call, pool):
    """Binding ruling: HARBOR_PASSWORD attached as both HARBOR_PASSWORD and
    HARBOR_ADMIN_PASSWORD through two PUTs; the second keeps the first's refs."""
    made = await make(app, "harbor", {"PLAIN": {"value": "p"}})
    first = await call(
        "PUT", f"{A}/harbor/vault/{HARBOR}", json={"envs": {"HARBOR_PASSWORD": "HARBOR_PASSWORD"}}
    )
    assert first.status_code == 200, first.text
    second = await call(
        "PUT",
        f"{A}/harbor/vault/{HARBOR}",
        json={"envs": {"HARBOR_PASSWORD": "HARBOR_ADMIN_PASSWORD"}},
    )
    assert second.status_code == 200, second.text
    pw = ref(HARBOR, "HARBOR_PASSWORD")
    assert second.json()["spec"]["env"] == {
        "PLAIN": {"value": "p"},
        "HARBOR_PASSWORD": pw,
        "HARBOR_ADMIN_PASSWORD": pw,
    }
    stored = (await call("GET", f"{A}/harbor")).json()["spec"]["env"]
    assert stored == second.json()["spec"]["env"]
    rows = await vault_events(pool, made["id"])
    assert sorted((x["kind"], x["data"]["env"]) for x in rows) == [
        ("preset_vault_attached", "HARBOR_ADMIN_PASSWORD"),
        ("preset_vault_attached", "HARBOR_PASSWORD"),
    ]
    # replace only overwrites a taken name; it never drops the other refs
    third = await call(
        "PUT",
        f"{A}/harbor/vault/{HARBOR}",
        json={"envs": {"HARBOR_USER": "HARBOR_USER"}, "replace": True},
    )
    assert third.status_code == 200, third.text
    assert set(third.json()["spec"]["env"]) == {
        "PLAIN",
        "HARBOR_PASSWORD",
        "HARBOR_ADMIN_PASSWORD",
        "HARBOR_USER",
    }


async def test_two_entry_envs_to_one_preset_env_422(app, call):
    await make(app, "dup")
    r = await call("PUT", f"{A}/dup/vault/{ENTRY}", json={"envs": {"TOKEN": "X", "USER": "X"}})
    assert r.status_code == 422, r.text


async def test_detach_by_entry_and_by_env(app, call, pool):
    made = await make(app, "det")
    pw = "HARBOR_PASSWORD"
    await call("PUT", f"{A}/det/vault/{HARBOR}", json={"envs": {pw: pw}})
    await call("PUT", f"{A}/det/vault/{HARBOR}", json={"envs": {pw: "ADMIN_PW"}})
    await call("PUT", f"{A}/det/vault/{ENTRY}", json={"envs": {"TOKEN": "TOKEN"}})
    r = await call("DELETE", f"{A}/det/vault/{HARBOR}?env=ADMIN_PW")
    assert r.status_code == 200, r.text
    assert set(r.json()["spec"]["env"]) == {pw, "TOKEN"}
    assert_error(await call("DELETE", f"{A}/det/vault/{HARBOR}?env=ADMIN_PW"), 404, "not_attached")
    r = await call("DELETE", f"{A}/det/vault/{HARBOR}")
    assert set(r.json()["spec"]["env"]) == {"TOKEN"}
    rows = await vault_events(pool, made["id"])
    detached = sorted(x["data"]["env"] for x in rows if x["kind"] == "preset_vault_detached")
    assert detached == ["ADMIN_PW", pw]


async def test_detach_of_nothing_404(app, call):
    await make(app, "none", {"TOKEN": {"value": "plain"}, "X": ref(OTHER)})
    assert_error(await call("DELETE", f"{A}/none/vault/{ENTRY}"), 404, "not_attached")
    # a plain env of the same name is not a ref to the entry
    assert_error(await call("DELETE", f"{A}/none/vault/{ENTRY}?env=TOKEN"), 404, "not_attached")
    assert_error(await call("DELETE", f"{A}/nope/vault/{ENTRY}"), 404, "not_found")


async def test_generic_put_with_a_ref_obeys_the_same_checks(app, call):
    await make(app, "gen")
    body = {"name": "gen", "spec": {"env": {"X": ref(GONE)}}}
    assert_error(await call("PUT", f"{A}/gen", json=body), 422, "vault_entry_unknown")
    body = {"name": "gen", "spec": {"env": {"X": ref(ENTRY)}}, "labels": []}
    assert_error(await call("PUT", f"{A}/gen", json=body), 422, "vault_label_mismatch")
    body = {"name": "gen", "spec": {"env": {"X": ref(ENTRY)}}}
    assert (await call("PUT", f"{A}/gen", json=body)).status_code == 200


async def test_admin_call_carries_vault_names_only(app, call, pool, boss, caplog):
    caplog.set_level(logging.INFO)
    await make(app, "aud")
    await call("PUT", f"{A}/aud/vault/{ENTRY}", json={"envs": {"TOKEN": "GH"}})
    await call("DELETE", f"{A}/aud/vault/{ENTRY}")
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call' ORDER BY id",
            (boss.id,),
        )
        rows = [r["data"] for r in await cur.fetchall()]
    touched = [{"env": "GH", "entry": ENTRY, "entry_env": "TOKEN"}]
    assert [(r["method"], r["status"], r["vault"]) for r in rows] == [
        ("PUT", 200, touched),
        ("DELETE", 200, touched),
    ]
    assert CANARY not in json.dumps(rows)
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("admin ")]
    assert len(lines) == 2 and all(f"'entry': '{ENTRY}'" in line for line in lines)


async def test_concurrent_change_is_retried(app, call, monkeypatch):
    made = await make(app, "race", {"PLAIN": {"value": "p"}})
    real, calls = repo.update, []

    async def racing(conn, pid, name, spec, flag, labels, expected, project, recheck):
        if not calls:
            calls.append(1)
            other = {"env": {"PLAIN": {"value": "q"}}}
            async with app.state.pool.connection() as c:  # another writer, own transaction
                await real(c, pid, name, other, None, None, None, None)
        return await real(conn, pid, name, spec, flag, labels, expected, project, recheck)

    monkeypatch.setattr(repo, "update", racing)
    r = await call("PUT", f"{A}/race/vault/{ENTRY}", json={"envs": {"TOKEN": "TOKEN"}})
    assert r.status_code == 200, r.text
    assert r.json()["id"] == made["id"]
    assert r.json()["spec"]["env"] == {"PLAIN": {"value": "q"}, "TOKEN": ref()}
