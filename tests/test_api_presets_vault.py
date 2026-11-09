"""O4: preset saves validate vault refs through the vault, copy keeps them, ref changes leave
names-only events and the re-attach list shrinks."""

from __future__ import annotations

import json
import uuid

import httpx
import psycopg
import pytest
import pytest_asyncio
from test_agent_session_messaging import alone
from test_api_agent import client_for as agent_client
from test_api_agent import headers
from test_api_agent import make_settings as agent_settings
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as repo

P = "/api/v1/presets"
A = f"{ADMIN_PREFIX}/presets"
ENTRY = "11111111-1111-4111-8111-111111111111"
OTHER = "22222222-2222-4222-8222-222222222222"
GONE = "33333333-3333-4333-8333-333333333333"
CANARY = "canary-value-must-never-show"


class FakeVault:
    """Entries: id -> (name, envs, required label). The vault, not the orchestrator, decides
    whether labels match."""

    def __init__(self):
        self.calls: list[dict] = []
        self.down = False
        self.after_resolve = None
        self.entries = {
            ENTRY: ("github", {"TOKEN", "USER"}, "team-a"),
            OTHER: ("gitlab", {"TOKEN"}, None),
        }

    def handler(self, req: httpx.Request) -> httpx.Response:
        if self.down:
            return httpx.Response(503, text="down")
        body = json.loads(req.content)
        self.calls.append(body)
        labels = body["preset_labels"]
        out = {"resolved": {}, "unknown": [], "bad_env": [], "mismatched": []}
        for env, ref in body["refs"].items():
            found = self.entries.get(ref["entry"])
            if found is None:
                out["unknown"].append(env)
            elif ref["env"] not in found[1]:
                out["bad_env"].append(env)
            elif found[2] is not None and found[2] not in labels:
                out["mismatched"].append(env)
            else:
                out["resolved"][env] = {"name": found[0]}
        if self.after_resolve is not None:
            self.after_resolve()
        return httpx.Response(200, json=out)


@pytest.fixture
def vault():
    return FakeVault()


@pytest_asyncio.fixture
async def http(pool, dsn, vault):
    settings = make_settings(dsn, vault_url="http://vault", vault_token="t")
    app = create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))
    async with client_for(app) as c:
        c.app = app
        yield c


def agent_app(dsn, pool, vault):
    settings = agent_settings(dsn).model_copy(
        update={
            "vault_url": "http://vault",
            "vault_token": "t",
            "kube_cluster_access_enabled": True,
        }
    )
    return create_app(settings, pool, vault_transport=httpx.MockTransport(vault.handler))


def ref(entry=ENTRY, env="TOKEN"):
    return {"vault": entry, "env": env}


async def make(http, name, env=None, labels=None, project=None):
    body = {"name": name, "spec": {"env": env or {}}}
    if labels is not None:
        body["labels"] = labels
    if project is not None:
        body["project"] = project
    r = await http.post(P, json=body)
    assert r.status_code == 201, r.text
    return r.json()


async def test_save_with_unknown_entry_422(http):
    r = await http.post(P, json={"name": "a", "spec": {"env": {"X": ref(GONE)}}})
    assert_error(r, 422, "vault_entry_unknown")
    err = r.json()["error"]
    assert err["envs"] == ["X"] and err["entries"] == [GONE]


async def test_save_with_bad_env_422(http):
    r = await http.post(P, json={"name": "a", "spec": {"env": {"X": ref(ENTRY, "NOPE")}}})
    assert_error(r, 422, "vault_env_unknown")
    assert r.json()["error"]["envs"] == ["X"]


async def test_label_change_revalidates_refs(http):
    made = await make(http, "a", {"X": ref()}, labels=["team-a"])
    r = await http.put(
        f"{P}/{made['id']}", json={"name": "a", "spec": {"env": {"X": ref()}}, "labels": []}
    )
    assert_error(r, 422, "vault_label_mismatch")
    assert r.json()["error"]["envs"] == ["X"]
    assert (await http.get(f"{P}/{made['id']}")).json()["labels"] == ["team-a"]


async def test_omitted_labels_are_the_stored_ones(http, vault):
    made = await make(http, "a", {"X": ref()}, labels=["team-a"])
    r = await http.put(f"{P}/{made['id']}", json={"name": "a", "spec": {"env": {"X": ref()}}})
    assert r.status_code == 200, r.text
    assert "team-a" in vault.calls[-1]["preset_labels"]


async def test_project_move_revalidates_refs(http, vault):
    made = await make(http, "a", {"X": ref()}, labels=["team-a"])
    assert (
        await http.post("/api/v1/projects", json={"slug": "other", "name": "Other"})
    ).status_code == 201
    r = await http.put(
        f"{P}/{made['id']}",
        json={"name": "a", "spec": {"env": {"X": ref()}}, "project": "other"},
    )
    assert r.status_code == 200, r.text
    assert "project:other" in vault.calls[-1]["preset_labels"]
    assert vault.calls[-1]["preset_labels"][0] == "project:other"


async def test_vault_down_on_save_with_refs_503(http, vault):
    vault.down = True
    r = await http.post(P, json={"name": "a", "spec": {"env": {"X": ref()}}})
    assert_error(r, 503, "vault_unavailable")
    assert (await http.get(P)).json()[0]["name"] == "Default"
    assert len((await http.get(P)).json()) == 1


async def test_save_without_refs_never_calls_vault(http, vault):
    vault.down = True
    made = await make(http, "a", {"P": {"value": "x"}})
    r = await http.put(f"{P}/{made['id']}", json={"name": "b", "spec": {}})
    assert r.status_code == 200
    assert (await http.post(f"{P}/{made['id']}/copy")).status_code == 201
    assert vault.calls == []


async def test_copy_keeps_refs_and_validates(http, vault):
    made = await make(http, "a", {"X": ref(), "Y": ref(OTHER)}, labels=["team-a"])
    r = await http.post(f"{P}/{made['id']}/copy")
    assert r.status_code == 201, r.text
    assert r.json()["spec"]["env"] == {"X": ref(), "Y": ref(OTHER)}
    assert vault.calls[-1]["preset_labels"][0] == "project:default"
    vault.down = True
    assert_error(await http.post(f"{P}/{made['id']}/copy"), 503, "vault_unavailable")


async def test_copy_into_another_project_revalidates(http):
    made = await make(http, "a", {"X": ref()}, labels=["team-a"])
    assert (
        await http.post("/api/v1/projects", json={"slug": "other", "name": "Other"})
    ).status_code == 201
    ok = await http.post(f"{P}/{made['id']}/copy", json={"project": "other"})
    assert ok.status_code == 201, ok.text


async def test_superleader_copy_validates_and_keeps_refs(pool, dsn, vault):
    boss = await alone(pool, "o4-boss", superleader=True)
    app = agent_app(dsn, pool, vault)
    async with client_for(app) as main:
        src = await make(main, "src", {"X": ref()}, labels=["team-a"])
        await main.put(
            f"{P}/{src['id']}",
            json={"name": "src", "spec": {"env": {"X": ref()}}, "superleader_manageable": True},
        )
    async with agent_client(app, boss.leader_ip) as agent:
        r = await agent.post(f"{A}/src/copy", headers=headers(boss.id))
        assert r.status_code == 201, r.text
        assert r.json()["spec"]["env"] == {"X": ref()}


async def test_reattach_pruned_on_save(http, pool):
    made = await make(http, "a", {"P": {"value": "x"}}, labels=["team-a"])
    async with pool.connection() as conn:
        await repo.set_vault_reattach(conn, uuid.UUID(made["id"]), ["P", "Q", "R"])
    r = await http.put(
        f"{P}/{made['id']}", json={"name": "a", "spec": {"env": {"P": {"value": "y"}}}}
    )
    assert r.status_code == 200, r.text
    assert r.json()["vault_reattach"] == ["Q", "R"]
    r = await http.put(f"{P}/{made['id']}", json={"name": "a", "spec": {"env": {"Q": ref()}}})
    assert r.json()["vault_reattach"] == ["R"]
    assert (await http.get(f"{P}/{made['id']}")).json()["vault_reattach"] == ["R"]


async def test_ref_events_names_only(http, pool):
    made = await make(http, "a", {"X": ref(), "P": {"value": CANARY}}, labels=["team-a"])
    pid = uuid.UUID(made["id"])
    await http.put(
        f"{P}/{made['id']}",
        json={"name": "a", "spec": {"env": {"X": ref(OTHER), "Y": ref(OTHER)}}},
    )
    await http.put(f"{P}/{made['id']}", json={"name": "a", "spec": {"env": {"Y": ref(OTHER)}}})
    async with pool.connection() as conn:
        rows = await repo.vault_events(conn, pid)
    got = [(r["kind"], r["actor"], r["data"]["env"], r["data"]["entry"]) for r in rows]
    assert got == [
        ("preset_vault_attached", "user", "X", ENTRY),
        ("preset_vault_attached", "user", "X", OTHER),
        ("preset_vault_attached", "user", "Y", OTHER),
        ("preset_vault_detached", "user", "X", ENTRY),
        ("preset_vault_detached", "user", "X", OTHER),
    ]
    assert rows[0]["data"] == {
        "preset_id": made["id"],
        "env": "X",
        "entry": ENTRY,
        "entry_env": "TOKEN",
    }
    assert CANARY not in json.dumps(rows)


async def test_privileged_preset_ref_change_allowed_for_superleader(pool, dsn, vault):
    boss = await alone(pool, "o4-priv", superleader=True)
    app = agent_app(dsn, pool, vault)
    async with client_for(app) as main:
        r = await main.post(
            P,
            json={
                "name": "priv",
                "spec": {"kube_cluster_access": True, "env": {"X": ref(), "P": {"value": "p"}}},
                "superleader_manageable": True,
                "labels": ["team-a"],
            },
        )
        assert r.status_code == 201, r.text
    async with agent_client(app, boss.leader_ip) as agent:
        new = {
            "kube_cluster_access": True,
            "env": {"X": ref(OTHER), "Y": ref(), "P": {"value": "p"}},
        }
        r = await agent.put(
            f"{A}/priv",
            headers=headers(boss.id),
            json={"name": "priv", "spec": new, "labels": ["team-a"]},
        )
        assert r.status_code == 200, r.text
        assert r.json()["spec"]["env"]["X"] == ref(OTHER)
    async with app.state.pool.connection() as conn:
        rows = await repo.vault_events(conn, uuid.UUID(r.json()["id"]))
    assert {x["actor"] for x in rows if x["data"]["env"] == "Y"} == {boss.slug}


async def test_privileged_plain_env_still_refused(pool, dsn, vault):
    boss = await alone(pool, "o4-plain", superleader=True)
    app = agent_app(dsn, pool, vault)
    async with client_for(app) as main:
        r = await main.post(
            P,
            json={
                "name": "priv",
                "spec": {"kube_cluster_access": True, "env": {"P": {"value": "p"}}},
                "superleader_manageable": True,
            },
        )
        assert r.status_code == 201, r.text
    async with agent_client(app, boss.leader_ip) as agent:
        new = {"kube_cluster_access": True, "env": {"P": {"value": "q"}}}
        r = await agent.put(
            f"{A}/priv", headers=headers(boss.id), json={"name": "priv", "spec": new}
        )
        assert_error(r, 403, "privileged_target")
        assert r.json()["error"]["fields"] == ["env.P"]


async def test_changed_between_check_and_lock_409(http, vault):
    made = await make(http, "a", {"X": ref()}, labels=["team-a"])
    pid = uuid.UUID(made["id"])

    def concurrent_save():
        # another save drops the label after the vault check passed
        with psycopg.connect(http.app.state.settings.database_url, autocommit=True) as c:
            c.execute("UPDATE presets SET labels = '{}' WHERE id = %s", (pid,))

    vault.after_resolve = concurrent_save
    r = await http.put(f"{P}/{made['id']}", json={"name": "a", "spec": {"env": {"X": ref()}}})
    assert_error(r, 409, "conflict")
    assert "meanwhile" in r.json()["error"]["message"]
