"""api.session_spec.patch: the merge-patch of a live session's spec with the preset guards, the
fixed fields, the leader tighten-only rule, and the recheck on the locked row (service level,
real Postgres, a fake request)."""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets
import uuid
from types import SimpleNamespace

import psycopg
import pytest
from factories import attach_preset
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from test_agent_session_messaging import Sess, alone, hive
from test_api_agent import make_settings

from agents_orchestrator.api import schemas, session_spec
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.spec import SessionSpec, default_cache_key, fill_resource_defaults

GRAFANA = {"type": "stdio", "command": "mise", "args": ["x", "mcp-grafana@0.1.0", "--", "-t"]}
NEW_ARGS = ["x", "mcp-grafana@0.2.0", "--", "-t", "--disable-write"]
DENY = ["Bash(rm:*)"]


@pytest.fixture
def settings(dsn):
    return make_settings(dsn).model_copy(
        update={
            "docker_image": "docker:27-dind",
            "kube_namespace_access_namespaces": ["media", "games"],
            "kube_namespace_access_tiers": ["read", "exec"],
        }
    )


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


def _request() -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace())


async def _seed(pool, settings, sess: Sess, extra: dict, *, versions: dict | None = None) -> dict:
    """Store `extra` over the helper's replica fields as a full dump, as `sessions.create`
    would; returns the stored dict."""
    base = dict(sess.row["spec"])
    spec = fill_resource_defaults(SessionSpec.model_validate({**base, **extra}), settings)
    stored = spec.model_dump(mode="json", exclude_none=True)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET spec = %s, secret_versions = %s WHERE id = %s",
            (Jsonb(stored), Jsonb(versions or {}), sess.id),
        )
    return stored


async def _row(pool, session_id) -> dict:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT spec, generation, desired, preset_id FROM sessions WHERE id = %s",
            (session_id,),
        )
        return await cur.fetchone()


async def _preset_row(pool, preset_id) -> dict:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM presets WHERE id = %s", (preset_id,))
        return await cur.fetchone()


async def _exec(pool, query: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(query, params)


async def _patch(
    pool,
    settings,
    target: Sess,
    caller: Sess,
    spec: dict,
    *,
    request=None,
    agent_id=None,
) -> schemas.SessionView:
    """Run the service as the route would after its gate: superleader is the caller's
    session_messaging, the agent is the caller's leader unless given."""
    request = request if request is not None else _request()
    async with pool.connection() as conn:
        target_s = await sessions_repo.get(conn, target.id)
        caller_s = await sessions_repo.get(conn, caller.id)
        agent = await agents_repo.get_agent(conn, agent_id or caller.leader_id)
        return await session_spec.patch(
            request,
            conn,
            settings,
            target_s,
            schemas.SessionSpecPatch(spec=spec),
            caller=caller_s,
            caller_agent=agent,
            superleader=bool(caller_s.spec.session_messaging),
        )


async def _refused(
    pool, settings, target, caller, spec, status, code, fields=None, *, request=None, **kw
):
    before = await _row(pool, target.id)
    request = request if request is not None else _request()
    with pytest.raises(ApiError) as caught:
        await _patch(pool, settings, target, caller, spec, request=request, **kw)
    exc = caught.value
    assert (exc.status, exc.code) == (status, code), exc.message
    if fields is not None:
        assert request.state.preset_fields == fields
        assert exc.extra["fields"] == fields
    if code in ("human_only_field", "privileged_target"):
        assert exc.extra["proposable"] is True
        assert exc.message.endswith("; send it as a proposal: proposal_create")
    assert await _row(pool, target.id) == before
    return exc


@pytest.fixture
async def boss(pool):
    return await alone(pool, "svc-boss", superleader=True)


async def _managed(pool, settings, slug: str, extra: dict, **kw) -> tuple[Sess, uuid.UUID, dict]:
    """A foreign session whose preset the superleader may manage."""
    sess = await alone(pool, slug)
    stored = await _seed(pool, settings, sess, extra, **kw)
    preset_id = await attach_preset(pool, sess.id)
    return sess, preset_id, stored


# ---------------------------------------------------------------- the use case


@pytest.mark.parametrize("superleader", [False, True])
async def test_own_privileged_session_replaces_mcp_args(pool, settings, superleader):
    """The use case: a kube_cluster_access, auto-mode session's leader edits its own MCP args.
    Arrays replace, the rest of the server entry is kept, generation bumps once, the source
    preset is untouched, and a stored protected variable is resent unchanged."""
    me = await alone(pool, "svc-own", superleader=superleader)
    extra = {
        "kube_cluster_access": True,
        "wrapper": {"permission_mode": "auto", "mcp_servers": {"grafana": GRAFANA}},
        "env": {"K": {"protected": True}},
    }
    await _seed(pool, settings, me, extra, versions={"K": "agents-k-1"})
    preset_id = await attach_preset(pool, me.id)
    preset_before = await _preset_row(pool, preset_id)
    before = await _row(pool, me.id)
    request = _request()

    view = await _patch(
        pool,
        settings,
        me,
        me,
        {"wrapper": {"mcp_servers": {"grafana": {"args": NEW_ARGS}}}},
        request=request,
    )

    server = view.spec.wrapper.mcp_servers["grafana"]
    assert (server.type, server.command, server.args) == ("stdio", "mise", NEW_ARGS)
    assert view.spec.kube_cluster_access is True and view.spec.wrapper.permission_mode == "auto"
    assert view.generation == before["generation"] + 1
    after = await _row(pool, me.id)
    assert after["generation"] == before["generation"] + 1
    assert after["spec"]["wrapper"]["mcp_servers"]["grafana"]["args"] == NEW_ARGS
    assert after["spec"]["env"] == {"K": {"protected": True, "expand": False}}
    assert request.state.preset_fields == ["wrapper.mcp_servers"]
    assert await _preset_row(pool, preset_id) == preset_before


@pytest.mark.parametrize("cache", ["deleted", "attached"])
async def test_preset_cache_key_session_patches(pool, settings, cache):
    """A session whose cache keys are its preset's derived key patches its MCP args, with the
    preset attached or deleted since (preset_id NULL)."""
    me = await alone(pool, f"svc-cache-{cache}")
    if cache == "attached":
        preset_id = await attach_preset(pool, me.id)
    else:
        preset_id = uuid.uuid4()
    key = default_cache_key(preset_id)
    extra = {
        "cache": {"restore_key": key, "save_key": key},
        "wrapper": {"mcp_servers": {"grafana": GRAFANA}},
    }
    await _seed(pool, settings, me, extra)
    assert (await _row(pool, me.id))["preset_id"] == (preset_id if cache == "attached" else None)
    before = await _row(pool, me.id)
    view = await _patch(
        pool, settings, me, me, {"wrapper": {"mcp_servers": {"grafana": {"args": NEW_ARGS}}}}
    )
    assert view.spec.wrapper.mcp_servers["grafana"].args == NEW_ARGS
    assert view.spec.cache.restore_key == key
    assert (await _row(pool, me.id))["generation"] == before["generation"] + 1


async def test_empty_patch_is_a_no_op(pool, settings):
    me = await alone(pool, "svc-empty")
    await _seed(pool, settings, me, {"wrapper": {"mcp_servers": {"grafana": GRAFANA}}})
    before = await _row(pool, me.id)
    request = _request()
    view = await _patch(pool, settings, me, me, {}, request=request)
    assert view.generation == before["generation"]
    assert await _row(pool, me.id) == before
    assert request.state.preset_fields == []


# ---------------------------------------------------------------- human-only fields

HUMAN_ONLY = [
    ("kube_cluster_access", {}, {"kube_cluster_access": True}),
    ("kube_namespace_access", {}, {"kube_namespace_access": {"media": ["read"]}}),
    (
        "kube_namespace_access",
        {"kube_namespace_access": {"media": ["read"]}},
        {"kube_namespace_access": {"media": ["read", "exec"]}},
    ),
    (
        "kube_namespace_access",
        {"kube_namespace_access": {"media": ["read"]}},
        {"kube_namespace_access": {"media": ["read"], "games": ["read"]}},
    ),
    ("session_messaging", {}, {"session_messaging": True}),
    ("docker", {}, {"docker": True}),
    ("discard_hook_url", {}, {"discard_hook_url": "https://h.example/hook"}),
    (
        "wrapper.permission_mode",
        {"wrapper": {"permission_mode": "auto"}},
        {"wrapper": {"permission_mode": "bypassPermissions"}},
    ),
    ("wrapper.permissions.allow", {}, {"wrapper": {"permissions": {"allow": ["Bash"]}}}),
    (
        "wrapper.permissions.deny",
        {"wrapper": {"permissions": {"deny": DENY}}},
        {"wrapper": {"permissions": {"deny": []}}},
    ),
    ("wrapper.permissions.deny", {"wrapper": {"permissions": {"deny": DENY}}}, {"wrapper": None}),
    (
        "wrapper.permissions.deny",
        {"wrapper": {"permissions": {"deny": DENY}}},
        {"wrapper": {"permissions": None}},
    ),
]


@pytest.mark.parametrize("own", [True, False], ids=["own", "foreign"])
@pytest.mark.parametrize(("path", "stored", "patch"), HUMAN_ONLY)
async def test_human_only_field_refused(pool, settings, boss, own, path, stored, patch):
    if own:
        target = caller = await alone(pool, "svc-ho-own")
        await _seed(pool, settings, target, stored)
    else:
        target, _, _ = await _managed(pool, settings, "svc-ho-foreign", stored)
        caller = boss
    await _refused(pool, settings, target, caller, patch, 403, "human_only_field", [path])


TOWARD_SAFE = [
    ({"kube_cluster_access": True}, {"kube_cluster_access": False}),
    ({"docker": True}, {"docker": None}),
    (
        {"kube_namespace_access": {"media": ["read", "exec"], "games": ["read"]}},
        {"kube_namespace_access": {"media": ["read"]}},
    ),
    ({"kube_namespace_access": {"media": ["read"]}}, {"kube_namespace_access": None}),
    ({}, {"wrapper": {"permission_mode": "auto"}}),
    (
        {"wrapper": {"permissions": {"deny": DENY}}},
        {"wrapper": {"permissions": {"deny": [*DENY, "Bash(curl:*)"]}}},
    ),
    (
        {"wrapper": {"permissions": {"allow": ["Bash", "Read"]}}},
        {"wrapper": {"permissions": {"allow": ["Read"]}}},
    ),
]


@pytest.mark.parametrize("own", [True, False], ids=["own", "foreign"])
@pytest.mark.parametrize(("stored", "patch"), TOWARD_SAFE)
async def test_toward_safe_passes(pool, settings, boss, own, stored, patch):
    if own:
        target = caller = await alone(pool, "svc-safe-own")
        await _seed(pool, settings, target, stored)
    else:
        target, _, _ = await _managed(pool, settings, "svc-safe-foreign", stored)
        caller = boss
    before = await _row(pool, target.id)
    await _patch(pool, settings, target, caller, patch)
    assert (await _row(pool, target.id))["generation"] == before["generation"] + 1


# ---------------------------------------------------------------- privileged target

FLAGS = ["kube_cluster_access", "session_messaging", "docker"]


@pytest.mark.parametrize("flag", FLAGS)
async def test_foreign_privileged_target_refused(pool, settings, boss, flag):
    target, _, _ = await _managed(pool, settings, "svc-priv", {flag: True})
    await _refused(
        pool,
        settings,
        target,
        boss,
        {"start_prompt": "p", "init_scripts": ["x"]},
        403,
        "privileged_target",
        ["init_scripts", "start_prompt"],
    )


@pytest.mark.parametrize("flag", FLAGS)
async def test_foreign_privileged_target_non_security_passes(pool, settings, boss, flag):
    target, _, _ = await _managed(pool, settings, "svc-priv-nonsec", {flag: True})
    patch = {"peers": False, "max_attempts": 3, "inactivity": {"close_seconds": 120}}
    view = await _patch(pool, settings, target, boss, patch)
    assert view.spec.peers is False and view.spec.max_attempts == 3
    assert view.spec.inactivity.close_seconds == 120


@pytest.mark.parametrize("flag", FLAGS)
async def test_foreign_privileged_target_usage_quota_passes(pool, settings, boss, flag):
    target, _, _ = await _managed(pool, settings, "svc-priv-quota", {flag: True})
    view = await _patch(pool, settings, target, boss, {"usage_quota": {"five_hour_pct": 40}})
    assert view.spec.usage_quota.five_hour_pct == 40


async def test_regular_leader_on_a_foreign_session_refused(pool, settings):
    """The gate keeps a regular leader on its own slug; the service refuses on its own too,
    before reading the target, and notes no paths."""
    plain = await alone(pool, "svc-plain")
    target, _, _ = await _managed(pool, settings, "svc-plain-target", {})
    request = _request()
    exc = await _refused(
        pool, settings, target, plain, {"peers": False}, 403, "not_superleader", request=request
    )
    assert exc.extra is None
    assert not hasattr(request.state, "preset_fields")


# ---------------------------------------------------------------- fixed fields

FIXED = [
    ("replicas", {"replicas": 1}),
    ("max_replicas", {"max_replicas": 3}),
    ("static", {"static": True}),
    ("workdir", {"workdir": "other"}),
    ("pvc_size", {"pvc_size": "5Gi"}),
    ("cache", {"cache": {"restore_key": "mine", "save_key": "mine"}}),
]


@pytest.mark.parametrize(("path", "patch"), FIXED)
async def test_fixed_field_refused(pool, settings, path, patch):
    me = await hive(pool, "svc-fixed")
    await _seed(pool, settings, me, {})
    await _refused(pool, settings, me, me, patch, 422, "session_fixed_field", [path])


# ---------------------------------------------------------------- protected variables


async def test_new_protected_declaration_refused(pool, settings):
    me = await alone(pool, "svc-new-protected")
    await _seed(pool, settings, me, {})
    exc = await _refused(
        pool,
        settings,
        me,
        me,
        {"env": {"NEW": {"protected": True}}},
        422,
        "secret_unset",
        ["env.NEW"],
    )
    assert "NEW" in exc.message


@pytest.mark.parametrize(
    "patch",
    [{"env": {"K": None}}, {"env": {"K": {"protected": False, "value": "plain"}}}],
    ids=["remove", "unprotect"],
)
async def test_protected_removal_or_unprotect_refused(pool, settings, patch):
    me = await alone(pool, "svc-unprotect")
    await _seed(pool, settings, me, {"env": {"K": {"protected": True}}}, versions={"K": "s-1"})
    await _refused(pool, settings, me, me, patch, 403, "human_only_field", ["env.K"])


@pytest.mark.parametrize(
    "patch",
    [
        lambda c: {"env": {"K": {"value": c}}},
        lambda c: {"env": {"NEW": {"protected": True, "value": c}}},
    ],
    ids=["stored", "new"],
)
async def test_secret_canary_refused_and_never_echoed(pool, settings, canary, caplog, patch):
    me = await alone(pool, "svc-canary")
    await _seed(pool, settings, me, {"env": {"K": {"protected": True}}}, versions={"K": "s-1"})
    caplog.set_level(logging.DEBUG)
    exc = await _refused(pool, settings, me, me, patch(canary), 422, "secret_in_spec")
    assert canary not in exc.message and canary not in repr(exc.extra)
    assert canary not in caplog.text
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM events WHERE data::text LIKE %s", (f"%{canary}%",)
        )
        assert (await cur.fetchone())[0] == 0
    assert canary not in str((await _row(pool, me.id))["spec"])


# ---------------------------------------------------------------- leader tighten-only

LIMITS = {
    "usage_quota": {"five_hour_pct": 50},
    "inactivity": {"reminder_seconds": 1800, "close_seconds": 7200},
}
LOOSER = [
    (["usage_quota.five_hour_pct"], {"usage_quota": {"five_hour_pct": 80}}),
    (["usage_quota.five_hour_pct"], {"usage_quota": None}),
    (["inactivity.close_seconds"], {"inactivity": {"close_seconds": 14400}}),
    (["inactivity.close_seconds"], {"inactivity": {"close_seconds": 0}}),
    (["inactivity.reminder_seconds"], {"inactivity": {"reminder_seconds": 0}}),
]
TIGHTER = [
    {"usage_quota": {"five_hour_pct": 30, "seven_day_pct": 90}},
    {"inactivity": {"close_seconds": 3600}},
    {"inactivity": {"reminder_seconds": 600}},
]


@pytest.mark.parametrize(("fields", "patch"), LOOSER)
async def test_leader_cannot_loosen_limits(pool, settings, fields, patch):
    me = await alone(pool, "svc-loosen")
    await _seed(pool, settings, me, LIMITS)
    await _refused(pool, settings, me, me, patch, 403, "human_only_field", fields)


async def test_leader_cannot_loosen_against_the_setting_default(pool, settings):
    """An unset limit reads as the setting: with a 2 hour close setting, 4 hours is looser."""
    tight = settings.model_copy(
        update={"inactivity_reminder_seconds": 600, "inactivity_close_seconds": 7200}
    )
    me = await alone(pool, "svc-loosen-default")
    await _seed(pool, tight, me, {})
    await _refused(
        pool,
        tight,
        me,
        me,
        {"inactivity": {"close_seconds": 14400}},
        403,
        "human_only_field",
        ["inactivity.close_seconds"],
    )


@pytest.mark.parametrize("key", ["five_hour_pct", "seven_day_pct"])
async def test_leader_cannot_raise_an_unset_quota_above_the_setting_default(pool, settings, key):
    """An unset quota reads as `usage_quota_default`: with a default of 50, 80 is looser and
    40 is tighter."""
    low = settings.model_copy(update={"usage_quota_default": 50})
    me = await alone(pool, f"svc-quota-default-{key[:4]}")
    stored = await _seed(pool, low, me, {})
    assert "usage_quota" not in stored
    await _refused(
        pool,
        low,
        me,
        me,
        {"usage_quota": {key: 80}},
        403,
        "human_only_field",
        [f"usage_quota.{key}"],
    )
    before = await _row(pool, me.id)
    await _patch(pool, low, me, me, {"usage_quota": {key: 40}})
    assert (await _row(pool, me.id))["generation"] == before["generation"] + 1


@pytest.mark.parametrize("patch", TIGHTER)
async def test_leader_may_tighten_limits(pool, settings, patch):
    me = await alone(pool, "svc-tighten")
    await _seed(pool, settings, me, LIMITS)
    before = await _row(pool, me.id)
    await _patch(pool, settings, me, me, patch)
    assert (await _row(pool, me.id))["generation"] == before["generation"] + 1


@pytest.mark.parametrize("own", [True, False], ids=["own", "foreign"])
@pytest.mark.parametrize(("fields", "patch"), LOOSER)
async def test_superleader_may_loosen_limits(pool, settings, boss, own, fields, patch):
    if own:
        target = boss
        await _seed(pool, settings, boss, LIMITS)
    else:
        target, _, _ = await _managed(pool, settings, "svc-sl-loosen", LIMITS)
    before = await _row(pool, target.id)
    await _patch(pool, settings, target, boss, patch)
    assert (await _row(pool, target.id))["generation"] == before["generation"] + 1


# ---------------------------------------------------------------- races (recheck on the lock)


def _between_read_and_write(monkeypatch, change, *, times: int = 1):
    """Run `change()` (another connection) after the service read the row and before
    `replace_spec` locks it, on the first `times` calls; returns the call count list."""
    real = sessions_repo.replace_spec
    calls: list[int] = []

    async def hooked(conn, session_id, expected, spec, recheck):
        calls.append(1)
        if len(calls) <= times:
            await change()
        return await real(conn, session_id, expected, spec, recheck)

    monkeypatch.setattr(sessions_repo, "replace_spec", hooked)
    return calls


async def test_race_grant_turned_on_refuses_foreign_edit(pool, settings, boss, monkeypatch):
    target, _, _ = await _managed(pool, settings, "svc-race-grant", {})

    async def grant():
        await _exec(
            pool,
            "UPDATE sessions SET spec = spec || '{\"kube_cluster_access\": true}'::jsonb "
            "WHERE id = %s",
            (target.id,),
        )

    calls = _between_read_and_write(monkeypatch, grant)
    await _refused_after_change(
        pool,
        settings,
        target,
        boss,
        {"start_prompt": "p"},
        403,
        "privileged_target",
        ["start_prompt"],
    )
    assert len(calls) == 1  # SpecChanged, then the re-read refuses before any lock
    assert (await _row(pool, target.id))["spec"]["kube_cluster_access"] is True


async def test_race_manageable_turned_off_404(pool, settings, boss, monkeypatch):
    target, preset_id, _ = await _managed(pool, settings, "svc-race-hidden", {})

    async def hide():
        await _exec(
            pool, "UPDATE presets SET superleader_manageable = false WHERE id = %s", (preset_id,)
        )

    _between_read_and_write(monkeypatch, hide)
    await _refused_after_change(pool, settings, target, boss, {"peers": False}, 404, "not_found")


async def test_race_preset_deleted_404(pool, settings, boss, monkeypatch):
    target, preset_id, _ = await _managed(pool, settings, "svc-race-gone", {})

    async def drop():
        await _exec(pool, "DELETE FROM presets WHERE id = %s", (preset_id,))

    _between_read_and_write(monkeypatch, drop)
    await _refused_after_change(pool, settings, target, boss, {"peers": False}, 404, "not_found")


async def test_race_leadership_moved_403(pool, settings, monkeypatch):
    me = await hive(pool, "svc-race-leader")
    await _seed(pool, settings, me, {})
    async with pool.connection() as conn:
        members = await agents_repo.list_agents(conn, me.id)
    other = next(a.id for a in members if a.id != me.leader_id)

    async def hand_over():
        await _exec(
            pool, "UPDATE session_leaders SET agent_id = %s WHERE session_id = %s", (other, me.id)
        )

    _between_read_and_write(monkeypatch, hand_over)
    await _refused_after_change(pool, settings, me, me, {"peers": False}, 403, "not_leader")


async def test_race_secret_versions_dropped_422(pool, settings, monkeypatch):
    """The spec is unchanged, so only the guards re-run on the locked row see that the
    protected variable lost its Secret: 422 secret_unset, nothing written."""
    me = await alone(pool, "svc-race-versions")
    await _seed(pool, settings, me, {"env": {"K": {"protected": True}}}, versions={"K": "s-1"})

    async def drop_versions():
        await _exec(pool, "UPDATE sessions SET secret_versions = '{}' WHERE id = %s", (me.id,))

    calls = _between_read_and_write(monkeypatch, drop_versions)
    await _refused_after_change(
        pool, settings, me, me, {"peers": False}, 422, "secret_unset", ["env.K"]
    )
    assert len(calls) == 1


async def test_race_session_deleting_409(pool, settings, monkeypatch):
    me = await alone(pool, "svc-race-deleting")
    await _seed(pool, settings, me, {})

    async def delete():
        await _exec(pool, "UPDATE sessions SET desired = 'deleting' WHERE id = %s", (me.id,))

    _between_read_and_write(monkeypatch, delete)
    await _refused_after_change(pool, settings, me, me, {"peers": False}, 409, "session_deleting")


async def test_race_concurrent_scale_retries_and_keeps_replicas(pool, settings, monkeypatch):
    me = await hive(pool, "svc-race-scale")
    await _seed(pool, settings, me, {"max_replicas": 3})
    before = await _row(pool, me.id)

    async def scale():
        async with pool.connection() as conn:
            await sessions_repo.set_replicas(conn, me.id, 3)

    calls = _between_read_and_write(monkeypatch, scale)
    view = await _patch(pool, settings, me, me, {"peers": False})
    assert len(calls) == 2
    after = await _row(pool, me.id)
    assert after["spec"]["replicas"] == 3 and after["spec"]["peers"] is False
    assert after["generation"] == before["generation"] + 1
    assert view.spec.replicas == 3 and view.spec.peers is False


async def test_race_session_messaging_turned_off_applies_leader_rules(pool, settings, monkeypatch):
    """A superleader loosening its own limit while a human turns `session_messaging` off: the
    retry derives the rules from the fresh row, not the gate, so the loosening is refused."""
    me = await alone(pool, "svc-race-demoted", superleader=True)
    await _seed(pool, settings, me, LIMITS)

    async def demote():
        await _exec(
            pool,
            "UPDATE sessions SET spec = spec || '{\"session_messaging\": false}'::jsonb"
            " WHERE id = %s",
            (me.id,),
        )

    calls = _between_read_and_write(monkeypatch, demote)
    request = _request()
    with pytest.raises(ApiError) as caught:
        await _patch(
            pool, settings, me, me, {"usage_quota": {"five_hour_pct": 80}}, request=request
        )
    assert (caught.value.status, caught.value.code) == (403, "human_only_field")
    assert caught.value.extra["fields"] == ["usage_quota.five_hour_pct"]
    assert request.state.preset_fields == ["usage_quota.five_hour_pct"]
    assert len(calls) == 1
    after = await _row(pool, me.id)
    assert after["spec"]["usage_quota"] == {"five_hour_pct": 50}
    assert after["spec"]["session_messaging"] is False


async def test_spec_changed_three_times_is_a_conflict(pool, settings, monkeypatch):
    me = await alone(pool, "svc-race-conflict")
    await _seed(pool, settings, me, {})
    calls: list[int] = []

    async def always_changed(*_args):
        calls.append(1)
        raise sessions_repo.SpecChanged(str(me.id))

    monkeypatch.setattr(sessions_repo, "replace_spec", always_changed)
    await _refused(pool, settings, me, me, {"peers": False}, 409, "conflict")
    assert len(calls) == 3


async def _refused_after_change(pool, settings, target, caller, spec, status, code, fields=None):
    """Like `_refused`, but the row is compared to the state after the concurrent change: the
    refusal writes nothing and bumps no generation on top of it."""
    request = _request()
    gen_before = (await _row(pool, target.id))["generation"]
    with pytest.raises(ApiError) as caught:
        await _patch(pool, settings, target, caller, spec, request=request)
    exc = caught.value
    assert (exc.status, exc.code) == (status, code), exc.message
    if fields is not None:
        assert request.state.preset_fields == fields
        assert exc.extra["fields"] == fields
    after = await _row(pool, target.id)
    assert after["generation"] == gen_before
    assert after["spec"].get("peers") is True


async def _wait_for_lock_wait(pool, timeout=5.0):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
            )
            if (await cur.fetchone())[0]:
                return
        await asyncio.sleep(0.05)
    raise AssertionError("no backend waits on a lock")


async def test_deadlock_with_preset_delete_is_retried(pool, dsn, settings, boss, monkeypatch):
    """The recheck takes the preset FOR SHARE while holding the session row; a preset delete
    holds the preset and waits on the session row (ON DELETE SET NULL). Postgres aborts the
    PATCH with DeadlockDetected (the delete waits longer before its own check); the service
    retries, finds the preset gone and answers 404, and the delete commits."""
    target, preset_id, _ = await _managed(pool, settings, "svc-deadlock", {})
    before = await _row(pool, target.id)
    entered, release = asyncio.Event(), asyncio.Event()
    real = sessions_repo.replace_spec
    calls: list[int] = []

    async def held(conn, session_id, expected, spec, recheck):
        calls.append(1)

        async def paused(locked):
            if len(calls) == 1:
                entered.set()
                await release.wait()
            await recheck(locked)

        return await real(conn, session_id, expected, spec, paused)

    monkeypatch.setattr(sessions_repo, "replace_spec", held)

    async def delete_preset():
        async with await psycopg.AsyncConnection.connect(dsn) as conn:
            await conn.execute("SET deadlock_timeout = '10s'")
            await conn.commit()
            await presets_repo.delete(conn, preset_id)
            await conn.commit()

    patching = asyncio.create_task(_patch(pool, settings, target, boss, {"peers": False}))
    await asyncio.wait_for(entered.wait(), 5)
    deleting = asyncio.create_task(delete_preset())
    await _wait_for_lock_wait(pool)
    release.set()
    results = await asyncio.gather(patching, deleting, return_exceptions=True)
    assert isinstance(results[0], ApiError), results[0]
    assert (results[0].status, results[0].code) == (404, "not_found")
    assert results[1] is None
    assert len(calls) == 2
    after = await _row(pool, target.id)
    assert after["preset_id"] is None
    assert after["spec"] == before["spec"] and after["generation"] == before["generation"]
