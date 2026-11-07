"""api.proposals: create, revise, withdraw and read preset change proposals (plan 3.4), with
protected values staged as Secret versions (Q13 B) and the whole-proposal stale flag (Q6 B).
Service level: real Postgres, a fake kube, a fake request carrying `state`.

Every test that sends a value checks the canary never reaches a view, a table row or the log."""

from __future__ import annotations

import logging
import secrets as stdlib_secrets
from types import SimpleNamespace

import pytest
from fakes.fake_kube import FakeKube
from fastapi.exceptions import RequestValidationError
from pydantic import SecretStr
from test_agent_session_messaging import alone
from test_api_agent import make_settings

from agents_orchestrator.api import proposals, schemas, secret_store
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.presets import SECRET_MAX_BYTES
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as proposals_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.k8s import secrets as store
from agents_orchestrator.k8s.kube import KubeError

NS = "agents"
GRAFANA = {"type": "stdio", "command": "mise", "args": ["x", "mcp-grafana@0.1.0", "--", "-t"]}
MCP_PATCH = {"wrapper": {"mcp_servers": {"grafana": GRAFANA}}}
TABLES = (
    "change_proposals",
    "change_proposal_revisions",
    "change_proposal_items",
    "change_proposal_secrets",
    "events",
    "presets",
    "preset_secrets",
)


@pytest.fixture
def settings(dsn):
    return make_settings(dsn).model_copy(update={"docker_image": "docker:27-dind"})


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def canary(caplog):
    caplog.set_level(logging.DEBUG)
    value = "canary-" + stdlib_secrets.token_hex(16)
    yield value
    assert value not in caplog.text


def _request(kube) -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(admin_secrets=[]),
        app=SimpleNamespace(state=SimpleNamespace(kube=kube, kube_owned=False)),
    )


async def _session(pool, slug: str) -> sessions_repo.Session:
    sess = await alone(pool, slug, superleader=True)
    async with pool.connection() as conn:
        found = await sessions_repo.get(conn, sess.id)
    assert found is not None
    return found


@pytest.fixture
async def boss(pool):
    return await _session(pool, "prop-boss")


@pytest.fixture
async def preset(pool, kube, canary):
    """A privileged manageable preset with a protected TOKEN whose value is set (version 1)."""
    spec = {
        "docker": True,
        "env": {"TOKEN": {"protected": True}, "PLAIN": {"value": "plain"}},
    }
    async with pool.connection() as conn:
        found = await presets_repo.create(conn, "infra", spec, superleader_manageable=True)
        await store.set_secret(conn, kube, NS, found.id, "TOKEN", SecretStr(canary + "-live"))
    kube.reset_records()
    return found


async def _create(pool, kube, settings, caller, **body):
    request = _request(kube)
    body = {"kind": "preset", "target": "infra", "reason": "needs grafana", **body}
    async with pool.connection() as conn:
        view, created = await proposals.create(
            request,
            conn,
            settings,
            schemas.ProposalIn.model_validate(body),
            caller=caller,
            superleader=True,
        )
    return view, created, request


async def _revise(pool, kube, settings, caller, proposal_id, **body):
    async with pool.connection() as conn:
        return await proposals.revise(
            _request(kube),
            conn,
            settings,
            proposal_id,
            schemas.ProposalRevise.model_validate(body),
            caller=caller,
        )


async def _withdraw(pool, kube, settings, caller, proposal_id):
    async with pool.connection() as conn:
        return await proposals.withdraw(_request(kube), conn, settings, proposal_id, caller=caller)


async def _get(pool, settings, proposal_id, proposer):
    async with pool.connection() as conn:
        return await proposals.get_view(conn, settings, proposal_id, proposer=proposer)


async def _rows(pool, query: str, params: tuple = ()) -> list:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        return await cur.fetchall()


async def _reservations(pool, preset_id) -> list:
    return await _rows(
        pool,
        "SELECT name, version, proposal_id, revision, state FROM change_proposal_secrets"
        " WHERE preset_id = %s ORDER BY name, version",
        (preset_id,),
    )


async def _assert_nowhere(pool, value: str, *views) -> None:
    for view in views:
        assert value not in view.model_dump_json()
    for table in TABLES:
        for (row,) in await _rows(pool, f"SELECT row_to_json(t)::text FROM {table} t"):  # noqa: S608
            assert value not in row, table


def _secrets(kube) -> set[str]:
    return {k[2] for k in kube.objects if k[0] == "Secret"}


def _error(exc_info, status: int, code: str) -> ApiError:
    err = exc_info.value
    assert (err.status, err.code) == (status, code), err.message
    return err


# ---------------------------------------------------------------- create


async def test_create_stages_the_value_and_records_redacted_items(
    pool, kube, settings, boss, preset, canary
):
    before = await _rows(pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,))
    view, created, request = await _create(
        pool, kube, settings, boss, patch=MCP_PATCH, secrets={"TOKEN": canary}
    )
    assert created
    assert (view.kind, view.status, view.revision, view.stale) == ("preset", "open", 1, False)
    assert (view.target_id, view.target_name, view.proposer_slug) == (
        str(preset.id),
        "infra",
        "prop-boss",
    )
    assert view.proposer_superleader
    [rev] = view.revisions
    items = {i.path: i for i in rev.items}
    assert sorted(items) == ["env.TOKEN", "wrapper.mcp_servers"]
    token = items["env.TOKEN"]
    assert (token.security, token.grant, token.secret, token.decision) == (
        True,
        False,
        "changed",
        "pending",
    )
    assert token.before == {"protected": True, "expand": False, "set": True, "version": 1}
    assert token.after == {"protected": True, "expand": False}
    mcp = items["wrapper.mcp_servers"]
    assert (mcp.security, mcp.grant, mcp.secret) == (True, False, None)
    assert (mcp.before, mcp.after) == (None, {"grafana": GRAFANA})
    # one staged Secret, the value only in its stringData
    staged = store.secret_name(preset.id, "TOKEN", 2)
    assert [a.name for a in kube.applied("Secret")] == [staged]
    assert kube.applied("Secret")[0].obj["stringData"] == {"value": canary}
    assert staged in _secrets(kube)
    assert await _reservations(pool, preset.id) == [("TOKEN", 2, view.id, 1, "staged")]
    # the preset row and its live secret are untouched
    after = await _rows(pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,))
    assert after == before
    async with pool.connection() as conn:
        live = await store.list_secrets(conn, preset.id)
    assert [(m.name, m.version) for m in live] == [("TOKEN", 1)]
    # the audit gets names and paths only
    assert request.state.admin_secrets == ["TOKEN"]
    assert request.state.preset_fields == ["env.TOKEN", "wrapper.mcp_servers"]
    await _assert_nowhere(pool, canary, view)


async def test_a_new_protected_variable_is_a_set_item(pool, kube, settings, boss, preset, canary):
    view, _, _ = await _create(
        pool,
        kube,
        settings,
        boss,
        patch={"env": {"NEW": {"protected": True}}},
        secrets={"NEW": canary},
    )
    [item] = view.revisions[0].items
    assert (item.path, item.secret, item.before) == ("env.NEW", "set", None)
    assert item.after == {"protected": True, "expand": False}
    assert store.secret_name(preset.id, "NEW", 1) in _secrets(kube)
    await _assert_nowhere(pool, canary, view)


async def test_a_value_on_an_unchanged_declaration_is_one_item(
    pool, kube, settings, boss, preset, canary
):
    view, _, _ = await _create(pool, kube, settings, boss, patch={}, secrets={"TOKEN": canary})
    [item] = view.revisions[0].items
    assert (item.path, item.secret) == ("env.TOKEN", "changed")
    assert item.before == {"protected": True, "expand": False, "set": True, "version": 1}


async def test_non_security_and_grant_items_are_classified(pool, kube, settings, boss, preset):
    """Any path may be proposed (ruling F8): non-security and grant paths included."""
    view, _, _ = await _create(
        pool, kube, settings, boss, patch={"replicas": 2, "session_messaging": True}
    )
    items = {i.path: (i.security, i.grant) for i in view.revisions[0].items}
    assert items == {"replicas": (False, False), "session_messaging": (True, True)}


async def test_value_inside_patch_is_secret_in_spec(pool, kube, settings, boss, preset, canary):
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, patch={"env": {"TOKEN": {"value": canary}}})
    err = _error(exc, 422, "secret_in_spec")
    assert canary not in err.message
    assert kube.applied("Secret") == []
    await _assert_nowhere(pool, canary)


async def test_value_for_a_variable_not_protected_names_the_variable(
    pool, kube, settings, boss, preset, canary
):
    for name in ("PLAIN", "MISSING"):
        with pytest.raises(ApiError) as exc:
            await _create(pool, kube, settings, boss, patch={}, secrets={name: canary})
        err = _error(exc, 422, "invalid_request")
        assert name in err.message
        assert canary not in err.message
    assert kube.applied("Secret") == []
    await _assert_nowhere(pool, canary)


async def test_more_than_20_values_is_refused(pool, kube, settings, boss, preset, canary):
    env = {f"V{i}": {"protected": True} for i in range(21)}
    with pytest.raises(ApiError) as exc:
        await _create(
            pool,
            kube,
            settings,
            boss,
            patch={"env": env},
            secrets={n: canary for n in env},
        )
    _error(exc, 422, "invalid_request")
    assert kube.applied("Secret") == []


async def test_a_value_over_1_mib_is_413(pool, kube, settings, boss, preset):
    with pytest.raises(ApiError) as exc:
        await _create(
            pool, kube, settings, boss, patch={}, secrets={"TOKEN": "x" * (SECRET_MAX_BYTES + 1)}
        )
    _error(exc, 413, "secret_too_large")
    assert kube.applied("Secret") == []


async def test_invalid_merged_spec_is_422_and_stages_nothing(
    pool, kube, settings, boss, preset, canary
):
    with pytest.raises(ApiError) as exc:
        await _create(
            pool, kube, settings, boss, patch={"model": "no-such-model"}, secrets={"TOKEN": canary}
        )
    _error(exc, 422, "invalid_request")
    with pytest.raises(RequestValidationError):
        await _create(
            pool, kube, settings, boss, patch={"replicas": "many"}, secrets={"TOKEN": canary}
        )
    assert kube.applied("Secret") == []
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_an_empty_diff_is_nothing_changed(pool, kube, settings, boss, preset):
    for patch in ({}, {"docker": True}, {"env": {"PLAIN": {"value": "plain"}}}):
        with pytest.raises(ApiError) as exc:
            await _create(pool, kube, settings, boss, patch=patch)
        _error(exc, 422, "nothing_changed")


async def test_a_hidden_or_unknown_preset_is_404_before_any_staging(
    pool, kube, settings, boss, preset, canary
):
    async with pool.connection() as conn:
        await presets_repo.create(conn, "hidden", {"env": {"TOKEN": {"protected": True}}})
    for target in ("hidden", "nope", "00000000-0000-0000-0000-000000000000"):
        with pytest.raises(ApiError) as exc:
            await _create(
                pool, kube, settings, boss, target=target, patch={}, secrets={"TOKEN": canary}
            )
        _error(exc, 404, "not_found")
    assert kube.applied("Secret") == []


async def test_the_21st_active_proposal_is_409(pool, kube, settings, boss, preset):
    assert settings.proposals_open_max == 20
    for _ in range(20):
        await _create(pool, kube, settings, boss, patch={"replicas": 2})
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, patch={"replicas": 2})
    _error(exc, 409, "too_many_proposals")
    # a closed one frees a slot
    first = (await _rows(pool, "SELECT min(id) FROM change_proposals"))[0][0]
    await _withdraw(pool, kube, settings, boss, first)
    await _create(pool, kube, settings, boss, patch={"replicas": 2})


async def test_client_key_replay_returns_the_same_proposal_and_stages_once(
    pool, kube, settings, boss, preset, canary
):
    first, created, _ = await _create(
        pool, kube, settings, boss, patch=MCP_PATCH, secrets={"TOKEN": canary}, client_key="k1"
    )
    again, created_again, _ = await _create(
        pool, kube, settings, boss, patch=MCP_PATCH, secrets={"TOKEN": canary}, client_key="k1"
    )
    assert (created, created_again) == (True, False)
    assert again.id == first.id
    assert len(kube.applied("Secret")) == 1
    assert len(await _reservations(pool, preset.id)) == 1
    assert len(await _rows(pool, "SELECT id FROM change_proposals")) == 1


async def test_kinds_schedule_and_session_are_not_supported_yet(pool, kube, settings, boss):
    for kind in ("schedule", "session"):
        with pytest.raises(ApiError) as exc:
            await _create(pool, kube, settings, boss, kind=kind, target="x", patch={"a": 1})
        err = _error(exc, 422, "invalid_request")
        assert "not supported yet" in err.message


async def test_a_kube_failure_while_staging_leaves_no_rows_and_deletes_written_secrets(
    pool, kube, settings, boss, preset, canary
):
    env = {"A": {"protected": True}, "B": {"protected": True}}
    kube.fail(
        "Secret", store.secret_name(preset.id, "B", 1), KubeError(f"500 {canary}"), verbs=["patch"]
    )
    with pytest.raises(ApiError) as exc:
        await _create(
            pool,
            kube,
            settings,
            boss,
            patch={"env": env},
            secrets={"A": canary, "B": canary},
        )
    _error(exc, 502, "secret_store_failed")
    written = store.secret_name(preset.id, "A", 1)
    assert written in [a.name for a in kube.applied("Secret")]
    assert written not in _secrets(kube)  # deleted again after the rollback
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    assert await _reservations(pool, preset.id) == []
    await _assert_nowhere(pool, canary)


async def test_a_rollback_after_staging_deletes_the_staged_secrets(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    """Ruling: a create transaction that rolls back after `stage_secret` deletes the Secret
    names it returned (`release` would find no reservation row to drive it)."""
    real = secret_store.stage_secret
    calls = []

    async def flaky(*args, **kwargs):
        calls.append(args[4])
        if len(calls) == 2:
            raise RuntimeError("database went away")
        return await real(*args, **kwargs)

    monkeypatch.setattr(secret_store, "stage_secret", flaky)
    env = {"A": {"protected": True}, "B": {"protected": True}}
    with pytest.raises(RuntimeError):
        await _create(
            pool, kube, settings, boss, patch={"env": env}, secrets={"A": canary, "B": canary}
        )
    assert calls == ["A", "B"]
    written = store.secret_name(preset.id, "A", 1)
    assert written in [a.name for a in kube.applied("Secret")]
    assert written not in _secrets(kube)
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    assert await _reservations(pool, preset.id) == []


# ---------------------------------------------------------------- revise and withdraw


async def test_revise_from_rework_rebases_and_releases_the_old_value(
    pool, kube, settings, boss, preset, canary
):
    view, _, _ = await _create(
        pool, kube, settings, boss, patch=MCP_PATCH, secrets={"TOKEN": canary}
    )
    r1_secret = store.secret_name(preset.id, "TOKEN", 2)
    async with pool.connection() as conn:
        await proposals_repo.transition(
            conn, view.id, from_states=("open",), to="rework", outcome="rework"
        )
        edited = await presets_repo.update(conn, preset.id, "infra", {**preset.spec, "replicas": 2})
    assert (await _get(pool, settings, view.id, boss.id)).stale
    revised = await _revise(
        pool,
        kube,
        settings,
        boss,
        view.id,
        patch={"wrapper": {"mcp_servers": {"grafana": GRAFANA}}, "model": None},
        secrets={"TOKEN": canary + "2"},
        reason="rebased",
    )
    assert (revised.status, revised.revision, revised.stale) == ("open", 2, False)
    r1, r2 = revised.revisions
    assert (r1.outcome, r1.reason, r2.outcome, r2.reason) == (
        "rework",
        "needs grafana",
        None,
        "rebased",
    )
    assert {i.path for i in r2.items} == {"env.TOKEN", "wrapper.mcp_servers"}
    async with pool.connection() as conn:
        stored = await proposals_repo.get(conn, view.id)
        live = await store.list_metas(conn, [preset.id])
    assert stored is not None
    versions = {m.name: m.version for m in live[preset.id]}
    assert stored.revisions[1].fingerprint == proposals.fingerprint_preset(edited, versions)
    assert stored.revisions[0].fingerprint != stored.revisions[1].fingerprint
    assert stored.revisions[1].base["replicas"] == 2  # the base is the target read now
    # r1's value released after the commit, r2's staged
    r2_secret = store.secret_name(preset.id, "TOKEN", 3)
    assert r1_secret not in _secrets(kube)
    assert r2_secret in _secrets(kube)
    assert await _reservations(pool, preset.id) == [("TOKEN", 3, view.id, 2, "staged")]
    await _assert_nowhere(pool, canary, revised)


async def test_revise_without_a_reason_keeps_the_previous_one(pool, kube, settings, boss, preset):
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    revised = await _revise(pool, kube, settings, boss, view.id, patch={"replicas": 3})
    assert [r.reason for r in revised.revisions] == ["needs grafana", "needs grafana"]
    assert [r.outcome for r in revised.revisions] == ["revised", None]


async def test_revise_of_a_terminal_proposal_is_409(pool, kube, settings, boss, preset):
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    await _withdraw(pool, kube, settings, boss, view.id)
    with pytest.raises(ApiError) as exc:
        await _revise(pool, kube, settings, boss, view.id, patch={"replicas": 3})
    _error(exc, 409, "not_open")
    with pytest.raises(ApiError) as exc:
        await _withdraw(pool, kube, settings, boss, view.id)
    _error(exc, 409, "not_open")


async def test_withdraw_releases_every_staged_value(pool, kube, settings, boss, preset, canary):
    view, _, _ = await _create(pool, kube, settings, boss, patch={}, secrets={"TOKEN": canary})
    staged = store.secret_name(preset.id, "TOKEN", 2)
    assert staged in _secrets(kube)
    done = await _withdraw(pool, kube, settings, boss, view.id)
    assert done.status == "withdrawn"
    assert done.closed_at is not None
    assert done.revisions[0].outcome == "withdrawn"
    assert staged not in _secrets(kube)
    assert await _reservations(pool, preset.id) == []


async def test_another_sessions_proposal_is_404(pool, kube, settings, boss, preset):
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    other = await _session(pool, "prop-other")
    with pytest.raises(ApiError) as exc:
        await _get(pool, settings, view.id, other.id)
    _error(exc, 404, "not_found")
    with pytest.raises(ApiError) as exc:
        await _revise(pool, kube, settings, other, view.id, patch={"replicas": 3})
    _error(exc, 404, "not_found")
    with pytest.raises(ApiError) as exc:
        await _withdraw(pool, kube, settings, other, view.id)
    _error(exc, 404, "not_found")
    async with pool.connection() as conn:
        assert (
            await proposals.list_views(conn, settings, proposer=other.id, status="all", limit=10)
            == []
        )
    # untouched, and visible without a proposer (the main port)
    seen = await _get(pool, settings, view.id, None)
    assert (seen.status, seen.revision) == ("open", 1)


# ---------------------------------------------------------------- reads


@pytest.mark.parametrize("write", ["spec", "labels", "manageable", "secret"])
async def test_stale_after_any_preset_write_or_secret_rotation(
    pool, kube, settings, boss, preset, write
):
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    assert not (await _get(pool, settings, view.id, boss.id)).stale
    assert not (await _get(pool, settings, view.id, boss.id)).stale  # a read changes nothing
    async with pool.connection() as conn:
        if write == "spec":
            await presets_repo.update(conn, preset.id, "infra", {**preset.spec, "image": None})
        elif write == "labels":
            await presets_repo.update(conn, preset.id, "infra", preset.spec, labels=["x"])
        elif write == "manageable":
            await presets_repo.update(conn, preset.id, "infra", preset.spec, True)
        else:
            await store.set_secret(conn, kube, NS, preset.id, "TOKEN", SecretStr("rotated"))
    seen = await _get(pool, settings, view.id, boss.id)
    assert (seen.stale, seen.status) == (True, "open")  # read-only: the status stays
    async with pool.connection() as conn:
        [listed] = await proposals.list_views(
            conn, settings, proposer=boss.id, status="active", limit=10
        )
    assert listed.stale


async def test_a_deleted_target_reads_stale(pool, kube, settings, boss):
    async with pool.connection() as conn:
        await presets_repo.create(conn, "keep", {})
        await presets_repo.create(conn, "infra", {}, superleader_manageable=True)
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    await _rows(pool, "DELETE FROM presets WHERE name = 'infra' RETURNING id")
    assert (await _get(pool, settings, view.id, boss.id)).stale


async def test_list_filters_by_status(pool, kube, settings, boss, preset):
    open_one, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    closed, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 3})
    await _withdraw(pool, kube, settings, boss, closed.id)
    async with pool.connection() as conn:

        async def ids(status: str) -> list[int]:
            views = await proposals.list_views(
                conn, settings, proposer=boss.id, status=status, limit=10
            )
            return [v.id for v in views]

        assert await ids("active") == [open_one.id]
        assert await ids("open") == [open_one.id]
        assert await ids("closed") == [closed.id]
        assert await ids("all") == [closed.id, open_one.id]
        assert not (
            await proposals.list_views(conn, settings, proposer=boss.id, status="closed", limit=10)
        )[0].stale
        with pytest.raises(ApiError) as exc:
            await ids("bogus")
        _error(exc, 422, "invalid_request")
