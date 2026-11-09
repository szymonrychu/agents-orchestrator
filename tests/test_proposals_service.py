"""api.proposals: create, revise, withdraw and read preset change proposals (plan 3.4) and the
whole-proposal stale flag (Q6 B). A proposal carries no value: a preset proposal may carry vault
references, which the vault checks. Service level: real Postgres, a fake kube, a fake vault, a
fake request carrying `state`.

The audit `events` row is written by the AdminRoute wrapper, not by this service: the route task
asserts it."""

from __future__ import annotations

import logging
import secrets as stdlib_secrets
from types import SimpleNamespace

import httpx
import pydantic
import pytest
from fakes.fake_kube import FakeKube
from fastapi.exceptions import RequestValidationError
from pydantic import SecretStr
from test_agent_session_messaging import alone
from test_api_agent import make_settings
from test_api_presets_vault import ENTRY, GONE, OTHER, FakeVault

from agents_orchestrator.api import proposals, schemas
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.vault_client import VaultClient
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as proposals_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.k8s import secrets as store

NS = "agents"
GRAFANA = {"type": "stdio", "command": "mise", "args": ["x", "mcp-grafana@0.1.0", "--", "-t"]}
MCP_PATCH = {"wrapper": {"mcp_servers": {"grafana": GRAFANA}}}
TABLES = (
    "change_proposals",
    "change_proposal_revisions",
    "change_proposal_items",
    "change_proposal_secrets",
    "presets",
    "preset_secrets",
)


@pytest.fixture
def settings(dsn):
    return make_settings(dsn).model_copy(update={"docker_image": "docker:27-dind"})


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


_current: dict[str, FakeVault] = {}


@pytest.fixture(autouse=True)
def vault():
    """The vault every `_request` talks to; the test may change its entries or take it down."""
    _current["vault"] = fake = FakeVault()
    return fake


@pytest.fixture
def canary(caplog):
    caplog.set_level(logging.DEBUG)
    value = "canary-" + stdlib_secrets.token_hex(16)
    yield value
    assert value not in caplog.text


def _request(kube) -> SimpleNamespace:
    client = VaultClient(
        "http://vault", "t", transport=httpx.MockTransport(_current["vault"].handler)
    )
    return SimpleNamespace(
        state=SimpleNamespace(admin_secrets=[]),
        app=SimpleNamespace(state=SimpleNamespace(kube=kube, kube_owned=False, vault=client)),
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


async def _create(pool, kube, settings, caller, *, superleader=True, **body):
    request = _request(kube)
    body = {"kind": "preset", "target": "infra", "reason": "needs grafana", **body}
    async with pool.connection() as conn:
        view, created = await proposals.create(
            request,
            conn,
            settings,
            schemas.ProposalIn.model_validate(body),
            caller=caller,
            superleader=superleader,
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


async def test_create_records_redacted_items_and_writes_no_secret(
    pool, kube, settings, boss, preset, canary
):
    before = await _rows(pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,))
    view, created, request = await _create(
        pool, kube, settings, boss, patch={**MCP_PATCH, "env": {"NEW": {"protected": True}}}
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
    assert sorted(items) == ["env.NEW", "wrapper.mcp_servers"]
    declared = items["env.NEW"]
    assert (declared.before, declared.after, declared.decision) == (
        None,
        {"protected": True, "expand": False},
        "pending",
    )
    mcp = items["wrapper.mcp_servers"]
    assert (mcp.security, mcp.grant) == (True, False)
    assert (mcp.before, mcp.after) == (None, {"grafana": GRAFANA})
    assert all("secret" not in i.model_dump() for i in rev.items)
    # nothing was staged or reserved, the preset row and its live secret are untouched
    assert kube.applied("Secret") == []
    assert await _reservations(pool, preset.id) == []
    after = await _rows(pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,))
    assert after == before
    async with pool.connection() as conn:
        live = await store.list_secrets(conn, preset.id)
    assert [(m.name, m.version) for m in live] == [("TOKEN", 1)]
    # the audit gets paths only
    assert request.state.admin_secrets == []
    assert request.state.preset_fields == ["env.NEW", "wrapper.mcp_servers"]
    await _assert_nowhere(pool, canary, view)


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


async def test_invalid_merged_spec_is_422_and_files_nothing(pool, kube, settings, boss, preset):
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, patch={"model": "no-such-model"})
    _error(exc, 422, "invalid_request")
    with pytest.raises(RequestValidationError):
        await _create(pool, kube, settings, boss, patch={"replicas": "many"})
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_an_empty_diff_is_nothing_changed(pool, kube, settings, boss, preset):
    for patch in ({}, {"docker": True}, {"env": {"PLAIN": {"value": "plain"}}}):
        with pytest.raises(ApiError) as exc:
            await _create(pool, kube, settings, boss, patch=patch)
        _error(exc, 422, "nothing_changed")


async def test_a_hidden_or_unknown_preset_is_404(pool, kube, settings, boss, preset):
    async with pool.connection() as conn:
        await presets_repo.create(conn, "hidden", {"env": {"TOKEN": {"protected": True}}})
    for target in ("hidden", "nope", "00000000-0000-0000-0000-000000000000"):
        with pytest.raises(ApiError) as exc:
            await _create(pool, kube, settings, boss, target=target, patch={"replicas": 2})
        _error(exc, 404, "not_found")


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


async def test_client_key_replay_returns_the_same_proposal(pool, kube, settings, boss, preset):
    first, created, _ = await _create(pool, kube, settings, boss, patch=MCP_PATCH, client_key="k1")
    again, created_again, _ = await _create(
        pool, kube, settings, boss, patch=MCP_PATCH, client_key="k1"
    )
    assert (created, created_again) == (True, False)
    assert again.id == first.id
    assert len(await _rows(pool, "SELECT id FROM change_proposals")) == 1


async def test_client_key_replay_on_a_hidden_target_is_404(pool, kube, settings, boss, preset):
    """Minor 3 (final review A-O): a replay follows the one visibility rule, as `GET` does."""
    await _create(pool, kube, settings, boss, patch={"replicas": 2}, client_key="k1")
    async with pool.connection() as conn:
        await presets_repo.update(conn, preset.id, "infra", preset.spec, False)
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, patch={"replicas": 2}, client_key="k1")
    _error(exc, 404, "not_found")


async def test_kind_session_on_an_unknown_session_is_404(pool, kube, settings, boss):
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, kind="session", target="x", patch={"a": 1})
    _error(exc, 404, "not_found")


async def test_a_non_superleader_create_is_403(pool, kube, settings, boss, preset):
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, superleader=False, patch={"replicas": 2})
    _error(exc, 403, "not_superleader")
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_proposal_secrets_field_refused(canary):
    """Values are gone from proposals: `secrets` is an unknown field of the body (422 at the
    route), on create and on revise."""
    for model, body in (
        (schemas.ProposalIn, {"kind": "preset", "target": "x", "patch": {}, "reason": "r"}),
        (schemas.ProposalRevise, {"patch": {}}),
    ):
        with pytest.raises(pydantic.ValidationError) as exc:
            model.model_validate({**body, "secrets": {"TOKEN": canary}})
        [err] = exc.value.errors(include_input=False)
        assert (err["type"], err["loc"]) == ("extra_forbidden", ("secrets",))


def _ref(entry=OTHER, env="TOKEN") -> dict:
    return {"vault": entry, "env": env}


async def test_preset_proposal_with_ref_validated(pool, kube, settings, boss, preset, vault):
    view, _, request = await _create(
        pool, kube, settings, boss, patch={"env": {"GH": _ref()}}, reason="attach"
    )
    [item] = view.revisions[0].items
    assert (item.path, item.before, item.after) == ("env.GH", None, _ref())
    # the vault got the refs and the preset's effective labels (its project first)
    [call] = vault.calls
    assert call["refs"] == {"GH": {"entry": OTHER, "env": "TOKEN"}}
    assert call["preset_labels"][0] == f"project:{preset.project}"
    assert kube.applied("Secret") == []
    assert request.state.preset_fields == ["env.GH"]
    # a revise validates again
    vault.calls.clear()
    revised = await _revise(pool, kube, settings, boss, view.id, patch={"env": {"GH": _ref()}})
    assert revised.revision == 2
    assert len(vault.calls) == 1


@pytest.mark.parametrize(
    ("ref", "code", "extra"),
    [
        (_ref(GONE), "vault_entry_unknown", {"envs": ["GH"], "entries": [GONE]}),
        (_ref(OTHER, "NOPE"), "vault_env_unknown", {"envs": ["GH"]}),
        (_ref(ENTRY, "TOKEN"), "vault_label_mismatch", {"envs": ["GH"]}),
    ],
)
async def test_preset_proposal_ref_refused_by_the_vault_is_422(
    pool, kube, settings, boss, preset, ref, code, extra
):
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, patch={"env": {"GH": ref}})
    err = _error(exc, 422, code)
    assert err.extra == extra
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    with pytest.raises(ApiError) as exc:
        await _revise(pool, kube, settings, boss, view.id, patch={"env": {"GH": ref}})
    assert _error(exc, 422, code).extra == extra
    assert (await _get(pool, settings, view.id, boss.id)).revision == 1


async def test_preset_proposal_with_the_vault_down_is_503(
    pool, kube, settings, boss, preset, vault
):
    vault.down = True
    with pytest.raises(ApiError) as exc:
        await _create(pool, kube, settings, boss, patch={"env": {"GH": _ref()}})
    _error(exc, 503, "vault_unavailable")
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_a_proposal_without_refs_never_calls_the_vault(
    pool, kube, settings, boss, preset, vault
):
    vault.down = True
    await _create(pool, kube, settings, boss, patch={"replicas": 2})
    assert vault.calls == []


# ---------------------------------------------------------------- revise and withdraw


async def test_revise_after_the_target_became_non_manageable_is_404(
    pool, kube, settings, boss, preset
):
    view, _, _ = await _create(pool, kube, settings, boss, patch=MCP_PATCH)
    async with pool.connection() as conn:
        await presets_repo.update(conn, preset.id, "infra", preset.spec, False)
    with pytest.raises(ApiError) as exc:
        await _revise(pool, kube, settings, boss, view.id, patch=MCP_PATCH)
    _error(exc, 404, "not_found")
    async with pool.connection() as conn:
        stored = await proposals_repo.get(conn, view.id)
    assert stored is not None
    assert (stored.status, stored.revision) == ("open", 1)


async def test_revise_after_the_target_was_deleted_is_the_unknown_proposal_404(
    pool, kube, settings, boss, preset
):
    """Minor 4 (final review A-O): a deleted target answers like a hidden one, the uniform
    unknown-proposal 404, and changes nothing (the sweeper closes it `target_gone`)."""
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    with pytest.raises(ApiError) as unknown:
        await _revise(pool, kube, settings, boss, 999999, patch={"replicas": 3})
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset.id,))
    with pytest.raises(ApiError) as exc:
        await _revise(pool, kube, settings, boss, view.id, patch={"replicas": 3})
    assert _error(exc, 404, "not_found").message == unknown.value.message
    assert (await _get(pool, settings, view.id, boss.id)).revision == 1


async def test_writes_refuse_a_connection_inside_a_transaction(pool, kube, settings, boss, preset):
    """Inside an open transaction `conn.transaction()` is only a savepoint, so the release after
    it would run before the real commit: every write refuses such a connection up front."""
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    async with pool.connection() as conn:
        await conn.execute("SELECT 1")
        with pytest.raises(RuntimeError, match="outside a transaction"):
            await proposals.revise(
                _request(kube),
                conn,
                settings,
                view.id,
                schemas.ProposalRevise.model_validate({"patch": {"replicas": 3}}),
                caller=boss,
            )
        with pytest.raises(RuntimeError, match="outside a transaction"):
            await proposals.withdraw(_request(kube), conn, settings, view.id, caller=boss)
        with pytest.raises(RuntimeError, match="outside a transaction"):
            await proposals.create(
                _request(kube),
                conn,
                settings,
                schemas.ProposalIn.model_validate(
                    {"kind": "preset", "target": "infra", "patch": {"replicas": 3}, "reason": "r"}
                ),
                caller=boss,
                superleader=True,
            )
    assert (await _get(pool, settings, view.id, boss.id)).revision == 1


async def test_revise_of_a_stale_proposal_rebases_it_to_open(pool, kube, settings, boss, preset):
    view, _, _ = await _create(pool, kube, settings, boss, patch={"replicas": 2})
    await _rows(
        pool, "UPDATE change_proposals SET status = 'stale' WHERE id = %s RETURNING id", (view.id,)
    )
    async with pool.connection() as conn:
        await presets_repo.update(conn, preset.id, "infra", {**preset.spec, "image": None})
    revised = await _revise(pool, kube, settings, boss, view.id, patch={"replicas": 3})
    assert (revised.status, revised.revision, revised.stale) == ("open", 2, False)


async def test_revise_from_rework_rebases_on_the_target_now(pool, kube, settings, boss, preset):
    view, _, _ = await _create(pool, kube, settings, boss, patch=MCP_PATCH)
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
    assert {i.path for i in r2.items} == {"wrapper.mcp_servers"}
    async with pool.connection() as conn:
        stored = await proposals_repo.get(conn, view.id)
        live = await store.list_metas(conn, [preset.id])
    assert stored is not None
    versions = {m.name: m.version for m in live[preset.id]}
    assert stored.revisions[1].fingerprint == proposals.fingerprint_preset(edited, versions)
    assert stored.revisions[0].fingerprint != stored.revisions[1].fingerprint
    assert stored.revisions[1].base["replicas"] == 2  # the base is the target read now
    assert kube.applied("Secret") == []


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


async def test_withdraw_closes_the_proposal(pool, kube, settings, boss, preset):
    view, _, _ = await _create(pool, kube, settings, boss, patch=MCP_PATCH)
    done = await _withdraw(pool, kube, settings, boss, view.id)
    assert done.status == "withdrawn"
    assert done.closed_at is not None
    assert done.revisions[0].outcome == "withdrawn"
    assert kube.applied("Secret") == []


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
