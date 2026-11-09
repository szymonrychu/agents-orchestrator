"""api.proposals for `kind: "session"` (plan 3.4, 3.5 sessions, 3.8): a superleader proposes a
change to a live session's spec, protected values staged in the session preset's Secret family;
the human review writes the spec through `replace_spec`, binds the approved staged values into
`secret_versions` and bumps the generation once. Service level: real Postgres, a fake kube.

Every test that sends a value checks the canary never reaches a view, a table row, a notice or the
log (the `canary` fixture checks the log on teardown)."""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets

import pytest
from fakes.fake_kube import FakeKube
from pydantic import SecretStr
from test_agent_session_messaging import alone, hive
from test_api_agent import make_settings
from test_proposals_review import _review
from test_proposals_service import (
    GRAFANA,
    MCP_PATCH,
    NS,
    TABLES,
    _create,
    _error,
    _reservations,
    _revise,
    _rows,
    _secrets,
    _session,
    _withdraw,
)
from test_session_spec_service import _seed, _wait_for_lock_wait

from agents_orchestrator.api import proposal_sweep, proposals
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.k8s import secrets as store

SLUG = "infra-1"
EXTRA = {
    "kube_cluster_access": True,
    "env": {"TOKEN": {"protected": True}, "PLAIN": {"value": "plain"}},
}


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


@pytest.fixture
async def boss(pool):
    return await _session(pool, "prop-boss")


@pytest.fixture
async def preset(pool, kube, canary):
    """A manageable preset with a protected TOKEN whose value is set (version 1)."""
    async with pool.connection() as conn:
        found = await presets_repo.create(
            conn, "infra", {"env": {"TOKEN": {"protected": True}}}, superleader_manageable=True
        )
        await store.set_secret(conn, kube, NS, found.id, "TOKEN", SecretStr(canary + "-live"))
    kube.reset_records()
    return found


async def _attach(pool, session_id, preset_id) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET preset_id = %s WHERE id = %s", (preset_id, session_id)
        )


@pytest.fixture
async def target(pool, settings, preset):
    """A foreign privileged hive (2 replicas) of the manageable preset, running TOKEN v1."""
    sess = await hive(pool, SLUG)
    await _seed(
        pool, settings, sess, EXTRA, versions={"TOKEN": store.secret_name(preset.id, "TOKEN", 1)}
    )
    await _attach(pool, sess.id, preset.id)
    return sess


async def _row(pool, session_id) -> tuple:
    [row] = await _rows(
        pool,
        "SELECT spec, secret_versions, generation, desired FROM sessions WHERE id = %s",
        (session_id,),
    )
    return row


async def _status(pool, proposal_id) -> str:
    [(status,)] = await _rows(
        pool, "SELECT status FROM change_proposals WHERE id = %s", (proposal_id,)
    )
    return status


async def _notices(pool, session_id) -> list[str]:
    rows = await _rows(
        pool,
        "SELECT text FROM messages WHERE session_id = %s AND notice = 'proposal' ORDER BY id",
        (session_id,),
    )
    return [r[0] for r in rows]


async def _assert_nowhere(pool, value: str, *views) -> None:
    for view in views:
        assert value not in view.model_dump_json()
    for table in (*TABLES, "sessions", "messages", "events"):
        for (row,) in await _rows(pool, f"SELECT row_to_json(t)::text FROM {table} t"):  # noqa: S608
            assert value not in row, table


async def _filed(pool, kube, settings, boss, canary, **body):
    body = {
        "kind": "session",
        "target": SLUG,
        "patch": MCP_PATCH,
        "secrets": {"TOKEN": canary},
        **body,
    }
    view, _, _ = await _create(pool, kube, settings, boss, **body)
    return view


APPROVE_ALL = {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"}


# ---------------------------------------------------------------- create


async def test_create_stages_the_value_in_the_preset_family_and_records_items(
    pool, kube, settings, boss, preset, target, canary
):
    before = await _row(pool, target.id)
    view, created, request = await _create(
        pool,
        kube,
        settings,
        boss,
        kind="session",
        target=SLUG,
        patch=MCP_PATCH,
        secrets={"TOKEN": canary},
    )
    assert created
    assert (view.kind, view.status, view.revision, view.stale) == ("session", "open", 1, False)
    assert (view.target_id, view.target_name) == (str(target.id), SLUG)
    items = {i.path: i for i in view.revisions[0].items}
    assert sorted(items) == ["env.TOKEN", "wrapper.mcp_servers"]
    token = items["env.TOKEN"]
    assert (token.security, token.grant, token.secret) == (True, False, "changed")
    assert token.before == {"protected": True, "expand": False, "set": True, "version": 1}
    assert token.after == {"protected": True, "expand": False}
    assert (items["wrapper.mcp_servers"].before, items["wrapper.mcp_servers"].after) == (
        None,
        {"grafana": GRAFANA},
    )
    staged = store.secret_name(preset.id, "TOKEN", 2)
    assert [a.name for a in kube.applied("Secret")] == [staged]
    assert await _reservations(pool, preset.id) == [("TOKEN", 2, view.id, 1, "staged")]
    assert await _row(pool, target.id) == before
    assert request.state.admin_secrets == ["TOKEN"]
    assert request.state.preset_fields == ["env.TOKEN", "wrapper.mcp_servers"]
    await _assert_nowhere(pool, canary, view)


async def test_own_session_without_a_manageable_preset_may_be_proposed(pool, kube, settings, boss):
    view, created, _ = await _create(
        pool, kube, settings, boss, kind="session", target=boss.slug, patch=MCP_PATCH
    )
    assert created and view.target_id == str(boss.id)
    assert [i.path for i in view.revisions[0].items] == ["wrapper.mcp_servers"]


async def test_hidden_or_unknown_session_is_404(pool, kube, settings, boss, preset, canary):
    sess = await alone(pool, "hidden-1")
    async with pool.connection() as conn:
        hidden = await presets_repo.create(conn, "hidden", {}, superleader_manageable=False)
    await _attach(pool, sess.id, hidden.id)
    for slug in ("hidden-1", "nope"):
        with pytest.raises(ApiError) as exc:
            await _create(
                pool,
                kube,
                settings,
                boss,
                kind="session",
                target=slug,
                patch=MCP_PATCH,
                secrets={"TOKEN": canary},
            )
        _error(exc, 404, "not_found")
    assert _secrets(kube) == {store.secret_name(preset.id, "TOKEN", 1)}


@pytest.mark.parametrize("patch", [{"replicas": 1}, {"workdir": "other"}])
async def test_fixed_field_is_422(pool, kube, settings, boss, preset, target, canary, patch):
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, canary, patch=patch)
    err = _error(exc, 422, "session_fixed_field")
    assert err.extra["fields"] == sorted(patch)
    assert await _reservations(pool, preset.id) == []
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_fixed_field_is_checked_before_the_spec(
    pool, kube, settings, boss, preset, target, canary, monkeypatch
):
    """Same order as the session PATCH: a fixed field answers before an invalid spec."""

    def refuse(*args, **kw):
        raise ApiError(422, "invalid_request", "image not allowed")

    monkeypatch.setattr(proposals.session_spec, "check_spec", refuse)
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, canary, patch={"replicas": 1})
    err = _error(exc, 422, "session_fixed_field")
    assert err.extra["fields"] == ["replicas"]
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, canary, patch=MCP_PATCH)
    _error(exc, 422, "invalid_request")


async def test_secret_on_a_preset_less_session_is_422(pool, kube, settings, boss, canary):
    with pytest.raises(ApiError) as exc:
        await _create(
            pool,
            kube,
            settings,
            boss,
            kind="session",
            target=boss.slug,
            patch={"env": {"NEW": {"protected": True}}},
            secrets={"NEW": canary},
        )
    err = _error(exc, 422, "invalid_request")
    assert "preset" in err.message and canary not in err.message
    assert _secrets(kube) == set()
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_secret_for_an_unprotected_name_is_422(
    pool, kube, settings, boss, preset, target, canary
):
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, canary, secrets={"PLAIN": canary})
    err = _error(exc, 422, "invalid_request")
    assert "PLAIN" in err.message and canary not in err.message
    assert await _reservations(pool, preset.id) == []


async def test_a_new_protected_declaration_needs_a_value(
    pool, kube, settings, boss, preset, target, canary
):
    with pytest.raises(ApiError) as exc:
        await _filed(
            pool, kube, settings, boss, canary, patch={"env": {"NEW": {"protected": True}}}
        )
    err = _error(exc, 422, "secret_unset")
    assert err.extra["fields"] == ["env.NEW"]
    view = await _filed(
        pool,
        kube,
        settings,
        boss,
        canary,
        patch={"env": {"NEW": {"protected": True}}},
        secrets={"NEW": canary},
    )
    [item] = view.revisions[0].items
    assert (item.path, item.secret) == ("env.NEW", "set")


async def test_secret_in_patch_is_422(pool, kube, settings, boss, preset, target, canary):
    with pytest.raises(ApiError) as exc:
        await _filed(
            pool, kube, settings, boss, canary, patch={"env": {"TOKEN": {"value": canary}}}
        )
    err = _error(exc, 422, "secret_in_spec")
    assert canary not in err.message
    await _assert_nowhere(pool, canary)


async def test_deleting_session_is_409_at_create(
    pool, kube, settings, boss, preset, target, canary
):
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (target.id,))
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, canary)
    _error(exc, 409, "session_deleting")
    assert await _reservations(pool, preset.id) == []


# ---------------------------------------------------------------- review


async def test_approve_writes_the_spec_binds_the_secret_and_bumps_generation_once(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    spec_before, _, generation, _ = await _row(pool, target.id)
    preset_before = await _rows(
        pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,)
    )
    done = await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    assert (done.status, done.stale) == ("applied", False)
    spec, versions, generation_after, _ = await _row(pool, target.id)
    assert generation_after == generation + 1
    assert spec["wrapper"]["mcp_servers"] == {"grafana": GRAFANA}
    assert spec["kube_cluster_access"] is True and spec["replicas"] == spec_before["replicas"]
    staged = store.secret_name(preset.id, "TOKEN", 2)
    assert versions == {"TOKEN": staged}
    assert await _reservations(pool, preset.id) == [("TOKEN", 2, view.id, 1, "bound")]
    assert staged in _secrets(kube)
    # the preset and its live version are untouched: only this session runs the new value
    assert (
        await _rows(pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,))
        == preset_before
    )
    async with pool.connection() as conn:
        live = await store.list_secrets(conn, preset.id)
    assert [(m.name, m.version) for m in live] == [("TOKEN", 1)]
    [text] = await _notices(pool, boss.id)
    assert text.startswith(f"Proposal P-{view.id} r1 (session {SLUG}) was reviewed: applied ")
    assert "env.TOKEN (value changed)" in text and "wrapper.mcp_servers" in text
    await _assert_nowhere(pool, canary, view, done)


async def test_partial_approve_writes_only_the_approved_path_and_releases_the_value(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    _, versions_before, generation, _ = await _row(pool, target.id)
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.TOKEN": "deny", "wrapper.mcp_servers": "approve"}
    )
    assert done.status == "applied"
    spec, versions, generation_after, _ = await _row(pool, target.id)
    assert spec["wrapper"]["mcp_servers"] == {"grafana": GRAFANA}
    assert versions == versions_before and generation_after == generation + 1
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    await _assert_nowhere(pool, canary, done)


async def test_a_scale_between_create_and_review_is_stale_and_writes_nothing(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    before = await _row(pool, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    err = _error(exc, 409, "proposal_stale")
    assert canary not in err.message
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "stale"
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    [text] = await _notices(pool, boss.id)
    assert "is stale: the session changed" in text
    await _assert_nowhere(pool, canary)


async def test_a_secret_versions_change_under_the_row_lock_is_stale(
    pool, kube, settings, boss, preset, target, canary, monkeypatch
):
    """The recheck compares the fingerprint under the row lock: a binding change between the
    review's read and `replace_spec` (the spec itself equal) is stale, nothing written."""
    view = await _filed(pool, kube, settings, boss, canary)
    real = sessions_repo.replace_spec

    async def racing(conn, session_id, *args, **kw):
        async with pool.connection() as other:
            await other.execute(
                "UPDATE sessions SET secret_versions = %s::jsonb WHERE id = %s",
                ('{"TOKEN": "elsewhere-v9"}', session_id),
            )
        return await real(conn, session_id, *args, **kw)

    monkeypatch.setattr(sessions_repo, "replace_spec", racing)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 409, "proposal_stale")
    _, versions, _, _ = await _row(pool, target.id)
    assert versions == {"TOKEN": "elsewhere-v9"}
    assert await _status(pool, view.id) == "stale"
    assert await _reservations(pool, preset.id) == []


async def test_a_deleting_session_is_409_and_target_gone(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (target.id,))
    before = await _row(pool, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 409, "session_deleting")
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "target_gone"
    assert await _reservations(pool, preset.id) == []
    [text] = await _notices(pool, boss.id)
    assert "was closed" in text


async def test_a_purged_session_is_409_target_gone(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await sessions_repo.purge(conn, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 409, "target_gone")
    assert await _status(pool, view.id) == "target_gone"
    assert await _reservations(pool, preset.id) == []


async def test_rework_writes_nothing_and_releases(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _row(pool, target.id)
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.TOKEN": "approve", "wrapper.mcp_servers": ("rework", "pin the version")},
    )
    assert done.status == "rework"
    assert await _row(pool, target.id) == before
    assert await _reservations(pool, preset.id) == []
    [text] = await _notices(pool, boss.id)
    assert "needs rework: wrapper.mcp_servers: pin the version" in text
    await _assert_nowhere(pool, canary, done)


async def test_deny_all_writes_nothing_and_releases(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _row(pool, target.id)
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.TOKEN": "deny", "wrapper.mcp_servers": "deny"}
    )
    assert done.status == "denied"
    assert await _row(pool, target.id) == before
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)


async def test_revise_rebases_and_releases_the_old_value_and_withdraw_releases(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    assert (await proposals_view(pool, settings, view.id, boss)).stale is True
    revised = await _revise(
        pool, kube, settings, boss, view.id, patch=MCP_PATCH, secrets={"TOKEN": canary}
    )
    assert (revised.status, revised.revision, revised.stale) == ("open", 2, False)
    assert await _reservations(pool, preset.id) == [("TOKEN", 3, view.id, 2, "staged")]
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    done = await _withdraw(pool, kube, settings, boss, view.id)
    assert done.status == "withdrawn"
    assert await _reservations(pool, preset.id) == []
    await _assert_nowhere(pool, canary, view, revised, done)


async def proposals_view(pool, settings, proposal_id, caller):
    async with pool.connection() as conn:
        return await proposals.get_view(conn, settings, proposal_id, proposer=caller.id)


async def test_applying_one_session_proposal_makes_the_other_stale(
    pool, kube, settings, boss, preset, target, canary
):
    first = await _filed(pool, kube, settings, boss, canary)
    second = await _filed(
        pool, kube, settings, boss, canary, patch={"env": {"PLAIN": {"value": "other"}}}, secrets={}
    )
    await _review(pool, kube, settings, first.id, 1, APPROVE_ALL)
    assert await _status(pool, second.id) == "stale"


async def test_apply_takes_the_preset_before_the_variable_lock(
    pool, kube, settings, boss, preset, target, canary
):
    """Lock order (C1): `set_secret` holds the preset row FOR UPDATE and then takes the variable
    lock, so the apply must wait on the preset (FOR SHARE) before `bind_staged` takes that lock;
    the other order deadlocks a concurrent rotation."""
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as blocker, blocker.transaction():
        await blocker.execute("SELECT 1 FROM presets WHERE id = %s FOR UPDATE", (preset.id,))
        review = asyncio.create_task(_review(pool, kube, settings, view.id, 1, APPROVE_ALL))
        await _wait_for_lock_wait(pool)
        cur = await blocker.execute(
            "SELECT pg_try_advisory_xact_lock(hashtextextended(%s, 0))", (f"{preset.id}/TOKEN",)
        )
        assert (await cur.fetchone())[0] is True  # the review does not hold the variable
    done = await asyncio.wait_for(review, timeout=30)
    assert done.status == "applied"


async def test_human_review_runs_no_agent_guard_on_a_grant(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(
        pool, kube, settings, boss, canary, patch={"session_messaging": True}, secrets={}
    )
    [item] = view.revisions[0].items
    assert (item.path, item.grant) == ("session_messaging", True)
    await _review(pool, kube, settings, view.id, 1, {"session_messaging": "approve"})
    spec, _, _, _ = await _row(pool, target.id)
    assert spec["session_messaging"] is True


async def test_an_invalid_spec_at_review_is_422_and_records_nothing(
    pool, kube, settings, boss, preset, target, canary, monkeypatch
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _row(pool, target.id)

    def refuse(*args, **kw):
        raise ApiError(422, "invalid_request", "image not allowed")

    monkeypatch.setattr(proposals.session_spec, "check_spec", refuse)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 422, "invalid_spec")
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "open"
    assert await _reservations(pool, preset.id) == [("TOKEN", 2, view.id, 1, "staged")]


# ---------------------------------------------------------------- sweeper


async def test_sweeper_marks_a_changed_session_stale_and_releases(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    await proposal_sweep.sweep_once(pool, lambda: kube, settings)
    assert await _status(pool, view.id) == "open"
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    await proposal_sweep.sweep_once(pool, lambda: kube, settings)
    assert await _status(pool, view.id) == "stale"
    assert await _reservations(pool, preset.id) == []
    assert len(await _notices(pool, boss.id)) == 1


@pytest.mark.parametrize("how", ["deleting", "purged"])
async def test_sweeper_marks_a_gone_session_target_gone_and_releases(
    pool, kube, settings, boss, preset, target, canary, how
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        if how == "deleting":
            await conn.execute(
                "UPDATE sessions SET desired = 'deleting' WHERE id = %s", (target.id,)
            )
        else:
            await sessions_repo.purge(conn, target.id)
    await proposal_sweep.sweep_once(pool, lambda: kube, settings)
    assert await _status(pool, view.id) == "target_gone"
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    [text] = await _notices(pool, boss.id)
    assert "was closed" in text


async def test_view_of_a_session_proposal_reports_stale(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary, secrets={})
    assert view.stale is False
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    assert (await proposals_view(pool, settings, view.id, boss)).stale is True


async def test_removing_a_bound_protected_var_prunes_secret_versions_and_frees_the_secret(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    staged = store.secret_name(preset.id, "TOKEN", 2)
    assert (await _row(pool, target.id))[1] == {"TOKEN": staged}
    assert await store.collect_bound(pool, kube, NS) == 0
    removal, _, _ = await _create(
        pool, kube, settings, boss, kind="session", target=SLUG, patch={"env": {"TOKEN": None}}
    )
    done = await _review(pool, kube, settings, removal.id, 1, {"env.TOKEN": "approve"})
    assert done.status == "applied"
    spec, versions, _, _ = await _row(pool, target.id)
    assert "TOKEN" not in spec["env"] and not versions
    assert await store.collect_bound(pool, kube, NS) == 1
    assert staged not in _secrets(kube)
    # declared protected again without a value: refused, the old Secret is not silently rebound
    with pytest.raises(ApiError) as exc:
        await _create(
            pool,
            kube,
            settings,
            boss,
            kind="session",
            target=SLUG,
            patch={"env": {"TOKEN": {"protected": True}}},
        )
    _error(exc, 422, "secret_unset")
