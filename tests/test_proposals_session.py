"""api.proposals for `kind: "session"` (plan 3.4, 3.5 sessions, 3.8): a superleader proposes a
change to a live session's spec; the human review writes the spec through `replace_spec` and
bumps the generation once, the session's pins untouched. A proposal holds no value and refuses
vault reference changes (refs enter and leave a session only through its preset). Service level:
real Postgres, a fake kube, a fake vault.

The `canary` fixture checks on teardown that a canary never reaches the log; the tests check it
never reaches a view, a table row or a notice."""

from __future__ import annotations

import json
import logging
import secrets as stdlib_secrets

import pytest
from fakes.fake_kube import FakeKube
from fastapi.exceptions import RequestValidationError
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
    _revise,
    _rows,
    _session,
    _withdraw,
    vault,  # noqa: F401  (an autouse fixture)
)
from test_session_spec_service import _seed

from agents_orchestrator.api import proposal_sweep, proposals
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo

SLUG = "infra-1"
LEGACY_PINS = {"OLD_KEY": "ps-00000000-0a0b0c-v1"}
EXTRA = {
    "kube_cluster_access": True,
    "env": {"TOKEN": {"value": "token"}, "PLAIN": {"value": "plain"}},
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
    """A manageable preset with a plain TOKEN."""
    async with pool.connection() as conn:
        found = await presets_repo.create(
            conn, "infra", {"env": {"TOKEN": {"value": "token"}}}, superleader_manageable=True
        )
    kube.reset_records()
    return found


async def _attach(pool, session_id, preset_id) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET preset_id = %s WHERE id = %s", (preset_id, session_id)
        )


@pytest.fixture
async def target(pool, settings, preset):
    """A foreign privileged hive (2 replicas) of the manageable preset, created before 0041: it
    still holds a legacy pin."""
    sess = await hive(pool, SLUG)
    await _seed(pool, settings, sess, EXTRA, versions=LEGACY_PINS)
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


async def _filed(pool, kube, settings, boss, **body):
    """A proposal with two items: `env.PLAIN` and `wrapper.mcp_servers`."""
    body = {"kind": "session", "target": SLUG, "patch": {**MCP_PATCH, **PLAIN_PATCH}, **body}
    view, _, _ = await _create(pool, kube, settings, boss, **body)
    return view


PLAIN_PATCH = {"env": {"PLAIN": {"value": "changed"}}}
APPROVE_ALL = {"env.PLAIN": "approve", "wrapper.mcp_servers": "approve"}


# ---------------------------------------------------------------- create


async def test_create_records_items_and_stages_nothing(
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
        patch={**MCP_PATCH, **PLAIN_PATCH},
    )
    assert created
    assert (view.kind, view.status, view.revision, view.stale) == ("session", "open", 1, False)
    assert (view.target_id, view.target_name) == (str(target.id), SLUG)
    items = {i.path: i for i in view.revisions[0].items}
    assert sorted(items) == ["env.PLAIN", "wrapper.mcp_servers"]
    plain = items["env.PLAIN"]
    assert (plain.before["value"], plain.after["value"]) == ("plain", "changed")
    assert all("secret" not in i.model_dump() for i in items.values())
    assert (items["wrapper.mcp_servers"].before, items["wrapper.mcp_servers"].after) == (
        None,
        {"grafana": GRAFANA},
    )
    assert kube.applied("Secret") == []
    assert await _row(pool, target.id) == before
    assert request.state.admin_secrets == []
    assert request.state.preset_fields == ["env.PLAIN", "wrapper.mcp_servers"]
    await _assert_nowhere(pool, canary, view)


async def test_own_session_without_a_manageable_preset_may_be_proposed(pool, kube, settings, boss):
    view, created, _ = await _create(
        pool, kube, settings, boss, kind="session", target=boss.slug, patch=MCP_PATCH
    )
    assert created and view.target_id == str(boss.id)
    assert [i.path for i in view.revisions[0].items] == ["wrapper.mcp_servers"]


async def test_unmanageable_session_is_403_and_unknown_404(
    pool, kube, settings, boss, preset, canary
):
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
            )
        if slug == "nope":
            _error(exc, 404, "not_found")
        else:
            _error(exc, 403, "not_manageable")
    assert kube.applied("Secret") == []


@pytest.mark.parametrize("patch", [{"replicas": 1}, {"workdir": "other"}])
async def test_fixed_field_is_422(pool, kube, settings, boss, preset, target, canary, patch):
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, patch=patch)
    err = _error(exc, 422, "session_fixed_field")
    assert err.extra["fields"] == sorted(patch)
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_fixed_field_is_checked_before_the_spec(
    pool, kube, settings, boss, preset, target, canary, monkeypatch
):
    """Same order as the session PATCH: a fixed field answers before an invalid spec."""

    def refuse(*args, **kw):
        raise ApiError(422, "invalid_request", "image not allowed")

    monkeypatch.setattr(proposals.session_spec, "check_spec", refuse)
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, patch={"replicas": 1})
    err = _error(exc, 422, "session_fixed_field")
    assert err.extra["fields"] == ["replicas"]
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, patch=MCP_PATCH)
    _error(exc, 422, "invalid_request")


async def test_a_vault_ref_change_is_refused(pool, kube, settings, boss, preset, target, canary):
    ref = {"vault": "11111111-1111-4111-8111-111111111111", "env": "TOKEN"}
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, patch={"env": {"NEW": ref}})
    err = _error(exc, 422, "vault_ref_not_allowed")
    assert err.extra["envs"] == ["NEW"]
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_a_protected_flag_is_refused(pool, kube, settings, boss, preset, target, canary):
    """O10: `protected` is an unknown env field; nothing is filed, the value is never kept."""
    patch = {"env": {"TOKEN": {"value": canary, "protected": True}}}
    with pytest.raises(RequestValidationError):
        await _filed(pool, kube, settings, boss, patch=patch)
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    await _assert_nowhere(pool, canary)


async def test_a_session_naming_its_legacy_pin_takes_a_proposal(
    pool, kube, settings, boss, preset, target
):
    """A hook of a session created before 0041 may name its legacy pin (`${OLD_KEY}`); the
    proposal merge and the review apply validate it with the session's pins."""
    hook = {"type": "webhook", "url": "http://h/x", "secret": "${OLD_KEY}"}
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET spec = jsonb_set(spec, '{wrapper}', %s) WHERE id = %s",
            (json.dumps({"hooks": {"onStart": [hook]}}), target.id),
        )
    view = await _filed(pool, kube, settings, boss)
    done = await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    assert done.status == "applied"
    spec, versions, *_ = await _row(pool, target.id)
    assert spec["wrapper"]["hooks"]["onStart"] == [hook] and versions == LEGACY_PINS


async def test_deleting_session_is_409_at_create(
    pool, kube, settings, boss, preset, target, canary
):
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (target.id,))
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss)
    _error(exc, 409, "session_deleting")


# ---------------------------------------------------------------- review


async def test_approve_writes_the_spec_and_bumps_generation_once(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    spec_before, versions_before, generation, _ = await _row(pool, target.id)
    preset_before = await _rows(
        pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,)
    )
    done = await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    assert (done.status, done.stale) == ("applied", False)
    spec, versions, generation_after, _ = await _row(pool, target.id)
    assert generation_after == generation + 1
    assert spec["wrapper"]["mcp_servers"] == {"grafana": GRAFANA}
    assert spec["env"]["PLAIN"]["value"] == "changed"
    assert spec["kube_cluster_access"] is True and spec["replicas"] == spec_before["replicas"]
    assert versions == versions_before  # the pins are untouched
    assert kube.applied("Secret") == []
    # the preset is untouched
    assert (
        await _rows(pool, "SELECT spec, updated_at FROM presets WHERE id = %s", (preset.id,))
        == preset_before
    )
    [text] = await _notices(pool, boss.id)
    assert text.startswith(f"Proposal P-{view.id} r1 (session {SLUG}) was reviewed: applied ")
    assert "env.PLAIN" in text and "wrapper.mcp_servers" in text
    await _assert_nowhere(pool, canary, view, done)


async def test_partial_approve_writes_only_the_approved_path(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    _, versions_before, generation, _ = await _row(pool, target.id)
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.PLAIN": "deny", "wrapper.mcp_servers": "approve"}
    )
    assert done.status == "applied"
    spec, versions, generation_after, _ = await _row(pool, target.id)
    assert spec["wrapper"]["mcp_servers"] == {"grafana": GRAFANA}
    assert spec["env"]["PLAIN"]["value"] == "plain"
    assert versions == versions_before and generation_after == generation + 1
    await _assert_nowhere(pool, canary, done)


async def test_a_scale_between_create_and_review_is_stale_and_writes_nothing(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    before = await _row(pool, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    err = _error(exc, 409, "proposal_stale")
    assert canary not in err.message
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "stale"
    [text] = await _notices(pool, boss.id)
    assert "is stale: the session changed" in text
    await _assert_nowhere(pool, canary)


async def test_a_secret_versions_change_under_the_row_lock_is_stale(
    pool, kube, settings, boss, preset, target, canary, monkeypatch
):
    """The recheck compares the fingerprint under the row lock: a binding change between the
    review's read and `replace_spec` (the spec itself equal) is stale, nothing written."""
    view = await _filed(pool, kube, settings, boss)
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


async def test_a_deleting_session_is_409_and_target_gone(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (target.id,))
    before = await _row(pool, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 409, "session_deleting")
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "target_gone"
    [text] = await _notices(pool, boss.id)
    assert "was closed" in text


async def test_a_purged_session_is_409_target_gone(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await sessions_repo.purge(conn, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 409, "target_gone")
    assert await _status(pool, view.id) == "target_gone"


async def test_rework_writes_nothing(pool, kube, settings, boss, preset, target, canary):
    view = await _filed(pool, kube, settings, boss)
    before = await _row(pool, target.id)
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.PLAIN": "approve", "wrapper.mcp_servers": ("rework", "pin the version")},
    )
    assert done.status == "rework"
    assert await _row(pool, target.id) == before
    [text] = await _notices(pool, boss.id)
    assert "needs rework: wrapper.mcp_servers: pin the version" in text
    await _assert_nowhere(pool, canary, done)


async def test_deny_all_writes_nothing(pool, kube, settings, boss, preset, target, canary):
    view = await _filed(pool, kube, settings, boss)
    before = await _row(pool, target.id)
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.PLAIN": "deny", "wrapper.mcp_servers": "deny"}
    )
    assert done.status == "denied"
    assert await _row(pool, target.id) == before


async def test_revise_rebases_and_withdraw_closes(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    assert (await proposals_view(pool, settings, view.id, boss)).stale is True
    revised = await _revise(pool, kube, settings, boss, view.id, patch=MCP_PATCH)
    assert (revised.status, revised.revision, revised.stale) == ("open", 2, False)
    done = await _withdraw(pool, kube, settings, boss, view.id)
    assert done.status == "withdrawn"
    assert kube.applied("Secret") == []
    await _assert_nowhere(pool, canary, view, revised, done)


async def proposals_view(pool, settings, proposal_id, caller):
    async with pool.connection() as conn:
        return await proposals.get_view(conn, settings, proposal_id, proposer=caller.id)


async def test_applying_one_session_proposal_makes_the_other_stale(
    pool, kube, settings, boss, preset, target, canary
):
    first = await _filed(pool, kube, settings, boss)
    second = await _filed(pool, kube, settings, boss, patch={"env": {"PLAIN": {"value": "other"}}})
    await _review(pool, kube, settings, first.id, 1, APPROVE_ALL)
    assert await _status(pool, second.id) == "stale"


async def test_human_review_runs_no_agent_guard_on_a_grant(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss, patch={"session_messaging": True})
    [item] = view.revisions[0].items
    assert (item.path, item.grant) == ("session_messaging", True)
    await _review(pool, kube, settings, view.id, 1, {"session_messaging": "approve"})
    spec, _, _, _ = await _row(pool, target.id)
    assert spec["session_messaging"] is True


async def test_an_invalid_spec_at_review_is_422_and_records_nothing(
    pool, kube, settings, boss, preset, target, canary, monkeypatch
):
    view = await _filed(pool, kube, settings, boss)
    before = await _row(pool, target.id)

    def refuse(*args, **kw):
        raise ApiError(422, "invalid_request", "image not allowed")

    monkeypatch.setattr(proposals.session_spec, "check_spec", refuse)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, APPROVE_ALL)
    _error(exc, 422, "invalid_spec")
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "open"


# ---------------------------------------------------------------- sweeper


async def test_sweeper_marks_a_changed_session_stale(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    await proposal_sweep.sweep_once(pool)
    assert await _status(pool, view.id) == "open"
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    await proposal_sweep.sweep_once(pool)
    assert await _status(pool, view.id) == "stale"
    assert len(await _notices(pool, boss.id)) == 1


@pytest.mark.parametrize("how", ["deleting", "purged"])
async def test_sweeper_marks_a_gone_session_target_gone(
    pool, kube, settings, boss, preset, target, canary, how
):
    view = await _filed(pool, kube, settings, boss)
    async with pool.connection() as conn:
        if how == "deleting":
            await conn.execute(
                "UPDATE sessions SET desired = 'deleting' WHERE id = %s", (target.id,)
            )
        else:
            await sessions_repo.purge(conn, target.id)
    await proposal_sweep.sweep_once(pool)
    assert await _status(pool, view.id) == "target_gone"
    [text] = await _notices(pool, boss.id)
    assert "was closed" in text


async def test_view_of_a_session_proposal_reports_stale(
    pool, kube, settings, boss, preset, target, canary
):
    view = await _filed(pool, kube, settings, boss)
    assert view.stale is False
    async with pool.connection() as conn:
        await sessions_repo.set_replicas(conn, target.id, 1)
    assert (await proposals_view(pool, settings, view.id, boss)).stale is True


async def test_session_proposal_ref_refused(pool, kube, settings, boss, preset, target):
    """Refs enter, change and leave a live session only through its preset: a session proposal
    that adds, re-points or drops one is 422 `vault_ref_not_allowed`, naming the preset env."""
    ref = {"vault": "11111111-1111-4111-8111-111111111111", "env": "TOKEN"}
    other = {"vault": "22222222-2222-4222-8222-222222222222", "env": "TOKEN"}
    holder = await hive(pool, "ref-1")
    await _seed(pool, settings, holder, {"env": {"GH": ref}})
    await _attach(pool, holder.id, preset.id)
    for patch in ({"env": {"GH": other}}, {"env": {"GH": None}}):
        with pytest.raises(ApiError) as exc:
            await _create(pool, kube, settings, boss, kind="session", target="ref-1", patch=patch)
        assert _error(exc, 422, "vault_ref_not_allowed").extra["envs"] == ["GH"]
    with pytest.raises(ApiError) as exc:
        await _filed(pool, kube, settings, boss, patch={"env": {"NEW": ref}})
    assert _error(exc, 422, "vault_ref_not_allowed").extra["envs"] == ["NEW"]
    assert await _rows(pool, "SELECT id FROM change_proposals") == []
    # a proposal that leaves the ref alone is fine
    view, _, _ = await _create(
        pool, kube, settings, boss, kind="session", target="ref-1", patch=MCP_PATCH
    )
    assert [i.path for i in view.revisions[0].items] == ["wrapper.mcp_servers"]


async def test_review_refuses_a_ref_change_again(pool, kube, settings, boss, preset, target):
    """Defense in depth: even an item that reached the table with a ref is refused when the
    review applies it, and the session stays as it was."""
    ref = {"vault": "11111111-1111-4111-8111-111111111111", "env": "TOKEN"}
    view = await _filed(pool, kube, settings, boss, patch=PLAIN_PATCH)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE change_proposal_items SET path = 'env.GH', after = %s::jsonb"
            " WHERE proposal_id = %s",
            (json.dumps(ref), view.id),
        )
    before = await _row(pool, target.id)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, {"env.GH": "approve"})
    assert _error(exc, 422, "vault_ref_not_allowed").extra["envs"] == ["GH"]
    assert await _row(pool, target.id) == before
    assert await _status(pool, view.id) == "open"
