"""api.proposals.review: the human review of a preset proposal (plan 3.5 steps 1-9).

A final submit applies the approved items all or nothing (Q14 A) on a target unchanged since the
revision was based (Q6 B); any rework writes nothing (D1); approved staged values are promoted in
the same transaction as the preset write (Q13 B). Service level: real Postgres, a fake kube.

Every test that sends a value checks the canary never reaches a view, a table row, a notice or the
log (the `canary` fixture checks the log on teardown)."""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets
from datetime import UTC, datetime, timedelta

import pytest
from fakes.fake_kube import FakeKube
from pydantic import SecretStr
from test_api_agent import make_settings
from test_proposals_service import (
    GRAFANA,
    MCP_PATCH,
    NS,
    TABLES,
    _create,
    _error,
    _request,
    _reservations,
    _revise,
    _rows,
    _secrets,
    _session,
    _withdraw,
)

from agents_orchestrator.api import proposals, schemas
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.k8s import secrets as store

PLAIN_PATCH = {"env": {"PLAIN": {"value": "changed"}}}


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
    """A privileged manageable labelled preset with docker sizes and a protected TOKEN whose value
    is set (version 1)."""
    spec = {
        "docker": True,
        "docker_memory": "2Gi",
        "env": {"TOKEN": {"protected": True}, "PLAIN": {"value": "plain"}},
    }
    async with pool.connection() as conn:
        found = await presets_repo.create(
            conn, "infra", spec, superleader_manageable=True, labels=["team-a"]
        )
        await store.set_secret(conn, kube, NS, found.id, "TOKEN", SecretStr(canary + "-live"))
    kube.reset_records()
    return found


async def _review(pool, kube, settings, proposal_id, revision, decisions):
    """`decisions` maps path to a decision or a (decision, note) pair."""
    items = [
        {"path": p, "decision": d, "note": None}
        if isinstance(d, str)
        else {"path": p, "decision": d[0], "note": d[1]}
        for p, d in decisions.items()
    ]
    body = schemas.ReviewIn.model_validate({"revision": revision, "items": items})
    async with pool.connection() as conn:
        return await proposals.review(_request(kube), conn, settings, proposal_id, body)


async def _spec(pool, preset_id):
    [(spec, updated_at, name, labels, manageable)] = await _rows(
        pool,
        "SELECT spec, updated_at, name, labels, superleader_manageable FROM presets WHERE id = %s",
        (preset_id,),
    )
    return spec, updated_at, name, labels, manageable


async def _notices(pool, session_id) -> list[tuple[str, str, str]]:
    return await _rows(
        pool,
        "SELECT text, notice, lane FROM messages WHERE session_id = %s ORDER BY id",
        (session_id,),
    )


async def _live(pool, preset_id) -> dict[str, int]:
    rows = await _rows(
        pool,
        "SELECT name, version FROM preset_secrets WHERE preset_id = %s AND retired_at IS NULL",
        (preset_id,),
    )
    return dict(rows)


async def _status(pool, proposal_id) -> tuple[str, list]:
    [(status,)] = await _rows(
        pool, "SELECT status FROM change_proposals WHERE id = %s", (proposal_id,)
    )
    items = await _rows(
        pool,
        "SELECT revision, path, decision, note FROM change_proposal_items WHERE proposal_id = %s"
        " ORDER BY revision, path",
        (proposal_id,),
    )
    return status, items


async def _assert_nowhere(pool, value: str, *views) -> None:
    for view in views:
        assert value not in view.model_dump_json()
    for table in (*TABLES, "messages"):
        for (row,) in await _rows(pool, f"SELECT row_to_json(t)::text FROM {table} t"):  # noqa: S608
            assert value not in row, table


async def _filed(pool, kube, settings, boss, canary, **body):
    body = {"patch": MCP_PATCH, "secrets": {"TOKEN": canary}, **body}
    view, _, _ = await _create(pool, kube, settings, boss, **body)
    return view


# ---------------------------------------------------------------- final submits


async def test_approve_all_applies_the_spec_and_promotes_the_staged_secret(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    staged = store.secret_name(preset.id, "TOKEN", 2)
    previous = store.secret_name(preset.id, "TOKEN", 1)
    assert {staged, previous} <= _secrets(kube)
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
    )
    assert (done.status, done.stale) == ("applied", False)
    assert done.closed_at is not None
    [rev] = done.revisions
    assert rev.outcome == "applied" and rev.reviewed_at is not None
    assert {i.path: i.decision for i in rev.items} == {
        "env.TOKEN": "approved",
        "wrapper.mcp_servers": "approved",
    }
    spec, _, name, labels, manageable = await _spec(pool, preset.id)
    assert spec["wrapper"] == {"mcp_servers": {"grafana": GRAFANA}}
    assert spec["env"]["TOKEN"] == {"protected": True, "expand": False}
    # F8: the columns are passed through unchanged
    assert (name, labels, manageable) == ("infra", ["team-a"], True)
    assert await _live(pool, preset.id) == {"TOKEN": 2}
    assert await _reservations(pool, preset.id) == []
    assert staged in _secrets(kube)
    assert previous not in _secrets(kube)  # collected by gc after the commit
    [(text, notice, lane)] = await _notices(pool, boss.id)
    assert (notice, lane) == ("proposal", "regular")
    assert text.startswith(f"Proposal P-{view.id} r1 (preset infra) was reviewed: applied ")
    assert "env.TOKEN (value changed)" in text and "wrapper.mcp_servers" in text
    assert f"proposal_get {view.id}" in text
    await _assert_nowhere(pool, canary, view, done)


async def test_partial_approve_applies_only_approved_paths_and_releases_the_denied_secret(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    staged = store.secret_name(preset.id, "TOKEN", 2)
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.TOKEN": ("deny", "keep the old token"), "wrapper.mcp_servers": "approve"},
    )
    assert done.status == "applied"
    spec, *_ = await _spec(pool, preset.id)
    assert spec["wrapper"] == {"mcp_servers": {"grafana": GRAFANA}}
    assert await _live(pool, preset.id) == {"TOKEN": 1}
    assert await _reservations(pool, preset.id) == []
    assert staged not in _secrets(kube)
    assert store.secret_name(preset.id, "TOKEN", 1) in _secrets(kube)
    [(text, _, _)] = await _notices(pool, boss.id)
    assert "applied wrapper.mcp_servers" in text
    assert "denied env.TOKEN (value changed): keep the old token" in text
    await _assert_nowhere(pool, canary, done)


async def test_any_rework_writes_nothing_and_records_every_verdict(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _spec(pool, preset.id)
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.TOKEN": "approve", "wrapper.mcp_servers": ("rework", "pin the version")},
    )
    assert done.status == "rework"
    assert done.closed_at is None
    [rev] = done.revisions
    assert rev.outcome == "rework" and rev.reviewed_at is not None
    assert {i.path: (i.decision, i.note) for i in rev.items} == {
        "env.TOKEN": ("approved", None),
        "wrapper.mcp_servers": ("rework", "pin the version"),
    }
    assert await _spec(pool, preset.id) == before
    assert await _live(pool, preset.id) == {"TOKEN": 1}
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    [(text, notice, _)] = await _notices(pool, boss.id)
    assert notice == "proposal"
    assert text.startswith(f"Proposal P-{view.id} r1 (preset infra) needs rework: ")
    assert "wrapper.mcp_servers: pin the version" in text
    assert "approved as proposed: env.TOKEN (value changed)" in text
    assert f"proposal_revise {view.id}" in text
    await _assert_nowhere(pool, canary, done)


async def test_all_deny_closes_denied_and_releases_the_secret(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _spec(pool, preset.id)
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.TOKEN": "deny", "wrapper.mcp_servers": "deny"}
    )
    assert done.status == "denied" and done.closed_at is not None
    assert done.revisions[0].outcome == "denied"
    assert await _spec(pool, preset.id) == before
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    [(text, _, _)] = await _notices(pool, boss.id)
    assert "denied env.TOKEN (value changed), wrapper.mcp_servers" in text
    assert "applied" not in text


async def test_an_applied_new_declaration_without_a_value_is_declared_unset(
    pool, kube, settings, boss, preset
):
    view, _, _ = await _create(
        pool, kube, settings, boss, patch={"env": {"NEW": {"protected": True}}}
    )
    done = await _review(pool, kube, settings, view.id, 1, {"env.NEW": "approve"})
    assert done.status == "applied"
    spec, *_ = await _spec(pool, preset.id)
    assert spec["env"]["NEW"] == {"protected": True, "expand": False}
    assert await _live(pool, preset.id) == {"TOKEN": 1}


async def test_an_approved_removal_retires_the_dropped_protected_variable(
    pool, kube, settings, boss, preset
):
    view, _, _ = await _create(pool, kube, settings, boss, patch={"env": {"TOKEN": None}})
    done = await _review(pool, kube, settings, view.id, 1, {"env.TOKEN": "approve"})
    assert done.status == "applied"
    spec, *_ = await _spec(pool, preset.id)
    assert "TOKEN" not in spec["env"]
    assert await _live(pool, preset.id) == {}


# ---------------------------------------------------------------- body checks


@pytest.mark.parametrize(
    "decisions",
    [
        {"env.TOKEN": "approve"},  # an item missing
        {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve", "model": "approve"},  # unknown
        {"env.TOKEN": "approve", "wrapper.mcp_servers": ("rework", None)},  # rework without note
        {"env.TOKEN": "approve", "wrapper.mcp_servers": ("rework", "  ")},  # blank note
    ],
)
async def test_a_body_not_covering_the_revision_exactly_is_422(
    pool, kube, settings, boss, preset, canary, decisions
):
    view = await _filed(pool, kube, settings, boss, canary)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, decisions)
    _error(exc, 422, "invalid_request")
    status, items = await _status(pool, view.id)
    assert status == "open"
    assert {i[2] for i in items} == {"pending"}
    assert len(await _reservations(pool, preset.id)) == 1


async def test_a_duplicate_path_is_422(pool, kube, settings, boss, preset, canary):
    view = await _filed(pool, kube, settings, boss, canary)
    body = schemas.ReviewIn.model_validate(
        {
            "revision": 1,
            "items": [
                {"path": "env.TOKEN", "decision": "approve"},
                {"path": "env.TOKEN", "decision": "deny"},
                {"path": "wrapper.mcp_servers", "decision": "approve"},
            ],
        }
    )
    async with pool.connection() as conn:
        with pytest.raises(ApiError) as exc:
            await proposals.review(_request(kube), conn, settings, view.id, body)
    _error(exc, 422, "invalid_request")


async def test_an_unknown_proposal_is_404(pool, kube, settings, boss, preset):
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, 999999, 1, {"replicas": "approve"})
    _error(exc, 404, "not_found")


# ---------------------------------------------------------------- Q14 A: all or nothing


async def test_an_approved_subset_failing_check_spec_is_422_and_writes_nothing(
    pool, kube, settings, boss, preset
):
    view, _, _ = await _create(
        pool, kube, settings, boss, patch={"docker": None, "docker_memory": None}
    )
    before = await _spec(pool, preset.id)
    with pytest.raises(ApiError) as exc:
        await _review(
            pool, kube, settings, view.id, 1, {"docker": "approve", "docker_memory": "deny"}
        )
    err = _error(exc, 422, "invalid_spec")
    assert "docker_memory needs docker: true" in err.message
    assert await _spec(pool, preset.id) == before
    status, items = await _status(pool, view.id)
    assert status == "open"
    assert {i[2] for i in items} == {"pending"}
    [(reviewed_at, outcome)] = await _rows(
        pool,
        "SELECT reviewed_at, outcome FROM change_proposal_revisions WHERE proposal_id = %s",
        (view.id,),
    )
    assert (reviewed_at, outcome) == (None, None)
    assert await _notices(pool, boss.id) == []


async def test_a_failing_check_spec_keeps_the_staged_secret_and_the_live_version(
    pool, kube, settings, boss, preset, canary
):
    patch = {"docker": None, "docker_memory": None}
    view = await _filed(pool, kube, settings, boss, canary, patch=patch)
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"docker": "approve", "docker_memory": "deny", "env.TOKEN": "approve"},
        )
    _error(exc, 422, "invalid_spec")
    assert await _live(pool, preset.id) == {"TOKEN": 1}
    assert [r[4] for r in await _reservations(pool, preset.id)] == ["staged"]
    assert store.secret_name(preset.id, "TOKEN", 2) in _secrets(kube)
    # the human fixes the selection: the same revision still applies
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"docker": "approve", "docker_memory": "approve", "env.TOKEN": "approve"},
    )
    assert done.status == "applied"
    assert await _live(pool, preset.id) == {"TOKEN": 2}


async def test_the_notice_exists_iff_the_apply_committed(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _spec(pool, preset.id)
    real = proposals.enqueue_message

    async def then_fail(*args, **kwargs):
        await real(*args, **kwargs)
        raise RuntimeError("commit failed")

    monkeypatch.setattr(proposals, "enqueue_message", then_fail)
    with pytest.raises(RuntimeError):
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    assert await _notices(pool, boss.id) == []
    assert await _spec(pool, preset.id) == before
    assert await _live(pool, preset.id) == {"TOKEN": 1}
    assert [r[4] for r in await _reservations(pool, preset.id)] == ["staged"]
    status, items = await _status(pool, view.id)
    assert status == "open" and {i[2] for i in items} == {"pending"}


@pytest.mark.parametrize("gone", ["deleting", "purged"])
async def test_a_proposer_session_being_deleted_gets_no_notice_and_the_apply_succeeds(
    pool, kube, settings, boss, preset, canary, gone
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        if gone == "deleting":
            await conn.execute("UPDATE sessions SET desired = 'deleting' WHERE id = %s", (boss.id,))
        else:
            await conn.execute("DELETE FROM sessions WHERE id = %s", (boss.id,))
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"}
    )
    assert done.status == "applied"
    assert await _notices(pool, boss.id) == []
    assert await _live(pool, preset.id) == {"TOKEN": 2}


# ---------------------------------------------------------------- Q6 B: stale


async def _relabel(pool, preset):
    async with pool.connection() as conn:
        stored = await presets_repo.get(conn, preset.id)
        await presets_repo.update(conn, preset.id, "infra", stored.spec, labels=["team-b"])


async def _unrelated_write(pool, preset):
    async with pool.connection() as conn:
        stored = await presets_repo.get(conn, preset.id)
        spec = {**stored.spec, "env": {**stored.spec["env"], "PLAIN": {"value": "other"}}}
        await presets_repo.update(conn, preset.id, "infra", spec)


async def _rotate(pool, kube, preset, canary):
    async with pool.connection() as conn:
        await store.set_secret(conn, kube, NS, preset.id, "TOKEN", SecretStr(canary + "-rot"))


@pytest.mark.parametrize("change", ["unrelated_path", "label", "rotation"])
async def test_any_change_to_the_target_makes_approve_409_stale_then_revise_applies(
    pool, kube, settings, boss, preset, canary, change
):
    view = await _filed(pool, kube, settings, boss, canary)
    staged = store.secret_name(preset.id, "TOKEN", 2)
    if change == "unrelated_path":
        await _unrelated_write(pool, preset)
    elif change == "label":
        await _relabel(pool, preset)
    else:
        await _rotate(pool, kube, preset, canary)
    before = await _spec(pool, preset.id)
    live = await _live(pool, preset.id)
    approve = {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"}
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, approve)
    err = _error(exc, 409, "proposal_stale")
    assert err.extra == {"fields": []}
    assert await _spec(pool, preset.id) == before
    assert await _live(pool, preset.id) == live
    status, items = await _status(pool, view.id)
    assert status == "stale" and {i[2] for i in items} == {"pending"}
    [(outcome,)] = await _rows(
        pool,
        "SELECT outcome FROM change_proposal_revisions WHERE proposal_id = %s",
        (view.id,),
    )
    assert outcome == "stale"
    assert (
        await _rows(
            pool, "SELECT 1 FROM change_proposal_secrets WHERE proposal_id = %s", (view.id,)
        )
        == []
    )
    assert staged not in _secrets(kube)
    [(text, notice, _)] = await _notices(pool, boss.id)
    assert notice == "proposal"
    assert "is stale" in text and f"proposal_revise {view.id}" in text
    # D2: the proposer re-bases; the new revision applies
    await _revise(pool, kube, settings, boss, view.id, patch=MCP_PATCH, secrets={"TOKEN": canary})
    done = await _review(pool, kube, settings, view.id, 2, approve)
    assert done.status == "applied"
    spec, *_ = await _spec(pool, preset.id)
    assert spec["wrapper"] == {"mcp_servers": {"grafana": GRAFANA}}
    assert await _live(pool, preset.id) == {
        "TOKEN": live["TOKEN"] + 1
    }  # the released number is reused
    await _assert_nowhere(pool, canary, done)


async def _made_stale(pool, kube, settings, boss, preset, canary):
    view = await _filed(pool, kube, settings, boss, canary)
    await _relabel(pool, preset)
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    _error(exc, 409, "proposal_stale")
    return view


async def test_a_stale_proposal_refuses_approve_and_accepts_rework(
    pool, kube, settings, boss, preset, canary
):
    view = await _made_stale(pool, kube, settings, boss, preset, canary)
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "deny", "wrapper.mcp_servers": "approve"},
        )
    _error(exc, 422, "invalid_request")
    assert (await _status(pool, view.id))[0] == "stale"
    done = await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.TOKEN": "deny", "wrapper.mcp_servers": ("rework", "rebase on the new labels")},
    )
    assert done.status == "rework"
    assert done.revisions[0].outcome == "stale"  # the first outcome is kept
    assert {i.path: i.decision for i in done.revisions[0].items} == {
        "env.TOKEN": "denied",
        "wrapper.mcp_servers": "rework",
    }


async def test_a_stale_proposal_accepts_an_all_deny(pool, kube, settings, boss, preset, canary):
    view = await _made_stale(pool, kube, settings, boss, preset, canary)
    done = await _review(
        pool, kube, settings, view.id, 1, {"env.TOKEN": "deny", "wrapper.mcp_servers": "deny"}
    )
    assert done.status == "denied"


async def test_applying_one_proposal_marks_another_open_one_stale_and_notifies_it(
    pool, kube, settings, boss, preset, canary
):
    first = await _filed(pool, kube, settings, boss, canary)
    other = await _session(pool, "prop-other")
    second = await _filed(
        pool, kube, settings, other, canary, patch=PLAIN_PATCH, secrets={"TOKEN": canary + "b"}
    )
    second_staged = store.secret_name(preset.id, "TOKEN", 3)
    assert second_staged in _secrets(kube)
    done = await _review(
        pool,
        kube,
        settings,
        first.id,
        1,
        {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
    )
    assert done.status == "applied"
    status, _ = await _status(pool, second.id)
    assert status == "stale"
    assert second_staged not in _secrets(kube)
    assert (
        await _rows(
            pool, "SELECT 1 FROM change_proposal_secrets WHERE proposal_id = %s", (second.id,)
        )
        == []
    )
    [(text, notice, _)] = await _notices(pool, other.id)
    assert notice == "proposal"
    assert f"Proposal P-{second.id} r1 (preset infra) is stale" in text
    assert len(await _notices(pool, boss.id)) == 1
    await _assert_nowhere(pool, canary, done)


# ---------------------------------------------------------------- lifecycle conflicts


async def test_an_old_revision_is_409_revision_changed(pool, kube, settings, boss, preset, canary):
    view = await _filed(pool, kube, settings, boss, canary)
    await _revise(pool, kube, settings, boss, view.id, patch=MCP_PATCH)
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, {"wrapper.mcp_servers": "approve"})
    _error(exc, 409, "revision_changed")
    assert (await _status(pool, view.id))[0] == "open"


async def test_an_expired_proposal_is_409_and_marked_expired(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE change_proposals SET expires_at = %s WHERE id = %s",
            (datetime.now(UTC) - timedelta(seconds=1), view.id),
        )
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    _error(exc, 409, "proposal_expired")
    assert (await _status(pool, view.id))[0] == "expired"
    assert await _reservations(pool, preset.id) == []
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)
    [(text, _, _)] = await _notices(pool, boss.id)
    assert "expired" in text


async def test_a_deleted_preset_is_409_target_gone_and_released(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await presets_repo.create(conn, "spare", {})  # the last preset cannot be deleted
        await presets_repo.delete(conn, preset.id)
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    _error(exc, 409, "target_gone")
    assert (await _status(pool, view.id))[0] == "target_gone"
    assert (
        await _rows(
            pool, "SELECT 1 FROM change_proposal_secrets WHERE proposal_id = %s", (view.id,)
        )
        == []
    )
    assert store.secret_name(preset.id, "TOKEN", 2) not in _secrets(kube)


@pytest.mark.parametrize("closed", ["applied", "denied", "withdrawn"])
async def test_a_closed_proposal_is_409_not_open(
    pool, kube, settings, boss, preset, canary, closed
):
    view = await _filed(pool, kube, settings, boss, canary)
    both = {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"}
    if closed == "withdrawn":
        await _withdraw(pool, kube, settings, boss, view.id)
    else:
        verdict = "approve" if closed == "applied" else "deny"
        await _review(pool, kube, settings, view.id, 1, dict.fromkeys(both, verdict))
    with pytest.raises(ApiError) as exc:
        await _review(pool, kube, settings, view.id, 1, both)
    _error(exc, 409, "not_open")


async def test_a_rework_proposal_is_409_not_open_until_revised(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    await _review(
        pool,
        kube,
        settings,
        view.id,
        1,
        {"env.TOKEN": "approve", "wrapper.mcp_servers": ("rework", "pin it")},
    )
    with pytest.raises(ApiError) as exc:
        await _review(
            pool, kube, settings, view.id, 1, {"env.TOKEN": "deny", "wrapper.mcp_servers": "deny"}
        )
    _error(exc, 409, "not_open")


def _hold_first_review(monkeypatch) -> tuple[asyncio.Event, asyncio.Event]:
    """The first review to lock its preset waits (holding the proposal row lock) until `go`."""
    held, go = asyncio.Event(), asyncio.Event()
    real = presets_repo.get_for_update
    calls = []

    async def hooked(conn, preset_id):
        found = await real(conn, preset_id)
        calls.append(preset_id)
        if len(calls) == 1:
            held.set()
            await go.wait()
        return found

    monkeypatch.setattr(presets_repo, "get_for_update", hooked)
    return held, go


async def test_two_concurrent_reviews_one_applies_the_other_is_409_not_open(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    view = await _filed(pool, kube, settings, boss, canary)
    held, go = _hold_first_review(monkeypatch)
    both = {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"}
    first = asyncio.create_task(_review(pool, kube, settings, view.id, 1, both))
    await held.wait()
    second = asyncio.create_task(_review(pool, kube, settings, view.id, 1, both))
    await asyncio.sleep(0.3)  # the second now waits on the proposal row lock
    assert not second.done()
    go.set()
    done = await first
    assert done.status == "applied"
    with pytest.raises(ApiError) as exc:
        await second
    _error(exc, 409, "not_open")
    assert await _live(pool, preset.id) == {"TOKEN": 2}
    assert len(await _notices(pool, boss.id)) == 1


async def test_a_withdraw_racing_a_review_loses_to_the_review_holding_the_lock(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    view = await _filed(pool, kube, settings, boss, canary)
    held, go = _hold_first_review(monkeypatch)
    review = asyncio.create_task(
        _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    )
    await held.wait()
    withdraw = asyncio.create_task(_withdraw(pool, kube, settings, boss, view.id))
    await asyncio.sleep(0.3)
    assert not withdraw.done()
    go.set()
    assert (await review).status == "applied"
    with pytest.raises(ApiError) as exc:
        await withdraw
    _error(exc, 409, "not_open")
    assert (await _status(pool, view.id))[0] == "applied"


async def test_a_review_after_a_withdraw_is_409_not_open_and_writes_nothing(
    pool, kube, settings, boss, preset, canary
):
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _spec(pool, preset.id)
    await _withdraw(pool, kube, settings, boss, view.id)
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    _error(exc, 409, "not_open")
    assert await _spec(pool, preset.id) == before
    assert (await _status(pool, view.id))[0] == "withdrawn"


async def test_a_staged_value_gone_since_filing_is_the_stale_path_and_undoes_the_write(
    pool, kube, settings, boss, preset, canary
):
    """`promote_staged` finds no reservation (dropped meanwhile, say by the sweeper): the preset
    write already made in the transaction is undone and the proposal is marked stale."""
    view = await _filed(pool, kube, settings, boss, canary)
    before = await _spec(pool, preset.id)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM change_proposal_secrets WHERE proposal_id = %s", (view.id,))
    with pytest.raises(ApiError) as exc:
        await _review(
            pool,
            kube,
            settings,
            view.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    _error(exc, 409, "proposal_stale")
    assert await _spec(pool, preset.id) == before
    assert await _live(pool, preset.id) == {"TOKEN": 1}
    status, items = await _status(pool, view.id)
    assert status == "stale" and {i[2] for i in items} == {"pending"}
    [(text, _, _)] = await _notices(pool, boss.id)
    assert "is stale" in text


async def test_an_apply_skips_another_proposal_under_concurrent_review_without_deadlock(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    """Review A holds the preset; review B holds proposal B and waits for the preset. A must not
    wait for B's row (a deadlock): it skips it, and B then finds its target changed (stale)."""
    first = await _filed(pool, kube, settings, boss, canary)
    other = await _session(pool, "prop-other")
    second, _, _ = await _create(pool, kube, settings, other, patch=PLAIN_PATCH)
    held, go = _hold_first_review(monkeypatch)
    a = asyncio.create_task(
        _review(
            pool,
            kube,
            settings,
            first.id,
            1,
            {"env.TOKEN": "approve", "wrapper.mcp_servers": "approve"},
        )
    )
    await held.wait()
    b = asyncio.create_task(_review(pool, kube, settings, second.id, 1, {"env.PLAIN": "approve"}))
    await asyncio.sleep(0.3)  # B holds its proposal row and waits on the preset row
    assert not b.done()
    go.set()
    assert (await a).status == "applied"
    with pytest.raises(ApiError) as exc:
        await b
    _error(exc, 409, "proposal_stale")
    assert (await _status(pool, second.id))[0] == "stale"
    spec, *_ = await _spec(pool, preset.id)
    assert spec["env"]["PLAIN"] == {"value": "plain"}
    assert len(await _notices(pool, other.id)) == 1
