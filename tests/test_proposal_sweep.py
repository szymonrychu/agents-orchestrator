"""api.proposal_sweep: the proposals sweeper (plan 3.4 step 6, D3, Q9 A, Q13 B).

Each pass expires due proposals on the database clock, marks open preset proposals whose preset
changed `stale` and those whose preset is gone or no longer manageable `target_gone` (one notice
each, status-guarded, rows a review holds are skipped), releases the staged Secrets of every
proposal no longer open and of superseded revisions, collects bound reservations no session
names, and (when asked) reaps staged Secrets whose reservation row never committed. Service level:
real Postgres, a fake kube."""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets

import pytest
from fakes.fake_kube import FakeKube
from pydantic import SecretStr
from test_api_agent import make_settings
from test_proposals_review import _review
from test_proposals_service import (
    MCP_PATCH,
    NS,
    _create,
    _reservations,
    _revise,
    _rows,
    _secrets,
    _session,
)

from agents_orchestrator.api import proposal_sweep
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as proposals_repo
from agents_orchestrator.k8s import secrets as store
from agents_orchestrator.k8s.labels import MANAGED_BY, MANAGED_BY_LABEL


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
    spec = {"docker": True, "env": {"TOKEN": {"protected": True}, "PLAIN": {"value": "plain"}}}
    async with pool.connection() as conn:
        found = await presets_repo.create(conn, "infra", spec, superleader_manageable=True)
        await store.set_secret(conn, kube, NS, found.id, "TOKEN", SecretStr(canary + "-live"))
    kube.reset_records()
    return found


class Builds:
    """A `kube_of` that counts how often the client was asked for."""

    def __init__(self, kube) -> None:
        self.kube = kube
        self.count = 0

    def __call__(self):
        self.count += 1
        return self.kube


async def _sweep(pool, kube, settings, **kw) -> Builds:
    builds = Builds(kube)
    await proposal_sweep.sweep_once(pool, builds, settings, **kw)
    return builds


async def _staged(pool, kube, settings, boss, canary):
    """An open proposal staging a new TOKEN value (v2); returns (id, staged Secret name)."""
    view, _, _ = await _create(
        pool,
        kube,
        settings,
        boss,
        patch=MCP_PATCH,
        secrets={"TOKEN": SecretStr(canary + "-new")},
    )
    [name] = [n for n in _secrets(kube) if n.endswith("-v2")]
    return view.id, name


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


async def _backdate(pool, proposal_id) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE change_proposals SET expires_at = now() - interval '1 minute' WHERE id = %s",
            (proposal_id,),
        )


async def _touch_preset(pool, preset_id) -> None:
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET updated_at = now() + interval '1 second' WHERE id = %s",
            (preset_id,),
        )


# ---------------------------------------------------------------- expiry


async def test_expired_open_proposal_is_expired_once_and_its_secret_deleted(
    pool, kube, settings, boss, preset, canary
):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await _backdate(pool, pid)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "expired"
    [notice] = await _notices(pool, boss.id)
    assert f"P-{pid}" in notice and "expired" in notice
    assert staged not in _secrets(kube)
    assert await _reservations(pool, preset.id) == []
    await _sweep(pool, kube, settings)
    assert len(await _notices(pool, boss.id)) == 1  # a second pass sends no second notice


async def test_expiry_uses_the_database_clock_and_skips_a_proposal_not_due(
    pool, kube, settings, boss, preset, canary
):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "open"
    assert staged in _secrets(kube)
    assert await _notices(pool, boss.id) == []


async def test_proposal_revised_after_the_due_read_is_not_expired(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    """The due list is read before each row is locked: a revise in between resets expires_at,
    and the recheck under the lock (database clock) keeps the proposal open."""
    pid, staged = await _staged(pool, kube, settings, boss, canary)

    async def stale_read(conn, now=None):
        return [pid]

    monkeypatch.setattr(proposals_repo, "due", stale_read)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "open"
    assert staged in _secrets(kube)
    assert await _notices(pool, boss.id) == []


async def test_expiry_releases_the_secret_of_a_proposal_on_an_unmanageable_preset(
    pool, kube, settings, boss, preset, canary
):
    """Ruling (A6 carry): a proposal hidden from its proposer still loses its staged value."""
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (preset.id,)
        )
    await _backdate(pool, pid)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) in ("expired", "target_gone")
    assert staged not in _secrets(kube)
    assert await _reservations(pool, preset.id) == []


# ---------------------------------------------------------------- stale and target gone


async def test_preset_edit_makes_an_open_proposal_stale_in_one_pass(
    pool, kube, settings, boss, preset, canary
):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await _touch_preset(pool, preset.id)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "stale"
    [notice] = await _notices(pool, boss.id)
    assert "stale" in notice and f"proposal_revise {pid}" in notice
    assert staged not in _secrets(kube)
    assert await _reservations(pool, preset.id) == []
    await _sweep(pool, kube, settings)
    assert len(await _notices(pool, boss.id)) == 1


async def test_unchanged_preset_keeps_the_proposal_open(pool, kube, settings, boss, preset, canary):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "open"
    assert staged in _secrets(kube)
    assert [r[4] for r in await _reservations(pool, preset.id)] == ["staged"]


async def test_rework_proposal_is_not_marked_stale(pool, kube, settings, boss, preset, canary):
    """Only `open` goes stale: a proposal in rework re-bases on its next revision anyway."""
    pid, _ = await _staged(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await proposals_repo.transition(conn, pid, from_states=("open",), to="rework")
    await _touch_preset(pool, preset.id)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "rework"
    assert await _notices(pool, boss.id) == []


async def test_deleted_preset_gives_target_gone_and_releases(
    pool, kube, settings, boss, preset, canary
):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset.id,))
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "target_gone"
    [notice] = await _notices(pool, boss.id)
    assert "no longer exists" in notice
    assert staged not in _secrets(kube)
    assert await _reservations(pool, preset.id) == []


async def test_unmanageable_preset_gives_target_gone_and_releases(
    pool, kube, settings, boss, preset, canary
):
    """Ruling 2: a preset no longer manageable is as gone as a deleted one."""
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (preset.id,)
        )
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "target_gone"
    assert len(await _notices(pool, boss.id)) == 1
    assert staged not in _secrets(kube)


async def test_review_holding_the_proposal_lock_is_skipped_and_its_result_stands(
    pool, kube, settings, boss, preset, canary
):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await _backdate(pool, pid)
    async with pool.connection() as held, held.transaction():
        assert await proposals_repo.lock(held, pid) is not None
        # the sweeper must neither wait on the row nor change it
        await asyncio.wait_for(_sweep(pool, kube, settings), timeout=10)
        assert await _status(pool, pid) == "open"
        await proposals_repo.transition(
            held, pid, from_states=("open",), to="denied", outcome="denied"
        )
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "denied"
    assert await _notices(pool, boss.id) == []  # no expiry notice for a reviewed proposal
    assert staged not in _secrets(kube)  # the closed proposal's value is released


async def test_proposal_left_open_by_a_skip_locked_apply_is_marked_next_pass(
    pool, kube, settings, boss, preset, canary
):
    """Ruling 1: an apply skips another open proposal whose row is held (SKIP LOCKED); the
    sweeper marks it stale and releases its value on the next pass."""
    first, _, _ = await _create(pool, kube, settings, boss, patch=MCP_PATCH)
    other = await _session(pool, "prop-other")
    second, staged = await _staged(pool, kube, settings, other, canary)
    async with pool.connection() as held, held.transaction():
        assert await proposals_repo.lock(held, second) is not None
        await _review(pool, kube, settings, first.id, 1, {"wrapper.mcp_servers": "approve"})
    assert await _status(pool, first.id) == "applied"
    assert await _status(pool, second) == "open"  # skipped while held
    await _sweep(pool, kube, settings)
    assert await _status(pool, second) == "stale"
    assert len(await _notices(pool, other.id)) == 1
    assert staged not in _secrets(kube)


# ---------------------------------------------------------------- release


async def test_kube_delete_failure_keeps_the_reservation_and_the_next_pass_deletes(
    pool, kube, settings, boss, preset, canary
):
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await _backdate(pool, pid)
    kube.fail("Secret", staged, verbs=["delete"], times=1)
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "expired"
    assert staged in _secrets(kube)
    assert len(await _reservations(pool, preset.id)) == 1
    await _sweep(pool, kube, settings)
    assert staged not in _secrets(kube)
    assert await _reservations(pool, preset.id) == []
    assert len(await _notices(pool, boss.id)) == 1


async def test_superseded_revision_reservation_is_released(
    pool, kube, settings, boss, preset, canary
):
    """Ruling 1: a revise whose after-commit release failed leaves r1's value; the sweeper drops
    it and keeps the value of the current revision."""
    pid, old = await _staged(pool, kube, settings, boss, canary)
    kube.fail("Secret", old, verbs=["delete"], times=1)
    await _revise(
        pool, kube, settings, boss, pid, patch=MCP_PATCH, secrets={"TOKEN": SecretStr(canary + "x")}
    )
    assert old in _secrets(kube)
    [new] = [n for n in _secrets(kube) if n.endswith("-v3")]
    await _sweep(pool, kube, settings)
    assert await _status(pool, pid) == "open"
    assert old not in _secrets(kube)
    assert new in _secrets(kube)
    assert [(r[1], r[3]) for r in await _reservations(pool, preset.id)] == [(3, 2)]


async def test_release_tolerates_a_secret_already_missing(
    pool, kube, settings, boss, preset, canary
):
    """Ruling 5: a reservation whose Secret is gone is still dropped."""
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    await kube.delete("Secret", NS, staged)
    await _backdate(pool, pid)
    await _sweep(pool, kube, settings)
    assert await _reservations(pool, preset.id) == []


async def test_bound_reservation_whose_session_is_gone_is_collected(
    pool, kube, settings, preset, canary
):
    holder = await _session(pool, "prop-holder")
    gone = store.secret_name(preset.id, "TOKEN", 5)
    kept = store.secret_name(preset.id, "TOKEN", 6)
    async with pool.connection() as conn:
        for version in (5, 6):
            await conn.execute(
                "INSERT INTO change_proposal_secrets"
                " (preset_id, name, version, proposal_id, revision, state)"
                " VALUES (%s, 'TOKEN', %s, 99, 1, 'bound')",
                (preset.id, version),
            )
            await store._write(
                kube, NS, store.secret_name(preset.id, "TOKEN", version), SecretStr(canary), {}
            )
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s::jsonb WHERE id = %s",
            (f'{{"TOKEN": "{kept}"}}', holder.id),
        )
    await _sweep(pool, kube, settings)
    assert gone not in _secrets(kube)
    assert kept in _secrets(kube)
    assert [r[1] for r in await _reservations(pool, preset.id)] == [6]


async def test_no_reservations_never_build_the_kube_client(pool, kube, settings, boss):
    async with pool.connection() as conn:
        await presets_repo.create(conn, "infra", {"docker": True}, superleader_manageable=True)
    view, _, _ = await _create(pool, kube, settings, boss, patch=MCP_PATCH)
    await _backdate(pool, view.id)
    builds = await _sweep(pool, kube, settings, reap=True)
    assert await _status(pool, view.id) == "expired"
    assert builds.count == 0
    assert kube.calls == []


# ---------------------------------------------------------------- orphan reaping (ruling 4)


async def test_orphan_staged_secret_without_reservation_is_reaped(
    pool, kube, settings, preset, canary
):
    """A create that rolled back after writing its staged Secret (and could not delete it) left
    `v2` with no reservation row: the reaper deletes it, never the live v1."""
    live = store.secret_name(preset.id, "TOKEN", 1)
    orphan = store.secret_name(preset.id, "TOKEN", 2)
    labels = {MANAGED_BY_LABEL: MANAGED_BY, store.PROPOSAL_LABEL: "77"}
    await store._write(kube, NS, orphan, SecretStr(canary), labels)
    await _sweep(pool, kube, settings)
    assert orphan in _secrets(kube)  # only when asked (the loop asks at start and hourly)
    await _sweep(pool, kube, settings, reap=True)
    assert orphan not in _secrets(kube)
    assert live in _secrets(kube)


async def test_reaper_keeps_reserved_current_and_session_named_secrets(
    pool, kube, settings, boss, preset, canary
):
    """A staged reservation (v2), the current version (v1, promoted Secrets keep the proposal
    label) and a name a session references are never reaped."""
    pid, staged = await _staged(pool, kube, settings, boss, canary)
    live = store.secret_name(preset.id, "TOKEN", 1)
    named = store.secret_name(preset.id, "TOKEN", 3)  # above every row, but a session names it
    await store._write(kube, NS, named, SecretStr(canary), {})
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s::jsonb WHERE id = %s",
            (f'{{"TOKEN": "{named}"}}', boss.id),
        )
    await _sweep(pool, kube, settings, reap=True)
    assert {staged, live, named} <= _secrets(kube)
    assert await _status(pool, pid) == "open"


async def test_reaper_waits_for_a_stage_in_flight(pool, kube, settings, boss, preset, canary):
    """The probe runs under the variable lock: a stage holding it commits its reservation first,
    so the reaper never deletes a version whose row is about to commit."""
    async with pool.connection() as conn, conn.transaction():
        await conn.execute(
            "INSERT INTO change_proposals (kind, target_id, target_name, proposer_session,"
            " proposer_slug, proposer_superleader, expires_at)"
            " SELECT 'preset', %s, 'infra', id, slug, true, now() + interval '1 day'"
            " FROM sessions LIMIT 1",
            (str(preset.id),),
        )
        name = await store.stage_secret(
            conn, kube, NS, preset.id, "TOKEN", SecretStr(canary), proposal_id=1, revision=1
        )
        reaper = asyncio.create_task(_sweep(pool, kube, settings, reap=True))
        await asyncio.sleep(0.5)
        assert not reaper.done()  # blocked on the variable lock
    await asyncio.wait_for(reaper, timeout=10)
    assert name in _secrets(kube)


# ---------------------------------------------------------------- lifespan (F10)


async def test_sweeper_task_starts_ticks_and_stops_cleanly_in_the_lifespan(
    dsn, pool, kube, settings, monkeypatch, caplog
):
    ticks: list[bool] = []
    real = proposal_sweep.sweep_once

    async def counted(pool_, kube_of, settings_, *, reap=False):
        ticks.append(reap)
        await real(pool_, kube_of, settings_, reap=reap)

    monkeypatch.setattr(proposal_sweep, "sweep_once", counted)
    fast = settings.model_copy(update={"proposal_sweep_seconds": 1})
    app = create_app(fast, kube=kube)
    async with app.router.lifespan_context(app):
        task = app.state.proposal_sweep_task
        assert task.get_name() == "proposal_sweep"  # the task starts
        assert not task.done()
        for _ in range(60):
            if len(ticks) >= 2:
                break
            await asyncio.sleep(0.1)
        assert len(ticks) >= 2  # it ticks on the interval
        assert ticks[0] is True  # the first pass also reaps orphans
        assert ticks[1] is False
        assert not task.done()
    assert task.cancelled()  # it stops cleanly on shutdown
    failed = [
        r
        for r in caplog.records
        if r.name == proposal_sweep.log.name and r.levelno >= logging.WARNING
    ]
    assert failed == []  # and no pass failed
