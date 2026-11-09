"""api.proposal_sweep: the proposals sweeper (plan 3.4 step 6, D3, Q9 A).

Each pass expires due proposals on the database clock, marks open preset proposals whose preset
changed `stale` and those whose preset is gone or no longer manageable `target_gone` (one notice
each, status-guarded, rows a review holds are skipped). A proposal holds no value, so a pass never
touches Kubernetes. Service level: real Postgres, a fake kube that must stay untouched."""

from __future__ import annotations

import asyncio
import inspect
import logging
import secrets as stdlib_secrets

import pytest
from fakes.fake_kube import FakeKube
from test_api_agent import make_settings
from test_proposals_review import _review
from test_proposals_service import (
    MCP_PATCH,
    NS,
    _create,
    _rows,
    _session,
    vault,  # noqa: F401  (an autouse fixture)
)

from agents_orchestrator.api import proposal_sweep
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as proposals_repo


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
    spec = {"docker": True, "env": {"TOKEN": {"value": "token"}, "PLAIN": {"value": "plain"}}}
    async with pool.connection() as conn:
        found = await presets_repo.create(conn, "infra", spec, superleader_manageable=True)
    kube.reset_records()
    return found


async def _sweep(pool) -> None:
    await proposal_sweep.sweep_once(pool)


async def _proposal(pool, kube, settings, who) -> int:
    """An open proposal on the preset; returns its id."""
    view, _, _ = await _create(pool, kube, settings, who, patch=MCP_PATCH)
    return view.id


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


async def test_expired_open_proposal_is_expired_once(pool, kube, settings, boss, preset):
    pid = await _proposal(pool, kube, settings, boss)
    await _backdate(pool, pid)
    await _sweep(pool)
    assert await _status(pool, pid) == "expired"
    [notice] = await _notices(pool, boss.id)
    assert f"P-{pid}" in notice and "expired" in notice
    await _sweep(pool)
    assert len(await _notices(pool, boss.id)) == 1  # a second pass sends no second notice
    assert kube.calls == []


async def test_expiry_uses_the_database_clock_and_skips_a_proposal_not_due(
    pool, kube, settings, boss, preset
):
    pid = await _proposal(pool, kube, settings, boss)
    await _sweep(pool)
    assert await _status(pool, pid) == "open"
    assert await _notices(pool, boss.id) == []


async def test_proposal_revised_after_the_due_read_is_not_expired(
    pool, kube, settings, boss, preset, monkeypatch
):
    """The due list is read before each row is locked: a revise in between resets expires_at,
    and the recheck under the lock (database clock) keeps the proposal open."""
    pid = await _proposal(pool, kube, settings, boss)

    async def stale_read(conn, now=None):
        return [pid]

    monkeypatch.setattr(proposals_repo, "due", stale_read)
    await _sweep(pool)
    assert await _status(pool, pid) == "open"
    assert await _notices(pool, boss.id) == []


async def test_expiry_of_a_proposal_on_an_unmanageable_preset(pool, kube, settings, boss, preset):
    """Ruling (A6 carry): a proposal hidden from its proposer still expires."""
    pid = await _proposal(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (preset.id,)
        )
    await _backdate(pool, pid)
    await _sweep(pool)
    assert await _status(pool, pid) == "expired"  # expire runs before target_gone


async def test_a_failed_proposal_unit_leaves_the_others_committed(
    pool, kube, settings, boss, preset, canary, monkeypatch
):
    """Each proposal is its own committed transaction: the first expiry is visible to another
    connection while the second unit runs, and the second failing leaves the first expired."""
    first = await _proposal(pool, kube, settings, boss)
    second = await _proposal(pool, kube, settings, boss)
    for pid in (first, second):
        await _backdate(pool, pid)
    real = proposal_sweep.proposals.mark_closed
    seen: dict[int, str] = {}

    async def second_fails(conn, p, to, **kw):
        if seen:
            seen[p.id] = await _status(pool, next(iter(seen)))  # read on another connection
            raise RuntimeError("unit failed")
        seen[p.id] = ""
        await real(conn, p, to, **kw)

    monkeypatch.setattr(proposal_sweep.proposals, "mark_closed", second_fails)
    await _sweep(pool)
    done, failed = seen
    assert seen[failed] == "expired"
    assert await _status(pool, done) == "expired"
    assert await _status(pool, failed) == "open"
    monkeypatch.setattr(proposal_sweep.proposals, "mark_closed", real)
    await _sweep(pool)
    assert await _status(pool, failed) == "expired"


# ---------------------------------------------------------------- stale and target gone


async def test_preset_edit_makes_an_open_proposal_stale_in_one_pass(
    pool, kube, settings, boss, preset
):
    pid = await _proposal(pool, kube, settings, boss)
    await _touch_preset(pool, preset.id)
    await _sweep(pool)
    assert await _status(pool, pid) == "stale"
    [notice] = await _notices(pool, boss.id)
    assert "stale" in notice and f"proposal_revise {pid}" in notice
    await _sweep(pool)
    assert len(await _notices(pool, boss.id)) == 1


async def test_unchanged_preset_keeps_the_proposal_open(pool, kube, settings, boss, preset):
    pid = await _proposal(pool, kube, settings, boss)
    await _sweep(pool)
    assert await _status(pool, pid) == "open"


async def test_rework_proposal_is_not_marked_stale(pool, kube, settings, boss, preset):
    """Only `open` goes stale: a proposal in rework re-bases on its next revision anyway."""
    pid = await _proposal(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await proposals_repo.transition(conn, pid, from_states=("open",), to="rework")
    await _touch_preset(pool, preset.id)
    await _sweep(pool)
    assert await _status(pool, pid) == "rework"
    assert await _notices(pool, boss.id) == []


async def test_deleted_preset_gives_target_gone(pool, kube, settings, boss, preset):
    pid = await _proposal(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset.id,))
    await _sweep(pool)
    assert await _status(pool, pid) == "target_gone"
    [notice] = await _notices(pool, boss.id)
    assert "no longer exists" in notice


async def test_unmanageable_preset_gives_target_gone(pool, kube, settings, boss, preset):
    """Ruling 2: a preset no longer manageable is as gone as a deleted one."""
    pid = await _proposal(pool, kube, settings, boss)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (preset.id,)
        )
    await _sweep(pool)
    assert await _status(pool, pid) == "target_gone"
    assert len(await _notices(pool, boss.id)) == 1


async def test_review_holding_the_proposal_lock_is_skipped_and_its_result_stands(
    pool, kube, settings, boss, preset
):
    pid = await _proposal(pool, kube, settings, boss)
    await _backdate(pool, pid)
    async with pool.connection() as held, held.transaction():
        assert await proposals_repo.lock(held, pid) is not None
        # the sweeper must neither wait on the row nor change it
        await asyncio.wait_for(_sweep(pool), timeout=10)
        assert await _status(pool, pid) == "open"
        await proposals_repo.transition(
            held, pid, from_states=("open",), to="denied", outcome="denied"
        )
    await _sweep(pool)
    assert await _status(pool, pid) == "denied"
    assert await _notices(pool, boss.id) == []  # no expiry notice for a reviewed proposal


async def test_proposal_left_open_by_a_skip_locked_apply_is_marked_next_pass(
    pool, kube, settings, boss, preset
):
    """Ruling 1: an apply skips another open proposal whose row is held (SKIP LOCKED); the
    sweeper marks it stale on the next pass."""
    first = await _proposal(pool, kube, settings, boss)
    other = await _session(pool, "prop-other")
    second = await _proposal(pool, kube, settings, other)
    async with pool.connection() as held, held.transaction():
        assert await proposals_repo.lock(held, second) is not None
        await _review(pool, kube, settings, first, 1, {"wrapper.mcp_servers": "approve"})
    assert await _status(pool, first) == "applied"
    assert await _status(pool, second) == "open"  # skipped while held
    await _sweep(pool)
    assert await _status(pool, second) == "stale"
    assert len(await _notices(pool, other.id)) == 1


async def test_sweep_steps_without_secret_steps(pool, monkeypatch):
    """A pass is expire, stale, target_gone and nothing else: no release, collect_bound or reap
    step, no kube argument; the sweeper module no longer knows the secret store."""
    seen: list[str] = []

    def recording(name, real):
        async def step(pool_):
            seen.append(name)
            await real(pool_)

        return step

    for name in ("expire", "stale", "target_gone"):
        attr = f"_{name}"
        monkeypatch.setattr(proposal_sweep, attr, recording(name, getattr(proposal_sweep, attr)))
    await proposal_sweep.sweep_once(pool)
    assert seen == ["expire", "stale", "target_gone"]
    assert list(inspect.signature(proposal_sweep.sweep_once).parameters) == ["pool"]
    for gone in ("_release", "REAP_SECONDS", "secret_store", "_LazyKube"):
        assert not hasattr(proposal_sweep, gone), gone


# ---------------------------------------------------------------- lifespan (F10)


async def test_sweeper_task_starts_ticks_and_stops_cleanly_in_the_lifespan(
    dsn, pool, kube, settings, monkeypatch, caplog
):
    ticks: list[int] = []
    real = proposal_sweep.sweep_once

    async def counted(pool_):
        ticks.append(1)
        await real(pool_)

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
        assert not task.done()
    assert task.cancelled()  # it stops cleanly on shutdown
    failed = [
        r
        for r in caplog.records
        if r.name == proposal_sweep.log.name and r.levelno >= logging.WARNING
    ]
    assert failed == []  # and no pass failed
