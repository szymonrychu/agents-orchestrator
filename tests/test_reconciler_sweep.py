"""The reconciler's periodic cache sweep: gating, interval, stored result, resilience."""

from __future__ import annotations

import asyncio

import pytest
from fakes.fake_kube import FakeKube

from agents_orchestrator.cache.sweep import SweepResult
from agents_orchestrator.db import cache_sweeps
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler import sweeper as sweeper_mod
from agents_orchestrator.reconciler.sweeper import CacheSweeper
from agents_orchestrator.settings import Settings

pytestmark = pytest.mark.asyncio


def settings_for(dsn, **kw) -> Settings:
    base = {
        "cache_s3_endpoint": "http://rgw.example:80",
        "cache_s3_bucket": "bucket",
        "cache_s3_access_key_id": "AKIAEXAMPLE",
        "cache_s3_secret_access_key": "example-secret",
        "cache_image": "harbor.example/orch:1",
    }
    return Settings(
        database_url=dsn,
        master_key="k" * 32,
        agent_image="harbor.example/wrapper:1",
        **{**base, **kw},
    )


class Spy:
    def __init__(self, error: Exception | None = None) -> None:
        self.calls: list[bool] = []
        self.error = error

    async def __call__(self, settings, protected, now, *, dry_run, credentials=None):
        self.calls.append(dry_run)
        if self.error:
            raise self.error
        stamp = await now()
        return SweepResult(
            dry_run=dry_run,
            started_at=stamp,
            finished_at=stamp,
            listed=3,
            planned=1,
            planned_bytes=7,
        )


@pytest.fixture
def spy(monkeypatch):
    s = Spy()
    monkeypatch.setattr(sweeper_mod, "run_sweep", s)
    return s


async def test_disabled_flag_never_sweeps(pool, dsn, spy):
    sw = CacheSweeper(pool, settings_for(dsn, cache_sweep_enabled=False))
    assert await sw.tick() is None and spy.calls == []


async def test_unconfigured_cache_never_sweeps(pool, dsn, spy):
    sw = CacheSweeper(pool, Settings(database_url=dsn, master_key="k" * 32, agent_image="x:1"))
    assert await sw.tick() is None and spy.calls == []


async def test_first_tick_sweeps_and_stores_the_result_for_the_api(pool, dsn, spy):
    sw = CacheSweeper(pool, settings_for(dsn))
    result = await sw.tick()
    assert result is not None and sw.last is result
    async with pool.connection() as conn:
        stored = await cache_sweeps.last(conn)
    assert stored["listed"] == 3 and stored["planned"] == 1 and stored["planned_bytes"] == 7
    assert stored["dry_run"] is True  # the safe default


async def test_dry_run_flag_is_passed_through(pool, dsn, spy):
    await CacheSweeper(pool, settings_for(dsn, cache_sweep_dry_run=False)).tick()
    assert spy.calls == [False]


async def test_interval_is_respected_across_ticks_and_restarts(pool, dsn, spy):
    settings = settings_for(dsn, cache_sweep_interval_seconds=3600)
    await CacheSweeper(pool, settings).tick()
    assert await CacheSweeper(pool, settings).tick() is None  # a new leader, same database
    assert len(spy.calls) == 1
    async with pool.connection() as conn:
        await conn.execute("UPDATE cache_sweeps SET finished_at = now() - interval '2 hours'")
    assert await CacheSweeper(pool, settings).tick() is not None
    assert len(spy.calls) == 2


async def test_a_crashing_sweep_does_not_kill_the_caller_and_is_retried(pool, dsn, monkeypatch):
    bad = Spy(error=RuntimeError("boom"))
    monkeypatch.setattr(sweeper_mod, "run_sweep", bad)
    sw = CacheSweeper(pool, settings_for(dsn))
    assert await sw.tick() is None
    async with pool.connection() as conn:
        assert await cache_sweeps.last(conn) is None  # nothing stored: the next poll retries
    good = Spy()
    monkeypatch.setattr(sweeper_mod, "run_sweep", good)
    assert await sw.tick() is not None


async def test_the_reconciler_runs_the_sweep_task_until_stopped(pool, dsn, monkeypatch):
    started = asyncio.Event()

    async def fake_run(self, stop):
        started.set()
        await stop.wait()

    monkeypatch.setattr(CacheSweeper, "run", fake_run)
    rec = Reconciler(pool, FakeKube(namespace="agents"), settings_for(dsn))
    stop = asyncio.Event()
    task = asyncio.create_task(rec.run(stop))
    await asyncio.wait_for(started.wait(), timeout=10)
    stop.set()
    await asyncio.wait_for(task, timeout=10)


async def test_run_polls_until_stopped(pool, dsn, spy, monkeypatch):
    monkeypatch.setattr(sweeper_mod, "POLL_SECONDS", 0.05)
    sw = CacheSweeper(pool, settings_for(dsn, cache_sweep_interval_seconds=60))
    stop = asyncio.Event()
    task = asyncio.create_task(sw.run(stop))
    await asyncio.sleep(0.3)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
    assert len(spy.calls) == 1  # polled several times, swept once (interval not reached)
