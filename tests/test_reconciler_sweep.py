"""The reconciler's cache sweep: gating, stored policy, run requests, NOTIFY, resilience."""

from __future__ import annotations

import asyncio
import time

import pytest
from fakes.fake_kube import FakeKube

from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.cache.sweep import SweepResult
from agents_orchestrator.db import cache_sweep_settings, cache_sweeps
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler import sweeper as sweeper_mod
from agents_orchestrator.reconciler.sweeper import CacheSweeper
from agents_orchestrator.settings import Settings

pytestmark = pytest.mark.asyncio


def settings_for(dsn, **kw) -> Settings:
    base = {
        "cache_s3_endpoint": "http://rgw.example:80",
        "cache_s3_bucket": "bucket",
        "cache_s3_access_key_id": "TESTIDEXAMPLE",
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
        self.calls: list[SweepPolicy] = []
        self.error = error

    async def __call__(self, settings, protected, now, *, policy, credentials=None):
        stamp = await now()  # read before the call is visible, like run_sweep's started_at
        self.calls.append(policy)
        await self.during()
        if self.error:
            raise self.error
        return SweepResult(
            dry_run=policy.dry_run,
            started_at=stamp,
            finished_at=stamp,
            listed=3,
            planned=1,
            planned_bytes=7,
        )

    async def during(self) -> None:
        """What happens while the sweep runs; nothing by default."""


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
    assert [p.dry_run for p in spy.calls] == [False]


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


async def put(pool, policy: SweepPolicy) -> None:
    async with pool.connection() as conn:
        await cache_sweep_settings.put(conn, policy)


async def request_run(pool) -> None:
    async with pool.connection() as conn:
        await cache_sweep_settings.request_run(conn)


async def test_stored_policy_overrides_the_environment(pool, dsn, spy):
    stored = SweepPolicy(True, False, 10, 1, 60, 0)
    await put(pool, stored)
    await CacheSweeper(pool, settings_for(dsn, cache_sweep_dry_run=True)).tick()
    assert spy.calls == [stored]


async def test_disabled_row_stops_the_schedule(pool, dsn, spy):
    await put(pool, SweepPolicy(False, True, 30, 3, 3600, 0))
    sw = CacheSweeper(pool, settings_for(dsn, cache_sweep_enabled=True))
    assert await sw.tick() is None and spy.calls == []


async def test_interval_comes_from_the_row(pool, dsn, spy):
    await put(pool, SweepPolicy(True, True, 30, 3, 3600, 0))
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO cache_sweeps (id, finished_at, result)"
            " VALUES (1, now() - interval '120 seconds', '{}'::jsonb)"
        )
    sw = CacheSweeper(pool, settings_for(dsn, cache_sweep_interval_seconds=60))
    assert await sw.tick() is None and spy.calls == []


async def test_run_request_sweeps_when_disabled_with_the_same_guards(pool, dsn, spy):
    stored = SweepPolicy(False, False, 30, 3, 3600, 0)
    await put(pool, stored)
    await request_run(pool)
    sw = CacheSweeper(pool, settings_for(dsn))
    assert await sw.tick() is not None
    assert spy.calls == [stored] and spy.calls[0].dry_run is False
    assert await sw.tick() is None  # the request is satisfied: the sweep started after it
    assert len(spy.calls) == 1


async def test_run_request_honours_dry_run(pool, dsn, spy):
    await request_run(pool)
    await CacheSweeper(pool, settings_for(dsn, cache_sweep_dry_run=True)).tick()
    assert spy.calls[0].dry_run is True


async def test_request_during_a_sweep_is_not_lost(pool, dsn, monkeypatch):
    class Requesting(Spy):
        async def during(self) -> None:
            if len(self.calls) == 1:  # after this sweep's started_at was read
                await request_run(pool)

    s = Requesting()
    monkeypatch.setattr(sweeper_mod, "run_sweep", s)
    sw = CacheSweeper(pool, settings_for(dsn, cache_sweep_interval_seconds=3600))
    assert await sw.tick() is not None  # the schedule: no sweep yet
    assert await sw.tick() is not None  # the request made while the first one ran
    assert await sw.tick() is None
    assert len(s.calls) == 2


async def test_unconfigured_s3_ignores_run_requests(pool, dsn, spy):
    await request_run(pool)
    sw = CacheSweeper(pool, Settings(database_url=dsn, master_key="k" * 32, agent_image="x:1"))
    assert await sw.tick() is None and spy.calls == []


async def wait_until(check, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while not check():
        assert time.monotonic() < deadline, "condition not met in time"
        await asyncio.sleep(0.05)


async def test_notify_wakes_the_sweeper(pool, dsn, spy, monkeypatch):
    # The sweeper also wakes when its LISTEN is (re)established; route that signal to a separate
    # event so only a NOTIFY can wake the loop here, and send the request once LISTEN is up.
    monkeypatch.setattr(sweeper_mod, "POLL_SECONDS", 30)
    listening = asyncio.Event()
    real_listen = sweeper_mod.listen

    def listen_without_ready_wake(conninfo, channels, *, on_ready=None):
        return real_listen(conninfo, channels, on_ready=listening.set)

    monkeypatch.setattr(sweeper_mod, "listen", listen_without_ready_wake)
    sw = CacheSweeper(pool, settings_for(dsn, cache_sweep_interval_seconds=3600))
    stop = asyncio.Event()
    task = asyncio.create_task(sw.run(stop))
    await asyncio.wait_for(listening.wait(), timeout=10)
    await wait_until(lambda: len(spy.calls) == 1, 10)
    await request_run(pool)  # NOTIFYs cache_sweep_changed
    await wait_until(lambda: len(spy.calls) == 2, 5)
    stop.set()
    await asyncio.wait_for(task, timeout=5)
