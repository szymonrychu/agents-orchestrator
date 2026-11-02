"""db.cache_sweep_settings: the stored sweep policy and the run-now request (Postgres)."""

import asyncio
from datetime import timedelta

import psycopg
import pytest

from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.db import cache_sweep_settings as store
from agents_orchestrator.db import cache_sweeps
from agents_orchestrator.db.pool import listen
from agents_orchestrator.settings import Settings


def _settings():
    return Settings(database_url="postgresql://x/y", agent_image="img:1")


async def test_effective_without_a_row_is_the_environment(pool, dsn):
    settings = _settings()
    async with pool.connection() as conn:
        assert await store.get(conn) is None
        got = await store.effective(conn, settings)
    assert got.source == "environment"
    assert got.updated_at is None
    assert got.policy == SweepPolicy.from_settings(settings)


async def test_put_then_effective_is_the_database(pool, dsn):
    first = SweepPolicy(False, False, 10, 1, 600, 0)
    second = SweepPolicy(True, True, 20, 2, 3600, 5)
    async with pool.connection() as conn:
        stored = await store.put(conn, first)
        assert stored.policy == first
        got = await store.effective(conn, _settings())
        assert got.source == "database"
        assert got.policy == first
        assert got.updated_at is not None
        assert got.updated_at == stored.updated_at
        await store.put(conn, second)
        assert (await store.effective(conn, _settings())).policy == second
        assert (await store.get(conn)).policy == second
        cur = await conn.execute("SELECT count(*) FROM cache_sweep_settings")
        assert (await cur.fetchone())[0] == 1


async def test_put_notifies(pool, dsn):
    got: list = []

    async def reader():
        async for item in listen(dsn, [store.CHANNEL]):
            got.append(item)
            return

    task = asyncio.create_task(reader())
    for _ in range(50):
        await asyncio.sleep(0.1)
        async with pool.connection() as conn:
            await store.put(conn, SweepPolicy(True, True, 30, 3, 21600, 2400))
        if got:
            break
    await asyncio.wait_for(task, 5)
    assert got[0] == ("cache_sweep_changed", {})


@pytest.mark.parametrize(
    "values",
    [
        (True, True, 0, 3, 21600, 0),
        (True, True, 3651, 3, 21600, 0),
        (True, True, 30, 0, 21600, 0),
        (True, True, 30, 1001, 21600, 0),
        (True, True, 30, 3, 59, 0),
        (True, True, 30, 3, 2592001, 0),
        (True, True, 30, 3, 21600, -1),
        (True, True, 30, 3, 21600, 604801),
    ],
)
async def test_check_constraints_reject_out_of_range_rows(pool, values):
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                "INSERT INTO cache_sweep_settings (id, enabled, dry_run, retention_days,"
                " keep_newest, interval_seconds, grace_seconds, updated_at)"
                " VALUES (1, %s, %s, %s, %s, %s, %s, now())",
                values,
            )


async def test_only_row_id_1_exists(pool):
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                "INSERT INTO cache_sweep_settings (id, enabled, dry_run, retention_days,"
                " keep_newest, interval_seconds, grace_seconds, updated_at)"
                " VALUES (2, true, true, 30, 3, 21600, 0, now())"
            )
    async with pool.connection() as conn:
        with pytest.raises(psycopg.errors.CheckViolation):
            await conn.execute(
                "INSERT INTO cache_sweep_requests (id, requested_at) VALUES (2, now())"
            )


async def test_request_run_leaves_the_policy_unsaved(pool, dsn):
    async with pool.connection() as conn:
        assert await store.requested_at(conn) is None
        stamp = await store.request_run(conn)
        assert await store.get(conn) is None
        assert (await store.effective(conn, _settings())).source == "environment"
        assert await store.requested_at(conn) == stamp


async def test_run_pending_against_the_last_sweep(pool):
    async with pool.connection() as conn:
        assert await store.run_pending(conn) is False
        requested = await store.request_run(conn)
        assert await store.run_pending(conn) is True
        before = requested - timedelta(seconds=1)
        await cache_sweeps.record(conn, requested, {"started_at": before.isoformat()})
        assert await store.run_pending(conn) is True
        after = requested + timedelta(seconds=1)
        await cache_sweeps.record(conn, requested, {"started_at": after.isoformat()})
        assert await store.run_pending(conn) is False


async def test_repeated_requests_keep_one_row(pool):
    async with pool.connection() as conn:
        first = await store.request_run(conn)
        second = await store.request_run(conn)
        assert second >= first
        cur = await conn.execute("SELECT count(*) FROM cache_sweep_requests")
        assert (await cur.fetchone())[0] == 1
