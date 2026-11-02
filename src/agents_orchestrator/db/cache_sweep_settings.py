"""cache_sweep_settings and cache_sweep_requests: the sweep policy and the run-now request.

Writer of both tables: the api. Readers: the api and the reconciler's sweeper. Each table holds one
row (id 1). An absent settings row means the ORCH_CACHE_* environment policy applies. A run request
is pending until a sweep has started at or after it (both stamps come from the database clock).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import psycopg

from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.db.pool import notify
from agents_orchestrator.settings import Settings

CHANNEL = "cache_sweep_changed"


@dataclass(frozen=True)
class StoredPolicy:
    policy: SweepPolicy
    updated_at: datetime


@dataclass(frozen=True)
class EffectivePolicy:
    policy: SweepPolicy
    source: Literal["database", "environment"]
    updated_at: datetime | None


async def get(conn: psycopg.AsyncConnection) -> StoredPolicy | None:
    cur = await conn.execute(
        "SELECT enabled, dry_run, retention_days, keep_newest, interval_seconds, grace_seconds,"
        " updated_at FROM cache_sweep_settings WHERE id = 1"
    )
    row = await cur.fetchone()
    if row is None:
        return None
    return StoredPolicy(SweepPolicy(*row[:6]), row[6])


async def put(conn: psycopg.AsyncConnection, policy: SweepPolicy) -> StoredPolicy:
    async with conn.transaction():
        cur = await conn.execute(
            "INSERT INTO cache_sweep_settings (id, enabled, dry_run, retention_days, keep_newest,"
            " interval_seconds, grace_seconds, updated_at)"
            " VALUES (1, %s, %s, %s, %s, %s, %s, now())"
            " ON CONFLICT (id) DO UPDATE SET enabled = EXCLUDED.enabled,"
            " dry_run = EXCLUDED.dry_run, retention_days = EXCLUDED.retention_days,"
            " keep_newest = EXCLUDED.keep_newest, interval_seconds = EXCLUDED.interval_seconds,"
            " grace_seconds = EXCLUDED.grace_seconds, updated_at = EXCLUDED.updated_at"
            " RETURNING updated_at",
            (
                policy.enabled,
                policy.dry_run,
                policy.retention_days,
                policy.keep_newest,
                policy.interval_seconds,
                policy.grace_seconds,
            ),
        )
        row = await cur.fetchone()
        await notify(conn, CHANNEL, {})
    assert row is not None
    return StoredPolicy(policy, row[0])


async def effective(conn: psycopg.AsyncConnection, settings: Settings) -> EffectivePolicy:
    """The saved policy, or the environment policy when none was saved yet."""
    stored = await get(conn)
    if stored is None:
        return EffectivePolicy(SweepPolicy.from_settings(settings), "environment", None)
    return EffectivePolicy(stored.policy, "database", stored.updated_at)


async def request_run(conn: psycopg.AsyncConnection) -> datetime:
    """Ask the sweeper for one pass now. Repeated requests collapse into one row."""
    async with conn.transaction():
        cur = await conn.execute(
            "INSERT INTO cache_sweep_requests (id, requested_at) VALUES (1, now())"
            " ON CONFLICT (id) DO UPDATE SET requested_at = EXCLUDED.requested_at"
            " RETURNING requested_at"
        )
        row = await cur.fetchone()
        await notify(conn, CHANNEL, {})
    assert row is not None
    return row[0]


async def requested_at(conn: psycopg.AsyncConnection) -> datetime | None:
    cur = await conn.execute("SELECT requested_at FROM cache_sweep_requests WHERE id = 1")
    row = await cur.fetchone()
    return row[0] if row else None


async def run_pending(conn: psycopg.AsyncConnection) -> bool:
    """A run was requested and no sweep has started at or after the request."""
    cur = await conn.execute(
        "SELECT NOT coalesce((s.result->>'started_at')::timestamptz >= r.requested_at, false)"
        " FROM cache_sweep_requests r LEFT JOIN cache_sweeps s ON s.id = 1 WHERE r.id = 1"
    )
    row = await cur.fetchone()
    return bool(row[0]) if row else False
