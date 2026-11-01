"""The periodic cache cleanup sweep, run by the reconciler.

The reconciler is a singleton (an advisory lock held by one process, `runtime.singleton`), so
the sweep is leader-only by construction: only the lock holder constructs a `Reconciler` that runs
this task. The interval is read from the database (`cache_sweeps.finished_at`), so a restart or a
new leader does not sweep again before `cache_sweep_interval_seconds` have passed. The last result
is kept in memory and written to the `cache_sweeps` row the api reads. A sweep never kills the
loop: `run_sweep` records its own failures and anything else is logged and retried at the next poll.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.cache.sweep import SweepResult, protected_keys, run_sweep
from agents_orchestrator.db import cache_sweeps
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

POLL_SECONDS = 60


class CacheSweeper:
    def __init__(self, pool: AsyncConnectionPool, settings: Settings) -> None:
        self.pool, self.settings = pool, settings
        self.last: SweepResult | None = None

    @property
    def enabled(self) -> bool:
        return self.settings.cache_sweep_enabled and self.settings.reconciler_s3_configured

    async def tick(self) -> SweepResult | None:
        """Sweep when enabled and the interval has passed since the last one; else None."""
        if not self.enabled:
            return None
        try:
            async with self.pool.connection() as conn:
                since = await cache_sweeps.seconds_since(conn)
            if since is not None and since < self.settings.cache_sweep_interval_seconds:
                return None
            result = await run_sweep(
                self.settings,
                self._protected,
                self._now,
                dry_run=self.settings.cache_sweep_dry_run,
            )
            self.last = result
            async with self.pool.connection() as conn:
                await cache_sweeps.record(
                    conn, result.finished_at or await self._now(), result.to_dict()
                )
            return result
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("cache sweep pass failed")
            return None

    async def run(self, stop: asyncio.Event) -> None:
        """`tick` every minute until `stop`."""
        while not stop.is_set():
            await self.tick()
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=POLL_SECONDS)

    async def _protected(self) -> set[str]:
        async with self.pool.connection() as conn:
            return await protected_keys(conn, self.settings)

    async def _now(self) -> datetime:
        async with self.pool.connection() as conn:
            return (await (await conn.execute("SELECT now()")).fetchone())[0]
