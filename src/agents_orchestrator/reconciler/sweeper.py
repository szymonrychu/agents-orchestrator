"""The cache cleanup sweep, run by the reconciler on a schedule and on request.

The reconciler is a singleton (an advisory lock held by one process, `runtime.singleton`), so
the sweep is leader-only by construction: only the lock holder constructs a `Reconciler` that runs
this task. Each tick reads the effective policy (the `cache_sweep_settings` row, else the
`ORCH_CACHE_*` environment), a pending run request and the time since the last sweep finished, all
from the database, so a restart or a new leader neither sweeps early nor loses a request. A sweep
runs when a request is pending (whatever `enabled` says; `dry_run` and every safety guard still
apply) or when the policy is enabled and its interval has passed. The api NOTIFYs
`cache_sweep_changed` on a policy save or a run request, which wakes the loop at once; otherwise it
polls every `POLL_SECONDS`. The last result is kept in memory and written to the `cache_sweeps`
row the api reads. A sweep never kills the loop: `run_sweep` records its own failures and anything
else is logged and retried at the next poll.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.cache.sweep import SweepResult, protected_keys, run_sweep
from agents_orchestrator.db import cache_sweep_settings, cache_sweeps
from agents_orchestrator.db.pool import listen
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

POLL_SECONDS = 60


class CacheSweeper:
    def __init__(self, pool: AsyncConnectionPool, settings: Settings) -> None:
        self.pool, self.settings = pool, settings
        self.last: SweepResult | None = None

    async def tick(self) -> SweepResult | None:
        """Sweep when a run is requested, or when enabled and the interval has passed; else None."""
        if not self.settings.reconciler_s3_configured:
            return None
        try:
            async with self.pool.connection() as conn:
                policy = (await cache_sweep_settings.effective(conn, self.settings)).policy
                pending = await cache_sweep_settings.run_pending(conn)
                since = await cache_sweeps.seconds_since(conn)
            due = policy.enabled and (since is None or since >= policy.interval_seconds)
            if not (pending or due):
                return None
            result = await run_sweep(self.settings, self._protected, self._now, policy=policy)
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
        """`tick` every `POLL_SECONDS`, and at once on NOTIFY cache_sweep_changed, until `stop`.

        The listener also wakes the loop each time its LISTEN is (re)established, so a
        notification sent while it was down is not lost: the tick reads the state from the table.
        """
        wake = asyncio.Event()

        async def listener() -> None:
            async for _channel, _payload in listen(
                self.pool.conninfo, (cache_sweep_settings.CHANNEL,), on_ready=wake.set
            ):
                wake.set()

        listener_task = asyncio.create_task(listener())
        stop_wait = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                wake.clear()
                await self.tick()
                wake_wait = asyncio.create_task(wake.wait())
                try:
                    await asyncio.wait(
                        {wake_wait, stop_wait},
                        timeout=POLL_SECONDS,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    wake_wait.cancel()
        finally:
            listener_task.cancel()
            stop_wait.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task

    async def _protected(self) -> set[str]:
        async with self.pool.connection() as conn:
            return await protected_keys(conn, self.settings)

    async def _now(self) -> datetime:
        async with self.pool.connection() as conn:
            return (await (await conn.execute("SELECT now()")).fetchone())[0]
