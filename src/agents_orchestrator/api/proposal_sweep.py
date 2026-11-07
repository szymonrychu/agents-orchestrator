"""Proposals sweeper (preset-change-proposals 3.4 step 6, D3): staged values must go when a
proposal expires or goes stale even if nobody opens it, so the api runs this beside autoclose.

Each pass, every step on its own connection (a failed step is logged and retried next pass, the
next step still runs):

1. expire: active proposals past `expires_at` on the database clock become `expired`;
2. stale: open preset proposals whose live fingerprint differs from the current revision's
   become `stale` (also the ones a review's SKIP LOCKED pass left unmarked);
3. target_gone: active preset proposals whose preset is deleted or no longer manageable;
4. release: the staged reservations of every proposal no longer open and of superseded
   revisions, Secret first (a missing one is fine, a failed delete keeps the row);
5. collect_bound: bound reservations no session names any more;
6. reap (on the first pass and then every `REAP_SECONDS`): staged Secrets whose reservation row
   never committed.

Each proposal is handled in its own transaction under `FOR UPDATE SKIP LOCKED`: a proposal a
review (or revise, withdraw) holds is left to it and seen again next pass. Every transition is
status-guarded and notifies the proposer once; a `NotInState` (another writer won) is skipped.
Schedule proposals have no staged values; their staleness is checked on read and review only.
Session proposals (stage C) add their branch to steps 2 and 3.

The Kubernetes client is built only when a step has a Secret to delete.
"""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any, cast

from fastapi import FastAPI
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.api import proposals, secret_store
from agents_orchestrator.api.secret_store import KubeApi
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as repo
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

REAP_SECONDS = 3600.0  # orphans only come from failed rollbacks: an hourly probe is enough


class _LazyKube:
    """Builds the client on the first call a step makes, so a pass with nothing to delete never
    needs Kubernetes access."""

    def __init__(self, kube_of: Callable[[], KubeApi]) -> None:
        self._kube_of = kube_of
        self._kube: KubeApi | None = None

    def __getattr__(self, name: str) -> Any:
        if self._kube is None:
            self._kube = self._kube_of()
        return getattr(self._kube, name)


async def _each(
    conn: AsyncConnection,
    ids: list[int],
    step: str,
    act: Callable[[repo.Proposal], Awaitable[None]],
) -> None:
    """Run `act` on each proposal locked SKIP LOCKED, one transaction per proposal; a held row
    is skipped, a lost guard (NotInState) is skipped, a failure is logged and the rest still
    run."""
    for proposal_id in ids:
        try:
            async with conn.transaction():
                p = await repo.lock(conn, proposal_id, skip_locked=True)
                if p is not None and p.status in repo.ACTIVE:
                    await act(p)
        except repo.NotInState:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("proposal sweep %s failed for proposal %d", step, proposal_id)


async def _expire(conn: AsyncConnection) -> None:
    async def act(p: repo.Proposal) -> None:
        if await repo.is_expired(conn, p.id):  # a revise since the read reset expires_at
            await proposals.mark_closed(conn, p, "expired")

    await _each(conn, await repo.due(conn), "expire", act)


async def _target(conn: AsyncConnection, p: repo.Proposal) -> presets_repo.Preset | None:
    """The preset a proposal targets, None when deleted or no longer superleader-manageable."""
    preset = await presets_repo.get(conn, uuid.UUID(p.target_id))
    return preset if preset is not None and preset.superleader_manageable else None


async def _stale(conn: AsyncConnection) -> None:
    async def act(p: repo.Proposal) -> None:
        # only `open`: rework and stale re-base on their next revision anyway (NotInState)
        preset = await _target(conn, p)
        if preset is None:  # the target_gone step closes it
            return
        if await proposals.live_fingerprint(conn, preset) != p.revisions[0].fingerprint:
            await proposals.mark_closed(conn, p, "stale", from_states=("open",))

    await _each(conn, await repo.ids_in(conn, "preset", ("open",)), "stale", act)


async def _target_gone(conn: AsyncConnection) -> None:
    async def act(p: repo.Proposal) -> None:
        if await _target(conn, p) is None:
            await proposals.mark_closed(conn, p, "target_gone")

    await _each(conn, await repo.ids_in(conn, "preset", repo.ACTIVE), "target_gone", act)


async def _release(conn: AsyncConnection, kube: KubeApi, namespace: str) -> None:
    for proposal_id, revision in await secret_store.releasable(conn):
        try:
            async with conn.transaction():
                await secret_store.release(
                    conn, kube, namespace, proposal_id=proposal_id, revision=revision
                )
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("proposal sweep release failed for proposal %d", proposal_id)


async def _in_transaction(conn: AsyncConnection, work: Awaitable[object]) -> None:
    async with conn.transaction():
        await work


async def sweep_once(
    pool: AsyncConnectionPool,
    kube_of: Callable[[], KubeApi],
    settings: Settings,
    *,
    reap: bool = False,
) -> None:
    """One pass of the steps in the module docstring; `reap` adds the orphan probe."""
    kube = cast(KubeApi, _LazyKube(kube_of))
    ns = settings.namespace
    steps: list[tuple[str, Callable[[AsyncConnection], Awaitable[object]]]] = [
        ("expire", _expire),
        ("stale", _stale),
        ("target_gone", _target_gone),
        ("release", lambda conn: _release(conn, kube, ns)),
        (
            "collect_bound",
            lambda conn: _in_transaction(conn, secret_store.collect_bound(conn, kube, ns)),
        ),
    ]
    if reap:
        steps.append(
            (
                "reap",
                lambda conn: _in_transaction(conn, secret_store.reap_unreserved(conn, kube, ns)),
            )
        )
    for name, step in steps:
        try:
            async with pool.connection() as conn:
                await step(conn)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("proposal sweep step %s failed; retrying next pass", name)


async def sweep(app: FastAPI) -> None:
    """Until cancelled: a pass every `proposal_sweep_seconds`, reaping on the first pass and
    then every `REAP_SECONDS`."""
    settings: Settings = app.state.settings
    reaped_at: float | None = None
    while True:
        now = time.monotonic()
        reap = reaped_at is None or now - reaped_at >= REAP_SECONDS
        if reap:
            reaped_at = now
        await sweep_once(app.state.pool, lambda: secret_store.kube_of_app(app), settings, reap=reap)
        await asyncio.sleep(settings.proposal_sweep_seconds)
