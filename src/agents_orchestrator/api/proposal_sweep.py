"""Proposals sweeper (preset-change-proposals 3.4 step 6, D3): proposals must expire, go stale or
lose their target even if nobody opens them, so the api runs this beside autoclose.

Each pass runs the steps below in turn (a failed step is logged and retried next pass, the next
step still runs). A step reads its list on one connection and commits; then every proposal runs
in its own committed transaction on its own pooled connection, so its row lock is released at its
own commit:

1. expire: active proposals past `expires_at` on the database clock become `expired`;
2. stale: open preset, session and project grant proposals whose live fingerprint differs from
   the current revision's become `stale` (also the ones a review's SKIP LOCKED pass left
   unmarked);
3. target_gone: active preset (and preset grant) proposals whose preset is deleted or no longer
   manageable, and active session (and session grant) proposals whose session is purged or
   being deleted.

Each proposal is handled under `FOR UPDATE SKIP LOCKED`: a proposal a review (or revise,
withdraw) holds is left to it and seen again next pass. Every transition is status-guarded and
notifies the proposer once; a `NotInState` (another writer won) is skipped. A proposal holds no
value and no Secret, so the sweeper never touches Kubernetes. Schedule proposals' staleness is
checked at review only (views read `stale: false`).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi import FastAPI
from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.api import proposals
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import proposals as repo
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)


async def _read(
    pool: AsyncConnectionPool, read: Callable[[AsyncConnection], Awaitable[Any]]
) -> Any:
    """A listing read on its own connection, committed before any unit runs."""
    async with pool.connection() as conn:
        return await read(conn)


async def _each(
    pool: AsyncConnectionPool,
    ids: list[int],
    step: str,
    act: Callable[[AsyncConnection, repo.Proposal], Awaitable[None]],
) -> None:
    """Run `act` on each proposal locked SKIP LOCKED, each in its own committed transaction on
    its own connection (so its locks go at its commit); a held row is skipped, a lost guard
    (NotInState) is skipped, a failure is logged and the rest still run and commit."""
    for proposal_id in ids:
        try:
            async with pool.connection() as conn, conn.transaction():
                p = await repo.lock(conn, proposal_id, skip_locked=True)
                if p is not None and p.status in repo.ACTIVE:
                    await act(conn, p)
        except repo.NotInState:
            continue
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("proposal sweep %s failed for proposal %d", step, proposal_id)


async def _expire(pool: AsyncConnectionPool) -> None:
    async def act(conn: AsyncConnection, p: repo.Proposal) -> None:
        if await repo.is_expired(conn, p.id):  # a revise since the read reset expires_at
            await proposals.mark_closed(conn, p, "expired")

    await _each(pool, await _read(pool, repo.due), "expire", act)


async def _live(conn: AsyncConnection, p: repo.Proposal) -> str | None:
    """The live fingerprint of the preset or session a proposal targets; None when the preset is
    deleted or no longer superleader-manageable, or the session purged or being deleted."""
    if p.kind in proposals.PROJECT_KINDS:
        return await proposals.live_projects_fingerprint(conn, p.kind, p.target_id)
    if p.kind == "session":
        found = await proposals.live_session(conn, uuid.UUID(p.target_id))
        return (
            None
            if found is None
            else proposals.fingerprint_session(found[1], found[0].secret_versions)
        )
    preset = await presets_repo.get(conn, uuid.UUID(p.target_id))
    if preset is None or not preset.superleader_manageable:
        return None
    return proposals.fingerprint_preset(preset)


async def _ids(conn: AsyncConnection, statuses: tuple[str, ...]) -> list[int]:
    """The preset, session and project grant proposals in `statuses` (schedules are compared at
    review only)."""
    kinds = ("preset", "session", *proposals.PROJECT_KINDS)
    return [i for kind in kinds for i in await repo.ids_in(conn, kind, statuses)]


async def _stale(pool: AsyncConnectionPool) -> None:
    async def act(conn: AsyncConnection, p: repo.Proposal) -> None:
        # only `open`: rework and stale re-base on their next revision anyway (NotInState)
        live = await _live(conn, p)
        if live is None:  # the target_gone step closes it
            return
        if live != p.revisions[0].fingerprint:
            await proposals.mark_closed(conn, p, "stale", from_states=("open",))

    ids = await _read(pool, lambda conn: _ids(conn, ("open",)))
    await _each(pool, ids, "stale", act)


async def _target_gone(pool: AsyncConnectionPool) -> None:
    async def act(conn: AsyncConnection, p: repo.Proposal) -> None:
        if await _live(conn, p) is None:
            await proposals.mark_closed(conn, p, "target_gone")

    ids = await _read(pool, lambda conn: _ids(conn, repo.ACTIVE))
    await _each(pool, ids, "target_gone", act)


async def sweep_once(pool: AsyncConnectionPool) -> None:
    """One pass of the steps in the module docstring."""
    steps: list[tuple[str, Callable[[AsyncConnectionPool], Awaitable[object]]]] = [
        ("expire", _expire),
        ("stale", _stale),
        ("target_gone", _target_gone),
    ]
    for name, step in steps:
        try:
            await step(pool)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("proposal sweep step %s failed; retrying next pass", name)


async def sweep(app: FastAPI) -> None:
    """Until cancelled: a pass every `proposal_sweep_seconds`."""
    settings: Settings = app.state.settings
    while True:
        await sweep_once(app.state.pool)
        await asyncio.sleep(settings.proposal_sweep_seconds)
