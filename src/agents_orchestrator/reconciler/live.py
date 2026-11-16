"""Live delivery of new backup versions and the on-demand save of one entry (S-68, T-519).

Two level-triggered steps per session, both under the session's reconcile lock and both run at the
end of every pass (`Live.deliver`); a NOTIFY `backups_current` only wakes the pass sooner, and the
per-tick version comparison heals a missed notify.

Live delivery: a running session with a `live` restore entry whose recorded name has a current
version other than the one the entry stands on (`status.backup.entries[i].version`) gets one
restore Job (`LIVE`) for all such entries. The Job never overwrites unsaved edits: a directory that
differs from `backup-state/<index>.json` is `skipped_dirty` (decision 766, plain versioning: the
session's edits survive, the entry keeps its version, a warning event says so, and the same version
is not tried again; the next version is). After a delivery into `claude` `skills` or `.` every
agent of the session is sent the wrapper command `reload-skills` once (`reload_pending` on the run
record, retried only when an agent was unreachable).

On-demand save (decision 765: saves only at discard and through the tool): the agent API sets
`status.backup_save` (`{id, entry_index, ...}`); this step runs one save Job (`SAVE_NOW`) for that
entry, records the outcome through `record_save` like a discard save, sets the entry's version to
the saved one (the saver is not redelivered its own version) and drops the request. Delivery waits
while a request is open, and a request waits while a delivery runs, so a Job never restores into a
directory another is packing.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from dataclasses import replace
from typing import Any
from uuid import UUID

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.reconciler.cache import (
    LIVE,
    SAVE_NOW,
    CacheRunner,
    _new_run,
    _state,
    get_run,
)
from agents_orchestrator.spec import backups, restores
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.models import WrapperError, WrapperUnreachable

log = logging.getLogger(__name__)

RELOAD_COMMAND = "reload-skills"
RELOAD_ATTEMPTS = 5  # passes an unreachable agent is retried
DELIVERABLE_PHASES = frozenset({"ready", "degraded"})
# entry states after which the same version is not delivered again
NOT_RETRIED = frozenset({"skipped_dirty", "missing", "unreadable", "failed"})


def _running(run: dict[str, Any] | None) -> bool:
    return run is not None and run.get("state") == "running"


class Live:
    def __init__(
        self,
        runner: CacheRunner,
        wrapper_factory: Callable[[str, str], Any] | None,
        session_lock: Callable[[UUID], asyncio.Lock],
    ) -> None:
        self.runner = runner
        self.pool: AsyncConnectionPool = runner.pool
        self.wrapper_factory = wrapper_factory
        self._lock = session_lock

    # ------------------------------------------------------------------ the pass

    async def deliver(self) -> None:
        """One pass over the sessions that have live entries, a save request or a run to finish."""
        if not self.runner.settings.cache_available:
            return
        async with self.pool.connection() as conn:
            rows = await sessions.list_sessions(conn)
        for row in rows:
            if row.desired == "deleting" or not self._wants(row):
                continue
            try:
                async with self._lock(row.id):
                    async with self.pool.connection() as conn:
                        session = await sessions.get(conn, row.id)
                    if session is None or session.desired == "deleting":
                        continue
                    await self._session(session)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("live backup step of session %s failed", row.slug)

    @staticmethod
    def _wants(session: Session) -> bool:
        status = session.status
        return (
            any(e.live for _, e in restores(session.spec))
            or "backup_save" in status
            or _running(get_run(status, LIVE))
            or _running(get_run(status, SAVE_NOW))
            or bool((get_run(status, LIVE) or {}).get("reload_pending"))
        )

    async def _session(self, session: Session) -> None:
        session = await self._save_now(session)
        if _running(get_run(session.status, SAVE_NOW)) or "backup_save" in session.status:
            return
        if session.status.get("phase") not in DELIVERABLE_PHASES:
            return
        await self._live(session)

    # ------------------------------------------------------------------ on-demand save

    async def _save_now(self, session: Session) -> Session:
        request = session.status.get("backup_save")
        if not isinstance(request, dict):
            return session
        rid, index = str(request.get("id")), request.get("entry_index")
        run = get_run(session.status, SAVE_NOW)
        if run is None or run.get("request_id") != rid:
            if _running(get_run(session.status, LIVE)):
                return session  # a delivery restores into the directory: wait for it
            if not isinstance(index, int) or index not in {i for i, _ in backups(session.spec)}:
                log.warning("backup save request of session %s names no entry", session.slug)
                return await self._close_request(session)
            session, run = await self.runner.begin_save_now(session, rid, index)
        elif _running(run):
            session, run = await self.runner._observe(session, SAVE_NOW, run)
        elif run.get("job"):
            await self.runner._delete_job(session, SAVE_NOW)
        if run is not None and not _running(run):
            session = await self._close_request(session)
        return session

    async def _close_request(self, session: Session) -> Session:
        await self.runner.write_status(
            session, {}, session.observed_generation, drop=("backup_save",)
        )
        status = {k: v for k, v in session.status.items() if k != "backup_save"}
        return replace(session, status=status)

    # ------------------------------------------------------------------ live delivery

    async def _live(self, session: Session) -> None:
        run = get_run(session.status, LIVE)
        if _running(run):
            session, run = await self.runner._observe(session, LIVE, run)  # type: ignore[arg-type]
        elif run is not None and run.get("job"):
            await self.runner._delete_job(session, LIVE)  # crashed between outcome and delete
        if run is not None and not _running(run) and run.get("reload_pending"):
            session, run = await self._reload(session, run)
        if run is not None and _running(run):
            return
        stale = await self._stale(session)
        if stale:
            await self._begin(session, stale)

    async def _stale(self, session: Session) -> list[tuple[int, str, backups_repo.Version]]:
        """The live restore entries whose name has a current version they do not stand on."""
        entries = self.runner._entries(session)
        out: list[tuple[int, str, backups_repo.Version]] = []
        async with self.pool.connection() as conn:
            for index, entry in restores(session.spec):
                if not entry.live:
                    continue
                record = entries[index]
                restore = record.get("restore") or {}
                name = restore.get("name")
                if restore.get("state") in (None, "running", "failed") or not name:
                    continue  # the start restore has no outcome yet, or failed (orphan)
                current = await backups_repo.current(conn, name)
                if current is None or str(current.id) == record.get("version"):
                    continue
                last = record.get("live_delivery") or {}
                if last.get("version") == str(current.id) and last.get("state") in NOT_RETRIED:
                    continue
                out.append((index, name, current))
        return out

    async def _begin(
        self, session: Session, stale: list[tuple[int, str, backups_repo.Version]]
    ) -> None:
        now = await self.runner._now()
        entries = self.runner._entries(session)
        plan: list[dict[str, Any]] = []
        async with self.pool.connection() as conn:
            for index, name, version in stale:
                await backups_repo.touch_used(conn, name)
                plan.append(
                    {
                        "index": index,
                        "name": name,
                        "key": version.object_key,
                        "version": str(version.id),
                    }
                )
                entries[index]["live_delivery"] = _state("running", name, str(version.id), now)
        run = _new_run(now, plan)
        session = await self.runner._write(session, LIVE, run, entries)
        await self.runner._start(session, LIVE, run)

    # ------------------------------------------------------------------ reload-skills

    async def _reload(
        self, session: Session, run: dict[str, Any]
    ) -> tuple[Session, dict[str, Any]]:
        """Send `reload-skills` to every agent not yet sent it; an unreachable agent is retried
        on a later pass (bounded), one the wrapper refused is not."""
        done = set(run.get("reloaded") or [])
        retry = False
        if self.wrapper_factory is not None:
            async with self.pool.connection() as conn:
                agents = await agents_repo.list_agents(conn, session.id)
            token = tokens_for(self.runner.settings, session.id).auth
            for agent in agents:
                eligible = agent.gone_at is None and agent.ready and agent.wrapper_session_id
                if not eligible or agent.url is None or agent.pod_name in done:
                    continue
                client = self.wrapper_factory(agent.url, token)
                try:
                    await client.command(agent.wrapper_session_id, RELOAD_COMMAND)
                    done.add(agent.pod_name)
                except WrapperUnreachable as exc:
                    retry = True
                    log.warning("reload-skills of %s deferred: %s", agent.pod_name, exc)
                except WrapperError as exc:
                    done.add(agent.pod_name)  # refused: a retry would be refused again
                    log.warning("reload-skills refused by %s: %s", agent.pod_name, exc)
                finally:
                    aclose = getattr(client, "aclose", None)
                    if aclose is not None:
                        await aclose()
        attempts = int(run.get("reload_attempts") or 0) + 1
        run = {
            **run,
            "reloaded": sorted(done),
            "reload_attempts": attempts,
            "reload_pending": retry and attempts < RELOAD_ATTEMPTS,
        }
        return await self.runner._write(session, LIVE, run), run
