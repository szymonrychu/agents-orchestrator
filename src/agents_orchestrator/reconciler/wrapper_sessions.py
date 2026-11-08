"""Session bootstrap and adoption (D6): every ready agent pod gets exactly one wrapper session.

The wrapper allows few sessions per pod and session creation is synchronous, so a blind create
can answer 429. The order is therefore adopt first:
1. a creating, ready or busy session is adopted (the newest first)
2. a stopped or failed session that may have a transcript is resumed, then adopted; a refusal
   falls through
3. otherwise a new session is created in the one rendered workdir (`render.workdir_path`)

A recreated session whose Claude transcripts were restored (deploy-safe-agent-sessions D7) has
each pod's wrapper list its restored session as stopped with a transcript (pod names are stable
per slug), so step 2 resumes it and step 3 never replaces it.

A stored session is kept only while adoptable, resumed when stopped or failed with a transcript,
and otherwise cleared (also when the wrapper no longer lists it) so the agent bootstraps again.
Every successful resume, of the stored session or in step 2, records `agents.wrapper_resumed_at`
(deploy-safe-agent-sessions D3) so the ingester re-attaches to the resumed session, also after
it stored the session's `session_closed`. So does keeping a live stored session of an agent whose
`agent_status` is dead: the wrapper resumed it itself (`sessions.resume_on_start`, D5, after a
pod restart) once the ingester had honoured its close and ended the stream, which nothing else
would restart. Any other adopt, or a create, records none.
Errors never raise out of `bootstrap`: they come back as a message for the session status and
the next tick bootstraps it again.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db.agents import Agent
from agents_orchestrator.reconciler.checks import log_tail
from agents_orchestrator.redact import redact_text
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.wrapper.models import (
    Snapshot,
    WrapperError,
    WrapperTimeout,
    WrapperUnreachable,
    describe_exception,
)

log = logging.getLogger(__name__)

ADOPTABLE = frozenset({"creating", "ready", "busy"})
RESUMABLE = frozenset({"stopped", "failed"})
MAX_CONCURRENT_BOOTSTRAPS = 8
FAILURE_MESSAGE_MAX = 200
STEP_HINT_TIMEOUT = 5.0

WrapperFactory = Callable[[str, str], Any]


def _newest_first(sessions: list[Snapshot]) -> list[Snapshot]:
    """Newest created first; sessions without a timestamp keep the wrapper's order, reversed."""
    indexed = list(enumerate(sessions))
    indexed.sort(
        key=lambda p: (p[1].created_at.timestamp() if p[1].created_at else float("-inf"), p[0])
    )
    return [s for _i, s in reversed(indexed)]


def _describe(exc: Exception) -> str:
    if isinstance(exc, WrapperError):
        return f"{exc.status} {exc.code}"
    return describe_exception(exc)


def failure_of(snap: Snapshot) -> dict[str, Any]:
    """`{step, message, log_tail}` of a failed wrapper session, redacted.

    The step is the wrapper's error step, else its first failed bootstrap step; `message` is the
    first line of the error text (at most 200 characters) and `log_tail` the cut, redacted text.
    """
    steps = snap.bootstrap.steps if snap.bootstrap else []
    failed = next((b for b in steps if b.status == "failed"), None)
    err = snap.error
    step = (err.step if err else None) or (failed.step if failed else None) or snap.step
    raw = (err.message if err else None) or (failed.message if failed else None) or ""
    redacted = redact_text(raw) or ""
    first = next((line.strip() for line in redacted.splitlines() if line.strip()), "")
    return {
        "step": step or "bootstrap",
        "message": first[:FAILURE_MESSAGE_MAX] or f"session {snap.state}",
        "log_tail": log_tail(redacted) or None,
    }


class WrapperSessionBootstrapper:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        wrapper_factory: WrapperFactory,
        limit: int = MAX_CONCURRENT_BOOTSTRAPS,
    ) -> None:
        self.pool = pool
        self.wrapper_factory = wrapper_factory
        self._slots = asyncio.Semaphore(limit)

    async def bootstrap_many(self, agents: list[Agent], spec: SessionSpec, token: str) -> list[str]:
        """Bootstrap the agents concurrently; returns one message per agent that failed."""
        results = await asyncio.gather(*(self.bootstrap(a, spec, token) for a in agents))
        return [m for m in results if m]

    async def bootstrap(self, agent: Agent, spec: SessionSpec, token: str) -> str | None:
        """Make `agent` hold a session. Returns None on success, else a status message."""
        if agent.url is None:
            return f"{agent.pod_name}: no url"
        async with self._slots:
            client = self.wrapper_factory(agent.url, token)
            try:
                return await self._bootstrap(client, agent, spec)
            except asyncio.CancelledError:
                raise
            except (WrapperError, WrapperUnreachable) as exc:
                log.warning("session bootstrap of %s deferred: %s", agent.pod_name, exc)
                message = f"session bootstrap of {agent.pod_name}: {_describe(exc)}"
                return redact_text(message) or message
            finally:
                aclose = getattr(client, "aclose", None)
                if aclose is not None:
                    await aclose()

    async def _bootstrap(self, client: Any, agent: Agent, spec: SessionSpec) -> str | None:
        listed = await client.list_sessions()
        if agent.wrapper_session_id is not None:
            stored = next(
                (s for s in listed if s.wrapper_session_id == agent.wrapper_session_id), None
            )
            if stored is not None and stored.state in ADOPTABLE:
                if await self._left_dead(agent):  # resumed by the wrapper itself: re-attach
                    await self._record(agent, stored, resumed=True)
                return None
            if (
                stored is not None
                and stored.state in RESUMABLE
                and stored.has_transcript is not False
            ):
                try:
                    resumed = await client.resume_session(stored.wrapper_session_id)
                except WrapperError as exc:
                    if exc.status >= 500 or exc.status == 429:
                        raise
                    log.info("resume of stored %s refused: %s", stored.wrapper_session_id, exc)
                else:
                    await self._record(agent, resumed, resumed=True)
                    return None
            async with self.pool.connection() as conn:
                await agents_repo.clear_wrapper_session(conn, agent.id)
            log.info(
                "session %s of %s is %s, bootstrapping again",
                agent.wrapper_session_id,
                agent.pod_name,
                stored.state if stored else "gone",
            )
        snap, resumed = await self._pick(client, agent, listed, spec)
        await self._record(agent, snap, resumed=resumed)
        return None

    async def _left_dead(self, agent: Agent) -> bool:
        async with self.pool.connection() as conn:
            return await agents_repo.agent_state(conn, agent.id) == "dead"

    async def _record(self, agent: Agent, snap: Snapshot, *, resumed: bool) -> None:
        async with self.pool.connection() as conn:
            await agents_repo.set_wrapper_session(
                conn, agent.id, snap.wrapper_session_id, snap.created_at, resumed=resumed
            )

    async def _pick(
        self, client: Any, agent: Agent, listed: list[Snapshot], spec: SessionSpec
    ) -> tuple[Snapshot, bool]:
        """The session to record and whether it was resumed."""
        ordered = _newest_first(listed)
        for snap in ordered:
            if snap.state in ADOPTABLE:
                return snap, False
        for snap in ordered:
            if snap.state in RESUMABLE and snap.has_transcript is not False:
                try:
                    return await client.resume_session(snap.wrapper_session_id), True
                except WrapperError as exc:
                    if exc.status >= 500 or exc.status == 429:
                        raise
                    log.info("resume of %s refused: %s", snap.wrapper_session_id, exc)
        failed = next((s for s in ordered if s.state == "failed"), None)
        if failed is not None:  # the failed session is passed over: keep why it failed
            await self._record_failure(agent, failure_of(failed))
        try:
            created = await client.create_session(
                spec.workdir, spec.workdir, model=spec.model, effort=spec.effort
            )
        except WrapperTimeout as exc:
            step = await self._wrapper_step(client)
            message = f"create timed out after {exc.timeout:g}s, wrapper still at step {step}"
            await self._record_failure(agent, _create_failure(message), keep_other_step=True)
            raise WrapperUnreachable(message) from exc
        except (WrapperError, WrapperUnreachable) as exc:
            if not (isinstance(exc, WrapperError) and exc.status == 429):  # 429: wrapper full
                message = redact_text(f"create failed: {_describe(exc)}") or "create failed"
                await self._record_failure(agent, _create_failure(message), keep_other_step=True)
            raise
        return created, False

    async def _record_failure(
        self, agent: Agent, failure: dict[str, Any], *, keep_other_step: bool = False
    ) -> None:
        async with self.pool.connection() as conn:
            await agents_repo.set_start_failure(
                conn, agent.id, failure, keep_other_step=keep_other_step
            )

    @staticmethod
    async def _wrapper_step(client: Any) -> str:
        """The step the wrapper's newest unfinished session is at, "unknown" when it cannot say."""
        try:
            listed = await client.list_sessions(timeout=STEP_HINT_TIMEOUT)
        except (WrapperError, WrapperUnreachable, TypeError):
            return "unknown"
        for snap in _newest_first(listed):
            if snap.state != "creating":
                continue
            steps = snap.bootstrap.steps if snap.bootstrap else []
            running = next((b.step for b in reversed(steps) if b.status == "running"), None)
            return running or snap.step or "unknown"
        return "unknown"


def _create_failure(message: str) -> dict[str, Any]:
    return {"step": "create", "message": message, "log_tail": None}
