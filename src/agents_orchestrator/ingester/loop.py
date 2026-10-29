"""The ingester (D5): one SSE subscription per agent session, stored and projected.

`Ingester.run(stop)` keeps one task per agent row with a session, a URL and no `gone_at`,
refreshing that set every `settings.ingest_refresh_seconds`. Each task attaches (snapshot and
peers into `agent_status`), then streams events from `Last-Event-ID = last_seq` of the session,
storing every envelope with `insert_events` and projecting state, context and turns. A dropped
stream reconnects with backoff (1 s doubling to 30 s, reset by a received event).

`session_closed` ends the task only when the wrapper's live snapshot maps to `dead`
(deploy-safe-agent-sessions D3): the wrapper is the authority on whether a session is closed.
On attach, a last stored (non-gap) event `session_closed` with a dead snapshot marks the agent
dead and ends the task. A fresh `session_closed` in the stream is stored but not projected; the
task then reads the snapshot: dead marks the agent dead and ends the task, live returns to
re-attach from the close seq with the backoff reset. Database order cannot decide this: when
the ingester is down across a shutdown and a resume, the close the wrapper emitted before the
resume is stored after it, on the replay.

Re-attach after a resume: `AgentRef` carries `agents.wrapper_resumed_at`, which the reconciler
writes on every successful resume and when it finds the stored session of a dead agent live (the
wrapper resumed it itself on start), so `refresh` restarts the task of a resumed session as it
does for a new one; the marker is a re-attach trigger only. The attach projects the snapshot (a
resumed `ready` becomes `idle` and notifies `agent_free`, so the dispatcher delivers) and
streams from the last stored seq: the wrapper continues seq across a resume, and a replay stops
at a replayed close, so streaming after it reads the resumed events. Replayed events at or below
the last stored seq are never re-projected, so a replayed, already stored close reads no
snapshot.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import events, leadership, queue, status
from agents_orchestrator.db.pool import notify
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper import Envelope, WrapperClient, WrapperError, WrapperUnreachable

log = logging.getLogger(__name__)

WrapperFactory = Callable[[str, uuid.UUID], WrapperClient]
Sleep = Callable[[float], Awaitable[Any]]

_STATE_MAP = {
    "ready": "idle",
    "busy": "busy",
    "creating": "booting",
    "stopped": "dead",
    "failed": "dead",
    "dead": "dead",
}
_TURN_END = ("turn_completed", "turn_failed")
_COMPACTION = ("compacting", "compacted")


def map_state(wrapper_state: str | None) -> str:
    """Wrapper public state -> agent_status.state; anything unrecognised is `unknown`."""
    return _STATE_MAP.get(wrapper_state or "", "unknown")


@dataclass(frozen=True)
class AgentRef:
    agent_id: uuid.UUID
    session_id: uuid.UUID
    wrapper_session_id: str
    url: str
    resumed_at: datetime | None


@dataclass(frozen=True)
class _Projected:
    became_idle: bool = False
    closed: bool = False


class _AgentStream:
    """The attach/stream/reconnect loop of one agent session."""

    def __init__(self, ingester: Ingester, ref: AgentRef) -> None:
        self.ing = ingester
        self.ref = ref
        self.last_seq = 0
        self.delay = ingester.backoff_initial

    async def run(self) -> None:
        client = self.ing.wrapper_factory(self.ref.url, self.ref.session_id)
        try:
            while True:
                try:
                    if await self._attach_and_stream(client):
                        log.info(
                            "agent %s session %s closed",
                            self.ref.agent_id,
                            self.ref.wrapper_session_id,
                        )
                        return
                    log.info("agent %s stream ended; reconnecting", self.ref.agent_id)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:  # any failure: back off and attach again
                    log.warning("agent %s stream failed: %s", self.ref.agent_id, exc)
                delay = self.delay
                self.delay = min(delay * 2, self.ing.backoff_max)
                await self.ing.sleep(delay)
        finally:
            with contextlib.suppress(Exception):
                await client.aclose()

    # ------------------------------------------------------------------ attach

    async def _attach_and_stream(self, client: WrapperClient) -> bool:
        """Attach, then stream until the stream ends. True when the session is closed."""
        ref = self.ref
        snap = await client.get_session(ref.wrapper_session_id)
        async with self.ing.pool.connection() as conn:
            self.last_seq = await events.last_seq(conn, ref.agent_id, ref.wrapper_session_id)
            closed = map_state(snap.state) == "dead" and await self._stored_closed(conn)
            state = "dead" if closed else map_state(snap.state)
            change = await status.upsert_status(
                conn,
                ref.agent_id,
                state=state,
                wrapper_idle_since=snap.idle_since,
                context=snap.context.model_dump() if snap.context is not None else None,
                current_turn_id=None if closed else snap.current_turn_id,
                last_seq=self.last_seq,
            )
            if change.became_idle:
                await status.notify_agent_free(conn, ref.session_id, ref.agent_id)
            await status.notify_status(conn, ref.session_id, ref.agent_id)
        if closed:
            return True
        await self._refresh_peers(client)

        async for env in client.events(ref.wrapper_session_id, self.last_seq):
            self.delay = self.ing.backoff_initial  # a received event resets the backoff
            projected = await self._store(env)
            if projected.became_idle:
                await self._refresh_peers(client)
            if projected.closed:
                return await self._closed(client)
        return False

    async def _closed(self, client: WrapperClient) -> bool:
        """A fresh `session_closed` was stored: the live snapshot decides. True when dead."""
        ref = self.ref
        snap = await client.get_session(ref.wrapper_session_id)
        if map_state(snap.state) != "dead":
            # resumed since the close: re-attach from the close seq
            self.delay = self.ing.backoff_initial
            return False
        async with self.ing.pool.connection() as conn:
            change = await status.set_state(conn, ref.agent_id, "dead")
            if change.changed:
                await status.notify_status(conn, ref.session_id, ref.agent_id)
        return True

    async def _stored_closed(self, conn: Any) -> bool:
        """The last stored event, gaps aside, is `session_closed`."""
        cur = await conn.execute(
            "SELECT type = 'session_closed' FROM events"
            " WHERE agent_id = %s AND wrapper_session_id = %s AND type <> 'gap'"
            " ORDER BY seq DESC LIMIT 1",
            (self.ref.agent_id, self.ref.wrapper_session_id),
        )
        row = await cur.fetchone()
        return row is not None and bool(row[0])

    async def _refresh_peers(self, client: WrapperClient) -> None:
        try:
            view = await client.peers()
        except (WrapperError, WrapperUnreachable) as exc:
            log.warning("agent %s peers view unavailable: %s", self.ref.agent_id, exc)
            return
        async with self.ing.pool.connection() as conn:
            await status.set_peers(conn, self.ref.agent_id, view.model_dump(mode="json"))
            await status.notify_status(conn, self.ref.session_id, self.ref.agent_id)

    # ------------------------------------------------------------------ envelopes

    async def _store(self, env: Envelope) -> _Projected:
        ref = self.ref
        fresh = env.seq > self.last_seq
        projected = _Projected()
        async with self.ing.pool.connection() as conn:
            if env.seq > self.last_seq + 1:
                await events.insert_gap(
                    conn,
                    ref.session_id,
                    ref.agent_id,
                    ref.wrapper_session_id,
                    self.last_seq,
                    env.seq,
                )
            new_id = await events.insert_events(
                conn, ref.session_id, ref.agent_id, ref.wrapper_session_id, [env.model_dump()]
            )
            # a replayed (already stored) or late (below last_seq) event is never re-projected
            if fresh and new_id is not None:
                projected = await self._project(conn, env)
            if fresh:
                await status.set_last_seq(conn, ref.agent_id, env.seq)
        if fresh:
            self.last_seq = env.seq
        return projected

    async def _project(self, conn: Any, env: Envelope) -> _Projected:
        ref, data = self.ref, env.data
        if env.type == "session_state":
            change = await status.set_state(
                conn, ref.agent_id, map_state(data.get("state")), since=data.get("since")
            )
            if change.became_idle:
                await status.notify_agent_free(conn, ref.session_id, ref.agent_id)
            if change.changed:
                await status.notify_status(conn, ref.session_id, ref.agent_id)
            return _Projected(became_idle=change.became_idle)
        if env.type == "session_closed":
            return _Projected(closed=True)  # not projected: `_closed` reads the snapshot
        if env.type == "context":
            await status.set_context(conn, ref.agent_id, data)
            await status.notify_status(conn, ref.session_id, ref.agent_id)
        elif env.type in _COMPACTION:
            # every compaction counts (D2), at the time the wrapper reported it (D3), and wakes
            # the dispatcher's leadership pass for the session (D15)
            await status.set_compaction(conn, ref.agent_id, env.ts)
            await status.notify_status(conn, ref.session_id, ref.agent_id)
            await notify(conn, leadership.CHANNEL, {"session_id": str(ref.session_id)})
        elif env.type == "turn_started" and data.get("turn_id"):
            turn_id = str(data["turn_id"])
            await status.upsert_turn(
                conn,
                ref.agent_id,
                turn_id,
                origin=data.get("origin"),
                started_at=env.ts,
                status="running",
            )
            await status.set_current_turn(conn, ref.agent_id, turn_id)
            await status.notify_status(conn, ref.session_id, ref.agent_id)
        elif env.type in _TURN_END:
            record = data.get("record") if isinstance(data.get("record"), dict) else data
            if record.get("turn_id"):
                turn_id = str(record["turn_id"])
                default = "completed" if env.type == "turn_completed" else "failed"
                await status.upsert_turn(
                    conn,
                    ref.agent_id,
                    turn_id,
                    origin=record.get("origin"),
                    started_at=record.get("started_at"),
                    completed_at=record.get("completed_at") or env.ts,
                    status=record.get("state") or default,
                    final_text=record.get("final_text"),
                    usage=record.get("usage"),
                )
                await status.clear_current_turn(conn, ref.agent_id, turn_id)
                await status.notify_status(conn, ref.session_id, ref.agent_id)
        elif env.type == "command_completed":
            record = data.get("record")
            # a prompt command promoted to a turn: its message keeps the turn id (D10)
            if (
                isinstance(record, dict)
                and record.get("result") == "turn"
                and record.get("id")
                and record.get("turn_id")
            ):
                await queue.link_command_turn(conn, str(record["id"]), str(record["turn_id"]))
        # user_message and every other type: stored as an events row only
        return _Projected()


class Ingester:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        settings: Settings,
        wrapper_factory: WrapperFactory | None = None,
        *,
        backoff_initial: float = 1.0,
        backoff_max: float = 30.0,
        sleep: Sleep = asyncio.sleep,
        cancel_grace: float = 1.0,
    ) -> None:
        self.pool = pool
        self.settings = settings
        self.wrapper_factory: WrapperFactory = wrapper_factory or self._default_factory
        self.backoff_initial = backoff_initial
        self.backoff_max = backoff_max
        self.sleep = sleep
        self.cancel_grace = cancel_grace
        self._tasks: dict[uuid.UUID, tuple[AgentRef, asyncio.Task[None]]] = {}

    def _default_factory(self, url: str, session_id: uuid.UUID) -> WrapperClient:
        return WrapperClient(
            url,
            tokens_for(self.settings, session_id).auth,
            send_timeout=self.settings.wrapper_send_timeout_seconds,
        )

    def agent_ids(self) -> set[uuid.UUID]:
        """Agents that currently have a stream task (running, or ended by session_closed)."""
        return set(self._tasks)

    async def run(self, stop: asyncio.Event) -> None:
        try:
            while not stop.is_set():
                try:
                    await self.refresh()
                except Exception:
                    log.exception("ingester refresh failed")
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(stop.wait(), self.settings.ingest_refresh_seconds)
        finally:
            await self._cancel(list(self._tasks))

    async def _wanted(self) -> dict[uuid.UUID, AgentRef]:
        async with self.pool.connection() as conn:
            cur = await conn.execute(
                "SELECT id, session_id, wrapper_session_id, url, wrapper_resumed_at FROM agents"
                " WHERE wrapper_session_id IS NOT NULL AND url IS NOT NULL AND gone_at IS NULL"
            )
            rows = await cur.fetchall()
        return {r[0]: AgentRef(*r) for r in rows}

    async def refresh(self) -> None:
        """Start a task for each new agent, cancel removed ones, restart changed sessions.

        A changed ref is a new session, a moved url or a recorded resume (D3); its task is
        restarted even when it ended on `session_closed`.
        """
        wanted = await self._wanted()
        stale = []
        for agent_id, (ref, task) in self._tasks.items():
            if wanted.get(agent_id) != ref:
                stale.append(agent_id)
            elif task.done() and not task.cancelled() and task.exception() is not None:
                log.error("agent %s stream task died: %s", agent_id, task.exception())
                stale.append(agent_id)
        await self._cancel(stale)
        for agent_id, ref in wanted.items():
            if agent_id not in self._tasks:
                task = asyncio.create_task(_AgentStream(self, ref).run(), name=f"ingest-{agent_id}")
                self._tasks[agent_id] = (ref, task)

    async def _cancel(self, agent_ids: list[uuid.UUID]) -> None:
        """Cancel these agents' tasks and wait for them to end.

        A cancellation can be absorbed (a shielded close swallows it), so a task still running
        after `cancel_grace` is cancelled again rather than wedging refresh or shutdown.
        """
        pending = {self._tasks.pop(a)[1] for a in agent_ids if a in self._tasks}
        while pending:
            for task in pending:
                task.cancel()
            done, pending = await asyncio.wait(pending, timeout=self.cancel_grace)
            for task in done:
                if not task.cancelled() and task.exception() is not None:
                    log.error("ingest task %s ended with %r", task.get_name(), task.exception())
