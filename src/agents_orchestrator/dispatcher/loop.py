"""Dispatcher loop: leadership, claim, send, map the outcome, run commands, verify stale rows.

The dispatcher never writes agent_status (free_since moves only through the ingester) and never
re-sends an ambiguous or stale turn: those go to `verifying` and the verifier decides (D24).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from typing import Any

import httpx
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import leadership, queue
from agents_orchestrator.db.pool import listen
from agents_orchestrator.dispatcher import hive
from agents_orchestrator.dispatcher.session_frame import frame
from agents_orchestrator.dispatcher.verify import Verifier, session_max_attempts
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import (
    Ambiguous,
    Busy,
    ConfigError,
    DialogOpen,
    Gone,
    Rejected,
    TurnAccepted,
    Unavailable,
)

log = logging.getLogger(__name__)

MAX_IN_FLIGHT = 8
CHANNELS = ("agent_free", "message_queued", leadership.CHANNEL, leadership.LEADER_CHANGED_CHANNEL)
COMMAND_KINDS = ("interrupt", "compact")


class Dispatcher:
    def __init__(self, pool: AsyncConnectionPool, settings: Settings) -> None:
        self.pool = pool
        self.settings = settings
        self._http: httpx.AsyncClient | None = None
        self.verifier = Verifier(pool, settings, self._wrapper)
        self.hive = hive.HiveState()
        self._push_wake = asyncio.Event()

    # ------------------------------------------------------------------ lifecycle

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _wrapper(self, url: str, token: str) -> WrapperClient:
        """A client on the shared connection pool (aclose leaves the pool open)."""
        if self._http is None:
            self._http = httpx.AsyncClient()
        return WrapperClient(
            url,
            token,
            http=self._http,
            send_timeout=self.settings.wrapper_send_timeout_seconds,
        )

    def _client(self, session_id: Any, url: str) -> WrapperClient:
        return self._wrapper(url, tokens_for(self.settings, session_id).auth)

    async def run(self, stop: asyncio.Event) -> None:
        wake = asyncio.Event()

        async def listener() -> None:
            async for channel, _payload in listen(self.pool.conninfo, CHANNELS):
                wake.set()
                if channel == leadership.LEADER_CHANGED_CHANNEL:
                    self._push_wake.set()

        listener_task = asyncio.create_task(listener())
        push_task = asyncio.create_task(self._push_loop(stop))
        stop_wait = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                wake.clear()
                try:
                    await self.drain()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("dispatcher drain failed")
                wake_wait = asyncio.create_task(wake.wait())
                try:
                    await asyncio.wait(
                        {wake_wait, stop_wait},
                        timeout=self.settings.dispatch_poll_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    wake_wait.cancel()
        finally:
            listener_task.cancel()
            push_task.cancel()
            stop_wait.cancel()
            for task in (listener_task, push_task):
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await self.aclose()

    async def _push_loop(self, stop: asyncio.Event) -> None:
        """Hive state pushes: their own task, so a slow wrapper never delays drain.

        Wakes on `leader_changed` (from any dispatcher), after a local leader change and every
        PUSH_INTERVAL_SECONDS; `push_once` throttles each session itself.
        """
        while not stop.is_set():
            self._push_wake.clear()
            try:
                await hive.push_once(self.pool, self.settings, self._client, self.hive)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("hive push failed")
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._push_wake.wait(), hive.PUSH_INTERVAL_SECONDS)

    # ------------------------------------------------------------------ one wake

    async def drain(self) -> None:
        await self._drain_leadership()
        await self._drain_messages()
        await self._drain_commands()
        async with self.pool.connection() as conn:
            await queue.verify_stale_dispatching(conn, self.settings.dispatching_stale_seconds)
        await self.verify_pending()

    async def _drain_leadership(self) -> None:
        """Run the leadership pass for every hive session that needs one (D7).

        Before the claims, so a message claimed on this wake already sees the new leader. One
        failing session is logged and never holds up the others or the message drain. A pass
        may have decided a request without a leader change (a rejection), so the push loop is
        woken after any pass: the requester learns the outcome (D6).

        The candidates read opens a transaction on the pooled connection; it is committed before
        the passes so each pass's own transaction is the outermost one and commits when the pass
        returns. Left open, a pass would be a savepoint inside it and commit only when the
        connection is released, after the wake: the push loop would read the request as still
        queued and the requester's note would wait for the next timer tick.
        """
        async with self.pool.connection() as conn:
            candidates = await leadership.leadership_candidates(conn)
            await conn.commit()
            for session_id in candidates:
                try:
                    change = await leadership.leadership_pass(conn, session_id)
                except Exception:
                    log.exception("leadership pass for session %s failed", session_id)
                    continue
                self._push_wake.set()
                if change is not None:
                    log.info(
                        "session %s leader %s epoch %s (%s)",
                        session_id,
                        change.agent_id,
                        change.epoch,
                        change.reason,
                    )

    async def verify_pending(self) -> int:
        """Check verifying messages against their wrappers, once per wake and poll (D24)."""
        return await self.verifier.verify_once()

    async def _drain_messages(self) -> None:
        slots = asyncio.Semaphore(MAX_IN_FLIGHT)
        tasks: set[asyncio.Task[None]] = set()
        try:
            while True:
                await slots.acquire()
                try:
                    async with self.pool.connection() as conn:
                        claim = await queue.claim_and_pick(conn)
                except BaseException:
                    slots.release()
                    raise
                if claim is None:
                    slots.release()
                    break
                task = asyncio.create_task(self._guarded_send(claim, slots))
                tasks.add(task)
                task.add_done_callback(tasks.discard)
        finally:
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _guarded_send(self, claim: queue.Claim, slots: asyncio.Semaphore) -> None:
        try:
            await self._send(claim)
        except Exception:
            # The row stays dispatching; verify_stale_dispatching moves it to verifying (D24).
            log.exception("send of message %s failed unexpectedly", claim.message_id)
        finally:
            slots.release()

    # ------------------------------------------------------------------ one send

    async def _heartbeat(self, message_id: int) -> None:
        while True:
            await asyncio.sleep(self.settings.dispatching_heartbeat_seconds)
            async with self.pool.connection() as conn:
                await queue.heartbeat_dispatching(conn, message_id)

    async def _send(self, claim: queue.Claim) -> None:
        s = self.settings
        if not claim.agent_url:
            outcome: Any = Unavailable("agent has no url")
        else:
            client = self._client(claim.session_id, claim.agent_url)
            text = claim.text
            metadata: dict[str, Any] = {
                "orch_message_id": claim.message_id,
                "orch_attempt": claim.attempts + 1,
            }
            if claim.sender_slug is not None:
                text = frame(claim.text, claim.sender_slug, claim.message_id)
                metadata["orch_sender_session"] = claim.sender_slug
            heartbeat = asyncio.create_task(self._heartbeat(claim.message_id))
            try:
                outcome = await client.submit_turn(
                    claim.wrapper_session_id, text, metadata, claim.lane
                )
            finally:
                heartbeat.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await heartbeat

        async with self.pool.connection() as conn:
            mid = claim.message_id
            if isinstance(outcome, TurnAccepted):
                await queue.mark_sent(conn, mid, outcome.turn_id)
                return
            if isinstance(outcome, Ambiguous):
                # The turn may be running: keep the reservation, never re-send here.
                await queue.mark_verifying(conn, mid, "ambiguous")
                return
            await queue.release_agent(
                conn, claim.agent_id, claim.previous_dispatched_at, claimed_at=claim.dispatched_at
            )
            if isinstance(outcome, Busy):
                await queue.requeue(conn, mid)
            elif isinstance(outcome, ConfigError):
                await queue.fail_message(conn, mid, "config_error")
            elif isinstance(outcome, DialogOpen):
                await queue.fail_message(conn, mid, "dialog_open")
            elif isinstance(outcome, Rejected):
                await queue.fail_message(conn, mid, "rejected")
            elif isinstance(outcome, Unavailable | Gone):
                max_attempts = await session_max_attempts(conn, claim.session_id)
                error = "session_gone" if isinstance(outcome, Gone) else "unavailable"
                await queue.record_failure(
                    conn,
                    mid,
                    error,
                    max_attempts,
                    s.retry_backoff_base_seconds,
                    s.retry_backoff_max_seconds,
                )
                if isinstance(outcome, Gone):
                    await queue.enqueue_command(conn, claim.agent_id, "rebootstrap")
            else:  # pragma: no cover - SubmitOutcome is exhaustive
                log.error("unmapped outcome %r for message %s", outcome, mid)

    # ------------------------------------------------------------------ commands

    async def _drain_commands(self) -> None:
        while True:
            async with self.pool.connection() as conn:
                cmd = await queue.claim_command(conn, COMMAND_KINDS)
            if cmd is None:
                return
            status, result = await self._run_command(cmd)
            async with self.pool.connection() as conn:
                await queue.complete_command(conn, cmd.id, status, result)

    async def _run_command(self, cmd: queue.Command) -> tuple[str, dict[str, Any]]:
        if not cmd.agent_url or not cmd.wrapper_session_id:
            return "failed", {"error": "agent has no session"}
        client = self._client(cmd.session_id, cmd.agent_url)
        try:
            if cmd.kind == "interrupt":
                response = await client.interrupt(cmd.wrapper_session_id)
            else:
                response = await client.command(cmd.wrapper_session_id, cmd.kind)
        except Exception as exc:
            return "failed", {"error": f"{type(exc).__name__}: {exc}"}
        return "done", {"response": response}
