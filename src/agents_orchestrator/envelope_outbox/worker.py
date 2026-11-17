"""The envelope relay worker (S-70): sends the chat envelopes the ingester queued.

Per session, oldest row first and one row at a time (the message order). A row is claimed
`queued -> sending`, forwarded through the request-free relay core (`api/chat_core.py`, the same
target, `X-Orch-Agent` header, occupier relay and activity accounting as the agent routes) and
settled by the answer:

- 2xx: `accepted` with the upstream `send_id`;
- 429, 5xx, an unreachable upstream: back to `queued` with a backoff that honours `Retry-After`
  (the session waits, so the order holds), `failed` `upstream_unavailable` after 5 attempts;
- any other 4xx or a refusal of the relay (`files_not_relayable`, ...): `refused` with the code.

Delivery is at most once: on start, `sending` rows older than 60 s become `failed`
`unknown_outcome`, and an unexpected error after the claim does the same. A refusal of a row whose
turn is already over (its Stop was allowed) is told to the agent's session as a message naming the
envelope index and the code, since the Stop hook can no longer carry it.

The worker runs inside the ingester process (a singleton, so one claimer per session) when the
chat upstream is configured. It wakes on NOTIFY `envelope_outbox` and polls as a fallback.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import time
import uuid
from collections.abc import Callable
from typing import Any

import httpx
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.api import chat_core, forward
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import envelope_outbox as outbox
from agents_orchestrator.db import leadership, queue
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.pool import listen
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

# the connector's NOTICE (agents-matrix-connector tracker.py): the user sees one apology
NOTICE = "Sorry, the agent could not answer that message."
MAX_ATTEMPTS = 5
POLL_SECONDS = 5.0
MAX_ERROR_CHARS = 500


def _error_of(content: bytes, status: int) -> tuple[str, str | None]:
    """(code, message) of an upstream error body; falls back to `http_<status>`."""
    try:
        body = json.loads(content)
    except ValueError:
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict) and isinstance(error.get("code"), str):
        message = error.get("message")
        return error["code"], message[:MAX_ERROR_CHARS] if isinstance(message, str) else None
    return f"http_{status}", None


def _send_id(content: bytes) -> str | None:
    try:
        body = json.loads(content)
    except ValueError:
        return None
    if not isinstance(body, dict):
        return None
    value = body.get("send_id") or body.get("id")
    return str(value) if value is not None else None


def _payload(row: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    """(path, JSON body) of the upstream call for a row."""
    if row["kind"] == "react":
        return "/reactions", {"to": row["reply_to"], "key": row["react_key"]}
    if row["kind"] == "notice":
        data: dict[str, Any] = {"text": NOTICE}
    else:
        data = {"text": row["body"]}
        if row["reply_to"]:
            data["reply_to"] = row["reply_to"]
        if row["files"]:
            data["files"] = list(row["files"])
    if row["room"]:
        data["room"] = row["room"]
    return "/messages", data


def refusal_text(row: dict[str, Any], code: str) -> str:
    where = (
        f"envelope {row['envelope_index']}" if row["envelope_index"] is not None else "an envelope"
    )
    return (
        f"Chat {where} of an earlier message of yours was refused after your turn ended: {code}."
        " It was not delivered; send a corrected one if it still matters."
    )


class EnvelopeRelay:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        settings: Settings,
        http: httpx.AsyncClient | None = None,
        *,
        poll_seconds: float = POLL_SECONDS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.pool = pool
        self.settings = settings
        self._http = http
        self.poll_seconds = poll_seconds
        self.clock = clock
        self._blocked: dict[uuid.UUID, float] = {}  # session -> clock time it may go on

    # ------------------------------------------------------------------ loop

    async def run(self, stop: asyncio.Event) -> None:
        owned = self._http is None
        http = self._http or forward.new_client()
        self._http = http
        wake = asyncio.Event()
        listener = asyncio.create_task(self._listen(wake))
        try:
            async with self.pool.connection() as conn:
                swept = await outbox.sweep_stale_sending(conn)
            if swept:
                log.warning("envelope relay: %d sending rows became unknown_outcome", swept)
            while not stop.is_set():
                wake.clear()
                try:
                    progressed = await self.drain()
                except Exception:
                    log.exception("envelope relay pass failed")
                    progressed = False
                if progressed:
                    continue
                await self._wait(stop, wake)
        finally:
            listener.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await listener
            if owned:
                self._http = None
                await http.aclose()

    async def _listen(self, wake: asyncio.Event) -> None:
        async for _channel, _payload in listen(
            self.settings.database_url, [outbox.CHANNEL], on_ready=wake.set
        ):
            wake.set()

    async def _wait(self, stop: asyncio.Event, wake: asyncio.Event) -> None:
        timeout = self.poll_seconds
        if self._blocked:
            timeout = min(timeout, max(0.0, min(self._blocked.values()) - self.clock()))
        waiters = {asyncio.create_task(stop.wait()), asyncio.create_task(wake.wait())}
        try:
            await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for w in waiters:
                w.cancel()

    async def drain(self) -> bool:
        """One pass over every queued session that is not backing off; True when a row moved."""
        async with self.pool.connection() as conn:
            sessions = await outbox.queued_sessions(conn)
        now = self.clock()
        progressed = False
        for session_id in sessions:
            if self._blocked.get(session_id, 0.0) > now:
                continue
            self._blocked.pop(session_id, None)
            while session_id not in self._blocked and await self.step(session_id):
                progressed = True
        return progressed

    # ------------------------------------------------------------------ one row

    async def step(self, session_id: uuid.UUID) -> bool:
        """Claim and settle the session's oldest queued row; False when there is none."""
        async with self.pool.connection() as conn:
            row = await outbox.claim_next(conn, session_id)
        if row is None:
            return False
        try:
            await self._settle(row)
        except Exception:
            log.exception("envelope relay: row %s failed unexpectedly", row["id"])
            async with self.pool.connection() as conn:
                await outbox.mark(
                    conn, row["id"], "sending", "failed", error_code="unknown_outcome"
                )
        return True

    async def _settle(self, row: dict[str, Any]) -> None:
        rid = row["id"]
        async with self.pool.connection() as conn:
            own = await sessions_repo.get(conn, row["session_id"])
            agent_id = row["agent_id"]
            if agent_id is None and own is not None:
                leader = await leadership.current_leader(conn, own.id)
                agent_id = leader.agent_id if leader else None
        if own is None or agent_id is None or not self.settings.chat_available:
            code = "chat_unconfigured" if not self.settings.chat_available else "session_gone"
            async with self.pool.connection() as conn:
                await outbox.mark(conn, rid, "sending", "failed", error_code=code)
            return
        path, data = _payload(row)
        retry_after: list[str] = []
        try:
            assert self._http is not None
            status, content = await chat_core.relay_call(
                self.pool,
                self._http,
                self.settings,
                chat_core.chat_target(self.settings),
                own,
                agent_id,
                "POST",
                path,
                json.dumps(data).encode(),
                counts_as_update=row["kind"] != "react",
                relay_bound=row["kind"] != "react",
                records_activity=row["kind"] != "notice",
                retry_after=retry_after,
            )
        except ApiError as exc:
            if exc.status == 429 or exc.status >= 500:
                await self._retry(row, exc.code, exc.retry_after)
            else:
                await self._refuse(row, exc.code, exc.message[:MAX_ERROR_CHARS])
            return
        if 200 <= status < 300:
            async with self.pool.connection() as conn:
                await outbox.mark(conn, rid, "sending", "accepted", send_id=_send_id(content))
        elif status == 429 or status >= 500:
            await self._retry(row, f"http_{status}", _seconds(retry_after))
        else:
            code, message = _error_of(content, status)
            await self._refuse(row, code, message)

    async def _retry(self, row: dict[str, Any], code: str, retry_after: float | None) -> None:
        attempts = row["attempts"]
        async with self.pool.connection() as conn:
            if attempts >= MAX_ATTEMPTS:
                await outbox.mark(
                    conn, row["id"], "sending", "failed", error_code=code, attempts=attempts
                )
                log.warning("envelope relay: row %s failed after %d attempts", row["id"], attempts)
                return
            await outbox.mark(conn, row["id"], "sending", "queued", error_code=code)
        delay = min(
            self.settings.retry_backoff_base_seconds * 2 ** (attempts - 1),
            self.settings.retry_backoff_max_seconds,
        )
        if retry_after is not None:
            delay = max(delay, retry_after)
        self._blocked[row["session_id"]] = self.clock() + delay

    async def _refuse(self, row: dict[str, Any], code: str, message: str | None) -> None:
        async with self.pool.connection() as conn:
            await outbox.mark(conn, row["id"], "sending", "refused", error_code=code, error=message)
            late = (
                await outbox.turn_ended(conn, row["agent_id"], row["turn_id"])
                if row["agent_id"]
                else False
            )
            if late:
                try:
                    await queue.enqueue_message(
                        conn,
                        row["session_id"],
                        refusal_text(row, code),
                        row["agent_id"],
                        lane="regular",
                    )
                except queue.SessionBeingDeleted:
                    log.info(
                        "envelope relay: session %s is closing; refusal not told", row["session_id"]
                    )


def _seconds(values: list[str]) -> float | None:
    if not values:
        return None
    try:
        return max(0.0, float(values[0]))
    except ValueError:
        return None
