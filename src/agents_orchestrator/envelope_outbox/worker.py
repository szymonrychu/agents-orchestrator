"""The envelope relay worker (S-70, S-71): settles the envelopes the ingester queued.

Per session, oldest row first and one row at a time (the message order). A row is claimed
`queued -> sending`, then settled by kind.

Hive kinds (`session`, `project_post`) need no HTTP hop: the request-free core
(`api/session_core.py`, `api/project_post_core.py`) queues the `messages` rows, and the outbox
row becomes `accepted` (with `message_id`, or the post id as `send_id` `p<id>`) in the SAME
transaction; a refusal of the core rolls it back and the row becomes `refused` with the code.
The hop is the one of the turn that wrote the envelope. These kinds settle without a chat
upstream.

A chat row is forwarded through the request-free relay core (`api/chat_core.py`, the same
target, `X-Orch-Agent` header, occupier relay and activity accounting as the agent routes) and
settled by the answer:

- 2xx: `accepted` with the upstream `send_id`;
- 429, 5xx, an unreachable upstream: back to `queued` with a backoff that honours `Retry-After`
  (the session waits, so the order holds), `failed` `upstream_unavailable` after 5 attempts;
- any other 4xx or a refusal of the relay (`files_not_relayable`, ...): `refused` with the code.

Delivery is at most once: on start, `sending` rows older than 60 s become `failed`
`unknown_outcome`, and an unexpected error after the claim does the same. A refusal (or a stale
`unknown_outcome`) of a row whose turn is already over (its Stop was allowed) is told to the
sending agent as a `<message-in channel="notice">` frame for `a<row id>`, since the Stop hook can
no longer carry it.

The worker always runs inside the ingester process (a singleton, so one claimer per session); a
chat row without a configured chat upstream fails `chat_unconfigured`. It wakes on NOTIFY
`envelope_outbox` and polls as a fallback.
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
from agents_common import frames
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.api import chat_core, forward
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.project_post_core import (
    ProjectPosted,
    post_project_effects,
    project_post_core,
)
from agents_orchestrator.api.session_core import session_send_core
from agents_orchestrator.db import agents as agents_repo
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


HIVE_KINDS = frozenset({"session", "project_post"})


def envelope_ref(row: dict[str, Any]) -> str:
    """The sender-side reference of an outbox row (`for` of a notice frame)."""
    return f"a{row['id']}"


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
        """One pass over every queued session that is not backing off; True when a row moved.

        Stale `sending` rows (a send cut off by a restart) are swept first on every pass, or
        they would block their session's queue for good."""
        async with self.pool.connection() as conn:
            swept = await outbox.sweep_stale_sending(conn)
            if swept:
                log.warning("envelope relay: %d sending rows became unknown_outcome", len(swept))
            for row in swept:
                await _tell_if_late(conn, row, "failed", "unknown_outcome")
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
        if row["kind"] in HIVE_KINDS:
            await self._settle_hive(row, own)
            return
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

    async def _settle_hive(self, row: dict[str, Any], own: sessions_repo.Session | None) -> None:
        """Queue a `session` or `project_post` row through its core and mark it accepted in
        one transaction; a refusal of the core writes nothing and refuses the row."""
        rid = row["id"]
        posted: ProjectPosted | None = None
        refusal: ApiError | None = None
        async with self.pool.connection() as conn:
            agent = await agents_repo.get_agent(conn, row["agent_id"]) if row["agent_id"] else None
            if own is None or agent is None:
                await outbox.mark(conn, rid, "sending", "failed", error_code="session_gone")
                return
            try:
                async with conn.transaction():
                    if row["turn_id"] is not None:
                        hop = await queue.turn_hop_for(conn, agent.id, row["turn_id"])
                    else:
                        hop = await queue.turn_hop(conn, agent.id) or 0
                    if row["kind"] == "session":
                        sent = await session_send_core(
                            conn,
                            own,
                            agent,
                            row["target"],
                            row["body"],
                            settings=self.settings,
                            lane=row["lane"] or "regular",
                            handled=list(row["handled"] or []),
                            hop=hop,
                            envelope_ref=envelope_ref(row),
                            allow_priority=False,
                        )
                        fields: dict[str, Any] = {"message_id": sent.message_id}
                    else:
                        posted = await project_post_core(
                            conn,
                            own,
                            agent,
                            settings=self.settings,
                            text=row["body"],
                            to=row["target"].split(",") if row["target"] else [],
                            reply_to=int(row["reply_to"]) if row["reply_to"] else None,
                            handled=list(row["handled"] or []),
                            project=row["project"],
                            hop=hop,
                            envelope_ref=envelope_ref(row),
                        )
                        fields = {"send_id": f"p{posted.post_id}"}
                    if not await outbox.mark(conn, rid, "sending", "accepted", **fields):
                        raise RuntimeError(f"envelope row {rid} left sending while settling")
            except ApiError as exc:
                refusal = exc
        if refusal is not None:
            await self._refuse(row, refusal.code, refusal.message[:MAX_ERROR_CHARS])
        elif posted is not None:
            await post_project_effects(self.pool, own, agent, posted)

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
            await _tell_if_late(conn, row, "refused", code)


async def _tell_if_late(conn: Any, row: dict[str, Any], status: str, code: str) -> None:
    """When the row's turn is over, queue a notice frame for the row to the sending agent."""
    if not row["agent_id"] or not await outbox.turn_ended(conn, row["agent_id"], row["turn_id"]):
        return
    own = await sessions_repo.get(conn, row["session_id"])
    if own is None:
        return
    try:
        await queue.enqueue_message(
            conn,
            row["session_id"],
            frames.notice(own.slug, envelope_ref(row), status, code),
            row["agent_id"],
            lane="regular",
        )
    except queue.SessionBeingDeleted:
        log.info("envelope relay: session %s is closing; %s not told", row["session_id"], status)


def _seconds(values: list[str]) -> float | None:
    if not values:
        return None
    try:
        return max(0.0, float(values[0]))
    except ValueError:
        return None
