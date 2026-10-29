"""The ambiguous-send verifier (D24): did a turn carrying this message id start?

A message lands in `verifying` after an ambiguous send (a read timeout or a reset after the
request was written) or a stale `dispatching` row. Before anything re-sends it, the original
agent's wrapper is asked whether a turn whose `metadata.orch_message_id` is this message exists.

- Started: the message is `sent` with that turn id; the agent stays reserved.
- Definitely not started (the wrapper answered and no record carries the id) once the settle
  window has passed: an attempt is counted and the message is requeued with backoff, or fails
  at max. The reservation is cleared only while it is still this message's.
- Unknown (unreachable wrapper, a 404 session, any other wrapper error, or inside the settle
  window): nothing changes and the next pass checks again.
- Deadline: no definite answer by `ambiguous_deadline_seconds` fails it with `ambiguous`; only
  the user's explicit Retry re-sends it.
- A command message (kind=command, D10) fails `ambiguous` at once: no turn carries its id, so a
  lookup could only ever answer "not started" and resend it blindly.

The wrapper records a turn, metadata included, before it answers 202, so a missing record after
the settle window means the request never reached its router. Every write is a guarded queue
function, so concurrent verifiers resolve a message once, and only `age_seconds` (database
clock) is compared with the windows; wrapper timestamps never are (D23).
"""

from __future__ import annotations

import enum
import logging
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import queue, sessions
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import TurnRecord, WrapperError, WrapperUnreachable

log = logging.getLogger(__name__)

BATCH = 50
NEWEST_TURNS = 10

WrapperFactory = Callable[[str, str], WrapperClient]
"""(base_url, token) -> WrapperClient for one agent."""


class Answer(enum.Enum):
    STARTED = "started"
    NOT_STARTED = "not_started"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Lookup:
    answer: Answer
    turn_id: str | None = None


async def session_max_attempts(conn: Any, session_id: uuid.UUID) -> int:
    """The session's `spec.max_attempts`, or the spec default for a vanished session."""
    session = await sessions.get(conn, session_id)
    return session.spec.max_attempts if session is not None else SessionSpec().max_attempts


def _carries(record: TurnRecord, message_id: int) -> bool:
    value = record.metadata.get("orch_message_id")
    return value is not None and str(value) == str(message_id)


class Verifier:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        settings: Settings,
        wrapper_factory: WrapperFactory,
    ) -> None:
        self.pool = pool
        self.settings = settings
        self.wrapper_factory = wrapper_factory

    async def verify_once(self) -> int:
        """Check at most BATCH verifying messages; return how many this pass resolved."""
        async with self.pool.connection() as conn:
            pending = await queue.list_verifying(conn, BATCH)
        resolved = 0
        for msg in pending:
            try:
                resolved += await self._verify(msg)
            except Exception:
                log.exception("verifying message %s failed", msg.message_id)
        return resolved

    async def _verify(self, msg: queue.Verifying) -> int:
        s = self.settings
        if msg.kind == "command":
            async with self.pool.connection() as conn:
                won = await queue.fail_ambiguous(conn, msg.message_id)
            if won:
                log.warning(
                    "command message %s may have reached the wrapper: failed", msg.message_id
                )
            return int(won)
        found = await self._lookup(msg)
        async with self.pool.connection() as conn:
            if found.answer is Answer.STARTED:
                assert found.turn_id is not None
                won = await queue.resolve_started(conn, msg.message_id, found.turn_id)
                if won:
                    log.info("message %s started as turn %s", msg.message_id, found.turn_id)
                return int(won)
            if found.answer is Answer.NOT_STARTED and msg.age_seconds >= s.ambiguous_settle_seconds:
                status = await queue.resolve_not_started(
                    conn,
                    msg.message_id,
                    await session_max_attempts(conn, msg.session_id),
                    s.retry_backoff_base_seconds,
                    s.retry_backoff_max_seconds,
                )
                if status is not None:
                    log.info("message %s did not start: now %s", msg.message_id, status)
                return int(status is not None)
            if msg.age_seconds >= s.ambiguous_deadline_seconds:
                won = await queue.fail_ambiguous(conn, msg.message_id)
                if won:
                    log.warning("message %s has no definite answer: failed", msg.message_id)
                return int(won)
        return 0

    async def _lookup(self, msg: queue.Verifying) -> Lookup:
        """Ask the original agent's wrapper whether a turn carrying this message id exists."""
        if not msg.agent_url or not msg.wrapper_session_id:
            return Lookup(Answer.UNKNOWN)
        token = tokens_for(self.settings, msg.session_id).auth
        client = self.wrapper_factory(msg.agent_url, token)
        session = msg.wrapper_session_id
        try:
            try:
                snap = await client.get_session(session)
                checked: set[str] = set()
                if snap.current_turn_id:
                    checked.add(snap.current_turn_id)
                    record = await client.get_turn(session, snap.current_turn_id)
                    if _carries(record, msg.message_id):
                        return Lookup(Answer.STARTED, record.turn_id)
                # the summaries carry no metadata, so each candidate's record is fetched
                turns = await client.list_turns(session)
                for summary in reversed(turns[-NEWEST_TURNS:]):
                    if summary.turn_id in checked:
                        continue
                    record = await client.get_turn(session, summary.turn_id)
                    if _carries(record, msg.message_id):
                        return Lookup(Answer.STARTED, record.turn_id)
            finally:
                await client.aclose()
        except WrapperUnreachable as exc:
            log.info("message %s: wrapper unreachable (%s)", msg.message_id, exc)
            return Lookup(Answer.UNKNOWN)
        except WrapperError as exc:
            # a 404 session (or any other refusal) is no definite answer about the turn
            log.info("message %s: wrapper answered %s", msg.message_id, exc)
            return Lookup(Answer.UNKNOWN)
        return Lookup(Answer.NOT_STARTED)
