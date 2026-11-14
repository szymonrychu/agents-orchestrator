"""Handover notice delivery (voluntary-handover, spec 3.4). The api sweeper is the single writer of
handover notices, of `handover_log` and of `sessions.handover_notice_epoch`: for every leader
change with a predecessor that has no notice yet, it renders the generated State and the
predecessor's last turns and queues them as an attached `handover` notice targeted to the new
leader, in the transaction that records the epoch."""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator import handover_transcript as ht
from agents_orchestrator.api.scope import scope_of
from agents_orchestrator.db import handoffs, leader_state
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.handoffs import SHUTDOWN_TEXT
from agents_orchestrator.db.queue import StoredAttachment, enqueue_message
from agents_orchestrator.db.sessions import _sweep_each
from agents_orchestrator.taskboard_client import OpenDecision

log = logging.getLogger(__name__)

# Main chain of the predecessor: rows a subagent emitted carry `data.agent_id`, except the
# subagent lifecycle rows, which the renderer keeps.
# The body starts after the latest `session_cleared` of that wrapper session (Q13: an ex-leader
# is cleared and keeps its wrapper session, so its pre-clear history is not handed over again);
# the clear row itself is left out. A clear counts only when the DB ingested it (`created_at`, the
# DB clock) at or before the change (`until`, also the DB clock): the clear this very change
# triggers is ingested after it, whatever the pod clock says. Comparing the clear's own `ts` (pod
# clock) with `until` would let a lagging pod clock sort that clear before the change and wipe
# the whole body. Only the clear bound uses the DB clock; the body bound `ts <= until` is kept.
# The body is then bounded to the last `turns` turns (main-chain `user_message` rows): from the
# `turns`-th last of them on, or all rows when there are fewer. `omitted_turns` counts the turns
# before that bound.
_SCOPE = """
    WITH ws AS (
        SELECT w.wrapper_session_id FROM events w
        WHERE w.agent_id = %(agent)s AND w.ts <= %(until)s
        ORDER BY w.id DESC LIMIT 1),
    cleared AS (
        SELECT coalesce(max(c.id), 0) AS id FROM events c
        WHERE c.agent_id = %(agent)s AND c.created_at <= %(until)s AND c.type = 'session_cleared'
          AND c.wrapper_session_id = (SELECT wrapper_session_id FROM ws)),
    turn_starts AS (
        SELECT e.id FROM events e
        WHERE e.agent_id = %(agent)s AND e.ts <= %(until)s
          AND e.wrapper_session_id = (SELECT wrapper_session_id FROM ws)
          AND e.id > (SELECT id FROM cleared)
          AND e.type = 'user_message' AND e.data->>'agent_id' IS NULL),
    bound AS (
        SELECT coalesce((SELECT id FROM turn_starts ORDER BY id DESC
                         OFFSET %(skip)s LIMIT 1), 0) AS id)
"""
_TRANSCRIPT_ROWS = (
    _SCOPE
    + """
    SELECT e.id, e.ts, e.type, e.data FROM events e
    WHERE e.agent_id = %(agent)s AND e.ts <= %(until)s
      AND e.wrapper_session_id = (SELECT wrapper_session_id FROM ws)
      AND e.id > (SELECT id FROM cleared) AND e.id >= (SELECT id FROM bound)
      AND (e.data->>'agent_id' IS NULL
           OR e.type IN ('subagent_started', 'subagent_stopped', 'subagent_linked'))
    ORDER BY e.id
"""
)
_OMITTED_TURNS = _SCOPE + " SELECT count(*) FROM turn_starts WHERE id < (SELECT id FROM bound)"

_SOURCE = "sessions s JOIN session_leaders l ON l.session_id = s.id"
# Pod shutdown (T-256): while the predecessor is stopping, its notice waits for the handoff the
# wrapper's shutdown routine captures, so that text leads the notice. The wait ends once the
# handoff pass opened the predecessor's shutdown row with a successor (or closed it), the routine
# reported no usable text, the stream attached to the next pod (the mark is cleared) or
# `shutdown_hold` seconds passed since the mark; a text that arrives later is sent to the
# successor by the pass on its own.
_SHUTDOWN_HOLD = f"""NOT EXISTS (
    SELECT 1 FROM agent_status st WHERE st.agent_id = l.previous_agent_id
      AND st.stopping_since > now() - make_interval(secs => %(shutdown_hold)s)
      AND NOT EXISTS (SELECT 1 FROM agent_handoffs h WHERE h.agent_id = st.agent_id
          AND h.trigger = 'shutdown' AND h.created_at >= st.stopping_since
          AND (h.successor_agent_id IS NOT NULL OR h.closed_at IS NOT NULL))
      AND NOT EXISTS (SELECT 1 FROM events e WHERE e.agent_id = st.agent_id
          AND e.type = 'shutdown_handoff' AND e.created_at >= st.stopping_since
          AND NOT {SHUTDOWN_TEXT}))"""
_PENDING = (
    "s.desired = 'active' AND l.previous_agent_id IS NOT NULL"
    f" AND l.epoch > s.handover_notice_epoch AND {_SHUTDOWN_HOLD}"
)
_COLUMNS = "s.slug, l.agent_id, l.epoch, l.since, l.reason, l.previous_agent_id, l.previous_since"

# All sessions' taskboard calls run concurrently and share this overall bound; a session whose
# call has not finished by then counts as unavailable.
FETCH_TIMEOUT_SECONDS = 10.0

# (session id, slug, project scope) -> open decisions, or None when the taskboard cannot say.
DecisionsFetcher = Callable[[uuid.UUID, str, str], Awaitable[list[OpenDecision] | None]]


@dataclass(frozen=True)
class TranscriptRows:
    rows: list[dict[str, Any]]
    omitted_turns: int


async def transcript_rows(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    until: datetime,
    turns: int = ht.HANDOVER_TURNS,
) -> TranscriptRows:
    """Events (`id`, `ts`, `type`, `data`) of the agent's latest wrapper session with an event at
    or before `until`, up to `until`, after that session's latest `session_cleared` ingested at or
    before `until` (DB clock), main chain only, ascending id, from the start of the last `turns`
    turns; `omitted_turns` counts the turns before that."""
    params = {"agent": agent_id, "until": until, "skip": max(turns, 1) - 1}
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_TRANSCRIPT_ROWS, params)
        rows = await cur.fetchall()
        await cur.execute(_OMITTED_TURNS, params)
        omitted = await cur.fetchone()
    return TranscriptRows(rows, omitted["count"] if omitted else 0)


async def _fetch_decisions(
    conn: psycopg.AsyncConnection, decisions: DecisionsFetcher, params: dict[str, Any]
) -> dict[uuid.UUID, list[OpenDecision] | None]:
    """Open decisions of every session a notice is pending for, asked outside any transaction
    concurrently under one overall timeout. A fetcher that raises counts as unavailable."""
    async with conn.transaction():
        cur = await conn.execute(f"SELECT s.id FROM {_SOURCE} WHERE {_PENDING}", params)
        ids = [row[0] for row in await cur.fetchall()]
        sessions = [s for s in [await sessions_repo.get(conn, i) for i in ids] if s is not None]

    async def one(session: sessions_repo.Session) -> list[OpenDecision] | None:
        try:
            return await decisions(session.id, session.slug, scope_of(session))
        except Exception as exc:
            log.warning(
                "handover: open decisions of %s unavailable: %s", session.id, type(exc).__name__
            )
            return None

    tasks = {s.id: asyncio.ensure_future(one(s)) for s in sessions}
    if tasks:
        await asyncio.wait(tasks.values(), timeout=FETCH_TIMEOUT_SECONDS)
    fetched: dict[uuid.UUID, list[OpenDecision] | None] = {}
    for session_id, task in tasks.items():
        if task.done():
            fetched[session_id] = task.result()
        else:
            task.cancel()
            log.warning("handover: open decisions of %s timed out", session_id)
            fetched[session_id] = None
    return fetched


async def deliver_handover(
    conn: psycopg.AsyncConnection,
    *,
    budget_bytes: int,
    output_chars: int,
    decisions: DecisionsFetcher,
    shutdown_hold_seconds: int,
) -> list[uuid.UUID]:
    """Queue one handover notice per pending leader change (epoch above
    `handover_notice_epoch`, a predecessor, session active); returns the sessions served. Two
    changes before one poll get one notice, for the current row. The epoch write is
    status-guarded and shares the transaction of the notice, so two sweepers queue one. A
    stopping predecessor's notice waits for its shutdown handoff, at most
    `shutdown_hold_seconds` from its stopping mark (`_SHUTDOWN_HOLD`)."""
    params = {"shutdown_hold": shutdown_hold_seconds}
    fetched = await _fetch_decisions(conn, decisions, params)

    async def deliver(session_id: uuid.UUID, row: dict[str, Any]) -> str | None:
        cur = await conn.execute(
            "UPDATE sessions SET handover_notice_epoch = %s"
            " WHERE id = %s AND handover_notice_epoch < %s RETURNING id",
            (row["epoch"], session_id, row["epoch"]),
        )
        if await cur.fetchone() is None:
            return None
        pod = await conn.execute(
            "SELECT pod_name FROM agents WHERE id = %s", (row["previous_agent_id"],)
        )
        found = await pod.fetchone()
        header = ht.Header(
            slug=row["slug"],
            epoch=row["epoch"],
            reason=row["reason"],
            predecessor_pod=found[0] if found else str(row["previous_agent_id"]),
            changed_at=row["since"],
            summary=ht.summary_of(row["reason"]),
        )
        state = replace(
            await leader_state.collect(conn, session_id), decisions=fetched.get(session_id)
        )
        turns = await transcript_rows(conn, row["previous_agent_id"], row["since"])
        body = ht.render_transcript(
            header,
            leader_state.render_state(state),
            turns.rows,
            omitted_turns=turns.omitted_turns,
            budget_bytes=budget_bytes,
            output_chars=output_chars,
        ).encode()
        notice = ht.notice_text(header, budget_bytes)
        # the predecessor's own handoff (graceful handover) leads the notice; marked delivered in
        # this transaction so a later change never repeats it. Only a row written during the
        # predecessor's own tenure counts: an older undelivered one is stale.
        handoff = await handoffs.take_undelivered(
            conn, row["previous_agent_id"], row["agent_id"], row["previous_since"]
        )
        if handoff is not None:
            notice = (
                f"Handoff from {header.predecessor_pod} (written by it):\n{handoff}\n\n{notice}"
            )
        await enqueue_message(
            conn,
            session_id,
            notice,
            row["agent_id"],
            lane="regular",
            notice=ht.NOTICE_HANDOVER,
            attachments=[StoredAttachment(ht.attachment_name(row["epoch"]), "text/markdown", body)],
        )
        tenure = (
            (row["since"] - row["previous_since"]).total_seconds()
            if row["previous_since"] is not None
            else None
        )
        await conn.execute(
            "INSERT INTO handover_log (session_id, epoch, kind, tenure_seconds, intake_bytes)"
            " VALUES (%s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (
                session_id,
                row["epoch"],
                ht.kind_of(row["reason"]),
                tenure,
                len(notice.encode()) + len(body),
            ),
        )
        return f"handover: session {session_id} epoch {row['epoch']} notice queued"

    return await _sweep_each(conn, _SOURCE, _PENDING, params, _COLUMNS, deliver)
