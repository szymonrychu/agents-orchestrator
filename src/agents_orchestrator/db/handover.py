"""Handover notice delivery (voluntary-handover, spec 3.4). The api sweeper is the single writer of
handover notices and of `sessions.handover_notice_epoch`: for every leader change with a
predecessor that has no notice yet, it renders the predecessor's transcript and queues it as an
attached `handover` notice targeted to the new leader, in the transaction that records the epoch."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator import handover_transcript as ht
from agents_orchestrator.db import file_locks
from agents_orchestrator.db.queue import StoredAttachment, enqueue_message
from agents_orchestrator.db.sessions import _sweep_each

# Main chain of the predecessor: rows a subagent emitted carry `data.agent_id`, except the
# subagent lifecycle rows, which the renderer needs for "In flight" and the launching prompt.
_TRANSCRIPT_ROWS = """
    SELECT e.id, e.ts, e.type, e.data FROM events e
    WHERE e.agent_id = %(agent)s AND e.ts <= %(until)s
      AND e.wrapper_session_id = (
          SELECT w.wrapper_session_id FROM events w
          WHERE w.agent_id = %(agent)s AND w.ts <= %(until)s
          ORDER BY w.id DESC LIMIT 1)
      AND (e.data->>'agent_id' IS NULL
           OR e.type IN ('subagent_started', 'subagent_stopped', 'subagent_linked'))
    ORDER BY e.id
"""

_SOURCE = "sessions s JOIN session_leaders l ON l.session_id = s.id"
_PENDING = (
    "s.desired = 'active' AND l.previous_agent_id IS NOT NULL AND l.epoch > s.handover_notice_epoch"
)
_COLUMNS = "s.slug, l.agent_id, l.epoch, l.since, l.reason, l.previous_agent_id"


async def transcript_rows(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, until: datetime
) -> list[dict[str, Any]]:
    """Events (`id`, `ts`, `type`, `data`) of the agent's latest wrapper session with an event at
    or before `until`, up to `until`, main chain only, ascending id."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_TRANSCRIPT_ROWS, {"agent": agent_id, "until": until})
        return await cur.fetchall()


async def deliver_handover(
    conn: psycopg.AsyncConnection, *, budget_bytes: int, output_chars: int
) -> list[uuid.UUID]:
    """Queue one handover notice per pending leader change (epoch above
    `handover_notice_epoch`, a predecessor, session active); returns the sessions served. Two
    changes before one poll get one notice, for the current row. The epoch write is
    status-guarded and shares the transaction of the notice, so two sweepers queue one."""

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
        queued = await conn.execute(
            "SELECT count(*) FROM messages WHERE session_id = %s AND status = 'queued'",
            (session_id,),
        )
        count = await queued.fetchone()
        locks = await file_locks.list_live(conn, session_id)
        in_flight = ht.InFlight(
            queued_messages=count[0] if count else 0,
            locks=[f"{lock.path} ({lock.agent})" for lock in locks],
        )
        rows = await transcript_rows(conn, row["previous_agent_id"], row["since"])
        body = ht.render_transcript(
            header, rows, in_flight, budget_bytes=budget_bytes, output_chars=output_chars
        )
        await enqueue_message(
            conn,
            session_id,
            ht.notice_text(header),
            row["agent_id"],
            lane="regular",
            notice=ht.NOTICE_HANDOVER,
            attachments=[
                StoredAttachment(ht.attachment_name(row["epoch"]), "text/markdown", body.encode())
            ],
        )
        return f"handover: session {session_id} epoch {row['epoch']} notice queued"

    return await _sweep_each(conn, _SOURCE, _PENDING, {}, _COLUMNS, deliver)
