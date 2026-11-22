"""Repository for `envelope_outbox`: envelopes parsed from an agent's own text.

Rows are inserted by the ingester and the onStop endpoint; every status change is a
status-guarded update. Delivery is at most once: a `sending` row that outlives a restart becomes
`failed` `unknown_outcome` instead of being sent again.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator.db.pool import notify

CHANNEL = "envelope_outbox"
STALE_SENDING_SECONDS = 60
_COLUMNS = (
    "id, session_id, agent_id, wrapper_session_id, event_seq, envelope_index, turn_id, kind,"
    " room, reply_to, files, body, react_key, status, error_code, error, send_id, attempts,"
    " target, lane, notify, handled, project, message_id, created_at, updated_at"
)
_FIELDS = frozenset({"error_code", "error", "send_id", "attempts", "message_id"})


@dataclass(frozen=True)
class Row:
    session_id: uuid.UUID
    kind: str
    agent_id: uuid.UUID | None = None
    wrapper_session_id: str | None = None
    event_seq: int | None = None
    envelope_index: int | None = None
    turn_id: str | None = None
    room: str | None = None
    reply_to: str | None = None
    files: tuple[str, ...] | None = None
    body: str | None = None
    react_key: str | None = None
    status: str = "queued"
    error_code: str | None = None
    error: str | None = None
    target: str | None = None
    lane: str | None = None
    notify: str | None = None
    handled: tuple[int, ...] | None = None
    project: str | None = None


async def insert_rows(conn: psycopg.AsyncConnection, rows: Sequence[Row]) -> int:
    """Insert rows; a row already stored for the same (agent, wrapper session, event, index) is
    ignored. Returns the number of new rows and NOTIFYs `envelope_outbox` per session that got
    a queued one."""
    inserted = 0
    sessions: set[uuid.UUID] = set()
    for r in rows:
        cur = await conn.execute(
            "INSERT INTO envelope_outbox (session_id, agent_id, wrapper_session_id, event_seq,"
            " envelope_index, turn_id, kind, room, reply_to, files, body, react_key, status,"
            " error_code, error, target, lane, notify, handled, project)"
            " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            " ON CONFLICT (agent_id, wrapper_session_id, event_seq, envelope_index)"
            " WHERE event_seq IS NOT NULL DO NOTHING RETURNING id",
            (
                r.session_id,
                r.agent_id,
                r.wrapper_session_id,
                r.event_seq,
                r.envelope_index,
                r.turn_id,
                r.kind,
                r.room,
                r.reply_to,
                list(r.files) if r.files is not None else None,
                r.body,
                r.react_key,
                r.status,
                r.error_code,
                r.error,
                r.target,
                r.lane,
                r.notify,
                list(r.handled) if r.handled is not None else None,
                r.project,
            ),
        )
        if await cur.fetchone() is not None:
            inserted += 1
            if r.status == "queued":
                sessions.add(r.session_id)
    for session_id in sessions:
        await notify(conn, CHANNEL, {"session_id": str(session_id)})
    return inserted


async def claim_next(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> dict[str, Any] | None:
    """Move the oldest queued row of the session to `sending`, unless one is already sending
    (one row at a time keeps the message order). Returns the claimed row.

    Assumes a single relay worker claims per session: the READ COMMITTED NOT EXISTS check is
    not race-free across concurrent claimers."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "UPDATE envelope_outbox SET status = 'sending', attempts = attempts + 1,"
            " updated_at = now() WHERE id = ("
            " SELECT id FROM envelope_outbox WHERE session_id = %s AND status = 'queued'"
            " AND NOT EXISTS (SELECT 1 FROM envelope_outbox WHERE session_id = %s"
            " AND status = 'sending')"
            " ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)"
            " AND status = 'queued' RETURNING " + _COLUMNS,
            (session_id, session_id),
        )
        return await cur.fetchone()


async def mark(
    conn: psycopg.AsyncConnection,
    row_id: int,
    from_status: str,
    to_status: str,
    **fields: Any,
) -> bool:
    """Status-guarded update; False when the row is no longer in `from_status`."""
    unknown = set(fields) - _FIELDS
    if unknown:
        raise ValueError(f"unknown envelope_outbox fields: {sorted(unknown)}")
    sets = "".join(f", {k} = %s" for k in fields)
    cur = await conn.execute(
        "UPDATE envelope_outbox SET status = %s, updated_at = now()"
        + sets
        + " WHERE id = %s AND status = %s",
        (to_status, *fields.values(), row_id, from_status),
    )
    return cur.rowcount == 1


async def rows_for_turn(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str
) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM envelope_outbox WHERE agent_id = %s AND turn_id = %s"
            " ORDER BY id",
            (agent_id, turn_id),
        )
        return await cur.fetchall()


async def sweep_stale_sending(
    conn: psycopg.AsyncConnection, older_than_s: int = STALE_SENDING_SECONDS
) -> list[dict[str, Any]]:
    """`sending` rows older than `older_than_s` become `failed` `unknown_outcome`: the send may
    or may not have happened, and a duplicate chat post is worse than a reported loss. Returns
    the swept rows."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "UPDATE envelope_outbox SET status = 'failed', error_code = 'unknown_outcome',"
            " updated_at = now() WHERE status = 'sending'"
            " AND updated_at < now() - make_interval(secs => %s) RETURNING " + _COLUMNS,
            (older_than_s,),
        )
        return await cur.fetchall()


async def queued_sessions(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    cur = await conn.execute(
        "SELECT DISTINCT session_id FROM envelope_outbox WHERE status = 'queued'"
    )
    return [r[0] for r in await cur.fetchall()]


async def turn_ended(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str | None
) -> bool:
    """True when the agent's turn is over, so its Stop was already allowed and a late refusal
    cannot reach the agent through the Stop reason. Over means: the onStop endpoint allowed a
    Stop of the turn (`envelope_stop_checks.allowed_at`), the `turns` row is completed, or the
    turn is not the agent's current running turn (`agent_status.current_turn_id`). A row
    without a turn id (a wrapper without the new fields) is never over: nothing can be told."""
    if turn_id is None:
        return False
    cur = await conn.execute(
        "SELECT EXISTS (SELECT 1 FROM envelope_stop_checks WHERE agent_id = %s AND turn_id = %s"
        " AND allowed_at IS NOT NULL)"
        " OR EXISTS (SELECT 1 FROM turns WHERE turn_id = %s AND agent_id = %s"
        " AND completed_at IS NOT NULL)"
        " OR NOT EXISTS (SELECT 1 FROM agent_status WHERE agent_id = %s AND current_turn_id = %s)",
        (agent_id, turn_id, turn_id, agent_id, agent_id, turn_id),
    )
    row = await cur.fetchone()
    return bool(row[0])
