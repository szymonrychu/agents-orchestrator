"""agent_handoffs and agent_limits: graceful handover of an agent's context (Postgres).

Writer of both tables: this module. The dispatcher decides every phase change and the agent
route inserts rows and extends a deadline, both through the functions below (the same split as
`leadership_requests`: the route inserts, the dispatcher decides). Every change is a
status-guarded update, so a row moves exactly once whichever caller commits first. At most one
open row exists per agent (partial unique index `agent_handoffs_one_open`).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator.db import queue

Phase = Literal[
    "notified",
    "extended",
    "requested",
    "handed",
    "discussing",
    "clearing",
    "returning",
    "done",
    "cancelled",
]
OPEN_PHASES: tuple[str, ...] = (
    "notified",
    "extended",
    "requested",
    "handed",
    "discussing",
    "clearing",
    "returning",
)

_COLUMNS = (
    "id, session_id, agent_id, trigger, phase, notified_at, deadline_at, extended_at,"
    " request_message_id, handoff_text, successor_agent_id, delivered_at, discuss_since,"
    " clear_command_id, clear_attempts, cleared_at, return_message_id, created_at, closed_at,"
    " close_reason"
)
# A `shutdown_handoff` event (alias `e`) that carries a handoff: the routine's turn completed
# with a non-blank reply. Anything else leaves today's resume and transcript notice alone.
SHUTDOWN_TEXT = "(e.data->>'status' = 'completed' AND coalesce(e.data->>'final_text', '') ~ '\\S')"
_SETTABLE = frozenset(
    {
        "notified_at",
        "deadline_at",
        "extended_at",
        "request_message_id",
        "handoff_text",
        "successor_agent_id",
        "delivered_at",
        "discuss_since",
        "clear_command_id",
        "clear_attempts",
        "cleared_at",
        "return_message_id",
    }
)


@dataclass(frozen=True)
class Handoff:
    id: int
    session_id: uuid.UUID
    agent_id: uuid.UUID
    trigger: str
    phase: str
    notified_at: datetime | None
    deadline_at: datetime | None
    extended_at: datetime | None
    request_message_id: int | None
    handoff_text: str | None
    successor_agent_id: uuid.UUID | None
    delivered_at: datetime | None
    discuss_since: datetime | None
    clear_command_id: int | None
    clear_attempts: int
    cleared_at: datetime | None
    return_message_id: int | None
    created_at: datetime
    closed_at: datetime | None
    close_reason: str | None


async def _fetch(
    conn: psycopg.AsyncConnection, query: str, params: Sequence[Any]
) -> Handoff | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        row = await cur.fetchone()
    return Handoff(**row) if row else None


async def open_row(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> Handoff | None:
    return await _fetch(
        conn,
        f"SELECT {_COLUMNS} FROM agent_handoffs WHERE agent_id = %s AND phase = ANY(%s)",
        (agent_id, list(OPEN_PHASES)),
    )


async def insert(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    trigger: str,
    phase: str,
    *,
    deadline_at: datetime | None = None,
    handoff_text: str | None = None,
) -> Handoff:
    """Open a row; raises UniqueViolation while the agent already has an open one.

    `notified_at` is set only for a row that opens with a notice (phase `notified`)."""
    row = await _fetch(
        conn,
        "INSERT INTO agent_handoffs (session_id, agent_id, trigger, phase, notified_at,"
        " deadline_at, handoff_text)"
        " VALUES (%s, %s, %s, %s, CASE WHEN %s THEN now() END, %s, %s)"
        f" RETURNING {_COLUMNS}",
        (session_id, agent_id, trigger, phase, phase == "notified", deadline_at, handoff_text),
    )
    assert row is not None
    return row


async def advance(
    conn: psycopg.AsyncConnection,
    row_id: int,
    from_phases: Sequence[str],
    to_phase: str,
    **fields: Any,
) -> bool:
    """Move a row to `to_phase` when it is in one of `from_phases`; True when a row changed.

    Open phases only, on both sides: a row is closed through `close` and never reopened."""
    if to_phase not in OPEN_PHASES or not set(from_phases) <= set(OPEN_PHASES):
        raise ValueError("advance moves between open phases only")
    unknown = set(fields) - _SETTABLE
    if unknown:
        raise ValueError(f"unknown agent_handoffs fields: {sorted(unknown)}")
    sets = ", ".join(f"{name} = %s" for name in fields)
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = %s"
        + (f", {sets}" if sets else "")
        + " WHERE id = %s AND phase = ANY(%s)",
        (to_phase, *fields.values(), row_id, list(from_phases)),
    )
    return cur.rowcount == 1


async def start_clear(
    conn: psycopg.AsyncConnection, row_id: int, from_phases: Sequence[str], command_id: int
) -> bool:
    """Move a row to `clearing` for the clear command `command_id`, counting the attempt in the
    same guarded UPDATE (`clear_attempts + 1`); True when a row changed."""
    if not set(from_phases) <= set(OPEN_PHASES):
        raise ValueError("start_clear moves from open phases only")
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = 'clearing', clear_command_id = %s,"
        " clear_attempts = clear_attempts + 1 WHERE id = %s AND phase = ANY(%s)",
        (command_id, row_id, list(from_phases)),
    )
    return cur.rowcount == 1


async def close(
    conn: psycopg.AsyncConnection,
    row_id: int,
    phase: Literal["done", "cancelled"],
    reason: str,
    from_phases: Sequence[str] = OPEN_PHASES,
) -> bool:
    """Close a row still in one of `from_phases` (any open phase by default); True when closed."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = %s, close_reason = %s, closed_at = now()"
        " WHERE id = %s AND phase = ANY(%s)",
        (phase, reason, row_id, list(from_phases)),
    )
    return cur.rowcount == 1


async def mark_delivered(conn: psycopg.AsyncConnection, row_id: int) -> bool:
    """Record that the row's handoff text reached its successor; True only for the first caller,
    so the text goes out once whoever sends it (the notice sweeper or the handoff pass)."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET delivered_at = now() WHERE id = %s AND delivered_at IS NULL",
        (row_id,),
    )
    return cur.rowcount == 1


async def take_undelivered(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    successor_agent_id: uuid.UUID,
    since: datetime | None,
) -> str | None:
    """Mark the newest undelivered handoff text of `agent_id` for its successor delivered and
    return it. Only a row written since `since` (the predecessor's tenure start) counts: an
    older undelivered one is stale."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET delivered_at = now() WHERE id = ("
        " SELECT id FROM agent_handoffs WHERE agent_id = %s AND successor_agent_id = %s"
        " AND handoff_text IS NOT NULL AND delivered_at IS NULL"
        " AND (%s::timestamptz IS NULL OR created_at >= %s)"
        " ORDER BY id DESC LIMIT 1) AND delivered_at IS NULL RETURNING handoff_text",
        (agent_id, successor_agent_id, since, since),
    )
    row = await cur.fetchone()
    return None if row is None else row[0]


async def extend(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, seconds: int
) -> Handoff | None:
    """Push the deadline of the agent's `notified` row out once; None when it is not notified
    or its deadline already passed (the request may be on its way)."""
    return await _fetch(
        conn,
        "UPDATE agent_handoffs SET phase = 'extended', extended_at = now(),"
        " deadline_at = deadline_at + make_interval(secs => %s)"
        " WHERE agent_id = %s AND phase = 'notified' AND deadline_at > now()"
        f" RETURNING {_COLUMNS}",
        (seconds, agent_id),
    )


async def open_rows(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> list[Handoff]:
    """The open rows of a session's agents, oldest first."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM agent_handoffs WHERE session_id = %s AND phase = ANY(%s)"
            " ORDER BY id",
            (session_id, list(OPEN_PHASES)),
        )
        return [Handoff(**row) for row in await cur.fetchall()]


async def countdown(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID
) -> tuple[int, str, int] | None:
    """The agent's strong row while it counts down (notified or extended): its id, phase and the
    minutes left until the handoff request (rounded up, at least 1); None otherwise."""
    cur = await conn.execute(
        "SELECT id, phase,"
        " greatest(1, ceil(extract(epoch FROM deadline_at - now()) / 60))::int"
        " FROM agent_handoffs WHERE agent_id = %s AND trigger = 'strong'"
        " AND phase IN ('notified', 'extended')",
        (agent_id,),
    )
    row = await cur.fetchone()
    return (row[0], row[1], row[2]) if row else None


async def queued_messages(
    conn: psycopg.AsyncConnection, row_id: int, *, notices_only: bool = False
) -> list[int]:
    """The row's notice, request and return messages still queued (or its notices only)."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT m.id FROM messages m, agent_handoffs h WHERE h.id = %s"
            " AND m.status = 'queued' AND m.target_agent_id = h.agent_id"
            " AND ((NOT %s AND m.id IN (h.request_message_id, h.return_message_id))"
            "  OR (m.notice = 'handoff_notice' AND m.created_at >= h.created_at))",
            (row_id, notices_only),
        )
        return [r[0] for r in await cur.fetchall()]


async def limits_start(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> int | None:
    cur = await conn.execute(
        "SELECT start_context_tokens FROM agent_limits WHERE agent_id = %s", (agent_id,)
    )
    row = await cur.fetchone()
    return row[0] if row else None


async def set_limits_start(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, tokens: int | None
) -> None:
    await conn.execute(
        "INSERT INTO agent_limits (agent_id, start_context_tokens) VALUES (%s, %s)"
        " ON CONFLICT (agent_id) DO UPDATE"
        " SET start_context_tokens = EXCLUDED.start_context_tokens, since = now()",
        (agent_id, tokens),
    )


# Phases before the discussion: a leader that moves while its row is in one of them carries the
# row on to `discussing`, or closes it when the move was not its own handover.
_PRE_DISCUSSION = ("notified", "extended", "requested", "handed")
# Reasons of the forced handover requests (the reconciler's rollout, the strong-limit handoff's
# limit): no leader's judgment, so no handoff text from them and they never expire.
FORCED_REASONS = ("rollout", "limit")


async def on_leader_moved(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    previous: uuid.UUID,
    successor: uuid.UUID,
    req: dict[str, Any],
) -> None:
    """The leadership pass moved the lead from `previous` to `successor` (same transaction).

    `req` is the applied request (`kind`, `reason`); a move that is not a leader's own
    `handover` (`user`, `scale_down`, `leader_lost`: pass a dict with that `kind`) closes a row
    still before its discussion `cancelled not_leader`. A leader's own handover moves such a row
    to `discussing` (`discuss_since` now, the successor recorded, the agent's own handoff text
    kept, else the request reason), or, with no row and a voluntary reason (not `rollout` or
    `limit`), opens a `voluntary` row already in `discussing` with the reason as its text. The
    ex-leader is cleared by the handoff pass after the discussion, never here (m890). A shutdown
    row belongs to the shutdown path and is left alone."""
    row = await open_row(conn, previous)
    if row is not None and row.trigger == "shutdown":
        return
    if req["kind"] != "handover":
        closed = (
            row is not None
            and row.phase in _PRE_DISCUSSION
            and await close(conn, row.id, "cancelled", "not_leader")
        )
        if closed and row is not None:
            for message_id in await queued_messages(conn, row.id):
                await queue.cancel_message(conn, message_id)
        return
    text = None if req["reason"] in FORCED_REASONS else req["reason"] or None
    now = (await (await conn.execute("SELECT now()")).fetchone())[0]  # type: ignore[index]
    if row is None:
        if text is not None:
            row = await insert(
                conn, session_id, previous, "voluntary", "discussing", handoff_text=text
            )
            await advance(
                conn,
                row.id,
                ["discussing"],
                "discussing",
                discuss_since=now,
                successor_agent_id=successor,
            )
        return
    if row.phase not in _PRE_DISCUSSION:
        return
    if (
        await advance(
            conn,
            row.id,
            [row.phase],
            "discussing",
            discuss_since=now,
            successor_agent_id=successor,
            handoff_text=row.handoff_text if row.handoff_text is not None else text,
        )
        and row.request_message_id is not None
    ):
        await queue.cancel_message(conn, row.request_message_id)  # no-op once it was claimed
