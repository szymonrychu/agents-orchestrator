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
    " clear_command_id, clear_attempts, cleared_at, return_message_id, closed_at, close_reason"
)
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
    """Open a row; raises UniqueViolation while the agent already has an open one."""
    row = await _fetch(
        conn,
        "INSERT INTO agent_handoffs (session_id, agent_id, trigger, phase, notified_at,"
        " deadline_at, handoff_text) VALUES (%s, %s, %s, %s, now(), %s, %s)"
        f" RETURNING {_COLUMNS}",
        (session_id, agent_id, trigger, phase, deadline_at, handoff_text),
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
    """Move a row to `to_phase` when it is in one of `from_phases`; True when a row changed."""
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


async def close(
    conn: psycopg.AsyncConnection,
    row_id: int,
    phase: Literal["done", "cancelled"],
    reason: str,
) -> bool:
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = %s, close_reason = %s, closed_at = now()"
        " WHERE id = %s AND phase = ANY(%s)",
        (phase, reason, row_id, list(OPEN_PHASES)),
    )
    return cur.rowcount == 1


async def extend(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, seconds: int
) -> Handoff | None:
    """Push the deadline of the agent's `notified` row out once; None when it is not notified."""
    return await _fetch(
        conn,
        "UPDATE agent_handoffs SET phase = 'extended', extended_at = now(),"
        " deadline_at = deadline_at + make_interval(secs => %s)"
        f" WHERE agent_id = %s AND phase = 'notified' RETURNING {_COLUMNS}",
        (seconds, agent_id),
    )


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
