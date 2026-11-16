"""agent_handoffs and agent_limits: cooperative handoff of an agent's context (Postgres).

Writer of both tables: this module. A row follows an agent under context pressure: it is opened
`pressuring`, the agent submits its own handoff (`submitted`), the successor takes over
(`moving`, `discussing`) and the ex-leader is cleared after an accepted submit only. Shutdown
rows (the pod-shutdown path) open at `handed` and keep their own flow. Every change is a
status-guarded update, so a row moves exactly once whichever caller commits first, and every
update stamps `progress_at` (except `mark_stuck_reported` and `mark_escalation_notified`). At
most one open row exists per agent (partial unique index `agent_handoffs_one_open`).

A `limit` leadership request exists only for a `moving` row (row-116 class): a row that leaves
`moving` other than through `on_leader_moved` (closed, or cleared after the request was
rejected) rejects a still-queued request `superseded` in the same transaction
(`leadership.supersede_request`, the writer of `leadership_requests` decisions).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db import queue

Phase = Literal[
    "pressuring",
    "submitted",
    "moving",
    "clearing",
    "discussing",
    "returning",
    "handed",
    "done",
    "cancelled",
]
OPEN_PHASES: tuple[str, ...] = (
    "pressuring",
    "submitted",
    "moving",
    "clearing",
    "discussing",
    "returning",
    "handed",
)

_COLUMNS = (
    "id, session_id, agent_id, trigger, phase, level, profile, handoff, handoff_text, rejects,"
    " reject_reasons, inflight, gate_blocks, submitted_at, escalated_at, nudged_level,"
    " l2_tokens_at, submit_tokens, progress_at, stuck_reported_at, leadership_request_id,"
    " successor_agent_id, delivered_at, discuss_since, clear_command_id, clear_attempts,"
    " cleared_at, return_message_id, created_at, closed_at, close_reason,"
    " escalation_message_ids, escalation_notified_at"
)
# A `shutdown_handoff` event (alias `e`) that carries a handoff: the routine's turn completed
# with a non-blank reply. Anything else leaves today's resume and transcript notice alone.
SHUTDOWN_TEXT = "(e.data->>'status' = 'completed' AND coalesce(e.data->>'final_text', '') ~ '\\S')"
_SETTABLE = frozenset(
    {
        "handoff",
        "handoff_text",
        "inflight",
        "leadership_request_id",
        "successor_agent_id",
        "delivered_at",
        "discuss_since",
        "clear_command_id",
        "clear_attempts",
        "cleared_at",
        "return_message_id",
    }
)
_JSON_FIELDS = frozenset({"handoff", "inflight"})
# The submit a transition was judged on is still the row's latest (`seen_submit`).
_SUBMIT_GUARD = " AND submitted_at = %s"


@dataclass(frozen=True)
class Handoff:
    id: int
    session_id: uuid.UUID
    agent_id: uuid.UUID
    trigger: str
    phase: str
    level: int
    profile: str | None
    handoff: dict[str, Any] | None
    handoff_text: str | None
    rejects: int
    reject_reasons: list[str]
    inflight: dict[str, Any] | None
    gate_blocks: int
    submitted_at: datetime | None
    escalated_at: datetime | None
    nudged_level: int
    l2_tokens_at: int | None
    submit_tokens: int | None
    progress_at: datetime
    stuck_reported_at: datetime | None
    leadership_request_id: int | None
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
    # the superleader escalation messages of the cycle and when the user's taskboard notice of
    # it was posted (`handoff.notify_escalations`)
    escalation_message_ids: list[int] = field(default_factory=list)
    escalation_notified_at: datetime | None = None


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
    handoff_text: str | None = None,
) -> Handoff:
    """Open a row in any phase (the shutdown and voluntary-move paths); raises UniqueViolation
    while the agent already has an open one. Context pressure opens through `open_pressure`."""
    row = await _fetch(
        conn,
        "INSERT INTO agent_handoffs (session_id, agent_id, trigger, phase, handoff_text)"
        f" VALUES (%s, %s, %s, %s, %s) RETURNING {_COLUMNS}",
        (session_id, agent_id, trigger, phase, handoff_text),
    )
    assert row is not None
    return row


async def open_pressure(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    level: int,
    profile: str,
    trigger: Literal["pressure", "voluntary"] = "pressure",
    used_tokens: int | None = None,
) -> Handoff:
    """Open a `pressuring` row at `level` for a `work` or `chat` agent; at level 2 or more
    `used_tokens` is kept in `l2_tokens_at`. Raises UniqueViolation while the agent already has
    an open row."""
    row = await _fetch(
        conn,
        "INSERT INTO agent_handoffs"
        " (session_id, agent_id, trigger, phase, level, profile, l2_tokens_at)"
        f" VALUES (%s, %s, %s, 'pressuring', %s, %s, %s) RETURNING {_COLUMNS}",
        (session_id, agent_id, trigger, level, profile, used_tokens if level >= 2 else None),
    )
    assert row is not None
    return row


async def set_level(
    conn: psycopg.AsyncConnection, row_id: int, level: int, used_tokens: int | None
) -> bool:
    """Raise the row's pressure level (never lowers it); the first time it reaches 2 or more the
    token count is kept in `l2_tokens_at`. True when the level rose."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET level = %(level)s, progress_at = now(),"
        " l2_tokens_at = CASE WHEN %(level)s >= 2 AND l2_tokens_at IS NULL"
        " THEN %(tokens)s::int ELSE l2_tokens_at END"
        " WHERE id = %(id)s AND level < %(level)s AND phase = ANY(%(open)s)",
        {"level": level, "tokens": used_tokens, "id": row_id, "open": list(OPEN_PHASES)},
    )
    return cur.rowcount == 1


async def record_reject(conn: psycopg.AsyncConnection, row_id: int, reasons: list[str]) -> int:
    """Count a rejected submit and keep its reasons; returns the new `rejects` (0 when the row
    is no longer open)."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET rejects = rejects + 1, progress_at = now(),"
        " reject_reasons = reject_reasons || %s WHERE id = %s AND phase = ANY(%s)"
        " RETURNING rejects",
        (Jsonb(reasons), row_id, list(OPEN_PHASES)),
    )
    row = await cur.fetchone()
    return 0 if row is None else row[0]


async def submit(
    conn: psycopg.AsyncConnection,
    row_id: int,
    handoff: dict[str, Any],
    inflight: dict[str, Any],
    submit_tokens: int | None,
    from_phases: Sequence[str] = ("pressuring", "submitted"),
) -> bool:
    """Store an accepted handoff and move the row to `submitted` from one of `from_phases`
    (a resubmit while `submitted` replaces it); True when a row changed."""
    if not set(from_phases) <= set(OPEN_PHASES):
        raise ValueError("submit moves from open phases only")
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = 'submitted', handoff = %s, inflight = %s,"
        " submit_tokens = %s, submitted_at = now(), progress_at = now()"
        " WHERE id = %s AND phase = ANY(%s)",
        (Jsonb(handoff), Jsonb(inflight), submit_tokens, row_id, list(from_phases)),
    )
    return cur.rowcount == 1


async def record_gate_block(
    conn: psycopg.AsyncConnection, row_id: int, inflight: dict[str, Any]
) -> None:
    """Count a Stop-gate block and keep the in-flight work it saw."""
    await conn.execute(
        "UPDATE agent_handoffs SET gate_blocks = gate_blocks + 1, inflight = %s,"
        " progress_at = now() WHERE id = %s AND phase = ANY(%s)",
        (Jsonb(inflight), row_id, list(OPEN_PHASES)),
    )


async def mark_escalated(
    conn: psycopg.AsyncConnection, row_id: int, message_ids: Sequence[int] = ()
) -> bool:
    """Stamp the escalation once per cycle with the superleader messages it queued; True only
    for the first caller."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET escalated_at = now(), progress_at = now(),"
        " escalation_message_ids = %s"
        " WHERE id = %s AND escalated_at IS NULL AND phase = ANY(%s)",
        (list(message_ids), row_id, list(OPEN_PHASES)),
    )
    return cur.rowcount == 1


async def mark_escalation_notified(conn: psycopg.AsyncConnection, row_id: int) -> bool:
    """Record that the user's taskboard notice of the escalation was posted; True only for the
    first caller (a still open, escalated row)."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET escalation_notified_at = now()"
        " WHERE id = %s AND escalated_at IS NOT NULL AND escalation_notified_at IS NULL"
        " AND phase = ANY(%s)",
        (row_id, list(OPEN_PHASES)),
    )
    return cur.rowcount == 1


async def escalations_to_notify(conn: psycopg.AsyncConnection) -> list[Handoff]:
    """Open escalated rows whose user notice is not posted yet, oldest first."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM agent_handoffs WHERE phase = ANY(%s)"
            " AND escalated_at IS NOT NULL AND escalation_notified_at IS NULL ORDER BY id",
            (list(OPEN_PHASES),),
        )
        return [Handoff(**row) for row in await cur.fetchall()]


async def stuck_rows(conn: psycopg.AsyncConnection, seconds: int) -> list[Handoff]:
    """Open rows not reported yet whose last progress is older than `seconds` (database clock),
    oldest first. A `discussing` row is judged by `discuss_since`: `progress_at` is bumped by
    every in-flight inventory change of the ex-leader, so a churning one would never be stuck."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM agent_handoffs WHERE phase = ANY(%s)"
            " AND stuck_reported_at IS NULL"
            " AND (CASE WHEN phase = 'discussing' THEN coalesce(discuss_since, progress_at)"
            " ELSE progress_at END) < now() - make_interval(secs => %s) ORDER BY id",
            (list(OPEN_PHASES), seconds),
        )
        return [Handoff(**row) for row in await cur.fetchall()]


async def mark_nudged(conn: psycopg.AsyncConnection, row_id: int, level: int) -> bool:
    """Record a nudge sent at `level`; True only when no nudge at `level` or higher was recorded
    yet (`nudged_level` starts at 0), so each level is nudged at most once."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET nudged_level = %s, progress_at = now()"
        " WHERE id = %s AND nudged_level < %s AND phase = ANY(%s)",
        (level, row_id, level, list(OPEN_PHASES)),
    )
    return cur.rowcount == 1


async def mark_stuck_reported(conn: psycopg.AsyncConnection, row_id: int) -> bool:
    """Stamp the stuck report once (leaves `progress_at` alone: a report is no progress)."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET stuck_reported_at = now()"
        " WHERE id = %s AND stuck_reported_at IS NULL AND phase = ANY(%s)",
        (row_id, list(OPEN_PHASES)),
    )
    return cur.rowcount == 1


async def advance(
    conn: psycopg.AsyncConnection,
    row_id: int,
    from_phases: Sequence[str],
    to_phase: str,
    *,
    seen_submit: datetime | None = None,
    **fields: Any,
) -> bool:
    """Move a row to `to_phase` when it is in one of `from_phases`; True when a row changed.

    Open phases only, on both sides: a row is closed through `close` and never reopened. With
    `seen_submit` the row must still carry that `submitted_at`: a submit that landed since the
    caller read the row makes the update lose (the submit route takes no lock)."""
    if to_phase not in OPEN_PHASES or not set(from_phases) <= set(OPEN_PHASES):
        raise ValueError("advance moves between open phases only")
    unknown = set(fields) - _SETTABLE
    if unknown:
        raise ValueError(f"unknown agent_handoffs fields: {sorted(unknown)}")
    sets = "".join(f", {name} = %s" for name in fields)
    values = [Jsonb(v) if k in _JSON_FIELDS and v is not None else v for k, v in fields.items()]
    cur = await conn.execute(
        f"UPDATE agent_handoffs SET phase = %s, progress_at = now(){sets}"
        f" WHERE id = %s AND phase = ANY(%s){_SUBMIT_GUARD if seen_submit else ''}"
        " RETURNING leadership_request_id",
        (to_phase, *values, row_id, list(from_phases), *([seen_submit] if seen_submit else [])),
    )
    return await _left_moving(conn, cur, to_phase)


async def _left_moving(
    conn: psycopg.AsyncConnection, cur: psycopg.AsyncCursor, to_phase: str
) -> bool:
    """After a guarded UPDATE ... RETURNING leadership_request_id: True when a row changed. A
    row now in any phase but `moving` keeps no queued `limit` request: a still-queued one is
    rejected `superseded` in the same transaction (an applied or decided one is left alone)."""
    got = await cur.fetchone()
    if got is None:
        return False
    if to_phase != "moving" and got[0] is not None:
        # leadership imports this module; the decision belongs to db/leadership.py
        from agents_orchestrator.db import leadership

        await leadership.supersede_request(conn, got[0])
    return True


async def start_clear(
    conn: psycopg.AsyncConnection,
    row_id: int,
    from_phases: Sequence[str],
    command_id: int,
    *,
    seen_submit: datetime | None = None,
) -> bool:
    """Move a row to `clearing` for the clear command `command_id`, counting the attempt in the
    same guarded UPDATE (`clear_attempts + 1`); True when a row changed. `seen_submit` as in
    `advance`."""
    if not set(from_phases) <= set(OPEN_PHASES):
        raise ValueError("start_clear moves from open phases only")
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = 'clearing', clear_command_id = %s,"
        " clear_attempts = clear_attempts + 1, progress_at = now()"
        f" WHERE id = %s AND phase = ANY(%s){_SUBMIT_GUARD if seen_submit else ''}"
        " RETURNING leadership_request_id",
        (command_id, row_id, list(from_phases), *([seen_submit] if seen_submit else [])),
    )
    return await _left_moving(conn, cur, "clearing")


async def close(
    conn: psycopg.AsyncConnection,
    row_id: int,
    phase: Literal["done", "cancelled"],
    reason: str,
    from_phases: Sequence[str] = OPEN_PHASES,
) -> bool:
    """Close a row still in one of `from_phases` (any open phase by default); True when closed.
    Its `limit` request, when still queued, is rejected `superseded` with it."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET phase = %s, close_reason = %s, closed_at = now(),"
        " progress_at = now() WHERE id = %s AND phase = ANY(%s) RETURNING leadership_request_id",
        (phase, reason, row_id, list(from_phases)),
    )
    return await _left_moving(conn, cur, phase)


async def mark_delivered(conn: psycopg.AsyncConnection, row_id: int) -> bool:
    """Record that the row's handoff text reached its successor; True only for the first caller,
    so the text goes out once whoever sends it (the notice sweeper or the handoff pass)."""
    cur = await conn.execute(
        "UPDATE agent_handoffs SET delivered_at = now(), progress_at = now()"
        " WHERE id = %s AND delivered_at IS NULL",
        (row_id,),
    )
    return cur.rowcount == 1


async def take_row(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    successor_agent_id: uuid.UUID,
    since: datetime | None,
) -> Handoff | None:
    """Mark the newest undelivered handoff of `agent_id` for its successor delivered and return
    that row (None when there is none). Only a row written since `since` (the predecessor's
    tenure start) counts: an older undelivered one is stale."""
    return await _fetch(
        conn,
        "UPDATE agent_handoffs SET delivered_at = now(), progress_at = now() WHERE id = ("
        " SELECT id FROM agent_handoffs WHERE agent_id = %s AND successor_agent_id = %s"
        " AND (handoff IS NOT NULL OR handoff_text IS NOT NULL) AND delivered_at IS NULL"
        " AND (%s::timestamptz IS NULL OR created_at >= %s)"
        f" ORDER BY id DESC LIMIT 1) AND delivered_at IS NULL RETURNING {_COLUMNS}",
        (agent_id, successor_agent_id, since, since),
    )


async def take_undelivered(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    successor_agent_id: uuid.UUID,
    since: datetime | None,
) -> tuple[dict[str, Any] | None, str | None]:
    """`take_row` as `(handoff, handoff_text)` (either may be None)."""
    row = await take_row(conn, agent_id, successor_agent_id, since)
    return (None, None) if row is None else (row.handoff, row.handoff_text)


async def latest_inflight(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID
) -> dict[str, Any] | None:
    """The newest in-flight inventory any row of the agent stored (the last one known)."""
    cur = await conn.execute(
        "SELECT inflight FROM agent_handoffs WHERE agent_id = %s AND inflight IS NOT NULL"
        " ORDER BY id DESC LIMIT 1",
        (agent_id,),
    )
    row = await cur.fetchone()
    return row[0] if row else None


async def open_rows(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> list[Handoff]:
    """The open rows of a session's agents, oldest first."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM agent_handoffs WHERE session_id = %s AND phase = ANY(%s)"
            " ORDER BY id",
            (session_id, list(OPEN_PHASES)),
        )
        return [Handoff(**row) for row in await cur.fetchall()]


async def open_rows_all(conn: psycopg.AsyncConnection) -> list[Handoff]:
    """Every open row, oldest first (the pressure push reads them)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM agent_handoffs WHERE phase = ANY(%s) ORDER BY id",
            (list(OPEN_PHASES),),
        )
        return [Handoff(**row) for row in await cur.fetchall()]


# An accepted handoff of a row closed `compacted` stays a reminder candidate this long.
REMINDER_WINDOW_SECONDS = 3600


async def unapplied_compacted(conn: psycopg.AsyncConnection) -> dict[uuid.UUID, Handoff]:
    """Per agent its latest row when that row closed `compacted` with an accepted handoff that
    was never delivered or returned: the handoff a compaction made moot, to remind of once."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM (SELECT DISTINCT ON (agent_id) * FROM agent_handoffs"
            " ORDER BY agent_id, id DESC) h WHERE phase = 'done' AND close_reason = 'compacted'"
            " AND handoff IS NOT NULL AND delivered_at IS NULL AND return_message_id IS NULL"
            " AND closed_at > now() - make_interval(secs => %s)",
            (REMINDER_WINDOW_SECONDS,),
        )
        return {row["agent_id"]: Handoff(**row) for row in await cur.fetchall()}


async def queued_messages(
    conn: psycopg.AsyncConnection, row_id: int, *, notices_only: bool = False
) -> list[int]:
    """The row's notice, nudge, escalation and return messages still queued (or its notices,
    nudges and escalations only), written since the row opened. Its escalations went to
    superleader sessions (`escalation_message_ids`)."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT m.id FROM messages m, agent_handoffs h WHERE h.id = %s"
            " AND m.status = 'queued' AND (m.id = ANY(h.escalation_message_ids)"
            " OR (m.target_agent_id = h.agent_id"
            " AND ((NOT %s AND m.id = h.return_message_id)"
            "  OR (m.notice IN ('handoff_notice', 'handoff_nudge', 'handoff_escalation')"
            "      AND m.created_at >= h.created_at))))",
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
_PRE_DISCUSSION = ("pressuring", "submitted", "moving", "handed")
# Reasons of the forced handover requests (the reconciler's rollout, the pressure handoff's
# limit): no leader's judgment, so no handoff text from them and they never expire.
FORCED_REASONS = ("rollout", "limit")
# The reason of the handoff pass's own handover request (one per `moving` row).
LIMIT_REASON = "limit"


async def _close_leader_again(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> None:
    row = await open_row(conn, agent_id)
    if row is None:
        return
    if row.phase == "clearing":
        # an ex-leader's clear still queued is cancelled with its row in one savepoint (a
        # cancelled clear with the row still `clearing` would read as clear_failed); a clear
        # already running is left alone, and the pass never retries it while the agent leads
        if row.discuss_since is None or row.trigger == "shutdown" or row.clear_command_id is None:
            return
        async with conn.transaction():
            if not await queue.cancel_queued_command(conn, row.clear_command_id):
                return
            if not await close(conn, row.id, "cancelled", "leader_again", ("clearing",)):
                raise psycopg.Rollback()
    elif row.phase not in ("discussing", "moving") or not await close(
        conn, row.id, "cancelled", "leader_again", ("discussing", "moving")
    ):
        return
    for message_id in await queued_messages(conn, row.id):
        await queue.cancel_message(conn, message_id)


async def on_leader_moved(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    previous: uuid.UUID,
    successor: uuid.UUID,
    req: dict[str, Any],
) -> None:
    """The leadership pass moved the lead from `previous` to `successor` (same transaction).

    `req` is the applied request (`kind`, `reason`). A move that neither the row's own `limit`
    request (row `moving`) nor the leader's voluntary `handover` caused (`user`, `rollout`,
    `scale_down`, `leader_lost`: pass a dict with that `kind`) closes a row still before its
    discussion `cancelled not_leader` and the ex-leader keeps its context (decision 716). A
    leader's own handover moves such a row to `discussing` (`discuss_since` now, the successor
    recorded, the agent's own handoff text kept, else the voluntary reason), or, with no row and
    a voluntary reason, opens a `voluntary` row already in `discussing` with the reason as its
    text. The ex-leader is cleared by the handoff pass after the discussion, never here (m890).
    A shutdown row belongs to the shutdown path and is left alone.

    The successor's own `discussing` or `moving` row (an ex-leader leading again, as in a
    2-agent hive's ping-pong, a user pick or a leader_lost move) closes `cancelled
    leader_again`: it is never cleared as an ex-leader while it leads, and the unique open-row
    index lets it open its own cycle. So does its `clearing` row whose clear is still queued
    (the clear cancelled in the same savepoint); a running clear is left alone."""
    await _close_leader_again(conn, successor)
    row = await open_row(conn, previous)
    if row is not None and row.trigger == "shutdown":
        return
    # decision 716: only the row's own `limit` request (queued for its accepted, gate-clear
    # submit, phase `moving`) or the leader's own voluntary handover leads to a discussion and
    # a clear; any other move (user, rollout, scale-down, lost leader) leaves the context alone
    own_limit = (
        row is not None
        and row.phase == "moving"
        and req.get("reason") == "limit"
        and req.get("id") is not None
        and req.get("id") == row.leadership_request_id
    )
    voluntary = req["kind"] == "handover" and req["reason"] not in FORCED_REASONS
    if not (own_limit or voluntary):
        closed = (
            row is not None
            and row.phase in _PRE_DISCUSSION
            and await close(conn, row.id, "cancelled", "not_leader")
        )
        if closed and row is not None:
            for message_id in await queued_messages(conn, row.id):
                await queue.cancel_message(conn, message_id)
        return
    text = (req["reason"] or None) if voluntary else None
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
    await advance(
        conn,
        row.id,
        [row.phase],
        "discussing",
        discuss_since=now,
        successor_agent_id=successor,
        handoff_text=row.handoff_text if row.handoff_text is not None else text,
    )


# ------------------------------------------------------------------ superleader reads (S-68)

_READ_COLUMNS = (
    "h.id, h.session_id, s.slug AS session_slug, h.agent_id, h.trigger AS kind, h.created_at,"
    " coalesce(char_length(h.handoff_text), 0) AS size"
)


async def list_handoffs(
    conn: psycopg.AsyncConnection,
    *,
    session_id: uuid.UUID | None,
    limit: int,
    before_id: int | None,
) -> tuple[list[dict[str, Any]], bool]:
    """The handoff rows newest first (id order), optionally of one session and older than
    `before_id`; `kind` is the row's trigger and `size` the characters of its text. The bool says
    whether more rows follow the page."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_READ_COLUMNS} FROM agent_handoffs h JOIN sessions s ON s.id = h.session_id"
            " WHERE (%(sid)s::uuid IS NULL OR h.session_id = %(sid)s)"
            " AND (%(before)s::bigint IS NULL OR h.id < %(before)s)"
            " ORDER BY h.id DESC LIMIT %(n)s",
            {"sid": session_id, "before": before_id, "n": limit + 1},
        )
        rows = await cur.fetchall()
    return rows[:limit], len(rows) > limit


async def get_handoff(conn: psycopg.AsyncConnection, id: int) -> dict[str, Any] | None:
    """One handoff row in full: the list fields plus `handoff_text`; None when absent."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_READ_COLUMNS}, h.handoff_text FROM agent_handoffs h"
            " JOIN sessions s ON s.id = h.session_id WHERE h.id = %s",
            (id,),
        )
        return await cur.fetchone()
