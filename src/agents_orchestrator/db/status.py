"""Repository for `agent_status` and `turns`. The ingester is the only writer.

One clock (D23): `free_since` is always the database `now()` at the moment a transition into
idle is recorded, and it moves on no other write. Wrapper timestamps (`idle_since`,
`data.since`) are stored in `wrapper_idle_since` for display and never compared.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from agents_orchestrator.db.pool import notify

STATUS_CHANNEL = "status"
AGENT_FREE_CHANNEL = "agent_free"
STATES = ("unknown", "booting", "idle", "busy", "dead")
_CONTEXT_KEYS = ("used_tokens", "window_tokens", "used_percentage")
BOOTSTRAP_MESSAGE_MAX = 240  # the wrapper already redacts and caps at 200; this is the backstop
_BOOTSTRAP_KEYS = ("step", "status", "message", "started_at", "finished_at")

Timestamp = datetime | str | None


@dataclass(frozen=True)
class StateChange:
    old: str | None
    new: str

    @property
    def changed(self) -> bool:
        return self.old != self.new

    @property
    def became_idle(self) -> bool:
        return self.new == "idle" and self.old != "idle"


def _check_state(state: str) -> None:
    if state not in STATES:
        raise ValueError(f"unknown agent state {state!r}")


def bootstrap_step(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    """A wrapper bootstrap step object, reduced to its known keys with the message truncated.

    The wrapper redacts failure messages; they pass through here only truncated.
    """
    if not raw.get("step"):
        return None
    step = {k: raw.get(k) for k in _BOOTSTRAP_KEYS}
    message = step["message"]
    if isinstance(message, str) and len(message) > BOOTSTRAP_MESSAGE_MAX:
        step["message"] = message[: BOOTSTRAP_MESSAGE_MAX - 3] + "..."
    return step


def bootstrap_from_steps(
    steps: list[Mapping[str, Any]],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    """(latest step, first failed step) of a snapshot's `bootstrap.steps`."""
    reduced = [b for b in map(bootstrap_step, steps) if b is not None]
    failed = next((b for b in reduced if b["status"] == "failed"), None)
    return (reduced[-1] if reduced else None), failed


def _context_values(context: Mapping[str, Any] | None) -> tuple[Any, Any, Any]:
    if context is None:
        return (None, None, None)
    used, window, pct = (context.get(k) for k in _CONTEXT_KEYS)
    return (used, window, pct)


async def _lock_state(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> str | None:
    """The stored state, row-locked for this transaction; None when there is no row yet."""
    cur = await conn.execute(
        "SELECT state FROM agent_status WHERE agent_id=%s FOR UPDATE", (agent_id,)
    )
    row = await cur.fetchone()
    return None if row is None else row[0]


async def _ensure_row(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> str | None:
    old = await _lock_state(conn, agent_id)
    if old is None:
        await conn.execute(
            "INSERT INTO agent_status (agent_id) VALUES (%s) ON CONFLICT (agent_id) DO NOTHING",
            (agent_id,),
        )
        await _lock_state(conn, agent_id)
    return old


async def upsert_status(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    *,
    state: str,
    wrapper_idle_since: Timestamp,
    context: Mapping[str, Any] | None,
    current_turn_id: str | None,
    last_seq: int,
    waiting: tuple[str, Timestamp] | None = None,
    bootstrap: tuple[dict[str, Any] | None, dict[str, Any] | None] = (None, None),
) -> StateChange:
    """Seed the row from a wrapper snapshot on attach.

    Every column comes from the snapshot (a null context clears the gauge; `waiting` is
    (reason, since) or None; `bootstrap` is (latest step, failed step)) except free_since,
    which moves to now() only when the snapshot is idle and the stored state was not.
    """
    _check_state(state)
    old = await _ensure_row(conn, agent_id)
    change = StateChange(old, state)
    used, window, pct = _context_values(context)
    await conn.execute(
        "UPDATE agent_status SET state=%s,"
        " free_since = CASE WHEN %s THEN now() ELSE free_since END,"
        " wrapper_idle_since=%s::timestamptz, current_turn_id=%s,"
        " context_used_tokens=%s, context_window_tokens=%s, context_pct=%s,"
        " last_seq=%s, waiting_reason=%s, waiting_since=%s::timestamptz,"
        " bootstrap_step=%s, bootstrap_failure=%s, updated_at=now() WHERE agent_id=%s",
        (
            state,
            change.became_idle,
            wrapper_idle_since if state == "idle" else None,
            current_turn_id,
            used,
            window,
            pct,
            last_seq,
            waiting[0] if waiting else None,
            waiting[1] if waiting else None,
            _jsonb(bootstrap[0]),
            _jsonb(bootstrap[1]),
            agent_id,
        ),
    )
    return change


def _jsonb(value: Mapping[str, Any] | None) -> Jsonb | None:
    return None if value is None else Jsonb(dict(value))


async def set_state(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    state: str,
    *,
    since: Timestamp = None,
) -> StateChange:
    """Record a session_state. A transition into idle sets free_since=now() and stores `since`
    as wrapper_idle_since; a repeated idle changes neither. Leaving idle clears
    wrapper_idle_since; idle and dead clear current_turn_id."""
    _check_state(state)
    old = await _ensure_row(conn, agent_id)
    change = StateChange(old, state)
    if state == "idle" and not change.became_idle:
        await conn.execute(
            "UPDATE agent_status SET current_turn_id=NULL, updated_at=now() WHERE agent_id=%s",
            (agent_id,),
        )
        return change
    await conn.execute(
        "UPDATE agent_status SET state=%s,"
        " free_since = CASE WHEN %s THEN now() ELSE free_since END,"
        " wrapper_idle_since=%s::timestamptz,"
        " current_turn_id = CASE WHEN %s THEN NULL ELSE current_turn_id END,"
        " waiting_reason = CASE WHEN %s THEN NULL ELSE waiting_reason END,"
        " waiting_since = CASE WHEN %s THEN NULL ELSE waiting_since END,"
        " updated_at=now() WHERE agent_id=%s",
        (
            state,
            change.became_idle,
            since if state == "idle" else None,
            state in ("idle", "dead"),
            state == "dead",
            state == "dead",
            agent_id,
        ),
    )
    return change


async def set_waiting(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, reason: str, since: Timestamp
) -> None:
    """The wrapper is waiting on a person (waiting_started). Does not touch `state`."""
    await _ensure_row(conn, agent_id)
    await conn.execute(
        "UPDATE agent_status SET waiting_reason=%s, waiting_since=COALESCE(%s::timestamptz, now()),"
        " updated_at=now() WHERE agent_id=%s",
        (reason, since, agent_id),
    )


async def clear_waiting(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, reason: str | None
) -> bool:
    """waiting_ended: clear only when `reason` is the stored one (None clears any).

    A reason change is ended(old) then started(new), so a late end of an old reason must not
    clear the new one. Returns whether a row was cleared.
    """
    await _ensure_row(conn, agent_id)
    cur = await conn.execute(
        "UPDATE agent_status SET waiting_reason=NULL, waiting_since=NULL, updated_at=now()"
        " WHERE agent_id=%s AND waiting_reason IS NOT NULL"
        " AND (%s::text IS NULL OR waiting_reason=%s)",
        (agent_id, reason, reason),
    )
    return cur.rowcount > 0


async def apply_bootstrap_step(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, raw: Mapping[str, Any]
) -> None:
    """A bootstrap_step event: it is the latest step; a failed one is also kept as the failure,
    and the same step later succeeding clears that failure."""
    step = bootstrap_step(raw)
    if step is None:
        return
    await _ensure_row(conn, agent_id)
    cur = await conn.execute(
        "SELECT bootstrap_failure FROM agent_status WHERE agent_id=%s", (agent_id,)
    )
    failure = (await cur.fetchone())[0]
    if step["status"] == "failed":
        failure = step
    elif failure is not None and failure.get("step") == step["step"]:
        failure = None
    await conn.execute(
        "UPDATE agent_status SET bootstrap_step=%s, bootstrap_failure=%s, updated_at=now()"
        " WHERE agent_id=%s",
        (_jsonb(step), _jsonb(failure), agent_id),
    )


async def set_context(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, context: Mapping[str, Any] | None
) -> None:
    """Set the gauge columns from a wrapper context object; None (or null values) clears them."""
    await _ensure_row(conn, agent_id)
    used, window, pct = _context_values(context)
    await conn.execute(
        "UPDATE agent_status SET context_used_tokens=%s, context_window_tokens=%s,"
        " context_pct=%s, updated_at=now() WHERE agent_id=%s",
        (used, window, pct, agent_id),
    )


async def set_compaction(conn: psycopg.AsyncConnection, agent_id: uuid.UUID, at: Timestamp) -> None:
    """Record a compaction reported at `at` (the event's own wrapper time, never the ingest
    time). compaction_at only moves forward, so a late or replayed event keeps its old time."""
    await _ensure_row(conn, agent_id)
    await conn.execute(
        "UPDATE agent_status SET compaction_at=GREATEST(compaction_at, %s::timestamptz),"
        " updated_at=now() WHERE agent_id=%s",
        (at, agent_id),
    )


def peer_members(view: Mapping[str, Any]) -> list[dict[str, Any]]:
    """The member list of a wrapper GET /v1/peers view; empty when the mesh is disabled."""
    if not view.get("enabled"):
        return []
    return [dict(m) for m in view.get("members") or []]


def peer_count(view: Mapping[str, Any]) -> int:
    return len(peer_members(view))


async def set_peers(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, view: Mapping[str, Any]
) -> int:
    """Store the peers view's member list (as returned) in agent_status.peers.

    `peers` is a JSON array, so the peer count is its length: len(members), and 0 when the
    view says enabled false. Returns that count.
    """
    await _ensure_row(conn, agent_id)
    members = peer_members(view)
    await conn.execute(
        "UPDATE agent_status SET peers=%s, updated_at=now() WHERE agent_id=%s",
        (Jsonb(members), agent_id),
    )
    return len(members)


async def set_current_turn(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str | None
) -> None:
    await _ensure_row(conn, agent_id)
    await conn.execute(
        "UPDATE agent_status SET current_turn_id=%s, updated_at=now() WHERE agent_id=%s",
        (turn_id, agent_id),
    )


async def clear_current_turn(
    conn: psycopg.AsyncConnection, agent_id: uuid.UUID, turn_id: str
) -> None:
    """Clear current_turn_id only if it still names this turn."""
    await conn.execute(
        "UPDATE agent_status SET current_turn_id=NULL, updated_at=now()"
        " WHERE agent_id=%s AND current_turn_id=%s",
        (agent_id, turn_id),
    )


async def set_last_seq(conn: psycopg.AsyncConnection, agent_id: uuid.UUID, seq: int) -> None:
    await conn.execute(
        "UPDATE agent_status SET last_seq=GREATEST(COALESCE(last_seq, 0), %s), updated_at=now()"
        " WHERE agent_id=%s",
        (seq, agent_id),
    )


async def upsert_turn(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    turn_id: str,
    *,
    origin: str | None = None,
    started_at: Timestamp = None,
    completed_at: Timestamp = None,
    status: str | None = None,
    final_text: str | None = None,
    usage: Mapping[str, Any] | None = None,
) -> None:
    """Insert or merge a turn row; a None argument keeps what is stored.

    started_at is written once, by the first event seen for the turn (normally turn_started).
    """
    await conn.execute(
        "INSERT INTO turns (turn_id, agent_id, origin, started_at, completed_at, status,"
        " final_text, usage)"
        " VALUES (%s,%s,%s,COALESCE(%s::timestamptz, now()),%s::timestamptz,%s,%s,%s)"
        " ON CONFLICT (turn_id) DO UPDATE SET"
        " origin=COALESCE(EXCLUDED.origin, turns.origin),"
        " completed_at=COALESCE(EXCLUDED.completed_at, turns.completed_at),"
        " status=COALESCE(EXCLUDED.status, turns.status),"
        " final_text=COALESCE(EXCLUDED.final_text, turns.final_text),"
        " usage=COALESCE(EXCLUDED.usage, turns.usage)",
        (
            turn_id,
            agent_id,
            origin,
            started_at,
            completed_at,
            status,
            final_text,
            Jsonb(dict(usage)) if usage is not None else None,
        ),
    )


async def notify_status(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID
) -> None:
    await notify(conn, STATUS_CHANNEL, {"session_id": str(session_id), "agent_id": str(agent_id)})


async def notify_agent_free(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID
) -> None:
    await notify(
        conn, AGENT_FREE_CHANNEL, {"session_id": str(session_id), "agent_id": str(agent_id)}
    )
