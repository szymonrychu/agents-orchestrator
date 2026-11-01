"""SQL of the three queue tables: messages, commands and checks (D2, D4, D23, D24).

Every status change is a guarded UPDATE (`WHERE status = <expected>`) and reports whether it
won. Every time comparison and every timestamp write uses the database clock (`now()`, D23).

Transactions: each function runs in `conn.transaction()`. On an idle connection that is its own
transaction, committed on return; inside a caller's open transaction it is a savepoint and the
caller controls the commit.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db.eligibility import ELIGIBLE, HANDOVER_TRIGGER
from agents_orchestrator.db.pool import notify

COMMAND_KINDS = frozenset({"interrupt", "compact", "restart", "rebootstrap"})
COMMAND_FINAL = frozenset({"done", "failed", "cancelled"})
CHECK_FINAL = frozenset({"passed", "failed", "cancelled"})

# First key of the two-int advisory lock, so the per-session claim lock cannot collide with
# other advisory locks (the singleton lock) taken with a different namespace.
_CLAIM_LOCK_NAMESPACE = 0x6F726331  # "orc1"
_SCAN_BATCH = 100


class SessionBeingDeleted(Exception):
    """The session is being deleted and takes no more edits or messages."""


@dataclass(frozen=True)
class Claim:
    message_id: int
    session_id: uuid.UUID
    text: str
    attempts: int
    target_agent_id: uuid.UUID | None
    agent_id: uuid.UUID
    agent_url: str | None
    wrapper_session_id: str
    free_since: datetime
    dispatched_at: datetime
    previous_dispatched_at: datetime | None
    sender_slug: str | None
    lane: str = "regular"


@dataclass(frozen=True)
class Pick:
    agent_id: uuid.UUID
    url: str | None
    wrapper_session_id: str
    free_since: datetime
    last_dispatched_at: datetime | None


@dataclass(frozen=True)
class Verifying:
    message_id: int
    session_id: uuid.UUID
    text: str
    attempts: int
    error: str | None
    agent_id: uuid.UUID | None
    agent_url: str | None
    wrapper_session_id: str | None
    dispatched_at: datetime | None
    ambiguous_at: datetime
    age_seconds: float


@dataclass(frozen=True)
class Command:
    id: int
    agent_id: uuid.UUID
    kind: str
    status: str
    created_at: datetime
    started_at: datetime
    session_id: uuid.UUID
    agent_url: str | None
    wrapper_session_id: str | None


@dataclass(frozen=True)
class Check:
    id: int
    session_id: uuid.UUID
    command: str
    status: str
    job_name: str | None
    created_at: datetime
    started_at: datetime


async def _one(conn: psycopg.AsyncConnection, query: str, params: Any = ()) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchone()


async def _all(conn: psycopg.AsyncConnection, query: str, params: Any = ()) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


async def _status(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> None:
    await notify(conn, "status", {"session_id": str(session_id)})


async def _transition(
    conn: psycopg.AsyncConnection, query: str, params: dict[str, Any]
) -> dict | None:
    """Run one guarded UPDATE ... RETURNING session_id, NOTIFY status when it won."""
    async with conn.transaction():
        row = await _one(conn, query, params)
        if row is not None:
            await _status(conn, row["session_id"])
        return row


# ---------------------------------------------------------------- messages: api side


async def enqueue_message(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    text: str,
    target_agent_id: uuid.UUID | None = None,
    *,
    sender_session_id: uuid.UUID | None = None,
    sender_slug: str | None = None,
    hop: int = 0,
    lane: str = "regular",
    carries_start_prompt: bool = False,
    held: bool = False,
) -> int:
    """Queue a message; a session message carries its sender and hop (D8).

    A `held` message is not deliverable until `release_note` clears its `not_before` (the hold
    is a day, so a note whose commands never finish still expires).

    A message with a sender is untargeted, so the leader routing delivers it to whoever leads
    the target session at claim time.
    """
    async with conn.transaction():
        # Share-lock the session row: the idle discard holds it FOR UPDATE while it re-checks, so
        # a message either lands before that check (and cancels the discard) or is refused.
        locked = await _one(
            conn, "SELECT desired FROM sessions WHERE id = %s FOR SHARE", (session_id,)
        )
        if locked is not None and locked["desired"] != "active":
            raise SessionBeingDeleted(str(session_id))
        row = await _one(
            conn,
            "INSERT INTO messages (session_id, text, target_agent_id, sender_session_id,"
            " sender_slug, hop, lane, carries_start_prompt, not_before)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s,"
            " CASE WHEN %s THEN now() + interval '1 day' END) RETURNING id",
            (
                session_id,
                text,
                target_agent_id,
                sender_session_id,
                sender_slug,
                hop,
                lane,
                carries_start_prompt,
                held,
            ),
        )
        assert row is not None
        await notify(conn, "message_queued", {"session_id": str(session_id)})
        await _status(conn, session_id)
        return row["id"]


_FIRST_MESSAGE_LOCK_NAMESPACE = 0x6F726332  # "orc2"


async def start_prompt_pending(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> bool:
    """True when no live message carries the session's start prompt; call inside a transaction.

    A carrier that failed or was cancelled no longer counts, so the next user message takes the
    prompt over; peer messages never carry it. The transaction-level advisory lock makes two
    concurrent posts agree: the second waits for the first to commit and then sees its row.
    """
    await conn.execute(
        "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))",
        (_FIRST_MESSAGE_LOCK_NAMESPACE, session_id),
    )
    row = await _one(
        conn,
        "SELECT NOT EXISTS (SELECT 1 FROM messages WHERE session_id = %s"
        " AND carries_start_prompt AND status NOT IN ('failed', 'cancelled')) AS pending",
        (session_id,),
    )
    assert row is not None
    return row["pending"]


async def _count(conn: psycopg.AsyncConnection, query: str, params: Any) -> int:
    row = await _one(conn, query, params)
    assert row is not None
    return row["n"]


async def turn_hop(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> int | None:
    """Hop of the session message the agent's current turn delivered, or None when the current
    turn (`agent_status.current_turn_id`) delivered no message with a sender (D10)."""
    row = await _one(
        conn,
        "SELECT m.hop FROM agent_status s JOIN agents a ON a.id = s.agent_id"
        " JOIN messages m ON m.session_id = a.session_id AND m.turn_id = s.current_turn_id"
        " WHERE s.agent_id = %s AND m.sender_session_id IS NOT NULL"
        " ORDER BY m.id DESC LIMIT 1",
        (agent_id,),
    )
    return row["hop"] if row is not None else None


async def count_sent_by(
    conn: psycopg.AsyncConnection, sender_session_id: uuid.UUID, seconds: float
) -> int:
    """Session messages of any status the sender session sent in the last `seconds`."""
    return await _count(
        conn,
        "SELECT count(*) AS n FROM messages WHERE sender_session_id = %s"
        " AND created_at > now() - make_interval(secs => %s::float8)",
        (sender_session_id, seconds),
    )


async def count_between(
    conn: psycopg.AsyncConnection, a: uuid.UUID, b: uuid.UUID, seconds: float
) -> int:
    """Session messages exchanged between sessions `a` and `b`, both directions, in the last
    `seconds`."""
    return await _count(
        conn,
        "SELECT count(*) AS n FROM messages"
        " WHERE ((sender_session_id = %(a)s AND session_id = %(b)s)"
        " OR (sender_session_id = %(b)s AND session_id = %(a)s))"
        " AND created_at > now() - make_interval(secs => %(secs)s::float8)",
        {"a": a, "b": b, "secs": seconds},
    )


async def count_queued_session_messages(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID
) -> int:
    """Queued messages with a sender waiting in the session."""
    return await _count(
        conn,
        "SELECT count(*) AS n FROM messages WHERE session_id = %s AND status = 'queued'"
        " AND sender_session_id IS NOT NULL",
        (session_id,),
    )


async def cancel_message(conn: psycopg.AsyncConnection, id: int) -> bool:
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'cancelled' WHERE id = %(id)s AND status = 'queued'"
        " RETURNING session_id",
        {"id": id},
    )
    return row is not None


async def _release_reservation(conn: psycopg.AsyncConnection, row: dict) -> None:
    """Clear the agent's reservation, only while it is still this message's."""
    if row.get("agent_id") is not None and row.get("dispatched_at") is not None:
        await conn.execute(
            "UPDATE agents SET last_dispatched_at = NULL WHERE id = %s AND last_dispatched_at = %s",
            (row["agent_id"], row["dispatched_at"]),
        )


async def retry_message(conn: psycopg.AsyncConnection, id: int) -> bool:
    """The user's explicit Retry of a failed message: a fresh attempt budget."""
    async with conn.transaction():
        row = await _transition(
            conn,
            "UPDATE messages SET status = 'queued', attempts = 0, error = NULL,"
            " not_before = NULL, ambiguous_at = NULL, heartbeat_at = NULL"
            " WHERE id = %(id)s AND status = 'failed'"
            " RETURNING session_id, agent_id, dispatched_at",
            {"id": id},
        )
        if row is None:
            return False
        await _release_reservation(conn, row)
        await notify(conn, "message_queued", {"session_id": str(row["session_id"])})
        return True


# ---------------------------------------------------------------- messages: the claim

# A deleting session is excluded: its history is being exported and its pods torn down (D15),
# so a turn started now would miss the discard-hook export. Its messages stay queued.
_CANDIDATES = """
SELECT m.session_id, min(m.id) AS head
FROM messages m JOIN sessions c ON c.id = m.session_id
WHERE m.status = 'queued' AND c.desired = 'active'
GROUP BY m.session_id
ORDER BY head
"""

_PICK = """
SELECT a.id AS agent_id, a.url, a.wrapper_session_id, s.free_since, a.last_dispatched_at
FROM agents a JOIN agent_status s ON s.agent_id = a.id
WHERE a.session_id = %(session_id)s
  AND a.ready
  AND a.gone_at IS NULL
  AND a.wrapper_session_id IS NOT NULL
  AND s.state = 'idle'
  AND s.free_since > coalesce(a.last_dispatched_at, '-infinity'::timestamptz)
  AND (%(target)s::uuid IS NULL OR a.id = %(target)s::uuid)
ORDER BY s.free_since ASC, a.id
LIMIT 1
FOR UPDATE OF a SKIP LOCKED
"""


# The regular and priority lanes bypass agent_free (D2): any ready agent of the session, an idle
# one first (longest free), because the wrapper takes the message mid-turn and answers 409
# booting, dead, busy or dialog_open itself.
_PICK_LIVE = """
SELECT a.id AS agent_id, a.url, a.wrapper_session_id, s.free_since, a.last_dispatched_at
FROM agents a JOIN agent_status s ON s.agent_id = a.id
WHERE a.session_id = %(session_id)s
  AND a.ready
  AND a.gone_at IS NULL
  AND a.wrapper_session_id IS NOT NULL
  AND (%(target)s::uuid IS NULL OR a.id = %(target)s::uuid)
ORDER BY (s.state = 'idle') DESC, s.free_since ASC, a.id
LIMIT 1
FOR UPDATE OF a SKIP LOCKED
"""


async def _pick_agent(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    target_agent_id: uuid.UUID | None,
    lane: str = "idle",
) -> Pick | None:
    """The agent free the longest whose free_since is newer than its last dispatch, locked.

    Reading `agents.last_dispatched_at` (a column the claim writes) is what makes a concurrent
    READ COMMITTED claim recheck the predicate on the new row version instead of trusting its
    snapshot (D23 c).
    """
    query = _PICK if lane == "idle" else _PICK_LIVE
    row = await _one(conn, query, {"session_id": session_id, "target": target_agent_id})
    return Pick(**row) if row is not None else None


async def _lock_queued(conn: psycopg.AsyncConnection, message_id: int) -> bool:
    row = await _one(
        conn,
        "SELECT id FROM messages WHERE id = %s AND status = 'queued' FOR UPDATE SKIP LOCKED",
        (message_id,),
    )
    return row is not None


async def _target_gone(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, target_agent_id: uuid.UUID
) -> bool:
    row = await _one(
        conn,
        "SELECT gone_at FROM agents WHERE id = %s AND session_id = %s",
        (target_agent_id, session_id),
    )
    return row is None or row["gone_at"] is not None


# A leader whose handover is pending (it compacted in its term, or its pod is leaving) while an
# agent that could take over exists: the pass's pick predicate (`ELIGIBLE`), so the hold never
# outlives a possible move. Outside the subquery `a` is the leader's agent, `l` its row and `c`
# the session (the trigger's aliases); inside it `a` is the candidate peer.
_HANDOVER_PENDING = f"""
SELECT {HANDOVER_TRIGGER}
       AND EXISTS (SELECT 1 FROM agents a WHERE a.session_id = l.session_id AND {ELIGIBLE}
                   AND a.id <> l.agent_id) AS pending
FROM session_leaders l
JOIN agents a ON a.id = l.agent_id
JOIN sessions c ON c.id = l.session_id
WHERE l.session_id = %s
"""


async def _handover_pending(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> bool:
    row = await _one(conn, _HANDOVER_PENDING, (session_id,))
    return bool(row and row["pending"])


def _lane_class(lane: str) -> str:
    """Idle messages wait for agent_free; regular and priority bypass it (D2): FIFO per class."""
    return "idle" if lane == "idle" else "live"


async def _claim_in_session(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Claim | None:
    """Scan one session's queued messages in id order; the caller holds the session lock."""
    leader = await _one(
        conn, "SELECT agent_id FROM session_leaders WHERE session_id = %s", (session_id,)
    )
    leader_id = leader["agent_id"] if leader is not None else None
    # An untargeted head waits on the leader: later untargeted messages of its lane class wait
    # too. A leader whose handover is pending and possible holds them from the start (D16): the
    # pass moves it first. `stalled` is the leaderless form: no free agent or a backoff head
    # holds the rest of its lane class (FIFO, D4).
    pending = leader_id is not None and await _handover_pending(conn, session_id)
    held = {"idle": pending, "live": pending}
    stalled: set[str] = set()
    after = 0
    while True:
        rows = await _all(
            conn,
            "SELECT id, target_agent_id, lane,"
            " (not_before IS NOT NULL AND not_before > now()) AS waiting"
            " FROM messages WHERE session_id = %s AND status = 'queued' AND id > %s"
            " ORDER BY id LIMIT %s",
            (session_id, after, _SCAN_BATCH),
        )
        if not rows:
            return None
        for row in rows:
            after = row["id"]
            target = row["target_agent_id"]
            lane = row["lane"]
            cls = _lane_class(lane)
            if cls in stalled:
                continue
            if target is None:
                if row["waiting"]:
                    if leader_id is None:
                        stalled.add(cls)  # an untargeted head in backoff holds its lane class
                    else:
                        held[cls] = True  # leader routing: hold untargeted, targeted still flow
                    continue
                if leader_id is not None:
                    # Leader-only routing: an untargeted message is the leader's. A gone leader
                    # does not fail it; it waits for the leadership pass to elect a successor.
                    if held[cls] or await _target_gone(conn, session_id, leader_id):
                        held[cls] = True
                        continue
                    target = leader_id
            else:
                if row["waiting"]:
                    continue
                if await _target_gone(conn, session_id, target):
                    await _transition(
                        conn,
                        "UPDATE messages SET status = 'failed', error = 'target_gone'"
                        " WHERE id = %(id)s AND status = 'queued' RETURNING session_id",
                        {"id": row["id"]},
                    )
                    continue
            if not await _lock_queued(conn, row["id"]):
                continue
            pick = await _pick_agent(conn, session_id, target, lane)
            if pick is None:
                if row["target_agent_id"] is None:
                    if leader_id is None:
                        stalled.add(cls)  # no free agent in this session: leave it queued
                    else:
                        held[cls] = True  # the leader is busy: the untargeted FIFO holds
                    continue
                continue  # a busy or booting target does not block later messages (D23 d)
            return await _dispatch(conn, row["id"], pick)


async def _dispatch(conn: psycopg.AsyncConnection, message_id: int, pick: Pick) -> Claim:
    await conn.execute(
        "UPDATE agents SET last_dispatched_at = now() WHERE id = %s", (pick.agent_id,)
    )
    msg = await _one(
        conn,
        "UPDATE messages SET status = 'dispatching', agent_id = %s, dispatched_at = now(),"
        " heartbeat_at = now() WHERE id = %s AND status = 'queued'"
        " RETURNING id, session_id, text, attempts, target_agent_id, dispatched_at, sender_slug,"
        " lane",
        (pick.agent_id, message_id),
    )
    assert msg is not None  # the row is locked and was queued
    await _status(conn, msg["session_id"])
    return Claim(
        message_id=msg["id"],
        session_id=msg["session_id"],
        text=msg["text"],
        attempts=msg["attempts"],
        target_agent_id=msg["target_agent_id"],
        agent_id=pick.agent_id,
        agent_url=pick.url,
        wrapper_session_id=pick.wrapper_session_id,
        free_since=pick.free_since,
        dispatched_at=msg["dispatched_at"],
        previous_dispatched_at=pick.last_dispatched_at,
        sender_slug=msg["sender_slug"],
        lane=msg["lane"],
    )


async def claim_and_pick(conn: psycopg.AsyncConnection) -> Claim | None:
    """Claim the next dispatchable message and reserve the agent free the longest for it.

    One transaction, one clock. Sessions are tried in the order of their oldest queued
    message, each under a transaction-level advisory lock that a concurrent dispatcher skips,
    so per-session FIFO holds with several dispatchers (D23 e). A session with no free agent is
    left queued and the next session is tried.
    """
    async with conn.transaction():
        for cand in await _all(conn, _CANDIDATES):
            lock = await _one(
                conn,
                "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s::text)) AS got",
                (_CLAIM_LOCK_NAMESPACE, cand["session_id"]),
            )
            if not (lock and lock["got"]):
                continue
            claim = await _claim_in_session(conn, cand["session_id"])
            if claim is not None:
                return claim
    return None


async def release_agent(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    previous_dispatched_at: datetime | None,
    *,
    claimed_at: datetime | None = None,
) -> bool:
    """Undo the claim's reservation after an outcome where no turn was accepted (D23 a).

    With `claimed_at` (the claim's `dispatched_at`) the restore happens only while the
    reservation is still that claim's, so a late release never undoes a newer claim.
    """
    async with conn.transaction():
        cur = await conn.execute(
            "UPDATE agents SET last_dispatched_at = %s WHERE id = %s"
            " AND (%s::timestamptz IS NULL OR last_dispatched_at = %s::timestamptz)",
            (previous_dispatched_at, agent_id, claimed_at, claimed_at),
        )
        return cur.rowcount == 1


# ---------------------------------------------------------------- messages: outcomes


async def heartbeat_dispatching(conn: psycopg.AsyncConnection, id: int) -> bool:
    async with conn.transaction():
        cur = await conn.execute(
            "UPDATE messages SET heartbeat_at = now() WHERE id = %s AND status = 'dispatching'",
            (id,),
        )
        return cur.rowcount == 1


async def mark_sent(conn: psycopg.AsyncConnection, id: int, turn_id: str) -> bool:
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'sent', turn_id = %(turn_id)s, error = NULL"
        " WHERE id = %(id)s AND status = 'dispatching' RETURNING session_id",
        {"id": id, "turn_id": turn_id},
    )
    return row is not None


async def requeue(
    conn: psycopg.AsyncConnection,
    id: int,
    backoff_base: float = 0.0,
    backoff_max: float = 0.0,
) -> bool:
    """409 busy/booting: back to queued, no attempt counted.

    A regular or priority message is not gated by agent_free, so it also waits a bounded
    exponential backoff (`backoff_base * 2^requeues`, at most `backoff_max`) and cannot be
    re-claimed in a hot loop. An idle message is gated by agent_free and gets none.
    """
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'queued', heartbeat_at = NULL,"
        " not_before = CASE WHEN lane = 'idle' OR %(base)s <= 0 THEN NULL ELSE now() +"
        " make_interval(secs => least(%(base)s * power(2, least(busy_requeues, 20)), %(max)s))"
        " END,"
        " busy_requeues = CASE WHEN lane = 'idle' THEN busy_requeues ELSE busy_requeues + 1 END"
        " WHERE id = %(id)s AND status = 'dispatching' RETURNING session_id",
        {"id": id, "base": backoff_base, "max": backoff_max},
    )
    return row is not None


async def fail_message(conn: psycopg.AsyncConnection, id: int, error: str) -> bool:
    """A permanent non-accept (rejected, config_error): failed at once, no retry (D24)."""
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'failed', error = %(error)s"
        " WHERE id = %(id)s AND status = 'dispatching' RETURNING session_id",
        {"id": id, "error": error},
    )
    return row is not None


_COUNT_FAILURE = """
UPDATE messages SET
    attempts = attempts + 1,
    error = %(error)s,
    heartbeat_at = NULL,
    status = CASE WHEN attempts + 1 >= %(max_attempts)s THEN 'failed' ELSE 'queued' END,
    not_before = CASE WHEN attempts + 1 >= %(max_attempts)s THEN not_before
        ELSE now() + make_interval(secs => least(
            %(base)s::float8 * power(2::float8, attempts), %(cap)s::float8))
        END
WHERE id = %(id)s AND status = %(expected)s
RETURNING session_id, status, agent_id, dispatched_at
"""


async def _count_failure(
    conn: psycopg.AsyncConnection,
    id: int,
    expected: str,
    error: str,
    max_attempts: int,
    backoff_base: float,
    backoff_max: float,
) -> dict | None:
    # In SET, `attempts` is the old value, so 2^attempts is 2^(new attempts - 1).
    return await _transition(
        conn,
        _COUNT_FAILURE,
        {
            "id": id,
            "expected": expected,
            "error": error,
            "max_attempts": max_attempts,
            "base": backoff_base,
            "cap": backoff_max,
        },
    )


async def record_failure(
    conn: psycopg.AsyncConnection,
    id: int,
    error: str,
    max_attempts: int,
    backoff_base: float,
    backoff_max: float,
) -> str | None:
    """A definite non-accept: count an attempt, requeue with backoff or fail at max.

    Returns the new status, or None when the message was no longer dispatching.
    """
    row = await _count_failure(
        conn, id, "dispatching", error, max_attempts, backoff_base, backoff_max
    )
    return row["status"] if row is not None else None


# ---------------------------------------------------------------- messages: ambiguous (D24)


async def mark_verifying(conn: psycopg.AsyncConnection, id: int, reason: str) -> bool:
    """An ambiguous send (or a lost dispatcher): the verifier decides; the agent stays reserved."""
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'verifying', ambiguous_at = now(), error = %(reason)s"
        " WHERE id = %(id)s AND status = 'dispatching' RETURNING session_id",
        {"id": id, "reason": reason},
    )
    return row is not None


async def list_verifying(conn: psycopg.AsyncConnection, limit: int) -> list[Verifying]:
    rows = await _all(
        conn,
        "SELECT m.id AS message_id, m.session_id, m.text, m.attempts, m.error, m.agent_id,"
        " a.url AS agent_url, a.wrapper_session_id, m.dispatched_at, m.ambiguous_at,"
        " extract(epoch from now() - m.ambiguous_at)::float8 AS age_seconds"
        " FROM messages m LEFT JOIN agents a ON a.id = m.agent_id"
        " WHERE m.status = 'verifying' ORDER BY m.ambiguous_at, m.id LIMIT %s",
        (limit,),
    )
    return [Verifying(**r) for r in rows]


async def resolve_started(conn: psycopg.AsyncConnection, id: int, turn_id: str) -> bool:
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'sent', turn_id = %(turn_id)s, error = NULL"
        " WHERE id = %(id)s AND status = 'verifying' RETURNING session_id",
        {"id": id, "turn_id": turn_id},
    )
    return row is not None


async def resolve_not_started(
    conn: psycopg.AsyncConnection,
    id: int,
    max_attempts: int,
    backoff_base: float,
    backoff_max: float,
) -> str | None:
    """A verified not-started send: count the attempt, requeue with backoff or fail at max.

    The reservation is cleared only while it is still this message's. Returns the new status,
    or None when the message was no longer verifying.
    """
    async with conn.transaction():
        row = await _count_failure(
            conn, id, "verifying", "ambiguous_not_started", max_attempts, backoff_base, backoff_max
        )
        if row is None:
            return None
        await _release_reservation(conn, row)
        return row["status"]


async def fail_ambiguous(conn: psycopg.AsyncConnection, id: int) -> bool:
    """No definite answer by the deadline: failed as ambiguous; only Retry re-sends it."""
    async with conn.transaction():
        row = await _transition(
            conn,
            "UPDATE messages SET status = 'failed', error = 'ambiguous'"
            " WHERE id = %(id)s AND status = 'verifying'"
            " RETURNING session_id, agent_id, dispatched_at",
            {"id": id},
        )
        if row is None:
            return False
        await _release_reservation(conn, row)
        return True


async def verify_stale_dispatching(conn: psycopg.AsyncConnection, older_than_seconds: float) -> int:
    """Hand stale dispatching rows to the verifier; never re-send them blindly (D24)."""
    async with conn.transaction():
        rows = await _all(
            conn,
            "UPDATE messages SET status = 'verifying', ambiguous_at = now(),"
            " error = 'dispatcher_lost'"
            " WHERE status = 'dispatching'"
            " AND coalesce(heartbeat_at, dispatched_at)"
            " < now() - make_interval(secs => %s::float8)"
            " RETURNING session_id",
            (older_than_seconds,),
        )
        for session_id in sorted({r["session_id"] for r in rows}):
            await _status(conn, session_id)
        return len(rows)


# ---------------------------------------------------------------- commands


async def enqueue_command(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    kind: str,
    *,
    issued_by_session: uuid.UUID | None = None,
    note_message_id: int | None = None,
) -> int:
    if kind not in COMMAND_KINDS:
        raise ValueError(f"unknown command kind {kind!r}")
    async with conn.transaction():
        row = await _one(
            conn,
            "INSERT INTO commands (agent_id, kind, issued_by_session, note_message_id)"
            " VALUES (%s, %s, %s, %s) RETURNING id",
            (agent_id, kind, issued_by_session, note_message_id),
        )
        assert row is not None
        return row["id"]


async def claim_command(conn: psycopg.AsyncConnection, kinds: Sequence[str]) -> Command | None:
    async with conn.transaction():
        row = await _one(
            conn,
            "WITH c AS ("
            " UPDATE commands SET status = 'running', started_at = now()"
            " WHERE id = (SELECT id FROM commands WHERE status = 'queued' AND kind = ANY(%s)"
            "  ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)"
            " AND status = 'queued'"
            " RETURNING id, agent_id, kind, status, created_at, started_at)"
            " SELECT c.*, a.session_id, a.url AS agent_url, a.wrapper_session_id"
            " FROM c JOIN agents a ON a.id = c.agent_id",
            (list(kinds),),
        )
        return Command(**row) if row is not None else None


async def complete_command(
    conn: psycopg.AsyncConnection, id: int, status: str, result: Any
) -> bool:
    if status not in COMMAND_FINAL:
        raise ValueError(f"not a final command status: {status!r}")
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE commands SET status = %s, result = %s, finished_at = now()"
            " WHERE id = %s AND status = 'running' RETURNING id",
            (status, Jsonb(result) if result is not None else None, id),
        )
        return row is not None


async def release_ready_notes(conn: psycopg.AsyncConnection) -> None:
    """Release each held self-restart note whose restarted agents are all back up.

    A restart command is `done` when its pod is deleted, which is before the replacement
    exists, so completion releases nothing. A note is released once, for every restart command
    naming it, the command ended without replacing a pod (failed, cancelled), its agent is gone,
    or the agent is ready with a wrapper session created or resumed after the command finished
    (the old pod, still terminating, has neither).
    """
    rows = await _all(
        conn,
        "UPDATE messages m SET not_before = NULL"
        " WHERE m.status = 'queued' AND m.not_before > now()"
        " AND EXISTS (SELECT 1 FROM commands c WHERE c.note_message_id = m.id)"
        " AND NOT EXISTS (SELECT 1 FROM commands c JOIN agents a ON a.id = c.agent_id"
        "  WHERE c.note_message_id = m.id AND (c.status IN ('queued', 'running')"
        "   OR (c.status = 'done' AND a.gone_at IS NULL AND NOT (a.ready AND"
        "    (coalesce(a.wrapper_session_created_at > c.finished_at, false)"
        "     OR coalesce(a.wrapper_resumed_at > c.finished_at, false))))))"
        " RETURNING m.session_id",
        {},
    )
    for row in rows:
        await _status(conn, row["session_id"])
        await notify(conn, "message_queued", {"session_id": str(row["session_id"])})


# ---------------------------------------------------------------- checks


async def enqueue_check(conn: psycopg.AsyncConnection, session_id: uuid.UUID, command: str) -> int:
    async with conn.transaction():
        row = await _one(
            conn,
            "INSERT INTO checks (session_id, command) VALUES (%s, %s) RETURNING id",
            (session_id, command),
        )
        assert row is not None
        await _status(conn, session_id)
        return row["id"]


async def claim_check(conn: psycopg.AsyncConnection) -> Check | None:
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE checks SET status = 'running', started_at = now()"
            " WHERE id = (SELECT id FROM checks WHERE status = 'queued'"
            "  ORDER BY id LIMIT 1 FOR UPDATE SKIP LOCKED)"
            " AND status = 'queued'"
            " RETURNING id, session_id, command, status, job_name, created_at, started_at",
        )
        if row is None:
            return None
        await _status(conn, row["session_id"])
        return Check(**row)


async def list_running_checks(conn: psycopg.AsyncConnection) -> list[Check]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, session_id, command, status, job_name, created_at, started_at"
            " FROM checks WHERE status = 'running' ORDER BY id"
        )
        return [Check(**r) for r in await cur.fetchall()]


async def set_check_job(conn: psycopg.AsyncConnection, id: int, job_name: str) -> bool:
    async with conn.transaction():
        cur = await conn.execute(
            "UPDATE checks SET job_name = %s WHERE id = %s AND status = 'running'",
            (job_name, id),
        )
        return cur.rowcount == 1


async def complete_check(
    conn: psycopg.AsyncConnection,
    id: int,
    status: str,
    exit_code: int | None,
    log_tail: str | None,
) -> bool:
    if status not in CHECK_FINAL:
        raise ValueError(f"not a final check status: {status!r}")
    row = await _transition(
        conn,
        "UPDATE checks SET status = %(status)s, exit_code = %(exit_code)s,"
        " log_tail = %(log_tail)s, finished_at = now()"
        " WHERE id = %(id)s AND status = 'running' RETURNING session_id",
        {"id": id, "status": status, "exit_code": exit_code, "log_tail": log_tail},
    )
    return row is not None


async def cancel_checks(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> int:
    """Cancel a session's queued checks (a discard); running ones end with their Job."""
    async with conn.transaction():
        cur = await conn.execute(
            "UPDATE checks SET status = 'cancelled', finished_at = now()"
            " WHERE session_id = %s AND status = 'queued'",
            (session_id,),
        )
        if cur.rowcount:
            await _status(conn, session_id)
        return cur.rowcount
