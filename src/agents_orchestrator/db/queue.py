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
# Per wrapper command id: serialises the two writers of a command message's turn link (D10).
_COMMAND_LINK_LOCK_NAMESPACE = 0x6F726332  # "orc2"
_SCAN_BATCH = 100


@dataclass(frozen=True)
class Claim:
    message_id: int
    session_id: uuid.UUID
    text: str
    kind: str
    attempts: int
    target_agent_id: uuid.UUID | None
    agent_id: uuid.UUID
    agent_url: str | None
    wrapper_session_id: str
    free_since: datetime | None
    dispatched_at: datetime
    previous_dispatched_at: datetime | None
    sender_slug: str | None


@dataclass(frozen=True)
class Pick:
    agent_id: uuid.UUID
    url: str | None
    wrapper_session_id: str
    free_since: datetime | None
    last_dispatched_at: datetime | None


@dataclass(frozen=True)
class Verifying:
    message_id: int
    session_id: uuid.UUID
    text: str
    kind: str
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
    kind: str = "turn",
    sender_session_id: uuid.UUID | None = None,
    sender_slug: str | None = None,
    hop: int = 0,
) -> int:
    """Queue a message of `kind` 'turn' or 'command' (D1); a session message carries its sender
    and hop (D8).

    A message with a sender is untargeted, so the leader routing delivers it to whoever leads
    the target session at claim time.
    """
    async with conn.transaction():
        row = await _one(
            conn,
            "INSERT INTO messages (session_id, text, target_agent_id, kind, sender_session_id,"
            " sender_slug, hop) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (session_id, text, target_agent_id, kind, sender_session_id, sender_slug, hop),
        )
        assert row is not None
        await notify(conn, "message_queued", {"session_id": str(session_id)})
        await _status(conn, session_id)
        return row["id"]


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

# A command message (D10) goes to the wrapper's command queue, which takes it busy or idle: no
# `state = 'idle'` and no `free_since` predicate, no lock (it writes no reservation). Untargeted
# with no leader, an idle agent is preferred, then the one free the longest (the leadership
# pass's free-longest order).
_PICK_COMMAND = """
SELECT a.id AS agent_id, a.url, a.wrapper_session_id, s.free_since, a.last_dispatched_at
FROM agents a JOIN agent_status s ON s.agent_id = a.id
WHERE a.session_id = %(session_id)s
  AND a.ready
  AND a.gone_at IS NULL
  AND a.wrapper_session_id IS NOT NULL
  AND (%(target)s::uuid IS NULL OR a.id = %(target)s::uuid)
ORDER BY (s.state = 'idle') IS NOT TRUE, s.free_since ASC NULLS LAST, a.id
LIMIT 1
"""


async def _pick_agent(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    target_agent_id: uuid.UUID | None,
    *,
    command: bool = False,
) -> Pick | None:
    """For a turn, the agent free the longest whose free_since is newer than its last dispatch,
    locked; for a command, any ready agent with a wrapper session (`_PICK_COMMAND`).

    Reading `agents.last_dispatched_at` (a column the turn claim writes) is what makes a
    concurrent READ COMMITTED claim recheck the predicate on the new row version instead of
    trusting its snapshot (D23 c).
    """
    query = _PICK_COMMAND if command else _PICK
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


async def _command_in_flight(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> int | None:
    """Id of the session's oldest `dispatching` command message, if any."""
    row = await _one(
        conn,
        "SELECT min(id) AS id FROM messages"
        " WHERE session_id = %s AND kind = 'command' AND status = 'dispatching'",
        (session_id,),
    )
    return row["id"] if row is not None else None


async def _claim_in_session(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Claim | None:
    """Scan one session's queued messages in id order; the caller holds the session lock.

    Untargeted messages are held two ways. `held` (an untargeted head in backoff, a gone leader,
    a leader that cannot take anything) holds every later untargeted message. `prose_held` (a
    busy leader, a pending handover) holds untargeted prose only, and `prose_stopped` (no free
    agent and no leader) holds all later prose: a command overtakes both, since the wrapper
    queues it busy or idle (D10).
    """
    leader = await _one(
        conn, "SELECT agent_id FROM session_leaders WHERE session_id = %s", (session_id,)
    )
    leader_id = leader["agent_id"] if leader is not None else None
    # Per-session order (D10): nothing after a command still in flight is claimed, so the
    # wrapper receives a session's commands, and a command and later prose, in id order.
    fence = await _command_in_flight(conn, session_id)
    held = False
    # An untargeted head waits on the leader: later untargeted messages wait too. A leader whose
    # handover is pending and possible holds them from the start (D16): the pass moves it first.
    prose_held = leader_id is not None and await _handover_pending(conn, session_id)
    prose_stopped = False
    after = 0
    while True:
        rows = await _all(
            conn,
            "SELECT id, target_agent_id, kind,"
            " (not_before IS NOT NULL AND not_before > now()) AS waiting"
            " FROM messages WHERE session_id = %s AND status = 'queued' AND id > %s"
            " ORDER BY id LIMIT %s",
            (session_id, after, _SCAN_BATCH),
        )
        if not rows:
            return None
        for row in rows:
            after = row["id"]
            if fence is not None and row["id"] > fence:
                return None
            command = row["kind"] == "command"
            target = row["target_agent_id"]
            if target is None and row["waiting"]:
                if leader_id is None:
                    return None  # an untargeted head in backoff holds its session (FIFO, D4)
                held = True  # leader routing: hold untargeted, targeted messages still flow
                continue
            if prose_stopped and not command:
                continue
            if target is None:
                if leader_id is not None:
                    # Leader-only routing: an untargeted message is the leader's. A gone leader
                    # does not fail it; it waits for the leadership pass to elect a successor.
                    if held or (prose_held and not command):
                        continue
                    if await _target_gone(conn, session_id, leader_id):
                        held = True
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
            pick = await _pick_agent(conn, session_id, target, command=command)
            if pick is None:
                if row["target_agent_id"] is None:
                    if command:
                        if leader_id is None:
                            return None  # no ready agent in this session: leave it queued
                        held = True  # the leader cannot take a command either
                    elif leader_id is None:
                        prose_stopped = True  # no free agent: prose waits, commands still flow
                    else:
                        prose_held = True  # the leader is busy: untargeted prose holds
                continue  # a busy or booting target does not block later messages (D23 d)
            return await _dispatch(conn, row["id"], pick, command=command)


async def _dispatch(
    conn: psycopg.AsyncConnection, message_id: int, pick: Pick, *, command: bool
) -> Claim:
    # A command takes no turn slot: it neither needs nor writes the reservation (D10).
    if not command:
        await conn.execute(
            "UPDATE agents SET last_dispatched_at = now() WHERE id = %s", (pick.agent_id,)
        )
    msg = await _one(
        conn,
        "UPDATE messages SET status = 'dispatching', agent_id = %s, dispatched_at = now(),"
        " heartbeat_at = now() WHERE id = %s AND status = 'queued'"
        " RETURNING id, session_id, text, kind, attempts, target_agent_id, dispatched_at,"
        " sender_slug",
        (pick.agent_id, message_id),
    )
    assert msg is not None  # the row is locked and was queued
    await _status(conn, msg["session_id"])
    return Claim(
        message_id=msg["id"],
        session_id=msg["session_id"],
        text=msg["text"],
        kind=msg["kind"],
        attempts=msg["attempts"],
        target_agent_id=msg["target_agent_id"],
        agent_id=pick.agent_id,
        agent_url=pick.url,
        wrapper_session_id=pick.wrapper_session_id,
        free_since=pick.free_since,
        dispatched_at=msg["dispatched_at"],
        previous_dispatched_at=pick.last_dispatched_at,
        sender_slug=msg["sender_slug"],
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


async def mark_sent_command(
    conn: psycopg.AsyncConnection, id: int, wrapper_command_id: str
) -> bool:
    """202 on a command message: the wrapper queued it as `wrapper_command_id` (D10).

    When the ingester already stored this command's `command_completed` with `result = 'turn'`
    (the event beat this 202), the same guarded update links the promoted turn (D10 hop guard).
    The command's link lock is taken in its own statement first, so the UPDATE's snapshot sees
    an event whose ingester transaction committed while this one waited for the lock.
    """
    async with conn.transaction():
        await _lock_command_link(conn, wrapper_command_id)
        row = await _transition(
            conn,
            "UPDATE messages SET status = 'sent', wrapper_command_id = %(cmd)s, error = NULL,"
            " turn_id = (SELECT NULLIF(e.data -> 'record' ->> 'turn_id', '') FROM events e"
            " WHERE e.agent_id = messages.agent_id AND e.type = 'command_completed'"
            " AND e.data -> 'record' ->> 'id' = %(cmd)s"
            " AND e.data -> 'record' ->> 'result' = 'turn' ORDER BY e.id LIMIT 1)"
            " WHERE id = %(id)s AND status = 'dispatching' AND kind = 'command'"
            " RETURNING session_id",
            {"id": id, "cmd": wrapper_command_id},
        )
    return row is not None


async def _lock_command_link(conn: psycopg.AsyncConnection, wrapper_command_id: str) -> None:
    """Until the transaction ends, hold the lock both writers of a command's turn link take
    first (D10 hop guard): whichever commits second sees the other's write."""
    await conn.execute(
        "SELECT pg_advisory_xact_lock(%s, hashtext(%s::text))",
        (_COMMAND_LINK_LOCK_NAMESPACE, wrapper_command_id),
    )


async def link_command_turn(
    conn: psycopg.AsyncConnection, wrapper_command_id: str, turn_id: str
) -> bool:
    """The one writer of "sent command, no turn -> linked turn" (D10 hop guard): a command that
    the wrapper promoted to a turn keeps that turn id on its message, so `turn_hop` finds it and
    a send from the promoted turn counts the command's hop plus one. Never overwrites. Takes
    the command's link lock first, so a `mark_sent_command` in flight commits before this
    UPDATE reads the row."""
    async with conn.transaction():
        await _lock_command_link(conn, wrapper_command_id)
        row = await _transition(
            conn,
            "UPDATE messages SET turn_id = %(turn_id)s"
            " WHERE kind = 'command' AND status = 'sent' AND wrapper_command_id = %(cmd)s"
            " AND turn_id IS NULL RETURNING session_id",
            {"turn_id": turn_id, "cmd": wrapper_command_id},
        )
    return row is not None


async def requeue_later(conn: psycopg.AsyncConnection, id: int, seconds: float) -> bool:
    """Not now (a booting or dead wrapper, a full command queue): back to queued with
    `not_before = now() + seconds`, no attempt counted (D10)."""
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'queued', heartbeat_at = NULL,"
        " not_before = now() + make_interval(secs => %(secs)s::float8)"
        " WHERE id = %(id)s AND status = 'dispatching' RETURNING session_id",
        {"id": id, "secs": seconds},
    )
    return row is not None


async def requeue(conn: psycopg.AsyncConnection, id: int) -> bool:
    """409 busy: back to queued at once, no attempt counted, no backoff."""
    row = await _transition(
        conn,
        "UPDATE messages SET status = 'queued', not_before = NULL, heartbeat_at = NULL"
        " WHERE id = %(id)s AND status = 'dispatching' RETURNING session_id",
        {"id": id},
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
        "SELECT m.id AS message_id, m.session_id, m.text, m.kind, m.attempts, m.error,"
        " m.agent_id,"
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


async def enqueue_command(conn: psycopg.AsyncConnection, agent_id: uuid.UUID, kind: str) -> int:
    if kind not in COMMAND_KINDS:
        raise ValueError(f"unknown command kind {kind!r}")
    async with conn.transaction():
        row = await _one(
            conn,
            "INSERT INTO commands (agent_id, kind) VALUES (%s, %s) RETURNING id",
            (agent_id, kind),
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
        cur = await conn.execute(
            "UPDATE commands SET status = %s, result = %s, finished_at = now()"
            " WHERE id = %s AND status = 'running'",
            (status, Jsonb(result) if result is not None else None, id),
        )
        return cur.rowcount == 1


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
