"""Leadership queue and the leadership pass (D7, D19, D21, D27).

Besides elections, queued requests and the lost-leader fallback, the pass hands a leader over
by itself in two cases, always on for hives: its pod is about to be removed by a scale-down
(`scale_down`, at once) and it compacted (`compaction`, at its turn boundary). A queued
`rollout` handover is likewise applied only at the leader's turn boundary; mid-turn it stays
queued. Every pick is restricted to agents that stay (`ordinal < spec.replicas`). A dead
agent (`agent_status.state`) never leads: it is not eligible, and a leader whose agent is dead
is a lost leader (D4).

The api and the reconciler only insert `leadership_requests` rows (status `queued`) and read the
leader record: the api the agents' handovers and the user overrides, the reconciler one
`handover` with reason `rollout` from an idle leader whose pod a rollout replaces next
(`reconciler/rollout.py`).
`session_leaders` and every request decision belong to the dispatcher, through
`leadership_pass`, which runs under the per-session claim lock of `db/queue.py` so a leader
change and a message claim never interleave.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator.db.eligibility import (
    COMPACTED,
    DEAD,
    ELIGIBLE,
    HANDOVER_TRIGGER,
    LEAVING,
    READY_STAYS,
)
from agents_orchestrator.db.pool import notify
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE

CHANNEL = "leadership_requested"
LEADER_CHANGED_CHANNEL = "leader_changed"
# A leader not ready for longer than this is replaced (D14, a module constant by design).
LEADER_GRACE_SECONDS = 30


@dataclass(frozen=True)
class Leader:
    agent_id: uuid.UUID
    pod_name: str
    epoch: int


async def insert_request(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    kind: str,
    from_agent_id: uuid.UUID | None,
    to_agent_id: uuid.UUID | None,
    reason: str,
    epoch: int | None,
) -> int:
    """Queue a request and NOTIFY `leadership_requested {session_id}`; returns its id."""
    async with conn.transaction():
        cur = await conn.execute(
            "INSERT INTO leadership_requests"
            " (session_id, kind, from_agent_id, to_agent_id, reason, epoch)"
            " VALUES (%s, %s, %s, %s, %s, %s) RETURNING id",
            (session_id, kind, from_agent_id, to_agent_id, reason, epoch),
        )
        row = await cur.fetchone()
        assert row is not None
        await notify(conn, CHANNEL, {"session_id": str(session_id)})
        return row[0]


async def current_leader(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Leader | None:
    cur = await conn.execute(
        "SELECT l.agent_id, a.pod_name, l.epoch FROM session_leaders l"
        " JOIN agents a ON a.id = l.agent_id WHERE l.session_id = %s",
        (session_id,),
    )
    row = await cur.fetchone()
    return Leader(agent_id=row[0], pod_name=row[1], epoch=row[2]) if row else None


# ---------------------------------------------------------------- the pass (dispatcher side)


@dataclass(frozen=True)
class LeaderChange:
    session_id: uuid.UUID
    agent_id: uuid.UUID
    epoch: int
    reason: str


_HIVE = "coalesce(c.spec->>'max_replicas', c.spec->>'replicas')::int > 1"

# Hive sessions whose leadership needs a pass: queued requests, no leader row while an agent
# could lead, a leader that is gone, not ready or dead, an unready mark to clear,
# a leader that compacted in its term, or a leader whose ordinal a scale-down removes.
_CANDIDATES = f"""
SELECT c.id FROM sessions c
LEFT JOIN session_leaders l ON l.session_id = c.id
LEFT JOIN agents a ON a.id = l.agent_id
WHERE c.desired = 'active' AND {_HIVE} AND (
    EXISTS (SELECT 1 FROM leadership_requests r
            WHERE r.session_id = c.id AND r.status = 'queued')
    OR (l.session_id IS NULL AND EXISTS (
        SELECT 1 FROM agents a WHERE a.session_id = c.id AND {ELIGIBLE}))
    OR a.gone_at IS NOT NULL OR NOT a.ready OR {DEAD} OR l.unready_since IS NOT NULL
    OR {HANDOVER_TRIGGER}
)
ORDER BY c.id
"""

# The ready agent free the longest (the _PICK order, free_since asc then id), idle agents
# first, so a hive whose other members are all busy still gets a leader.
_FREE_LONGEST = f"""
SELECT a.id FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id
WHERE a.session_id = %s AND {ELIGIBLE} AND a.id <> %s
ORDER BY (s.state = 'idle') IS NOT TRUE, s.free_since ASC NULLS LAST, a.id
LIMIT 1
"""


async def _one(conn: psycopg.AsyncConnection, query: str, params: Any = ()) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchone()


async def _all(conn: psycopg.AsyncConnection, query: str, params: Any = ()) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchall()


async def leadership_candidates(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """Hive sessions the dispatcher runs `leadership_pass` for on this wake (one query)."""
    return [r["id"] for r in await _all(conn, _CANDIDATES)]


async def hive_sessions(conn: psycopg.AsyncConnection) -> list[dict]:
    """Active hive sessions that have a leader row: id, slug, spec (the push loop's set)."""
    return await _all(
        conn,
        "SELECT c.id, c.slug, c.spec FROM sessions c"
        " JOIN session_leaders l ON l.session_id = c.id"
        f" WHERE c.desired = 'active' AND {_HIVE} ORDER BY c.id",
    )


async def _leader(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> dict | None:
    return await _one(
        conn,
        "SELECT l.agent_id, l.epoch, l.unready_since,"
        " EXISTS (SELECT 1 FROM agent_status s WHERE s.agent_id = l.agent_id"
        "   AND s.state = 'idle')"
        " AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.agent_id = l.agent_id"
        "   AND m.status IN ('dispatching', 'verifying')) AS at_boundary,"
        " l.unready_since IS NOT NULL AND l.unready_since"
        "   < now() - make_interval(secs => %s) AS grace_over,"
        " a.ready, a.gone_at IS NOT NULL AS gone,"
        f" {DEAD} AS dead,"
        f" {LEAVING} AS leaving,"
        f" {COMPACTED} AS compacted"
        " FROM session_leaders l JOIN agents a ON a.id = l.agent_id"
        " JOIN sessions c ON c.id = l.session_id"
        " WHERE l.session_id = %s FOR UPDATE OF l",
        (LEADER_GRACE_SECONDS, session_id),
    )


async def _free_longest(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, other_than: uuid.UUID
) -> uuid.UUID | None:
    row = await _one(conn, _FREE_LONGEST, (session_id, other_than))
    return row["id"] if row else None


async def _target_ready(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID | None
) -> bool:
    if agent_id is None:
        return False
    row = await _one(
        conn,
        f"SELECT 1 AS ok FROM agents a WHERE a.id = %s AND a.session_id = %s AND {READY_STAYS}",
        (agent_id, session_id),
    )
    return row is not None


async def _set_leader(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    reason: str,
) -> LeaderChange:
    """Insert the first leader row (epoch 1) or move the existing one to `epoch + 1`.

    Every change NOTIFYs `leader_changed`.
    """
    row = await _one(
        conn,
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
        " VALUES (%(c)s, %(a)s, 1, now(), %(r)s)"
        " ON CONFLICT (session_id) DO UPDATE SET agent_id = EXCLUDED.agent_id,"
        "  epoch = session_leaders.epoch + 1, since = now(), reason = EXCLUDED.reason,"
        "  unready_since = NULL"
        " RETURNING epoch",
        {"c": session_id, "a": agent_id, "r": reason},
    )
    assert row is not None
    change = LeaderChange(session_id, agent_id, row["epoch"], reason)
    await notify(
        conn,
        LEADER_CHANGED_CHANNEL,
        {
            "session_id": str(session_id),
            "agent_id": str(agent_id),
            "epoch": change.epoch,
            "reason": reason,
        },
    )
    return change


async def _decide(
    conn: psycopg.AsyncConnection, request_id: int, reject_reason: str | None
) -> bool:
    """Decide one queued request (status-guarded); False when it was no longer queued."""
    cur = await conn.execute(
        "UPDATE leadership_requests SET status = %s, reject_reason = %s, decided_at = now()"
        " WHERE id = %s AND status = 'queued'",
        ("rejected" if reject_reason else "applied", reject_reason, request_id),
    )
    return cur.rowcount == 1


async def _judge(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, req: dict, leader: dict | None
) -> tuple[str | None, uuid.UUID | None, str]:
    """(reject reason or None, new leader or None for no change, change reason)."""
    current = leader["agent_id"] if leader else None
    if req["kind"] == "user":
        # Q10 override: no epoch fence, a target that already leads is applied unchanged.
        if not await _target_ready(conn, session_id, req["to_agent_id"]):
            return "target_not_ready", None, ""
        target = req["to_agent_id"]
        return None, (None if target == current else target), "user"
    if leader is None or req["from_agent_id"] != current:
        return "not_leader", None, ""
    if req["epoch"] != leader["epoch"]:
        return "stale_epoch", None, ""
    reason = f"handover: {req['reason']}"
    if req["to_agent_id"] is not None:
        target = req["to_agent_id"]
        if target == current or not await _target_ready(conn, session_id, target):
            return "target_not_ready", None, ""
        return None, target, reason
    picked = await _free_longest(conn, session_id, current)
    if picked is None:
        return "no_peers", None, ""
    return None, picked, reason


def _mid_turn_rollout(req: dict, leader: dict | None) -> bool:
    """A `rollout` handover from the current leader while that leader is not at a boundary."""
    return (
        req["kind"] == "handover"
        and req["reason"] == "rollout"
        and leader is not None
        and req["from_agent_id"] == leader["agent_id"]
        and req["epoch"] == leader["epoch"]
        and not leader["at_boundary"]
    )


async def leadership_pass(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID
) -> LeaderChange | None:
    """Elect, apply queued requests, hand over (scale-down, lost, compaction): one lock.

    Returns the last change of this pass, or None when nothing changed, the session is not a
    hive, or another dispatcher holds the session's claim lock (the pass is skipped).
    """
    async with conn.transaction():
        lock = await _one(
            conn,
            "SELECT pg_try_advisory_xact_lock(%s, hashtext(%s::text)) AS got",
            (_CLAIM_LOCK_NAMESPACE, session_id),
        )
        if not (lock and lock["got"]):
            return None
        hive = await _one(
            conn, f"SELECT {_HIVE} AS hive FROM sessions c WHERE c.id = %s", (session_id,)
        )
        if not (hive and hive["hive"]):
            return None
        change: LeaderChange | None = None

        # 1. Initial leader (Q2): the lowest-ordinal ready agent with a session.
        leader = await _leader(conn, session_id)
        if leader is None:
            first = await _one(
                conn,
                f"SELECT a.id FROM agents a WHERE a.session_id = %s AND {ELIGIBLE}"
                " ORDER BY a.ordinal LIMIT 1",
                (session_id,),
            )
            if first is not None:
                change = await _set_leader(conn, session_id, first["id"], "initial")
                leader = await _leader(conn, session_id)

        # 2. Queued requests in id order, each re-checked against the leader as it is now. A
        # `rollout` handover from the current leader waits for its turn boundary, like the
        # compaction handover: while the leader is mid-turn it stays queued for a later pass.
        for req in await _all(
            conn,
            "SELECT id, kind, from_agent_id, to_agent_id, reason, epoch"
            " FROM leadership_requests WHERE session_id = %s AND status = 'queued'"
            " ORDER BY id FOR UPDATE",
            (session_id,),
        ):
            if _mid_turn_rollout(req, leader):
                continue
            reject, target, reason = await _judge(conn, session_id, req, leader)
            if not await _decide(conn, req["id"], reject):
                continue
            if target is not None:
                change = await _set_leader(conn, session_id, target, reason)
                leader = await _leader(conn, session_id)

        # 3. Scale-down: a leader whose ordinal a scale-down removes moves at once (no turn
        # boundary: a leaving agent is already out of routing), to the staying agent free the
        # longest. A leaving leader the reconciler already marked unready but not gone is
        # handled here, before step 4 can see it, so its reason is `scale_down`. With no
        # staying agent the leader stays and the next pass retries.
        if leader is not None and leader["leaving"] and not leader["gone"]:
            picked = await _free_longest(conn, session_id, leader["agent_id"])
            if picked is not None:
                change = await _set_leader(conn, session_id, picked, "scale_down")
                leader = await _leader(conn, session_id)

        # 4. Lost leader: gone, or not ready or dead past the grace (unready_since set on first
        # sight and cleared once live again, so separate short windows never add up).
        if leader is not None:
            if leader["ready"] and not leader["gone"] and not leader["dead"]:
                if leader["unready_since"] is not None:
                    await conn.execute(
                        "UPDATE session_leaders SET unready_since = NULL WHERE session_id = %s",
                        (session_id,),
                    )
            elif leader["gone"] or leader["grace_over"]:
                picked = await _free_longest(conn, session_id, leader["agent_id"])
                if picked is not None:
                    change = await _set_leader(conn, session_id, picked, "leader_lost")
                    leader = await _leader(conn, session_id)
            elif leader["unready_since"] is None:
                await conn.execute(
                    "UPDATE session_leaders SET unready_since = now() WHERE session_id = %s",
                    (session_id,),
                )

        # 5. Compaction: a leader that compacted in its term (`compaction_at` after `since`)
        # hands over at its turn boundary, never mid-turn. With no eligible
        # agent the leader stays and the move is retried for the rest of its term; any leader
        # change starts a clean term.
        if (
            leader is not None
            and leader["compacted"]
            and leader["ready"]
            and not leader["gone"]
            and leader["at_boundary"]
        ):
            picked = await _free_longest(conn, session_id, leader["agent_id"])
            if picked is not None:
                change = await _set_leader(conn, session_id, picked, "compaction")
                leader = await _leader(conn, session_id)
        return change
