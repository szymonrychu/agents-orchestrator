"""Leadership queue and the leadership pass (D7, D19, D21, D27).

Besides elections, queued requests and the lost-leader fallback, the pass hands a leader over
by itself when its pod is about to be removed by a scale-down (`scale_down`, at once). A
compaction is only an advisory: a compacted leader keeps leading. A queued `rollout` handover is
likewise applied only at the leader's turn boundary; mid-turn it stays queued. Every handover
has a deadline (`PassPolicy`): past it background work no longer holds it, and a voluntary one
the leader's turn still holds is rejected `expired`. Every pick is restricted to agents that
stay (`ordinal < spec.replicas`). A dead agent (`agent_status.state`) never leads: it is not
eligible, and a leader whose agent is dead is a lost leader (D4).

The api and the reconciler only insert `leadership_requests` rows (status `queued`) and read the
leader record: the api the agents' handovers and the user overrides, the reconciler one
`handover` with reason `rollout` from an idle leader whose pod a rollout replaces next
(`reconciler/rollout.py`). The one exception is `withdraw_handovers`: the api rejects a still
queued voluntary `handover` (never a `rollout` one) with reason `withdrawn`, through the
same status-guarded update the pass decides with, so a request is decided exactly once by
whichever commits first.
`session_leaders` and every other request decision belong to the dispatcher, through
`leadership_pass`, which runs under the per-session claim lock of `db/queue.py` so a leader
change and a message claim never interleave. A voluntary handover (an agent `handover` other
than `rollout`) also queues a `clear` command for the ex-leader in the same transaction (Q13);
forced moves and user overrides never do.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator.db.eligibility import (
    BACKGROUND_BUSY,
    DEAD,
    ELIGIBLE,
    HANDOVER_TRIGGER,
    HEADROOM,
    LEAVING,
    READY_STAYS,
    SUCCESSOR_ORDER,
)
from agents_orchestrator.db.pool import notify
from agents_orchestrator.db.queue import _CLAIM_LOCK_NAMESPACE, enqueue_command

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


async def withdraw_handovers(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, request_id: int | None = None
) -> list[int]:
    """Reject the session's still queued voluntary `handover` requests (only `request_id` when
    given) with reason `withdrawn`; the ids withdrawn, ascending. The reconciler's `rollout`
    request is never withdrawn: a re-queue would restart its cap, so a leader could defer the
    rollout forever. Status-guarded like the pass's decisions, so a request the pass decided
    first is left alone."""
    cur = await conn.execute(
        "UPDATE leadership_requests SET status = 'rejected', reject_reason = 'withdrawn',"
        " decided_at = now() WHERE session_id = %s AND kind = 'handover' AND status = 'queued'"
        " AND reason IS DISTINCT FROM 'rollout' AND (%s::bigint IS NULL OR id = %s) RETURNING id",
        (session_id, request_id, request_id),
    )
    return sorted(r[0] for r in await cur.fetchall())


async def handover_reason(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, request_id: int
) -> str | None:
    """The reason of the session's `handover` request with this id, whatever its status; None
    when there is no such request."""
    cur = await conn.execute(
        "SELECT reason FROM leadership_requests WHERE id = %s AND session_id = %s"
        " AND kind = 'handover'",
        (request_id, session_id),
    )
    row = await cur.fetchone()
    return None if row is None else row[0]


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
    previous_agent_id: uuid.UUID | None


@dataclass(frozen=True)
class PassPolicy:
    """The settings the pass reads: the pods' default compaction point (the successor
    threshold of a spec without `auto_compact_pct`) and the deadlines of queued handovers: the
    rollout deferral cap for the reconciler's `rollout` request, the max wait for any other."""

    default_auto_compact_pct: int = 95
    rollout_max_defer_seconds: int = 14400
    handover_max_wait_seconds: int = 1800


DEFAULT_POLICY = PassPolicy()


HIVE = "coalesce(c.spec->>'max_replicas', c.spec->>'replicas')::int > 1"
# A session that is not a hive (neither field set counts as one agent).
SOLO = "coalesce(coalesce(c.spec->>'max_replicas', c.spec->>'replicas')::int, 1) <= 1"

# Hive sessions whose leadership needs a pass: queued requests, no leader row while an agent
# could lead, a leader that is gone, not ready or dead, an unready mark to clear,
# or a leader whose ordinal a scale-down removes.
_CANDIDATES = f"""
SELECT c.id FROM sessions c
LEFT JOIN session_leaders l ON l.session_id = c.id
LEFT JOIN agents a ON a.id = l.agent_id
WHERE c.desired = 'active' AND {HIVE} AND (
    EXISTS (SELECT 1 FROM leadership_requests r
            WHERE r.session_id = c.id AND r.status = 'queued')
    OR (l.session_id IS NULL AND EXISTS (
        SELECT 1 FROM agents a WHERE a.session_id = c.id AND {ELIGIBLE}))
    OR a.gone_at IS NOT NULL OR NOT a.ready OR {DEAD} OR l.unready_since IS NOT NULL
    OR {HANDOVER_TRIGGER}
)
ORDER BY c.id
"""

# The successor (eligibility.SUCCESSOR_ORDER) for every pick that names nobody: agents with
# headroom (eligibility.HEADROOM) first, so a forced pick falls back to the best agent without it
# only when every peer lacks it. Any idle state is acceptable: a hive whose other members are all
# busy still gets a leader.
_SUCCESSOR = f"""
SELECT a.id FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id
JOIN sessions c ON c.id = a.session_id
WHERE a.session_id = %(session)s AND {ELIGIBLE} AND a.id <> %(other)s
ORDER BY NOT {HEADROOM}, {SUCCESSOR_ORDER}
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
    """Active hive sessions that have a leader row: id, slug, spec, status and the pins (the
    push loop's set)."""
    return await _all(
        conn,
        "SELECT c.id, c.slug, c.spec, c.status, c.secret_versions FROM sessions c"
        " JOIN session_leaders l ON l.session_id = c.id"
        f" WHERE c.desired = 'active' AND {HIVE} ORDER BY c.id",
    )


async def _leader(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> dict | None:
    return await _one(
        conn,
        "SELECT l.agent_id, l.epoch, l.unready_since,"
        " EXISTS (SELECT 1 FROM agent_status s WHERE s.agent_id = l.agent_id"
        "   AND s.state = 'idle')"
        " AND NOT EXISTS (SELECT 1 FROM messages m WHERE m.agent_id = l.agent_id"
        "   AND m.status IN ('dispatching', 'verifying')) AS at_boundary,"
        f" {BACKGROUND_BUSY} AS background_busy,"
        " l.unready_since IS NOT NULL AND l.unready_since"
        "   < now() - make_interval(secs => %s) AS grace_over,"
        " a.ready, a.gone_at IS NOT NULL AS gone,"
        f" {DEAD} AS dead,"
        f" {LEAVING} AS leaving"
        " FROM session_leaders l JOIN agents a ON a.id = l.agent_id"
        " JOIN sessions c ON c.id = l.session_id"
        " LEFT JOIN agent_status s ON s.agent_id = l.agent_id"
        " WHERE l.session_id = %s FOR UPDATE OF l",
        (LEADER_GRACE_SECONDS, session_id),
    )


async def successor(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    other_than: uuid.UUID,
    policy: PassPolicy,
) -> uuid.UUID | None:
    row = await _one(
        conn,
        _SUCCESSOR,
        {
            "session": session_id,
            "other": other_than,
            "compact": policy.default_auto_compact_pct,
        },
    )
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
    previous: uuid.UUID | None,
) -> LeaderChange:
    """Insert the first leader row (epoch 1) or move the existing one to `epoch + 1`.

    `previous` is the leader being replaced (None for `initial`), kept in `previous_agent_id`.
    Every change NOTIFYs `leader_changed`.
    """
    row = await _one(
        conn,
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason,"
        " previous_agent_id, start_context_tokens) VALUES (%(c)s, %(a)s, 1, now(), %(r)s, %(p)s,"
        "  (SELECT context_used_tokens FROM agent_status WHERE agent_id = %(a)s))"
        " ON CONFLICT (session_id) DO UPDATE SET agent_id = EXCLUDED.agent_id,"
        "  epoch = session_leaders.epoch + 1, since = now(), reason = EXCLUDED.reason,"
        "  unready_since = NULL, previous_agent_id = EXCLUDED.previous_agent_id,"
        "  previous_since = session_leaders.since,"
        "  start_context_tokens = EXCLUDED.start_context_tokens"
        " RETURNING epoch",
        {"c": session_id, "a": agent_id, "r": reason, "p": previous},
    )
    assert row is not None
    change = LeaderChange(session_id, agent_id, row["epoch"], reason, previous)
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
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    req: dict,
    leader: dict | None,
    policy: PassPolicy,
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
    picked = await successor(conn, session_id, current, policy)
    if picked is None:
        return "no_peers", None, ""
    return None, picked, reason


def _waits_for_boundary(req: dict, leader: dict | None) -> bool:
    """A `handover` from the current leader at the current epoch waits for the leader's true
    boundary: its turn ended and no background subagent running. A handover past its deadline
    (`overdue`: the deferral cap for `rollout`, the max wait for any other) stops waiting for
    background work, never for the turn. A `user` request never waits."""
    if not (
        req["kind"] == "handover"
        and leader is not None
        and req["from_agent_id"] == leader["agent_id"]
        and req["epoch"] == leader["epoch"]
    ):
        return False
    if not leader["at_boundary"]:
        return True
    return bool(leader["background_busy"]) and not req["overdue"]


def _expired(req: dict) -> bool:
    """A voluntary handover still waiting past its deadline (only the leader's turn can hold it
    then) is rejected `expired`. The reconciler's `rollout` request never expires: it waits for
    the turn, which no deadline interrupts."""
    return _voluntary(req) and req["overdue"]


def _voluntary(req: dict) -> bool:
    """The leader chose to hand over: a `handover` other than the reconciler's `rollout`."""
    return req["kind"] == "handover" and req["reason"] != "rollout"


async def leadership_pass(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, policy: PassPolicy = DEFAULT_POLICY
) -> LeaderChange | None:
    """Elect, apply queued requests, hand over (scale-down, lost): one lock.

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
            conn, f"SELECT {HIVE} AS hive FROM sessions c WHERE c.id = %s", (session_id,)
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
                change = await _set_leader(conn, session_id, first["id"], "initial", None)
                leader = await _leader(conn, session_id)

        # 2. Queued requests in id order, each re-checked against the leader as it is now. A
        # `handover` from the current leader waits for the leader's true boundary (see
        # _waits_for_boundary): until then it stays queued for a later pass, and a voluntary
        # one past its deadline is rejected `expired` instead (_expired).
        for req in await _all(
            conn,
            "SELECT id, kind, from_agent_id, to_agent_id, reason, epoch,"
            " created_at < now() - make_interval(secs => CASE WHEN reason = 'rollout'"
            "  THEN %s ELSE %s END) AS overdue"
            " FROM leadership_requests WHERE session_id = %s AND status = 'queued'"
            " ORDER BY id FOR UPDATE",
            (policy.rollout_max_defer_seconds, policy.handover_max_wait_seconds, session_id),
        ):
            if _waits_for_boundary(req, leader):
                if _expired(req):
                    await _decide(conn, req["id"], "expired")
                continue
            reject, target, reason = await _judge(conn, session_id, req, leader, policy)
            if not await _decide(conn, req["id"], reject):
                continue
            if target is not None:
                previous = leader["agent_id"] if leader else None
                change = await _set_leader(conn, session_id, target, reason, previous)
                leader = await _leader(conn, session_id)
                if previous is not None and _voluntary(req):
                    # Q13: the ex-leader starts clean (`/clear`) at its true boundary.
                    await enqueue_command(conn, previous, "clear")

        # 3. Scale-down: a leader whose ordinal a scale-down removes moves at once (no turn
        # boundary: a leaving agent is already out of routing), to the successor among the
        # staying agents. A leaving leader the reconciler already marked unready but not gone is
        # handled here, before step 4 can see it, so its reason is `scale_down`. With no
        # staying agent the leader stays and the next pass retries.
        if leader is not None and leader["leaving"] and not leader["gone"]:
            picked = await successor(conn, session_id, leader["agent_id"], policy)
            if picked is not None:
                change = await _set_leader(
                    conn, session_id, picked, "scale_down", leader["agent_id"]
                )
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
                picked = await successor(conn, session_id, leader["agent_id"], policy)
                if picked is not None:
                    change = await _set_leader(
                        conn, session_id, picked, "leader_lost", leader["agent_id"]
                    )
                    leader = await _leader(conn, session_id)
            elif leader["unready_since"] is None:
                await conn.execute(
                    "UPDATE session_leaders SET unready_since = now() WHERE session_id = %s",
                    (session_id,),
                )

        return change
