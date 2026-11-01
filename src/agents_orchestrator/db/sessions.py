"""sessions table repository. The api writes; the reconciler uses set_status and purge."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db import leadership
from agents_orchestrator.db.eligibility import ELIGIBLE
from agents_orchestrator.db.pool import notify
from agents_orchestrator.db.queue import SessionBeingDeleted
from agents_orchestrator.spec import SessionSpec, validate_slug

CHANNEL = "session_changed"


class SessionExists(Exception):
    """A session with this slug already has a row."""


class SessionNotFound(Exception):
    pass


class ScaleRefused(Exception):
    """A replica change was refused; `code` is one of session_deleting, not_leader,
    static_session, out_of_range, leader_would_leave (the leader would leave and no ready
    agent that stays can take over)."""

    def __init__(self, code: str, message: str = "") -> None:
        super().__init__(message or code)
        self.code = code


@dataclass(frozen=True)
class ScaleOutcome:
    session_id: uuid.UUID
    replicas: int
    previous_replicas: int
    max_replicas: int


@dataclass(frozen=True)
class Session:
    id: uuid.UUID
    slug: str
    spec: SessionSpec
    generation: int
    observed_generation: int
    desired: str
    status: dict[str, Any]
    created_at: datetime
    preset_id: uuid.UUID | None = None
    preset_name: str | None = None
    created_by_service: str | None = None
    # {ENV_NAME: Secret name} of the protected variables, snapshotted at create
    secret_versions: dict[str, str] = field(default_factory=dict)
    # the superleader session that created it through the admin routes
    created_by_session: uuid.UUID | None = None


def _from_row(row: dict[str, Any]) -> Session:
    return Session(
        id=row["id"],
        slug=row["slug"],
        spec=SessionSpec.model_validate(row["spec"]),
        generation=row["generation"],
        observed_generation=row["observed_generation"],
        desired=row["desired"],
        status=row["status"],
        created_at=row["created_at"],
        preset_id=row["preset_id"],
        preset_name=row["preset_name"],
        created_by_service=row["created_by_service"],
        secret_versions=row["secret_versions"] or {},
        created_by_session=row["created_by_session"],
    )


def _dump(spec: SessionSpec) -> Jsonb:
    return Jsonb(spec.model_dump(mode="json", exclude_none=True))


async def _fetch(conn: psycopg.AsyncConnection, query: str, params: tuple) -> list[Session]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return [_from_row(r) for r in await cur.fetchall()]


async def _notify(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> None:
    await notify(conn, CHANNEL, {"session_id": str(session_id)})


async def create(
    conn: psycopg.AsyncConnection,
    slug: str,
    spec: SessionSpec,
    preset_id: uuid.UUID | None = None,
    preset_name: str | None = None,
    created_by_service: str | None = None,
    secret_versions: dict[str, str] | None = None,
    created_by_session: uuid.UUID | None = None,
) -> Session:
    validate_slug(slug)
    try:
        async with conn.transaction():
            rows = await _fetch(
                conn,
                "INSERT INTO sessions (id, slug, spec, preset_id, preset_name, created_by_service,"
                " secret_versions, created_by_session)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
                (
                    uuid.uuid4(),
                    slug,
                    _dump(spec),
                    preset_id,
                    preset_name,
                    created_by_service,
                    Jsonb(secret_versions) if secret_versions else None,
                    created_by_session,
                ),
            )
            await _notify(conn, rows[0].id)
    except psycopg.errors.UniqueViolation:
        raise SessionExists(slug) from None
    return rows[0]


async def get(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Session | None:
    rows = await _fetch(conn, "SELECT * FROM sessions WHERE id = %s", (session_id,))
    return rows[0] if rows else None


async def get_by_slug(conn: psycopg.AsyncConnection, slug: str) -> Session | None:
    rows = await _fetch(conn, "SELECT * FROM sessions WHERE slug = %s", (slug,))
    return rows[0] if rows else None


@dataclass(frozen=True)
class SelfRestartClaim:
    claimed: bool
    retry_after: int  # whole seconds until the next claim can win; 0 when claimed


async def claim_self_restart(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, cooldown_seconds: int
) -> SelfRestartClaim:
    """Claim the session's self-restart slot: exactly one concurrent caller wins, and none
    again until `cooldown_seconds` have passed since the last winner."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "UPDATE sessions SET last_self_restart_at = now() WHERE id = %(id)s"
            " AND (last_self_restart_at IS NULL"
            "      OR last_self_restart_at <= now() - make_interval(secs => %(cd)s))"
            " RETURNING id",
            {"id": session_id, "cd": cooldown_seconds},
        )
        if await cur.fetchone() is not None:
            return SelfRestartClaim(True, 0)
        await cur.execute(
            "SELECT ceil(extract(epoch FROM last_self_restart_at + make_interval(secs => %(cd)s)"
            " - now())) AS wait FROM sessions WHERE id = %(id)s",
            {"id": session_id, "cd": cooldown_seconds},
        )
        row = await cur.fetchone()
    if row is None:
        raise SessionNotFound(str(session_id))
    return SelfRestartClaim(False, max(1, int(row["wait"])))


async def list_sessions(conn: psycopg.AsyncConnection) -> list[Session]:
    return await _fetch(conn, "SELECT * FROM sessions ORDER BY created_at, slug", ())


@dataclass(frozen=True)
class MessagingPeer:
    slug: str
    preset_name: str | None
    superleader: bool
    leader_present: bool


# Active sessions other than the caller's, with whether a not-gone agent holds the leader row
# and whether any not-gone agent exists; the hive rule picks which one is "leader present".
_PEERS = """
SELECT c.*,
       EXISTS (SELECT 1 FROM session_leaders l JOIN agents a ON a.id = l.agent_id
               WHERE l.session_id = c.id AND a.gone_at IS NULL) AS live_leader,
       EXISTS (SELECT 1 FROM agents a
               WHERE a.session_id = c.id AND a.gone_at IS NULL) AS live_agent
FROM sessions c
WHERE c.desired = 'active' AND c.id <> %s
ORDER BY c.slug
"""


async def list_messaging_peers(
    conn: psycopg.AsyncConnection, caller_session_id: uuid.UUID, *, superleaders_only: bool
) -> list[MessagingPeer]:
    """The sessions the caller may message (D6, D7): every other active session, or only the
    other active superleader sessions, ordered by slug."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_PEERS, (caller_session_id,))
        rows = await cur.fetchall()
    out = []
    for row in rows:
        spec = _from_row(row).spec
        superleader = bool(spec.session_messaging)
        if superleaders_only and not superleader:
            continue
        out.append(
            MessagingPeer(
                slug=row["slug"],
                preset_name=row["preset_name"],
                superleader=superleader,
                leader_present=row["live_leader"] if spec.hive else row["live_agent"],
            )
        )
    return out


async def count_active(conn: psycopg.AsyncConnection) -> int:
    cur = await conn.execute("SELECT count(*) FROM sessions")
    return (await cur.fetchone())[0]


async def request_delete(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Session:
    async with conn.transaction():
        rows = await _fetch(
            conn,
            "UPDATE sessions SET desired = 'deleting', generation = generation + 1"
            " WHERE id = %s AND desired = 'active' RETURNING *",
            (session_id,),
        )
        if rows:
            await _notify(conn, session_id)
            return rows[0]
    existing = await get(conn, session_id)
    if existing is None:
        raise SessionNotFound(str(session_id))
    return existing


async def close_expired(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """Flip active sessions past their autoclose time to deleting (the request_delete write)."""
    async with conn.transaction():
        cur = await conn.execute(
            "UPDATE sessions SET desired = 'deleting', generation = generation + 1"
            " WHERE desired = 'active' AND spec ? 'autoclose_after_seconds'"
            " AND created_at + make_interval(secs => (spec->>'autoclose_after_seconds')::int)"
            " <= now() RETURNING id"
        )
        ids = [row[0] for row in await cur.fetchall()]
        for session_id in ids:
            await _notify(conn, session_id)
    return ids


# A session is idle when it has at least one live agent, every live agent is idle with no turn
# and no attention, no message is queued, dispatching or verifying, and its init is not running.
# `idle_since` is the latest signal: creation, agent free_since and attention, turn start and
# end, message creation and dispatch, init start and finish.
_IDLE_PREDICATE = """
    s.desired = 'active'
    AND (s.spec->>'discard_after_idle_seconds') IS NOT NULL
    AND EXISTS (SELECT 1 FROM agents a WHERE a.session_id = s.id AND a.gone_at IS NULL)
    AND NOT EXISTS (
        SELECT 1 FROM agents a LEFT JOIN agent_status st ON st.agent_id = a.id
        WHERE a.session_id = s.id AND a.gone_at IS NULL
          AND (st.state IS DISTINCT FROM 'idle' OR st.current_turn_id IS NOT NULL
               OR st.waiting_since IS NOT NULL))
    AND NOT EXISTS (
        SELECT 1 FROM messages m
        WHERE m.session_id = s.id AND m.status IN ('queued', 'dispatching', 'verifying'))
    AND COALESCE(s.status->'init'->>'state', '') <> 'running'
    AND GREATEST(
        s.created_at,
        (SELECT max(GREATEST(st.free_since, st.waiting_since))
           FROM agents a JOIN agent_status st ON st.agent_id = a.id WHERE a.session_id = s.id),
        (SELECT max(GREATEST(t.started_at, t.completed_at))
           FROM agents a JOIN turns t ON t.agent_id = a.id WHERE a.session_id = s.id),
        (SELECT max(GREATEST(m.created_at, m.dispatched_at))
           FROM messages m WHERE m.session_id = s.id),
        NULLIF(s.status->'init'->>'started_at', '')::timestamptz,
        NULLIF(s.status->'init'->>'finished_at', '')::timestamptz
    ) + make_interval(secs => (s.spec->>'discard_after_idle_seconds')::int) <= now()
"""


async def discard_idle(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """Flip sessions idle for their `discard_after_idle_seconds` to deleting (request_delete write).

    Per candidate, in its own transaction: lock the session row first, then re-evaluate the idle
    predicate in a fresh statement and flip under the `desired = 'active'` guard. `enqueue_message`
    takes the same row lock (FOR SHARE) before inserting, so a message committed first is seen by
    the re-check and cancels the discard, and one arriving after the flip is refused.
    """
    cur = await conn.execute(f"SELECT s.id FROM sessions s WHERE {_IDLE_PREDICATE}")
    flipped: list[uuid.UUID] = []
    for (session_id,) in await cur.fetchall():
        async with conn.transaction():
            await conn.execute("SELECT 1 FROM sessions WHERE id = %s FOR UPDATE", (session_id,))
            cur = await conn.execute(
                "UPDATE sessions s SET desired = 'deleting', generation = generation + 1"
                f" WHERE s.id = %s AND {_IDLE_PREDICATE} RETURNING s.id",
                (session_id,),
            )
            if await cur.fetchone() is not None:
                await _notify(conn, session_id)
                flipped.append(session_id)
    return flipped


async def set_init_scripts(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, scripts: list[str]
) -> tuple[Session, bool]:
    """Write the init scripts to the session spec and to its source preset, in one transaction.

    No generation bump: the list is never rendered into pods. Lock order is sessions, then
    presets. Returns the updated session and whether a preset row was written.
    """
    async with conn.transaction():
        async with conn.cursor() as cur:
            await cur.execute(
                "SELECT desired, preset_id FROM sessions WHERE id = %s FOR UPDATE", (session_id,)
            )
            locked = await cur.fetchone()
        if locked is None:
            raise SessionNotFound(str(session_id))
        desired, preset_id = locked
        if desired == "deleting":
            raise SessionBeingDeleted(str(session_id))
        listed = Jsonb(scripts)
        rows = await _fetch(
            conn,
            "UPDATE sessions SET spec = jsonb_set(spec, '{init_scripts}', %s) WHERE id = %s"
            " RETURNING *",
            (listed, session_id),
        )
        preset_written = False
        if preset_id is not None:
            cur = await conn.execute("SELECT 1 FROM presets WHERE id = %s FOR UPDATE", (preset_id,))
            if await cur.fetchone() is not None:
                await conn.execute(
                    "UPDATE presets SET spec = jsonb_set(spec, '{init_scripts}', %s),"
                    " updated_at = now() WHERE id = %s",
                    (listed, preset_id),
                )
                preset_written = True
        await _notify(conn, session_id)
    return rows[0], preset_written


async def set_replicas(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    replicas: int,
    *,
    caller_agent_id: uuid.UUID | None = None,
) -> ScaleOutcome:
    """The one writer of the runtime replica count: spec.replicas, in place.

    No generation bump: a replica change never touches the pod template. Lock order is the
    session row, then reads of session_leaders and agents (never written here: the dispatcher
    moves the leader, deriving the move from spec.replicas).
    Refusals, in order: session_deleting, not_leader, static_session, out_of_range,
    leader_would_leave. A count at or below the leader's ordinal is refused leader_would_leave
    only when no other agent can lead after the change (`eligibility.ELIGIBLE`, the pass's
    pick predicate); accepted, it also sends `NOTIFY leadership_requested {session_id}` so the
    dispatcher hands leadership over at once. An unchanged value writes and notifies nothing.
    """
    async with conn.transaction():
        locked = await _fetch(
            conn, "SELECT * FROM sessions WHERE id = %s FOR UPDATE", (session_id,)
        )
        if not locked:
            raise SessionNotFound(str(session_id))
        session = locked[0]
        spec = session.spec
        if session.desired != "active":
            raise ScaleRefused("session_deleting")
        cur = await conn.execute(
            "SELECT l.agent_id, a.ordinal FROM session_leaders l"
            " JOIN agents a ON a.id = l.agent_id WHERE l.session_id = %s",
            (session_id,),
        )
        leader = await cur.fetchone()
        if caller_agent_id is not None and (leader is None or leader[0] != caller_agent_id):
            raise ScaleRefused("not_leader")
        if spec.static:
            raise ScaleRefused("static_session")
        max_replicas = spec.max_replicas or spec.replicas
        if not 1 <= replicas <= max_replicas:
            raise ScaleRefused("out_of_range", f"replicas must be 1..{max_replicas}")
        changed = replicas != spec.replicas
        if changed:
            await conn.execute(
                "UPDATE sessions SET spec = jsonb_set(spec, '{replicas}', to_jsonb(%s::int))"
                " WHERE id = %s AND desired = 'active'",
                (replicas, session_id),
            )
        # Checked after the write: ELIGIBLE reads spec.replicas, so it judges the new count;
        # a refusal leaves the transaction and rolls the write back.
        leaving = leader is not None and leader[1] >= replicas
        if leaving and not await _staying_peer(conn, session_id):
            raise ScaleRefused("leader_would_leave")
        if changed:
            if leaving:
                await notify(conn, leadership.CHANNEL, {"session_id": str(session_id)})
            await _notify(conn, session_id)
    return ScaleOutcome(session_id, replicas, spec.replicas, max_replicas)


async def _staying_peer(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> bool:
    """True when an agent of the session could lead at its current spec.replicas."""
    cur = await conn.execute(
        f"SELECT EXISTS (SELECT 1 FROM agents a WHERE a.session_id = %s AND {ELIGIBLE})",
        (session_id,),
    )
    row = await cur.fetchone()
    return bool(row and row[0])


async def set_status(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    status: dict[str, Any],
    observed_generation: int,
) -> Session:
    async with conn.transaction():
        rows = await _fetch(
            conn,
            "UPDATE sessions SET status = %s, observed_generation = %s WHERE id = %s RETURNING *",
            (Jsonb(status), observed_generation, session_id),
        )
        if not rows:
            raise SessionNotFound(str(session_id))
        await _notify(conn, session_id)
    return rows[0]


async def purge(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> None:
    """Delete the row; foreign keys cascade agents, messages, events, turns, commands, checks."""
    async with conn.transaction():
        await conn.execute("DELETE FROM sessions WHERE id = %s", (session_id,))
        await _notify(conn, session_id)
