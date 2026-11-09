"""sessions table repository. The api writes; the reconciler uses set_status and purge."""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db import leadership
from agents_orchestrator.db.eligibility import ELIGIBLE, background_busy
from agents_orchestrator.db.pool import notify
from agents_orchestrator.db.queue import SessionBeingDeleted, enqueue_message
from agents_orchestrator.db.start_state import start_reason, start_state
from agents_orchestrator.inactivity import NOTICE_CLOSE, NOTICE_REMINDER, close_reason
from agents_orchestrator.spec import SessionSpec, validate_slug

CHANNEL = "session_changed"

log = logging.getLogger(__name__)


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
    # a pending self-close request from the session's leader (hive-self-close); the api sets and
    # clears it, the autoclose sweeper acts on it once the session is idle
    close_requested_at: datetime | None = None
    close_reason: str | None = None
    labels: list[str] = field(default_factory=list)
    # inactivity limits: the api-written quiet-clock stamp, when the reminder went out, who asked
    # for the close ('self' or 'inactivity') and when an inactivity close is forced
    last_update_at: datetime | None = None
    inactivity_reminded_at: datetime | None = None
    close_kind: str | None = None
    close_deadline_at: datetime | None = None
    # set by a forced discard (DELETE ...?force=true): owed pre-discard hooks are abandoned
    discard_forced_at: datetime | None = None


@dataclass(frozen=True)
class CloseOutcome:
    session: Session
    already: bool  # a request was already pending: it is returned unchanged


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
        close_requested_at=row["close_requested_at"],
        close_reason=row["close_reason"],
        labels=row["labels"],
        last_update_at=row["last_update_at"],
        inactivity_reminded_at=row["inactivity_reminded_at"],
        close_kind=row["close_kind"],
        close_deadline_at=row["close_deadline_at"],
        discard_forced_at=row["discard_forced_at"],
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
    labels: list[str] | None = None,
) -> Session:
    validate_slug(slug)
    try:
        async with conn.transaction():
            rows = await _fetch(
                conn,
                "INSERT INTO sessions (id, slug, spec, preset_id, preset_name, created_by_service,"
                " secret_versions, created_by_session, labels)"
                " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING *",
                (
                    uuid.uuid4(),
                    slug,
                    _dump(spec),
                    preset_id,
                    preset_name,
                    created_by_service,
                    Jsonb(secret_versions) if secret_versions else None,
                    created_by_session,
                    labels or [],
                ),
            )
            await _notify(conn, rows[0].id)
    except psycopg.errors.UniqueViolation:
        raise SessionExists(slug) from None
    return rows[0]


async def get(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Session | None:
    rows = await _fetch(conn, "SELECT * FROM sessions WHERE id = %s", (session_id,))
    return rows[0] if rows else None


async def get_stored(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID
) -> tuple[Session, dict[str, Any]] | None:
    """The session and its spec JSONB as stored (the `expected` of `replace_spec`)."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM sessions WHERE id = %s", (session_id,))
        row = await cur.fetchone()
    return None if row is None else (_from_row(row), row["spec"])


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


async def list_sessions(
    conn: psycopg.AsyncConnection, labels: list[str] | None = None
) -> list[Session]:
    """Every session; with `labels`, only live (not being deleted) sessions sharing any."""
    if not labels:
        return await _fetch(conn, "SELECT * FROM sessions ORDER BY created_at, slug", ())
    return await _fetch(
        conn,
        "SELECT * FROM sessions WHERE labels && %s::text[] AND desired = 'active'"
        " ORDER BY created_at, slug",
        (labels,),
    )


async def set_labels(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, labels: list[str]
) -> Session:
    """Replace the session's labels. They are not part of the spec: no generation bump."""
    rows = await _fetch(
        conn, "UPDATE sessions SET labels = %s WHERE id = %s RETURNING *", (labels, session_id)
    )
    if not rows:
        raise SessionNotFound(str(session_id))
    return rows[0]


@dataclass(frozen=True)
class MessagingPeer:
    slug: str
    preset_name: str | None
    superleader: bool
    leader_present: bool
    start_state: str
    start_reason: str | None


# Active sessions other than the caller's, with whether a not-gone agent holds the leader row
# and whether any not-gone agent exists; the hive rule picks which one is "leader present".
_PEERS = """
SELECT c.*,
       EXISTS (SELECT 1 FROM session_leaders l JOIN agents a ON a.id = l.agent_id
               WHERE l.session_id = c.id AND a.gone_at IS NULL) AS live_leader,
       EXISTS (SELECT 1 FROM agents a
               WHERE a.session_id = c.id AND a.gone_at IS NULL) AS live_agent,
       (SELECT count(*) FROM agents a
         WHERE a.session_id = c.id AND a.gone_at IS NULL AND a.ready) AS ready_agents,
       coalesce((SELECT jsonb_agg(jsonb_build_object('pod', a.pod_name) || a.start_failure
                                  ORDER BY a.ordinal)
                 FROM agents a WHERE a.session_id = c.id AND a.gone_at IS NULL
                   AND a.start_failure IS NOT NULL), '[]'::jsonb) AS start_failures
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
        failures = row["start_failures"]
        state = start_state(row["ready_agents"], failures)
        out.append(
            MessagingPeer(
                start_state=state,
                start_reason=start_reason(
                    state, failures, str(_from_row(row).status.get("message") or "")
                ),
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


async def request_delete(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, *, force: bool = False
) -> Session:
    """Flip the session to deleting (a no-op when it already is). `force` also stamps
    `discard_forced_at` (kept once set), on an active session and on one already deleting, so
    the reconciler abandons the owed pre-discard hooks instead of retrying them."""
    async with conn.transaction():
        if force:
            rows = await _fetch(
                conn,
                "UPDATE sessions SET desired = 'deleting',"
                " generation = generation + (desired = 'active')::int,"
                " discard_forced_at = coalesce(discard_forced_at, now())"
                " WHERE id = %s RETURNING *",
                (session_id,),
            )
        else:
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


async def _lock_row(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> dict[str, Any]:
    """Lock the session row FOR UPDATE inside the caller's transaction; the row must exist and
    be active. Returns the row as stored."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM sessions WHERE id = %s FOR UPDATE", (session_id,))
        row = await cur.fetchone()
    if row is None:
        raise SessionNotFound(str(session_id))
    if row["desired"] != "active":
        raise SessionBeingDeleted(str(session_id))
    return row


async def _lock_active(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Session:
    return _from_row(await _lock_row(conn, session_id))


CLOSE_KINDS = ("self", "inactivity")


async def request_close(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, reason: str, *, kind: str = "self"
) -> CloseOutcome:
    """Record a request to close the session once idle, by its leader (`kind` 'self') or the
    orchestrator ('inactivity'). The first request wins: a pending one is returned unchanged
    (`already`) and its clock is not reset. Raises ValueError for any other `kind`."""
    if kind not in CLOSE_KINDS:
        raise ValueError(f"unknown close kind {kind!r}, expected one of {CLOSE_KINDS}")
    async with conn.transaction():
        existing = await _lock_active(conn, session_id)
        if existing.close_requested_at is not None:
            return CloseOutcome(existing, True)
        rows = await _fetch(
            conn,
            "UPDATE sessions SET close_requested_at = now(), close_reason = %s, close_kind = %s"
            " WHERE id = %s AND desired = 'active' AND close_requested_at IS NULL RETURNING *",
            (reason, kind, session_id),
        )
        await _notify(conn, session_id)
        return CloseOutcome(rows[0], False)


# Clears a pending close of either kind and restarts the quiet clock, so an inactivity close
# does not refire on the next poll.
_CLEAR_CLOSE = (
    "close_requested_at = NULL, close_reason = NULL, close_kind = NULL,"
    " close_deadline_at = NULL, last_update_at = now()"
)


async def cancel_close(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> Session:
    """Clear a pending close request (the user's "Keep session") and restart the quiet clock;
    idempotent when none is pending (no write)."""
    async with conn.transaction():
        existing = await _lock_active(conn, session_id)
        if existing.close_requested_at is None:
            return existing
        rows = await _fetch(
            conn,
            f"UPDATE sessions SET {_CLEAR_CLOSE} WHERE id = %s AND desired = 'active' RETURNING *",
            (session_id,),
        )
        await _notify(conn, session_id)
        return rows[0]


async def touch_update(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> None:
    """Count a user-visible update by an agent (a chat post, a taskboard write): stamp
    `last_update_at`. No NOTIFY and no generation bump: nothing reacts to it. A deleting or
    unknown session is left alone."""
    await conn.execute(
        "UPDATE sessions SET last_update_at = now() WHERE id = %s AND desired = 'active'",
        (session_id,),
    )


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


def quiet_clock_sql(alias: str) -> str:
    """SQL expression for the last user-visible update of session `alias` (spec 3.2): the
    latest of its creation, the api-written `last_update_at`, the newest inbound message that is
    not an orchestrator notice (user, trigger, connector or peer session), the completion of a
    turn that carried such a message, and the newest message the session sent to another
    session. Tool activity and turns that carried only a notice do not count."""
    inbound = (
        f"qm.session_id = {alias}.id AND qm.notice IS NULL"
        f" AND qm.sender_session_id IS DISTINCT FROM {alias}.id"
    )
    return (
        f"GREATEST({alias}.created_at, {alias}.last_update_at,"
        f" (SELECT max(qm.created_at) FROM messages qm WHERE {inbound}),"
        " (SELECT max(qt.completed_at) FROM messages qm JOIN turns qt ON qt.turn_id = qm.turn_id"
        f"  WHERE {inbound}),"
        f" (SELECT max(qm.created_at) FROM messages qm WHERE qm.sender_session_id = {alias}.id))"
    )


# Sessions joined to their quiet clock `q.quiet`.
_QUIET_FROM = f"sessions s CROSS JOIN LATERAL (SELECT {quiet_clock_sql('s')} AS quiet) q"
# Active, not a superleader (exempt, spec 3.1) and no close pending.
_LIMITED = (
    "s.desired = 'active' AND s.close_requested_at IS NULL"
    " AND COALESCE((s.spec->>'session_messaging')::boolean, false) = false"
)
_REMINDER = "COALESCE((s.spec->'inactivity'->>'reminder_seconds')::int, %(reminder_default)s)"
_CLOSE = "COALESCE((s.spec->'inactivity'->>'close_seconds')::int, %(close_default)s)"
_REMIND_PREDICATE = f"""
    {_LIMITED}
    AND {_REMINDER} > 0
    AND q.quiet + make_interval(secs => {_REMINDER}) <= now()
    AND (s.inactivity_reminded_at IS NULL OR s.inactivity_reminded_at < q.quiet)
    AND NOT EXISTS (
        SELECT 1 FROM agents a JOIN agent_status st ON st.agent_id = a.id
        WHERE a.session_id = s.id AND a.gone_at IS NULL AND st.waiting_since IS NOT NULL)
"""
_CLOSE_PREDICATE = f"""
    {_LIMITED}
    AND {_CLOSE} > 0
    AND q.quiet + make_interval(secs => {_CLOSE}) <= now()
"""
# An inactivity close is cancelled by an inbound message that is not a notice and came after the
# close request: created after it, or queued behind its close notice (a message whose transaction
# began before the close and waited on the row lock carries an earlier `created_at`, but a later
# id than the notice inserted under that lock).
_INACTIVITY_CLOSE_CANCELLED = """
    s.close_kind = 'inactivity' AND EXISTS (
        SELECT 1 FROM messages cm
        WHERE cm.session_id = s.id AND cm.notice IS NULL
          AND cm.sender_session_id IS DISTINCT FROM s.id
          AND (cm.created_at > s.close_requested_at
               OR cm.id > (SELECT max(n.id) FROM messages n
                           WHERE n.session_id = s.id AND n.notice = 'inactivity_close')))
"""
_RELEASE_PREDICATE = f"s.desired = 'active' AND {_INACTIVITY_CLOSE_CANCELLED}"
_OVERDUE_PREDICATE = f"""
    s.desired = 'active' AND s.close_kind = 'inactivity' AND s.close_deadline_at <= now()
    AND NOT ({_INACTIVITY_CLOSE_CANCELLED})
"""
_FLIP = (
    "UPDATE sessions SET desired = 'deleting', generation = generation + 1"
    " WHERE id = %s AND desired = 'active'"
)


async def _sweep_each(
    conn: psycopg.AsyncConnection,
    source: str,
    predicate: str,
    params: dict[str, Any],
    columns: str,
    act: Callable[[uuid.UUID, dict[str, Any]], Awaitable[str | None]],
) -> list[uuid.UUID]:
    """Run `act` on every session of `source` matching `predicate`; returns those it ran on.

    Per candidate, in its own transaction: lock the session row first, then re-evaluate the
    predicate in a fresh statement (selecting `columns`) and call `act(id, row)` only when it
    still holds; `act` writes and returns the line logged after the commit, or None when its
    own status-guarded write matched no row (nothing was done). `enqueue_message`
    takes the same row lock (FOR SHARE) before inserting, so a message committed first is seen
    by the re-check, and one arriving later waits for the commit."""
    async with conn.transaction():
        cur = await conn.execute(f"SELECT s.id FROM {source} WHERE {predicate}", params)
        candidates = [row[0] for row in await cur.fetchall()]
    done: list[uuid.UUID] = []
    for session_id in candidates:
        line = None
        async with conn.transaction():
            await conn.execute("SELECT 1 FROM sessions WHERE id = %s FOR UPDATE", (session_id,))
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(
                    f"SELECT {columns} FROM {source} WHERE s.id = %(id)s AND {predicate}",
                    {**params, "id": session_id},
                )
                row = await cur.fetchone()
            if row is not None:
                line = await act(session_id, row)
                if line is not None:
                    await _notify(conn, session_id)
        if line is not None:
            log.info("%s", line)
            done.append(session_id)
    return done


async def release_inactive_close(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """Cancel inactivity closes that an inbound message answered (spec Q10), like
    `cancel_close`: the close fields are cleared and the quiet clock restarts. A self-close is
    never released this way."""

    async def release(session_id: uuid.UUID, _: dict[str, Any]) -> str | None:
        cur = await conn.execute(
            f"UPDATE sessions SET {_CLEAR_CLOSE}"
            " WHERE id = %s AND desired = 'active' AND close_kind = 'inactivity'",
            (session_id,),
        )
        if cur.rowcount == 0:
            return None
        return f"inactivity: session {session_id} got a message, close cancelled"

    return await _sweep_each(conn, "sessions s", _RELEASE_PREDICATE, {}, "s.id", release)


async def close_overdue(conn: psycopg.AsyncConnection) -> list[uuid.UUID]:
    """Flip inactivity closes past their handoff deadline to deleting (request_delete write),
    busy or not, unless an inbound message cancelled them."""

    async def flip(session_id: uuid.UUID, _: dict[str, Any]) -> str:
        await conn.execute(_FLIP, (session_id,))
        return f"inactivity: session {session_id} deadline passed, discarding"

    return await _sweep_each(conn, "sessions s", _OVERDUE_PREDICATE, {}, "s.id", flip)


async def close_inactive(
    conn: psycopg.AsyncConnection,
    close_default: int,
    handoff_seconds: int,
    compose: Callable[[Session, datetime, datetime], str],
) -> list[uuid.UUID]:
    """Request the close of sessions quiet for their close limit (spec 3.4): `close_kind`
    'inactivity', the reason, a deadline `handoff_seconds` away, and the `inactivity_close`
    notice `compose(session, last_update, deadline)` to the leader, in one transaction."""

    async def close(session_id: uuid.UUID, row: dict[str, Any]) -> str | None:
        reason = close_reason(row["quiet_seconds"], row["close_seconds"])
        rows = await _fetch(
            conn,
            "UPDATE sessions SET close_requested_at = now(), close_kind = 'inactivity',"
            " close_reason = %s, close_deadline_at = now() + make_interval(secs => %s)"
            " WHERE id = %s AND desired = 'active' AND close_requested_at IS NULL RETURNING *",
            (reason, handoff_seconds, session_id),
        )
        if not rows:
            return None
        [session] = rows
        assert session.close_deadline_at is not None
        text = compose(session, row["quiet"], session.close_deadline_at)
        await enqueue_message(conn, session_id, text, lane="regular", notice=NOTICE_CLOSE)
        deadline = session.close_deadline_at.isoformat()
        return f"inactivity: session {session_id} closing, deadline {deadline}"

    columns = (
        "q.quiet, floor(extract(epoch FROM now() - q.quiet))::int AS quiet_seconds,"
        f" {_CLOSE} AS close_seconds"
    )
    params = {"close_default": close_default}
    return await _sweep_each(conn, _QUIET_FROM, _CLOSE_PREDICATE, params, columns, close)


async def remind_inactive(
    conn: psycopg.AsyncConnection,
    reminder_default: int,
    compose: Callable[[Session, datetime, int], str],
) -> list[uuid.UUID]:
    """Remind sessions quiet for their reminder limit, once per quiet period (spec 3.3):
    `inactivity_reminded_at` and the `inactivity_reminder` notice
    `compose(session, last_update, live_agent_count)` to the leader, in one transaction. Never
    while an agent waits on a person, or while the reminder is off (0)."""

    async def remind(session_id: uuid.UUID, row: dict[str, Any]) -> str | None:
        rows = await _fetch(
            conn,
            "UPDATE sessions SET inactivity_reminded_at = now()"
            " WHERE id = %s AND desired = 'active' RETURNING *",
            (session_id,),
        )
        if not rows:
            return None
        [session] = rows
        text = compose(session, row["quiet"], row["pods"])
        await enqueue_message(conn, session_id, text, lane="regular", notice=NOTICE_REMINDER)
        return f"inactivity: session {session_id} reminded"

    columns = (
        "q.quiet, (SELECT count(*) FROM agents a"
        " WHERE a.session_id = s.id AND a.gone_at IS NULL)::int AS pods"
    )
    params = {"reminder_default": reminder_default}
    return await _sweep_each(conn, _QUIET_FROM, _REMIND_PREDICATE, params, columns, remind)


# A session is idle when it has at least one live agent, every live agent is idle with no turn
# and no attention, no message is queued, dispatching or verifying, and its init is not running.
# `idle_since` is the latest signal: creation, agent free_since and attention, turn start and
# end, message creation and dispatch, init start and finish, and a close request. The threshold
# is the `%(grace)s` parameter while a close is requested (self-close or inactivity close); a
# session with no close request has no threshold and is never discarded.
_IDLE_THRESHOLD = "CASE WHEN s.close_requested_at IS NOT NULL THEN %(grace)s::int END"
_IDLE_PREDICATE = f"""
    s.desired = 'active'
    AND {_IDLE_THRESHOLD} IS NOT NULL
    AND EXISTS (SELECT 1 FROM agents a WHERE a.session_id = s.id AND a.gone_at IS NULL)
    AND NOT EXISTS (
        SELECT 1 FROM agents a LEFT JOIN agent_status st ON st.agent_id = a.id
        WHERE a.session_id = s.id AND a.gone_at IS NULL
          AND (st.state IS DISTINCT FROM 'idle' OR st.current_turn_id IS NOT NULL
               OR st.waiting_since IS NOT NULL OR {background_busy("st")}))
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
        NULLIF(s.status->'init'->>'finished_at', '')::timestamptz,
        s.close_requested_at
    ) + make_interval(secs => {_IDLE_THRESHOLD}) <= now()
"""


async def discard_idle(conn: psycopg.AsyncConnection, close_grace_seconds: int) -> list[uuid.UUID]:
    """Flip idle sessions to deleting (request_delete write): those with a pending close request
    (self-close or inactivity close) that have been idle for `close_grace_seconds`, unless an
    inbound message cancelled their inactivity close. Locked and re-checked per candidate
    (`_sweep_each`): a message or a `cancel_close` committed first fails the re-check, and a
    message arriving after the flip is refused."""
    predicate = (
        f"{_IDLE_PREDICATE} AND (s.close_kind IS DISTINCT FROM 'inactivity'"
        f" OR NOT ({_INACTIVITY_CLOSE_CANCELLED}))"
    )

    async def flip(session_id: uuid.UUID, row: dict[str, Any]) -> str:
        await conn.execute(_FLIP, (session_id,))
        kind = "inactivity" if row["close_kind"] == "inactivity" else "self-close"
        return f"{kind}: session {session_id} is idle, discarding"

    params = {"grace": close_grace_seconds}
    return await _sweep_each(conn, "sessions s", predicate, params, "s.close_kind", flip)


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


async def set_permission_mode(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, mode: str
) -> tuple[Session, bool]:
    """Write `spec.wrapper.permission_mode` to the session spec and its source preset.

    Same transaction and lock order as `set_init_scripts` (sessions, then presets). Unlike the
    init scripts the value is rendered into the pod config, so a changed value bumps the
    generation and the config hash rolls the pods through the normal Replacer path (idle pods
    restart, busy ones wait); an unchanged value bumps nothing. Returns the updated session and
    whether a preset row was written.
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
        patch = Jsonb({"permission_mode": mode})
        merge = (
            "jsonb_set(spec, '{wrapper}', CASE WHEN jsonb_typeof(spec->'wrapper') = 'object'"
            " THEN spec->'wrapper' ELSE '{}'::jsonb END || %s)"
        )
        rows = await _fetch(
            conn,
            f"UPDATE sessions SET spec = {merge},"
            " generation = generation + CASE WHEN spec #>> '{wrapper,permission_mode}'"
            " IS DISTINCT FROM %s THEN 1 ELSE 0 END WHERE id = %s RETURNING *",
            (patch, mode, session_id),
        )
        preset_written = False
        if preset_id is not None:
            cur = await conn.execute("SELECT 1 FROM presets WHERE id = %s FOR UPDATE", (preset_id,))
            if await cur.fetchone() is not None:
                await conn.execute(
                    f"UPDATE presets SET spec = {merge}, updated_at = now() WHERE id = %s",
                    (patch, preset_id),
                )
                preset_written = True
        await _notify(conn, session_id)
    return rows[0], preset_written


class SpecChanged(Exception):
    """The stored spec is no longer the one the caller validated against."""


@dataclass(frozen=True)
class LockedSession:
    spec: dict[str, Any]  # the stored JSONB, as stored
    preset_id: uuid.UUID | None
    secret_versions: dict[str, str]


async def replace_spec(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    expected: dict[str, Any],
    spec: SessionSpec,
    recheck: Callable[[LockedSession], Awaitable[None]],
    secret_versions: dict[str, str] | None = None,
) -> tuple[Session, bool]:
    """Replace the whole session spec in one transaction, under the row lock.

    SessionNotFound when the row is absent, SessionBeingDeleted unless it is active, SpecChanged
    when the stored JSONB is not `expected` (dict equality, not a parsed model). Then
    `recheck(locked)` runs on the fresh row; it may raise, and nothing is written then. Lock
    order within this call is the session row first; `recheck` may then take `presets` FOR SHARE
    (sessions, then presets, as `set_init_scripts`). A spec that differs from the stored one is
    written with generation + 1 and NOTIFY session_changed; an equal one writes and notifies
    nothing.

    `secret_versions` ({ENV_NAME: Secret name}, from `k8s.secrets.bind_staged` in the caller's
    transaction) is merged into the stored map in the same UPDATE; a changed map is a change on
    its own. Returns the row and whether it was written. A caller that binds holds more locks
    before this call: the preset FOR SHARE, then the variable advisory locks (`bind_staged`),
    then this row (presets, variables, sessions), because `set_secret` takes the preset FOR
    UPDATE and then the variable lock; binding before the preset lock can deadlock with it.
    """
    async with conn.transaction():
        row = await _lock_row(conn, session_id)
        if row["spec"] != expected:
            raise SpecChanged(str(session_id))
        # decided before the recheck, which gets the stored dict itself
        dumped = _dump(spec)
        stored_versions = row["secret_versions"] or {}
        versions = {**stored_versions, **(secret_versions or {})}
        unchanged = dumped.obj == row["spec"] and versions == stored_versions
        await recheck(LockedSession(row["spec"], row["preset_id"], stored_versions))
        if unchanged:
            return _from_row(row), False
        rows = await _fetch(
            conn,
            "UPDATE sessions SET spec = %s, secret_versions = %s, generation = generation + 1"
            " WHERE id = %s RETURNING *",
            (dumped, Jsonb(versions) if versions else None, session_id),
        )
        await _notify(conn, session_id)
    return rows[0], True


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
