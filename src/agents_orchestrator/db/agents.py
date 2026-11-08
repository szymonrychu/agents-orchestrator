"""agents table repository. The reconciler is the writer (D3).

`last_dispatched_at` is written only by the dispatch claim (db/queue.py, D23).
`wrapper_resumed_at` is the database clock of the last successful resume of the recorded
wrapper session, the ingester's re-attach trigger (deploy-safe-agent-sessions D3). Every change
that the session tabs and agents strip show (pod readiness, gone, wrapper session) NOTIFYs
`status {session_id}`.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db.pool import notify

_COLUMNS = (
    "id, session_id, ordinal, pod_name, url, pod_phase, ready, wrapper_session_id,"
    " wrapper_session_created_at, gone_at, last_dispatched_at, pod_ip, wrapper_resumed_at,"
    " start_failure"
)


@dataclass(frozen=True)
class Agent:
    id: uuid.UUID
    session_id: uuid.UUID
    ordinal: int
    pod_name: str
    url: str | None
    pod_phase: str | None
    ready: bool
    wrapper_session_id: str | None
    wrapper_session_created_at: datetime | None
    gone_at: datetime | None
    last_dispatched_at: datetime | None
    pod_ip: str | None = None
    wrapper_resumed_at: datetime | None = None
    start_failure: dict[str, Any] | None = None  # {step, message, log_tail}, see set_start_failure


async def _one(conn: psycopg.AsyncConnection, query: str, params: Any) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return await cur.fetchone()


async def _status(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> None:
    await notify(conn, "status", {"session_id": str(session_id)})


async def upsert_agent(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    ordinal: int,
    pod_name: str,
    url: str | None,
    pod_phase: str | None,
    ready: bool,
    pod_ip: str | None = None,
) -> Agent:
    """Insert or update the agent of one ordinal; an observed pod is never gone.

    The first insert also creates its `agent_status` row with state `unknown`. NOTIFYs
    `status` only when something changed, so a steady state is silent.
    """
    async with conn.transaction():
        before = await _one(
            conn,
            f"SELECT {_COLUMNS} FROM agents WHERE session_id = %s AND ordinal = %s"
            " FOR NO KEY UPDATE",
            (session_id, ordinal),
        )
        row = await _one(
            conn,
            "INSERT INTO agents (id, session_id, ordinal, pod_name, url, pod_phase, ready, pod_ip)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT (session_id, ordinal) DO UPDATE SET pod_name = EXCLUDED.pod_name,"
            " url = EXCLUDED.url, pod_phase = EXCLUDED.pod_phase, ready = EXCLUDED.ready,"
            " pod_ip = EXCLUDED.pod_ip,"
            f" gone_at = NULL RETURNING {_COLUMNS}",
            (uuid.uuid4(), session_id, ordinal, pod_name, url, pod_phase, ready, pod_ip),
        )
        assert row is not None
        await conn.execute(
            "INSERT INTO agent_status (agent_id) VALUES (%s) ON CONFLICT DO NOTHING",
            (row["id"],),
        )
        if before != row:
            await _status(conn, session_id)
        return Agent(**row)


async def mark_gone(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> bool:
    """Its pod vanished for good (scaled away): never picked, no IP. False when already gone."""
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE agents SET gone_at = now(), ready = false, pod_ip = NULL"
            " WHERE id = %s AND gone_at IS NULL RETURNING session_id",
            (agent_id,),
        )
        if row is None:
            return False
        await _status(conn, row["session_id"])
        return True


async def set_wrapper_session(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    wrapper_session_id: str,
    created_at: datetime | None,
    *,
    resumed: bool = False,
) -> bool:
    """Record the agent's wrapper session; `created_at` None means the database clock.

    `resumed` marks a successful resume: `wrapper_resumed_at` takes `clock_timestamp()`, so every
    resume writes a new value and the ingester's `AgentRef` changes and re-attaches. The
    bootstrapper also passes it when the wrapper resumed the stored session itself while the
    agent is dead. A created or adopted session clears the marker.
    """
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE agents SET wrapper_session_id = %s,"
            " wrapper_session_created_at = coalesce(%s, now()),"
            " wrapper_resumed_at = CASE WHEN %s THEN clock_timestamp() END,"
            " start_failure = NULL"
            " WHERE id = %s RETURNING session_id",
            (wrapper_session_id, created_at, resumed, agent_id),
        )
        if row is None:
            return False
        await _status(conn, row["session_id"])
        return True


async def set_start_failure(
    conn: psycopg.AsyncConnection,
    agent_id: uuid.UUID,
    failure: dict[str, Any],
    *,
    keep_other_step: bool = False,
) -> bool:
    """Record why the agent's wrapper session failed to start: `{step, message, log_tail}`.

    `keep_other_step` leaves a recorded failure of another step alone (a failed create must not
    replace the log tail of the failed session that preceded it). A session recorded later by
    `set_wrapper_session` clears it.
    """
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE agents SET start_failure = %s WHERE id = %s"
            " AND (NOT %s OR start_failure IS NULL OR start_failure->>'step' = %s)"
            " RETURNING session_id",
            (Jsonb(failure), agent_id, keep_other_step, failure.get("step")),
        )
        if row is None:
            return False
        await _status(conn, row["session_id"])
        return True


async def clear_wrapper_session(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> bool:
    async with conn.transaction():
        row = await _one(
            conn,
            "UPDATE agents SET wrapper_session_id = NULL, wrapper_session_created_at = NULL,"
            " wrapper_resumed_at = NULL WHERE id = %s RETURNING session_id",
            (agent_id,),
        )
        if row is None:
            return False
        await _status(conn, row["session_id"])
        return True


async def get_agent(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> Agent | None:
    row = await _one(conn, f"SELECT {_COLUMNS} FROM agents WHERE id = %s", (agent_id,))
    return Agent(**row) if row is not None else None


async def list_agents(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> list[Agent]:
    """Every agent of the session, gone ones included, by ordinal."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM agents WHERE session_id = %s ORDER BY ordinal",
            (session_id,),
        )
        return [Agent(**r) for r in await cur.fetchall()]


async def list_states(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> dict[uuid.UUID, str]:
    """The stored `agent_status.state` of each agent of the session that has a status row."""
    cur = await conn.execute(
        "SELECT s.agent_id, s.state FROM agent_status s JOIN agents a ON a.id = s.agent_id"
        " WHERE a.session_id = %s",
        (session_id,),
    )
    return {agent_id: state for agent_id, state in await cur.fetchall()}


async def agent_state(conn: psycopg.AsyncConnection, agent_id: uuid.UUID) -> str | None:
    """The stored `agent_status.state` of one agent (the ingester writes it), or None."""
    cur = await conn.execute("SELECT state FROM agent_status WHERE agent_id = %s", (agent_id,))
    row = await cur.fetchone()
    return row[0] if row is not None else None


async def by_pod_ip(conn: psycopg.AsyncConnection, session_id: uuid.UUID, ip: str) -> Agent | None:
    """The not-gone agent of the session whose pod has this IP, or None."""
    row = await _one(
        conn,
        f"SELECT {_COLUMNS} FROM agents"
        " WHERE session_id = %s AND pod_ip = %s AND gone_at IS NULL"
        " ORDER BY ordinal LIMIT 1",
        (session_id, ip),
    )
    return Agent(**row) if row is not None else None


async def leader_of(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID
) -> tuple[uuid.UUID, int] | None:
    """(agent id, epoch) of the session's current leader row, or None."""
    row = await _one(
        conn,
        "SELECT agent_id, epoch FROM session_leaders WHERE session_id = %s",
        (session_id,),
    )
    return (row["agent_id"], row["epoch"]) if row is not None else None


async def list_members(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> list[dict]:
    """Not-gone agents by ordinal with their stored state and context percentage."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT a.id, a.pod_name, a.ready, coalesce(s.state, 'unknown') AS state,"
            " s.context_pct FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id"
            " WHERE a.session_id = %s AND a.gone_at IS NULL ORDER BY a.ordinal",
            (session_id,),
        )
        return list(await cur.fetchall())
