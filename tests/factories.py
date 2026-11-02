"""Raw-SQL row factories so table-level tests never depend on another task's repository."""

from __future__ import annotations

import itertools
import uuid
from datetime import datetime
from typing import Any

from psycopg.types.json import Jsonb

_counter = itertools.count(1)
_UNSET: Any = object()


async def _one(pool, query: str, params: tuple) -> dict[str, Any]:
    async with pool.connection() as conn:
        cur = await conn.execute(query, params)
        cols = [d.name for d in cur.description]
        row = await cur.fetchone()
        return dict(zip(cols, row, strict=True))


async def insert_session(
    pool,
    *,
    slug: str | None = None,
    spec: dict | None = None,
    generation: int = 1,
    observed_generation: int = 0,
    desired: str = "active",
    status: dict | None = None,
    id: uuid.UUID | None = None,
    created_by_service: str | None = None,
    created_by_session: uuid.UUID | None = None,
) -> dict[str, Any]:
    n = next(_counter)
    return await _one(
        pool,
        "INSERT INTO sessions (id, slug, spec, generation, observed_generation, desired, status,"
        " created_by_service, created_by_session)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
        (
            id or uuid.uuid4(),
            slug or f"session-{n}",
            Jsonb(spec if spec is not None else {}),
            generation,
            observed_generation,
            desired,
            Jsonb(status if status is not None else {}),
            created_by_service,
            created_by_session,
        ),
    )


async def insert_agent(
    pool,
    session_id: uuid.UUID,
    *,
    ordinal: int | None = None,
    pod_name: str | None = None,
    url: str | None = None,
    pod_phase: str | None = None,
    ready: bool = False,
    wrapper_session_id: str | None = None,
    wrapper_session_created_at: datetime | None = None,
    gone_at: datetime | None = None,
    last_dispatched_at: datetime | None = None,
    id: uuid.UUID | None = None,
) -> dict[str, Any]:
    if ordinal is None:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT COALESCE(MAX(ordinal)+1, 0) FROM agents WHERE session_id=%s", (session_id,)
            )
            ordinal = (await cur.fetchone())[0]
    return await _one(
        pool,
        "INSERT INTO agents (id, session_id, ordinal, pod_name, url, pod_phase, ready,"
        " wrapper_session_id, wrapper_session_created_at, gone_at, last_dispatched_at)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
        (
            id or uuid.uuid4(),
            session_id,
            ordinal,
            pod_name or f"agent-{ordinal}",
            url,
            pod_phase,
            ready,
            wrapper_session_id,
            wrapper_session_created_at,
            gone_at,
            last_dispatched_at,
        ),
    )


async def set_agent_status(
    pool,
    agent_id: uuid.UUID,
    *,
    state: str = "idle",
    wrapper_idle_since: datetime | None = None,
    free_since: Any = _UNSET,
    current_turn_id: str | None = None,
    context_used_tokens: int | None = None,
    context_window_tokens: int | None = None,
    context_pct: float | None = None,
    last_seq: int | None = None,
    peers: list | dict | None = None,
) -> dict[str, Any]:
    """Upsert agent_status. free_since defaults to now() when omitted; pass None for NULL."""
    free_expr = "now()" if free_since is _UNSET else "%s"
    params: list[Any] = [
        agent_id,
        wrapper_idle_since,
        state,
    ]
    if free_since is not _UNSET:
        params.append(free_since)
    params += [
        current_turn_id,
        context_used_tokens,
        context_window_tokens,
        context_pct,
        last_seq,
        Jsonb(peers if peers is not None else []),
    ]
    cols = (
        "wrapper_idle_since=EXCLUDED.wrapper_idle_since, state=EXCLUDED.state,"
        " free_since=EXCLUDED.free_since, current_turn_id=EXCLUDED.current_turn_id,"
        " context_used_tokens=EXCLUDED.context_used_tokens,"
        " context_window_tokens=EXCLUDED.context_window_tokens,"
        " context_pct=EXCLUDED.context_pct, last_seq=EXCLUDED.last_seq,"
        " peers=EXCLUDED.peers, updated_at=now()"
    )
    return await _one(
        pool,
        "INSERT INTO agent_status (agent_id, wrapper_idle_since, state, free_since,"
        " current_turn_id, context_used_tokens, context_window_tokens, context_pct, last_seq,"
        f" peers) VALUES (%s,%s,%s,{free_expr},%s,%s,%s,%s,%s,%s)"
        f" ON CONFLICT (agent_id) DO UPDATE SET {cols} RETURNING *",
        tuple(params),
    )


async def insert_message(
    pool,
    session_id: uuid.UUID,
    *,
    text: str = "hello",
    status: str = "queued",
    target_agent_id: uuid.UUID | None = None,
    agent_id: uuid.UUID | None = None,
    turn_id: str | None = None,
    attempts: int = 0,
    error: str | None = None,
    heartbeat_at: datetime | None = None,
    dispatched_at: datetime | None = None,
    not_before: datetime | None = None,
    ambiguous_at: datetime | None = None,
    carries_start_prompt: bool = False,
    lane: str = "idle",  # queue tests pin the agent_free rules, which only idle messages obey
) -> dict[str, Any]:
    return await _one(
        pool,
        "INSERT INTO messages (session_id, target_agent_id, text, status, agent_id, turn_id,"
        " attempts, error, heartbeat_at, dispatched_at, not_before, ambiguous_at, lane,"
        " carries_start_prompt)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
        (
            session_id,
            target_agent_id,
            text,
            status,
            agent_id,
            turn_id,
            attempts,
            error,
            heartbeat_at,
            dispatched_at,
            not_before,
            ambiguous_at,
            lane,
            carries_start_prompt,
        ),
    )


async def insert_event(
    pool,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    *,
    wrapper_session_id: str = "s1",
    seq: int | None = None,
    type: str = "message",
    turn_id: str | None = None,
    data: dict | None = None,
    ts: datetime | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    if seq is None:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT COALESCE(MAX(seq)+1, 1) FROM events"
                " WHERE agent_id=%s AND wrapper_session_id=%s",
                (agent_id, wrapper_session_id),
            )
            seq = (await cur.fetchone())[0]
    return await _one(
        pool,
        "INSERT INTO events (session_id, agent_id, wrapper_session_id, seq, ts, type, turn_id,"
        " data, created_at) VALUES (%s,%s,%s,%s,COALESCE(%s, now()),%s,%s,%s,"
        " COALESCE(%s, clock_timestamp())) RETURNING *",
        (
            session_id,
            agent_id,
            wrapper_session_id,
            seq,
            ts,
            type,
            turn_id,
            Jsonb(data if data is not None else {}),
            created_at,
        ),
    )


async def insert_command(
    pool,
    agent_id: uuid.UUID,
    *,
    kind: str = "interrupt",
    status: str = "queued",
    result: dict | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> dict[str, Any]:
    return await _one(
        pool,
        "INSERT INTO commands (agent_id, kind, status, result, started_at, finished_at)"
        " VALUES (%s,%s,%s,%s,%s,%s) RETURNING *",
        (
            agent_id,
            kind,
            status,
            Jsonb(result) if result is not None else None,
            started_at,
            finished_at,
        ),
    )


async def insert_check(
    pool,
    session_id: uuid.UUID,
    *,
    command: str = "true",
    status: str = "queued",
    job_name: str | None = None,
    exit_code: int | None = None,
    log_tail: str | None = None,
    started_at: datetime | None = None,
    finished_at: datetime | None = None,
) -> dict[str, Any]:
    return await _one(
        pool,
        "INSERT INTO checks (session_id, command, status, job_name, exit_code, log_tail,"
        " started_at, finished_at) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING *",
        (session_id, command, status, job_name, exit_code, log_tail, started_at, finished_at),
    )


async def insert_turn(
    pool,
    agent_id: uuid.UUID,
    *,
    turn_id: str | None = None,
    origin: str = "user",
    status: str = "running",
    final_text: str | None = None,
    usage: dict | None = None,
    completed_at: datetime | None = None,
) -> dict[str, Any]:
    return await _one(
        pool,
        "INSERT INTO turns (turn_id, agent_id, origin, status, final_text, usage, completed_at)"
        " VALUES (%s,%s,%s,%s,%s,%s,%s) RETURNING *",
        (
            turn_id or uuid.uuid4().hex,
            agent_id,
            origin,
            status,
            final_text,
            Jsonb(usage) if usage is not None else None,
            completed_at,
        ),
    )


async def attach_preset(pool, session_id: uuid.UUID, *, manageable: bool = True) -> uuid.UUID:
    """Give a session a preset of its own, with the superleader flag as asked."""
    preset = await _one(
        pool,
        "INSERT INTO presets (id, name, spec, superleader_manageable)"
        " VALUES (%s,%s,%s,%s) RETURNING id",
        (uuid.uuid4(), f"preset-{uuid.uuid4().hex[:8]}", Jsonb({}), manageable),
    )
    await _one(
        pool,
        "UPDATE sessions SET preset_id = %s WHERE id = %s RETURNING id",
        (preset["id"], session_id),
    )
    return preset["id"]
