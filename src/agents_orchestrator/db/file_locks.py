"""Advisory file locks. The api is the only writer of `file_locks`."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg

_LIVE = "a.gone_at IS NULL"


@dataclass(frozen=True)
class Lock:
    path: str
    agent: str
    claimed_at: datetime


async def claim(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID, paths: list[str]
) -> tuple[list[str], list[tuple[str, str]]]:
    """Claim each path: free, own and gone-held paths are claimed; the rest come back as
    (path, holder pod name)."""
    claimed: list[str] = []
    held: list[tuple[str, str]] = []
    async with conn.transaction():
        for path in paths:
            cur = await conn.execute(
                "INSERT INTO file_locks (session_id, path, agent_id) VALUES (%s, %s, %s)"
                " ON CONFLICT (session_id, path) DO UPDATE SET agent_id = EXCLUDED.agent_id,"
                " claimed_at = CASE WHEN file_locks.agent_id = EXCLUDED.agent_id"
                " THEN file_locks.claimed_at ELSE now() END"
                " WHERE file_locks.agent_id = EXCLUDED.agent_id OR EXISTS"
                " (SELECT 1 FROM agents g WHERE g.id = file_locks.agent_id"
                " AND g.gone_at IS NOT NULL) RETURNING path",
                (session_id, path, agent_id),
            )
            if await cur.fetchone():
                claimed.append(path)
                continue
            cur = await conn.execute(
                "SELECT a.pod_name FROM file_locks f JOIN agents a ON a.id = f.agent_id"
                " WHERE f.session_id = %s AND f.path = %s",
                (session_id, path),
            )
            row = await cur.fetchone()
            if row:
                held.append((path, row[0]))
    return claimed, held


async def release(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID, paths: list[str]
) -> list[str]:
    cur = await conn.execute(
        "DELETE FROM file_locks WHERE session_id = %s AND agent_id = %s AND path = ANY(%s)"
        " RETURNING path",
        (session_id, agent_id, paths),
    )
    gone = {r[0] for r in await cur.fetchall()}
    return [p for p in paths if p in gone]


async def list_live(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> list[Lock]:
    """Locks held by not-gone agents, by path."""
    cur = await conn.execute(
        "SELECT f.path, a.pod_name, f.claimed_at FROM file_locks f"
        f" JOIN agents a ON a.id = f.agent_id WHERE f.session_id = %s AND {_LIVE}"
        " ORDER BY f.path",
        (session_id,),
    )
    return [Lock(*r) for r in await cur.fetchall()]


async def assign(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, path: str, agent_id: uuid.UUID | None
) -> None:
    """Leader override: give the lock to `agent_id`, or discard it when None."""
    if agent_id is None:
        await conn.execute(
            "DELETE FROM file_locks WHERE session_id = %s AND path = %s", (session_id, path)
        )
        return
    await conn.execute(
        "INSERT INTO file_locks (session_id, path, agent_id) VALUES (%s, %s, %s)"
        " ON CONFLICT (session_id, path) DO UPDATE SET agent_id = EXCLUDED.agent_id,"
        " claimed_at = now()",
        (session_id, path, agent_id),
    )


async def holder_for_update(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, path: str
) -> str | None:
    """Pod name holding `path`, with the lock row locked until the transaction ends; None when
    the path is free."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT a.pod_name FROM file_locks f JOIN agents a ON a.id = f.agent_id"
            " WHERE f.session_id = %s AND f.path = %s FOR UPDATE OF f",
            (session_id, path),
        )
        row = await cur.fetchone()
    return row[0] if row else None


async def other_holder(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, agent_id: uuid.UUID, path: str
) -> Lock | None:
    """The lock on `path` when a live agent other than `agent_id` holds it (one indexed query)."""
    cur = await conn.execute(
        "SELECT f.path, a.pod_name, f.claimed_at FROM file_locks f"
        f" JOIN agents a ON a.id = f.agent_id WHERE f.session_id = %s AND f.path = %s"
        f" AND f.agent_id <> %s AND {_LIVE}",
        (session_id, path, agent_id),
    )
    row = await cur.fetchone()
    return Lock(*row) if row else None
