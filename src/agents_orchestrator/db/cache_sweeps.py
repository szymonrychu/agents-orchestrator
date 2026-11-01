"""cache_sweeps: the one row holding the last cleanup sweep. Writer: the reconciler. Reader: api."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


async def record(
    conn: psycopg.AsyncConnection, finished_at: datetime, result: dict[str, Any]
) -> None:
    await conn.execute(
        "INSERT INTO cache_sweeps (id, finished_at, result) VALUES (1, %s, %s)"
        " ON CONFLICT (id) DO UPDATE SET finished_at = EXCLUDED.finished_at,"
        " result = EXCLUDED.result",
        (finished_at, Jsonb(result)),
    )


async def last(conn: psycopg.AsyncConnection) -> dict[str, Any] | None:
    """The last sweep's result dict, or None when none ran yet."""
    cur = await conn.execute("SELECT result FROM cache_sweeps WHERE id = 1")
    row = await cur.fetchone()
    return row[0] if row else None


async def seconds_since(conn: psycopg.AsyncConnection) -> float | None:
    """Seconds since the last sweep finished by the database clock, or None when none ran."""
    cur = await conn.execute(
        "SELECT extract(epoch FROM now() - finished_at)::float8 FROM cache_sweeps WHERE id = 1"
    )
    row = await cur.fetchone()
    return row[0] if row else None
