"""chat_activity: when a session's agents and its chat connector last spoke (Postgres).

Writer of the table: this module. An `in` row is a message the chat connector posted to the
session, an `out` row a chat post or reaction of an agent through the chat relay. The handoff
profile reads it (an agent that posts to the chat is a `chat` agent).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Literal

import psycopg


async def record(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    agent_id: uuid.UUID | None,
    direction: Literal["in", "out"],
) -> None:
    await conn.execute(
        "INSERT INTO chat_activity (session_id, agent_id, direction) VALUES (%s, %s, %s)",
        (session_id, agent_id, direction),
    )


async def seen_since(
    conn: psycopg.AsyncConnection,
    session_id: uuid.UUID,
    agent_id: uuid.UUID,
    since: datetime,
) -> bool:
    """True when this agent posted to the chat, or the connector posted to the session, since."""
    cur = await conn.execute(
        "SELECT EXISTS (SELECT 1 FROM chat_activity WHERE session_id = %s AND created_at >= %s"
        " AND (direction = 'in' OR (direction = 'out' AND agent_id = %s)))",
        (session_id, since, agent_id),
    )
    row = await cur.fetchone()
    return bool(row and row[0])


async def last_out(conn: psycopg.AsyncConnection, session_id: uuid.UUID) -> datetime | None:
    """The newest outbound chat post of any agent of the session."""
    cur = await conn.execute(
        "SELECT max(created_at) FROM chat_activity WHERE session_id = %s AND direction = 'out'",
        (session_id,),
    )
    row = await cur.fetchone()
    return row[0] if row else None
