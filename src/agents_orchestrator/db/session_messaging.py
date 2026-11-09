"""Relay marks on session messages: the chat relay's notice rows and the recent-send lookup.

Kept apart from `db/queue.py`; the queue transitions stay there.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from dataclasses import dataclass

import psycopg

from agents_orchestrator.db import queue


@dataclass(frozen=True)
class RelayMark:
    send_id: str
    room_id: str


async def sent_to_within(
    conn: psycopg.AsyncConnection, *, sender: uuid.UUID, receiver: uuid.UUID, seconds: float
) -> bool:
    """True when `sender` sent `receiver` a message of any status in the last `seconds`."""
    if seconds <= 0:
        return False
    row = await queue._one(
        conn,
        "SELECT EXISTS (SELECT 1 FROM messages WHERE sender_session_id = %s AND session_id = %s"
        " AND created_at > now() - make_interval(secs => %s::float8)) AS sent",
        (sender, receiver, seconds),
    )
    assert row is not None
    return bool(row["sent"])


async def enqueue_relay_notice(
    conn: psycopg.AsyncConnection,
    occupier_id: uuid.UUID,
    text: str,
    *,
    sender_session_id: uuid.UUID,
    sender_slug: str,
    hop: int,
    send_id: str,
    room_id: str,
) -> int:
    """Queue a session message to the occupier and mark it as a relay notice in one transaction.

    The NOTIFY lands at commit, so no claim sees an unmarked row. Raises
    `queue.SessionBeingDeleted` like `queue.enqueue_message`.
    """
    async with conn.transaction():
        message_id = await queue.enqueue_message(
            conn,
            occupier_id,
            text,
            sender_session_id=sender_session_id,
            sender_slug=sender_slug,
            hop=hop,
            lane="regular",
        )
        await conn.execute(
            "UPDATE messages SET chat_relay_send = %s, chat_relay_room = %s WHERE id = %s",
            (send_id, room_id, message_id),
        )
        return message_id


async def relay_mark(conn: psycopg.AsyncConnection, message_id: int) -> RelayMark | None:
    row = await queue._one(
        conn,
        "SELECT chat_relay_send, chat_relay_room FROM messages"
        " WHERE id = %s AND chat_relay_send IS NOT NULL",
        (message_id,),
    )
    if row is None:
        return None
    return RelayMark(row["chat_relay_send"], row["chat_relay_room"])


async def mark_handled(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, ids: Sequence[int]
) -> list[int]:
    """Mark incoming session messages of `session_id` as handled by its leader.

    Only an unhandled message that another session sent (not a notice) is marked; any other id
    is ignored. Returns the newly marked ids, ascending.
    """
    if not ids:
        return []
    async with conn.cursor() as cur:
        await cur.execute(
            "UPDATE messages SET handled_at = now() WHERE session_id = %s AND id = ANY(%s)"
            " AND sender_session_id IS NOT NULL AND notice IS NULL AND handled_at IS NULL"
            " RETURNING id",
            (session_id, list(ids)),
        )
        return sorted(row[0] for row in await cur.fetchall())
