"""Project room posts: one row per post, plus the fan-out messages that carry its id.

The api inserts posts and fills `recipients` in the post transaction; the api sweeper is the
only deleter. A fan-out message is an ordinary untargeted `messages` row marked with
`project_post_id`, like the chat relay's `chat_relay_send` (`db/session_messaging.py`).
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Literal

import psycopg

from agents_orchestrator.db import queue

_COLUMNS = (
    "id, project, sender_kind, sender_session_id, sender_slug, text, reply_to, addressed,"
    " recipients, created_at"
)


@dataclass(frozen=True)
class Post:
    id: int
    project: str
    sender_kind: str
    sender_session_id: uuid.UUID
    sender_slug: str
    text: str
    reply_to: int | None
    addressed: list[str]
    recipients: list[str]
    created_at: datetime


@dataclass(frozen=True)
class Recipient:
    id: uuid.UUID
    slug: str


@dataclass(frozen=True)
class ProjectMark:
    project: str
    post_id: int
    reply_to: int | None


async def insert_post(
    conn: psycopg.AsyncConnection,
    *,
    project: str,
    sender_kind: Literal["leader", "superleader"],
    sender_session_id: uuid.UUID,
    sender_slug: str,
    text: str,
    reply_to: int | None,
    addressed: list[str],
) -> int:
    """Insert a post and return its id. Raises `ValueError("reply_to")` when `reply_to` is not a
    post of the same project."""
    async with conn.transaction():
        if reply_to is not None:
            parent = await queue._one(
                conn, "SELECT project FROM project_posts WHERE id = %s", (reply_to,)
            )
            if parent is None or parent["project"] != project:
                raise ValueError("reply_to")
        row = await queue._one(
            conn,
            "INSERT INTO project_posts (project, sender_kind, sender_session_id, sender_slug,"
            " text, reply_to, addressed) VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING id",
            (project, sender_kind, sender_session_id, sender_slug, text, reply_to, addressed),
        )
        assert row is not None
        return row["id"]


async def set_recipients(conn: psycopg.AsyncConnection, post_id: int, slugs: list[str]) -> None:
    """The only UPDATE: record the slugs a fan-out row was queued for, in the post transaction
    before commit (the ids needed for the fan-out exist only after the insert)."""
    await conn.execute("UPDATE project_posts SET recipients = %s WHERE id = %s", (slugs, post_id))


async def recipients(
    conn: psycopg.AsyncConnection, project: str, sender_id: uuid.UUID
) -> list[Recipient]:
    """Active sessions of the project except the sender and any superleader
    (`spec.session_messaging`), ordered by slug."""
    rows = await queue._all(
        conn,
        "SELECT id, slug FROM sessions WHERE project = %s AND desired = 'active' AND id <> %s"
        " AND COALESCE((spec->>'session_messaging')::boolean, false) = false ORDER BY slug",
        (project, sender_id),
    )
    return [Recipient(id=r["id"], slug=r["slug"]) for r in rows]


async def enqueue_fanout(
    conn: psycopg.AsyncConnection,
    recipient_id: uuid.UUID,
    text: str,
    *,
    sender_session_id: uuid.UUID,
    sender_slug: str,
    hop: int,
    post_id: int,
) -> int:
    """Queue the post as an untargeted session message to the recipient and mark it with the
    post id in one transaction. Raises `queue.SessionBeingDeleted` like `queue.enqueue_message`."""
    async with conn.transaction():
        message_id = await queue.enqueue_message(
            conn,
            recipient_id,
            text,
            sender_session_id=sender_session_id,
            sender_slug=sender_slug,
            hop=hop,
            lane="regular",
        )
        await conn.execute(
            "UPDATE messages SET project_post_id = %s WHERE id = %s", (post_id, message_id)
        )
        return message_id


async def page(
    conn: psycopg.AsyncConnection,
    project: str,
    *,
    before_id: int | None,
    after_id: int | None,
    limit: int,
) -> list[Post]:
    """Posts of the project: newest first for `before_id` or no cursor, oldest first for
    `after_id`."""
    where, params = "project = %s", [project]
    if before_id is not None:
        where += " AND id < %s"
        params.append(before_id)
    if after_id is not None:
        where += " AND id > %s"
        params.append(after_id)
    order = "ASC" if after_id is not None else "DESC"
    rows = await queue._all(
        conn,
        f"SELECT {_COLUMNS} FROM project_posts WHERE {where} ORDER BY id {order} LIMIT %s",
        (*params, limit),
    )
    return [Post(**r) for r in rows]


async def project_mark(conn: psycopg.AsyncConnection, message_id: int) -> ProjectMark | None:
    """The room post a message fanned out, or None for any other message."""
    row = await queue._one(
        conn,
        "SELECT p.project, p.id, p.reply_to FROM messages m"
        " JOIN project_posts p ON p.id = m.project_post_id WHERE m.id = %s",
        (message_id,),
    )
    if row is None:
        return None
    return ProjectMark(project=row["project"], post_id=row["id"], reply_to=row["reply_to"])


async def count_posts_by(
    conn: psycopg.AsyncConnection, sender_session_id: uuid.UUID, seconds: float
) -> int:
    """Posts the session made in the last `seconds`, one per post whatever its fan-out."""
    return await queue._count(
        conn,
        "SELECT count(*) AS n FROM project_posts WHERE sender_session_id = %s"
        " AND created_at > now() - make_interval(secs => %s::float8)",
        (sender_session_id, seconds),
    )


async def count_project_posts(conn: psycopg.AsyncConnection, project: str, seconds: float) -> int:
    """Posts in the project in the last `seconds`."""
    return await queue._count(
        conn,
        "SELECT count(*) AS n FROM project_posts WHERE project = %s"
        " AND created_at > now() - make_interval(secs => %s::float8)",
        (project, seconds),
    )


async def sweep_older(conn: psycopg.AsyncConnection, days: int, batch: int = 1000) -> int:
    """Delete one batch of posts older than `days`; returns how many went. A newer post that
    replied to a deleted one keeps its row, `reply_to` becomes NULL."""
    cur = await conn.execute(
        "DELETE FROM project_posts WHERE id IN (SELECT id FROM project_posts"
        " WHERE created_at < now() - make_interval(days => %s) ORDER BY id LIMIT %s)",
        (days, batch),
    )
    return cur.rowcount


async def last_post_id(conn: psycopg.AsyncConnection, project: str) -> int | None:
    row = await queue._one(
        conn, "SELECT max(id) AS id FROM project_posts WHERE project = %s", (project,)
    )
    return row["id"] if row else None
