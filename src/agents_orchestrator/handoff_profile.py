"""The handoff profile of an agent: `work` (default) or `chat`.

A coding hive that posts to a chat once, or answers a connector message, is a chat agent for
the tenure; the sparse preset field `handoff_profile` overrides the detection either way.
"""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Literal

import psycopg

from agents_orchestrator.db import chat_activity

Profile = Literal["work", "chat"]
_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


async def detect(
    conn: psycopg.AsyncConnection,
    session: Mapping[str, Any],
    agent_id: uuid.UUID,
    tenure_start: datetime | None,
) -> Profile:
    """`session` is a row with `id` and `spec` (a dict). Without a tenure start every recorded
    activity of the session counts."""
    override = (session.get("spec") or {}).get("handoff_profile")
    if override in ("work", "chat"):
        return override
    since = tenure_start if tenure_start is not None else _EPOCH
    if await chat_activity.seen_since(conn, session["id"], agent_id, since):
        return "chat"
    return "work"
