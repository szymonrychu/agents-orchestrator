"""The request-free core of the chat relay, shared by the agent routes (`api/chat.py`) and the
envelope relay worker (`envelope_outbox/worker.py`).

One call forwards to the chat upstream as the caller's session (`X-Orch-Session`, `X-Orch-Slug`,
`X-Orch-Agent`), hands a `room_bound` conflict to the occupier relay when asked, stamps the
session's last update on a 2xx and records an outbound chat activity. It takes the already
resolved session, so the leader check, the configured check and the body cap stay with the caller.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

import httpx

from agents_orchestrator import chat_client
from agents_orchestrator.api import chat_relay
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.sessions import Pool
from agents_orchestrator.db import chat_activity
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

AGENT_HEADER = "X-Orch-Agent"  # the calling agent, whose workdir holds `files`


def chat_target(
    settings: Settings, timeout: float = forward_core.TIMEOUT_SECONDS
) -> forward_core.Target:
    return forward_core.Target(
        "chat relay",
        settings.chat_url.rstrip("/") + chat_client.UPSTREAM_PREFIX,
        settings.chat_token,
        "chat_http",
        "chat_unavailable",
        timeout,
    )


async def relay_call(
    pool: Pool,
    http: httpx.AsyncClient,
    settings: Settings,
    target: forward_core.Target,
    own: Session,
    agent_id: uuid.UUID,
    method: str,
    path: str,
    body: bytes | None,
    *,
    query: str | None = None,
    counts_as_update: bool = False,
    relay_bound: bool = False,
    records_activity: bool = False,
    retry_after: list[str] | None = None,
) -> tuple[int, bytes]:
    """Forward one call and return the upstream status and body (the occupier never in it).

    Raises `ApiError` like `forward_http` (503 unavailable) and `chat_relay.relay`.
    """
    status, content = await forward_core.forward_http(
        http,
        target,
        method,
        path,
        body,
        str(own.id),
        own.slug,
        query=query,
        extra_headers={AGENT_HEADER: str(agent_id)},
        retry_after=retry_after,
    )
    occupier = chat_relay.occupier_of(status, content) if relay_bound else None
    if occupier is not None:
        assert body is not None
        response = await chat_relay.relay(
            http, pool, settings, target, agent_id, own, body, occupier, content
        )
        status, content = response.status_code, bytes(response.body)
    else:
        content = chat_relay.without_occupier(content)
    if counts_as_update:
        await forward_core.touch_on_success(pool, own.id, status)
    if records_activity:
        await record_out(pool, own.id, agent_id, status)
    return status, content


async def record_out(pool: Pool, session_id: Any, agent_id: Any, status: int) -> None:
    """Record an outbound chat activity of the caller after a 2xx. Best effort like
    `touch_on_success`: the post already happened, so a failed write is only logged."""
    if not 200 <= status < 300:
        return
    try:
        async with pool.connection() as conn:
            await chat_activity.record(conn, session_id, agent_id, "out")
    except Exception:
        log.exception("chat relay: could not record activity of session %s", session_id)
