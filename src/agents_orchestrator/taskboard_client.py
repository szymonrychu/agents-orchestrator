"""Open decisions of a session from the taskboard, for the generated leader state.

One short, best-effort GET with the same identity headers the taskboard relay sends. It never
raises: an unconfigured, slow, failing or malformed taskboard gives `None` and the caller prints a
fallback line, so a handover notice is never blocked on it. Log lines carry the status only,
never the token or a body.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from typing import Any

import httpx

from agents_orchestrator.api.scope import PROJECTS_HEADER
from agents_orchestrator.api.tasks import UPSTREAM_PREFIX
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

TIMEOUT_SECONDS = 3.0


@dataclass(frozen=True)
class OpenDecision:
    task_key: str
    decision_id: str
    question: str


async def open_decisions(
    settings: Settings,
    session_id: uuid.UUID,
    slug: str,
    projects: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[OpenDecision] | None:
    """Decisions with status `open` asked by `session_id`, or None when they cannot be read."""
    if not settings.taskboard_available:
        return None
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + "/tasks"
    headers = {
        "Authorization": f"Bearer {settings.taskboard_token}",
        "X-Orch-Session": str(session_id),
        "X-Orch-Slug": slug,
        PROJECTS_HEADER: projects,
    }
    params = {"session": str(session_id), "waiting": "true"}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.get(url, params=params, headers=headers)
        if not response.is_success:
            log.warning("taskboard open decisions: status %s", response.status_code)
            return None
        return _open_decisions(response.json(), str(session_id))
    except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError) as exc:
        log.warning("taskboard open decisions unavailable: %s", type(exc).__name__)
        return None


def _open_decisions(body: dict[str, Any], session: str) -> list[OpenDecision]:
    found: list[OpenDecision] = []
    for task in body["items"]:
        for decision in task.get("decisions") or []:
            if decision.get("status") == "open" and decision.get("asking_session") == session:
                found.append(
                    OpenDecision(
                        str(task["key"]), str(decision["id"]), str(decision.get("question") or "")
                    )
                )
    return found
