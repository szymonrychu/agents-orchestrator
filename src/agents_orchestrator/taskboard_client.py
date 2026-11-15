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
from agents_orchestrator.runaway import TASK_LABEL
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

# the taskboard path prefix; `api/tasks.py` (the relay) builds its target from it too
UPSTREAM_PREFIX = "/taskboard/api/v1"
TIMEOUT_SECONDS = 3.0
# credential notices: one fixed actor, because a client_key is unique per creator
NOTICE_SESSION = "orchestrator-credential-watch"
NOTICE_SLUG = "orchestrator"
NOTICE_LABEL = "credential"


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


async def create_notice_task(
    settings: Settings,
    session_id: uuid.UUID,
    slug: str,
    projects: str,
    *,
    title: str,
    body: str,
    client_key: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bool:
    """Create the owner notice task of a runaway park; `client_key` makes a retry a no-op on the
    taskboard. True when the task exists (201 or 200), False when it could not be created."""
    if not settings.taskboard_available:
        return False
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + "/tasks"
    headers = {
        "Authorization": f"Bearer {settings.taskboard_token}",
        "X-Orch-Session": str(session_id),
        "X-Orch-Slug": slug,
        PROJECTS_HEADER: projects,
    }
    payload = {"title": title, "body": body, "labels": [TASK_LABEL], "client_key": client_key}
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.post(url, json=payload, headers=headers)
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("taskboard notice task unavailable: %s", type(exc).__name__)
        return False
    if response.status_code not in (200, 201):
        log.warning("taskboard notice task: status %s", response.status_code)
        return False
    return True


async def post_notice(
    settings: Settings,
    client_key: str,
    title: str,
    body: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bool:
    """Create one credential notice task in `credential_notice_project`; `client_key` makes a
    retry a no-op on the taskboard. True on 200 or 201, never raises; logs carry the status or the
    exception type only."""
    project = settings.credential_notice_project
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + "/tasks"
    headers = {
        "Authorization": f"Bearer {settings.taskboard_token}",
        "X-Orch-Session": NOTICE_SESSION,
        "X-Orch-Slug": NOTICE_SLUG,
        PROJECTS_HEADER: project,
    }
    payload = {
        "title": title,
        "body": body,
        "kind": "task",
        "project": project,
        "labels": [NOTICE_LABEL],
        "client_key": client_key,
    }
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.post(url, json=payload, headers=headers)
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("taskboard credential notice unavailable: %s", type(exc).__name__)
        return False
    if response.status_code not in (200, 201):
        log.warning("taskboard credential notice: status %s", response.status_code)
        return False
    return True


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
