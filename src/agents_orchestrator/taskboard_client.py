"""Small taskboard calls of the orchestrator itself: open decisions of a session (the generated
leader state), owner notice tasks (runaway park, credential expiry, context-pressure escalation),
a session's claimed task and a comment on it.

Short, best-effort calls with the same identity headers the taskboard relay sends. Only
`claimed_task` raises (`TaskboardUnavailable`), so an outage is never read as "no task"; the
others give `None` or False and the caller falls back or retries. Log lines carry the status
only, never the token or a body.
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


class TaskboardUnavailable(Exception):
    """The taskboard could not be read (no body, no token in the message)."""


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
    headers = _session_headers(settings, session_id, slug, projects)
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
    labels: list[str] | None = None,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bool:
    """Create an owner notice task as the session (a runaway park, a context-pressure
    escalation); `client_key` makes a retry a no-op on the taskboard, `labels` default to the
    runaway guard's. True when the task exists (201 or 200), False when it could not be
    created."""
    if not settings.taskboard_available:
        return False
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + "/tasks"
    headers = _session_headers(settings, session_id, slug, projects)
    payload = {
        "title": title,
        "body": body,
        "labels": [TASK_LABEL] if labels is None else labels,
        "client_key": client_key,
    }
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


def _session_headers(
    settings: Settings, session_id: uuid.UUID, slug: str, projects: str
) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {settings.taskboard_token}",
        "X-Orch-Session": str(session_id),
        "X-Orch-Slug": slug,
        PROJECTS_HEADER: projects,
    }


async def claimed_task(
    settings: Settings,
    session_id: uuid.UUID,
    slug: str,
    projects: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> str | None:
    """The key of an active task the session is the assignee of (its claimed task), the first
    the taskboard lists; None when it has none. Raises `TaskboardUnavailable` when the list
    cannot be read, so a caller never mistakes an outage for "no task"."""
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + "/tasks"
    headers = _session_headers(settings, session_id, slug, projects)
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.get(url, params={"session": str(session_id)}, headers=headers)
        if not response.is_success:
            raise TaskboardUnavailable(f"status {response.status_code}")
        for task in response.json()["items"]:
            if task.get("assignee_session") == str(session_id):
                return str(task["key"])
        return None
    except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise TaskboardUnavailable(type(exc).__name__) from None


async def claimed_tasks(
    settings: Settings,
    session_id: uuid.UUID,
    slug: str,
    projects: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> list[dict[str, Any]]:
    """Every active task the session is the assignee of, as `{id, title, status, todo}` with
    `todo` a list of `{text, status}`. Same route and filter as `claimed_task`
    (`GET /tasks?session=`); raises `TaskboardUnavailable` when the list cannot be read."""
    if not settings.taskboard_available:
        raise TaskboardUnavailable("not configured")
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + "/tasks"
    headers = _session_headers(settings, session_id, slug, projects)
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.get(url, params={"session": str(session_id)}, headers=headers)
        if not response.is_success:
            raise TaskboardUnavailable(f"status {response.status_code}")
        return [
            {
                "id": str(task["key"]),
                "title": str(task.get("title") or ""),
                "status": str(task.get("status") or ""),
                "todo": [
                    {"text": str(t.get("text") or ""), "status": str(t.get("status") or "")}
                    for t in task.get("todo") or []
                ],
            }
            for task in response.json()["items"]
            if task.get("assignee_session") == str(session_id)
        ]
    except (httpx.HTTPError, ValueError, KeyError, TypeError, AttributeError) as exc:
        raise TaskboardUnavailable(type(exc).__name__) from None


async def add_comment(
    settings: Settings,
    session_id: uuid.UUID,
    slug: str,
    projects: str,
    task_key: str,
    body: str,
    *,
    transport: httpx.AsyncBaseTransport | None = None,
) -> bool:
    """Comment on `task_key` as the session; True on 201 (or 200), never raises."""
    url = settings.taskboard_url.rstrip("/") + UPSTREAM_PREFIX + f"/tasks/{task_key}/comments"
    headers = _session_headers(settings, session_id, slug, projects)
    try:
        async with httpx.AsyncClient(timeout=TIMEOUT_SECONDS, transport=transport) as http:
            response = await http.post(url, json={"body": body}, headers=headers)
    except (httpx.HTTPError, ValueError) as exc:
        log.warning("taskboard comment unavailable: %s", type(exc).__name__)
        return False
    if response.status_code not in (200, 201):
        log.warning("taskboard comment: status %s", response.status_code)
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
