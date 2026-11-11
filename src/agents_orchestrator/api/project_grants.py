"""Extra projects of sessions and presets (T-120 slice 1): the checks and notices shared by the
grant routes (`project_grant_routes`) and the review that applies a grant (`proposals`).

A session belongs to its home `project` plus its `extra_projects`, with full membership access
in each (user answer Q5): tasks and specs (`X-Orch-Projects`), messaging peers, the project
room, `project_list`, init-scripts preset writes and `vault_list`. A preset's extra projects are
copied to every session created from it. Vault pins keep one `project:` label, the preset's home
project (secrets model invariant 7).
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence

from agents_orchestrator.api.errors import ApiError, project_error
from agents_orchestrator.db import projects
from agents_orchestrator.db.queue import SessionBeingDeleted, enqueue_message

# X-Orch-Projects carries at most 10 projects (agents-taskboard), the home one first.
MAX_EXTRA = 9
PREFIX = "projects."
NOTICE = "project_grant"


def item_path(project: str) -> str:
    return f"{PREFIX}{project}"


def project_of(path: str) -> str:
    return path.removeprefix(PREFIX)


def _too_many() -> ApiError:
    return ApiError(
        422, "too_many_projects", f"a session or preset has at most {MAX_EXTRA} extra projects"
    )


def check_count(extras: Sequence[str]) -> None:
    if len(extras) > MAX_EXTRA:
        raise _too_many()


async def require_active(conn, slug: str) -> None:
    """The project exists and is not archived (422 `unknown_project`, `project_archived`); the
    row is held FOR SHARE until the caller's transaction ends, so it is not archived meanwhile.
    Lock order: project rows before the session or preset row."""
    try:
        await projects.require_active(conn, slug, lock="share")
    except (projects.UnknownProject, projects.ProjectArchived) as exc:
        raise project_error(exc) from None


async def check_grant(conn, home: str, extras: Sequence[str], project: str) -> None:
    """A grant of `project` to a target in `home` with `extras`: 422 `own_project`,
    `already_granted`, `unknown_project`, `project_archived` or `too_many_projects`."""
    if project == home:
        raise ApiError(422, "own_project", f"project {project} is already the home project")
    if project in extras:
        raise ApiError(422, "already_granted", f"project {project} is already granted")
    await require_active(conn, project)
    check_count([*extras, project])


async def check_set(conn, home: str, extras: Sequence[str], wanted: Sequence[str]) -> list[str]:
    """The human's full set: distinct, not the home project, at most `MAX_EXTRA`; every newly
    added project must exist and be active, a kept one may have been archived since. Returns
    the set sorted."""
    if len(set(wanted)) != len(wanted):
        raise ApiError(422, "invalid_request", "extra_projects must be distinct")
    if home in wanted:
        raise ApiError(422, "own_project", f"project {home} is already the home project")
    check_count(wanted)
    for slug in sorted(set(wanted) - set(extras)):
        await require_active(conn, slug)
    return sorted(wanted)


def granted_text(project: str, by: str) -> str:
    return (
        f"Your session now also belongs to project {project} ({by}): its tasks, specs, project "
        "room and sessions are open to you as to its own members. See me and project_list."
    )


def revoked_text(project: str) -> str:
    return (
        f"Your session no longer belongs to project {project}: its tasks, specs, project room "
        "and sessions are closed to you from now on."
    )


async def notify(conn, session_id: uuid.UUID, text: str) -> None:
    """Queue the session's notice in the caller's transaction; a session being deleted gets
    none."""
    try:
        await enqueue_message(conn, session_id, text, lane="regular", notice=NOTICE)
    except SessionBeingDeleted:
        return
