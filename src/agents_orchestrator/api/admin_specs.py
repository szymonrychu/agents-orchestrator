"""Spec deletion and suggestion dismissal for a superleader (T-120 slice 3): the two taskboard
spec calls the agent relay (`specs.py`) refuses. Each forwards through the same relay as the
caller's session, so the taskboard gets `X-Orch-Projects: *` (superleader scope) and the answer
comes back unchanged. Only `/specs/S-<n>` and `/specs/S-<n>/comments/<n>/dismiss` pass the
allowlist (else 404 `not_found` without an upstream call). Accepting a suggestion stays
user-only. Nothing is stored or logged here beyond the admin audit row."""

from __future__ import annotations

import re

from fastapi import Request, Response

from agents_orchestrator.api.admin import caller_of, router
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.api.tasks import _RESPONSES, relay, taskboard_target

_DELETE = re.compile(r"/specs/S-[0-9]+")
_DISMISS = re.compile(r"/specs/S-[0-9]+/comments/[0-9]+/dismiss")
_FORBIDDEN = {
    "description": "not_superleader: the caller is not a superleader; or the taskboard's forbidden"
}


async def _forward(
    request: Request, pool: Pool, settings: Cfg, path: str, allowed: re.Pattern[str]
) -> Response:
    agent, _ = caller_of(request)
    return await relay(
        request,
        agent,
        pool,
        settings,
        path,
        allowed,
        target=taskboard_target(settings),
        available=settings.taskboard_available,
        unconfigured="taskboard_unconfigured",
        mine=False,
    )


@router.delete(
    "/specs/{key}",
    summary="Delete a spec through the taskboard, as every project",
    responses={**_RESPONSES, 204: {"description": "deleted"}, 403: _FORBIDDEN},
)
async def delete_spec(key: str, request: Request, pool: Pool, settings: Cfg) -> Response:
    return await _forward(request, pool, settings, f"/specs/{key}", _DELETE)


@router.post(
    "/specs/{key}/comments/{comment_id}/dismiss",
    summary="Dismiss a spec suggestion through the taskboard, as every project",
    responses={**_RESPONSES, 403: _FORBIDDEN},
)
async def dismiss_suggestion(
    key: str, comment_id: str, request: Request, pool: Pool, settings: Cfg
) -> Response:
    path = f"/specs/{key}/comments/{comment_id}/dismiss"
    return await _forward(request, pool, settings, path, _DISMISS)
