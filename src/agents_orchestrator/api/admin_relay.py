"""Admin relay routes of the agent port: webhooks, schedules and chats (superleader-crud D10).

Each route maps 1:1 to an upstream admin route and is registered on the admin router, so the
superleader gate and the audit cover it. The gate runs first, then the upstream must be
configured (503 `upstream_unconfigured`), then the body is capped at 256 KiB (413
`payload_too_large`). The forwarded request carries only the upstream's service bearer and the
caller's `X-Orch-Session` and `X-Orch-Slug` (see `forward`). The upstream status and JSON body
come back unchanged. A failed call
or a non-JSON answer is 503 `upstream_unavailable`. The upstream validates the bodies.
"""

from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from urllib.parse import quote

from fastapi import Request, Response

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.admin import caller_of, router
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg

# (method, path below the service prefix, success status). The relay path is
# ADMIN_PREFIX + <relay prefix> + path, the upstream path <upstream prefix> + path.
_WEBHOOKS = [
    ("GET", "/endpoints", 200),
    ("POST", "/endpoints", 201),
    ("PATCH", "/endpoints/{id}", 200),
    ("DELETE", "/endpoints/{id}", 204),
    ("POST", "/endpoints/{id}/clone", 201),
    ("PUT", "/endpoints/{id}/filters", 200),
    ("POST", "/endpoints/{id}/rotate", 200),
    ("GET", "/events", 200),
    ("GET", "/events/{id}", 200),
    ("GET", "/meta", 200),
    ("POST", "/templates/preview", 200),
]
_SCHEDULES = [
    ("GET", "/runs", 200),
    ("GET", "/runs/{run_id}", 200),
    ("GET", "/schedules", 200),
    ("POST", "/schedules", 201),
    ("POST", "/schedules/preview", 200),
    ("GET", "/schedules/{schedule_id}", 200),
    ("PUT", "/schedules/{schedule_id}", 200),
    ("DELETE", "/schedules/{schedule_id}", 204),
    ("POST", "/schedules/{schedule_id}/run", 202),
    ("GET", "/schedules/{schedule_id}/runs", 200),
]
_CHATS = [
    ("GET", "/rooms", 200),
    ("PUT", "/rooms/{room_id}/settings", 200),
    ("GET", "/rules", 200),
    ("POST", "/rules", 201),
    ("DELETE", "/rules/{rule_id}", 204),
]
# name -> (relay prefix, upstream prefix, label, routes)
SERVICES: dict[str, tuple[str, str, str, list[tuple[str, str, int]]]] = {
    "webhook": ("/webhooks", "/webhooks/api/v1", "webhook admin relay", _WEBHOOKS),
    "schedule": ("/schedules", "/schedules/api/v1", "schedule admin relay", _SCHEDULES),
    "connector": ("/chats", "/chat/v1/admin", "chat admin relay", _CHATS),
}
STATE_ATTR = "admin_http"
_BODY_METHODS = {"POST", "PUT", "PATCH"}


def _segment(value: str) -> str:
    # Dots are encoded too, so `.` and `..` stay one segment instead of walking up the path.
    return quote(value, safe="").replace(".", "%2E")


def _fill(template: str, params: dict[str, str]) -> str:
    return re.sub(r"\{([^}]+)\}", lambda m: _segment(params[m.group(1)]), template)


def _handler(service: str, method: str, template: str) -> Callable[..., Awaitable[Response]]:
    _, upstream_prefix, label, _ = SERVICES[service]

    async def relay(request: Request, settings: Cfg) -> Response:
        _, session = caller_of(request)
        upstream = settings.admin_upstream(service)
        if upstream is None:
            raise ApiError(503, "upstream_unconfigured", f"no {service} upstream is configured")
        body = None
        if method in _BODY_METHODS:
            body = await forward_core.capped_body(request) or None
        target = forward_core.Target(
            label, upstream[0], upstream[1], STATE_ATTR, "upstream_unavailable"
        )
        path = upstream_prefix + _fill(template, dict(request.path_params))
        status, content = await forward_core.forward(
            request, target, method, path, body, str(session.id), session.slug, empty_ok=True
        )
        return Response(content, status, media_type="application/json" if content else None)

    relay.__name__ = "relay_" + service + "_" + method.lower() + _slug(template)
    return relay


def _slug(template: str) -> str:
    return re.sub(r"\W+", "_", template).rstrip("_")


def _register() -> None:
    for service, (relay_prefix, _, _, routes) in SERVICES.items():
        for method, template, success in routes:
            router.add_api_route(
                relay_prefix + template,
                _handler(service, method, template),
                methods=[method],
                status_code=success,
                response_class=Response,
                operation_id="admin_" + service + "_" + method.lower() + _slug(template),
                summary=f"{method} {service} {template}",
                tags=["admin"],
            )


_register()
