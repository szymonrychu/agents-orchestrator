"""Admin relay routes of the agent port: webhooks, schedules and chats (superleader-crud D10).

Each route maps 1:1 to an upstream admin route and is registered on the admin router, so the
superleader gate and the audit cover it. The gate runs first, then the upstream must be
configured (503 `upstream_unconfigured`), then the body is capped at 256 KiB (413
`payload_too_large`). The forwarded request carries only the upstream's service bearer and the
caller's `X-Orch-Session` and `X-Orch-Slug` (see `forward`). The upstream status and JSON body
come back unchanged, except for webhook credentials (D9, `credentials` below). A failed call
or a non-JSON answer is 503 `upstream_unavailable`. The upstream validates the bodies.
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from typing import Any
from urllib.parse import quote

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import SecretStr

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api import secret_store
from agents_orchestrator.api.admin import caller_of, router, touched
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg

LOCAL_TOOL_HEADER = "x-orch-tool"
CREDENTIAL_KEYS = ("password", "secret")

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
# routes whose answer may carry a credential the upstream issues once
_ISSUING = {
    ("POST", "/endpoints"),
    ("PATCH", "/endpoints/{id}"),
    ("POST", "/endpoints/{id}/clone"),
    ("POST", "/endpoints/{id}/rotate"),
}
_BODY_METHODS = {"POST", "PUT", "PATCH"}


def _segment(value: str) -> str:
    # Dots are encoded too, so `.` and `..` stay one segment instead of walking up the path.
    return quote(value, safe="").replace(".", "%2E")


def _fill(template: str, params: dict[str, str]) -> str:
    return re.sub(r"\{([^}]+)\}", lambda m: _segment(params[m.group(1)]), template)


def _strip(payload: Any) -> dict[str, Any]:
    """Remove the credential fields from a JSON answer (a dict or a list of dicts) in place and
    return what was removed (non-null values only)."""
    found: dict[str, Any] = {}
    items = payload if isinstance(payload, list) else [payload]
    for item in items:
        if isinstance(item, dict):
            for key in CREDENTIAL_KEYS:
                if key in item:
                    value = item.pop(key)
                    if value is not None:
                        found[key] = value
    return found


async def sanitise(
    request: Request, settings: Cfg, status: int, content: bytes, issuing: bool
) -> Any:
    """Apply D9 to a webhook answer and return the JSON to send on.

    A credential is stripped from any answer. On a 2xx answer of an issuing route it is first
    stored in the Secret `webhook-<id>-password`, and the answer carries the reference
    (`password_secret`, `key`) in its place. With `X-Orch-Tool: local` the answer also carries
    `credential: {key, value}` once, for the local tool to write to a file. A credential
    anywhere else is dropped and the call fails, so it never reaches the caller.
    """
    payload = json.loads(content)
    found = _strip(payload)
    if not found:
        return payload
    if not (issuing and 200 <= status < 300 and isinstance(payload, dict)):
        raise ApiError(503, "upstream_unavailable", "unexpected credential in an upstream answer")
    endpoint_id = payload.get("id")
    if not isinstance(endpoint_id, int):
        raise ApiError(503, "upstream_unavailable", "an issued credential came without an id")
    key = "password" if "password" in found else "secret"
    try:
        name = await secret_store.put_webhook_credential(
            secret_store.kube_of(request),
            settings.namespace,
            endpoint_id,
            key,
            SecretStr(found[key]),
        )
    except secret_store.SecretStoreError as exc:
        raise ApiError(
            503,
            "credential_not_stored",
            f"{exc}; the endpoint changed upstream, rotate it to get a new credential",
        ) from None
    touched(request, name)
    payload.update({"password_secret": name, "key": key})
    if request.headers.get(LOCAL_TOOL_HEADER, "").lower() == "local":
        payload["credential"] = {"key": key, "value": found[key]}
    return payload


def _handler(service: str, method: str, template: str) -> Callable[..., Awaitable[Response]]:
    _, upstream_prefix, label, _ = SERVICES[service]
    issuing = service == "webhook" and (method, template) in _ISSUING

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
        if service == "webhook":
            if content:
                payload = await sanitise(request, settings, status, content, issuing)
                return JSONResponse(payload, status)
            if method == "DELETE" and 200 <= status < 300:
                endpoint_id = request.path_params.get("id", "")
                if endpoint_id.isdigit():
                    await secret_store.delete_webhook_credential(
                        secret_store.kube_of(request), settings.namespace, int(endpoint_id)
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
