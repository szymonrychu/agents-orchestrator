"""Admin relay routes of the agent port: webhooks, schedules, chats and history (D10).

Each route maps 1:1 to an upstream admin route and is registered on the admin router, so the
superleader gate and the audit cover it. The gate runs first, then the upstream must be
configured (503 `upstream_unconfigured`), then the body is capped at 256 KiB (413
`payload_too_large`). The forwarded request carries only the upstream's service bearer and the
caller's `X-Orch-Session` and `X-Orch-Slug` (see `forward`). The upstream status and JSON body
come back unchanged, except for webhook credentials (D9, `credentials` below). A failed call
or a non-JSON answer is 503 `upstream_unavailable`. The upstream validates the bodies.

The superleader sees only objects of presets it may manage (`superleader_manageable`). `GUARDS`
holds one `Guard` per route: a body naming a hidden or null preset, or a stored object
(endpoint, schedule, room) on a hidden preset, is 404 and never forwarded. A body without
`preset_id` passes: the upstream requires it on create and PUT, and the stored check covers
PATCH and clone. Lists are filtered (a non-list answer is 502 `upstream_shape`) and a single
event or run of a hidden preset is 404. An object without a preset counts as hidden. Run
objects are mapped to their preset through one upstream `GET /schedules`.

A write that creates, changes, clones or runs a webhook endpoint or schedule is 403
`privileged_target` when the body's preset or the stored object's preset has `kube_access`,
`session_messaging` or `docker` on (after the 404 checks, before anything is forwarded).

History is read-only for the superleader: three GET routes and no DELETE (deleting a stored
session is for the user, through the web UI), so the router answers any other method 405 and
nothing reaches the upstream. The session list is paged upstream by `before_id`, and agents-history
has no set filter, so the relay filters the page it got and, when that page was full, sends the
upstream's next cursor in `X-Next-Before-Id` (also on an empty page, so a caller can tell "keep
paging" from "end"). The records of a session are checked against one upstream lookup of that
single session (`GET /sessions/{session_id}`): hidden, null-preset or unknown is 404 and the
records are never requested.
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import SecretStr

from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api import secret_store
from agents_orchestrator.api.admin import caller_of, router, touched
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.sessions import Cfg, privileged_refused
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.preset_policy import privileged
from agents_orchestrator.settings import Settings

LOCAL_TOOL_HEADER = "x-orch-tool"
NEXT_CURSOR_HEADER = "X-Next-Before-Id"
HISTORY_DEFAULT_LIMIT = 50  # the page size agents-history uses without `limit`
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
# Read and search only: the superleader has no delete path (user decision, Q6).
_HISTORY = [
    ("GET", "/sessions", 200),
    ("GET", "/sessions/{session_id}", 200),
    ("GET", "/sessions/{session_id}/records", 200),
]
# name -> (relay prefix, upstream prefix, label, routes)
SERVICES: dict[str, tuple[str, str, str, list[tuple[str, str, int]]]] = {
    "webhook": ("/webhooks", "/webhooks/api/v1", "webhook admin relay", _WEBHOOKS),
    "schedule": ("/schedules", "/schedules/api/v1", "schedule admin relay", _SCHEDULES),
    "connector": ("/chats", "/chat/v1/admin", "chat admin relay", _CHATS),
    "history": ("/history", "/history/v1", "history admin relay", _HISTORY),
}
STATE_ATTR = "admin_http"


@dataclass(frozen=True)
class Guard:
    """What the superleader's remit changes on one relay route (see the module docstring)."""

    body_preset: bool = False
    stored: Literal["endpoint", "schedule", "room", "history_session"] | None = None
    list_filter: Literal["preset_id", "schedule_id"] | None = None
    answer: Literal["preset_id", "schedule_id"] | None = None
    cursor: bool = False  # a full list page also answers the upstream's next `before_id`
    privileged: bool = False  # a privileged target preset (body or stored) is 403


_E, _S, _R = "endpoint", "schedule", "room"
GUARDS: dict[tuple[str, str, str], Guard] = {
    ("webhook", "GET", "/endpoints"): Guard(list_filter="preset_id"),
    ("webhook", "POST", "/endpoints"): Guard(body_preset=True, privileged=True),
    ("webhook", "PATCH", "/endpoints/{id}"): Guard(body_preset=True, stored=_E, privileged=True),
    ("webhook", "DELETE", "/endpoints/{id}"): Guard(stored=_E),
    ("webhook", "POST", "/endpoints/{id}/clone"): Guard(
        body_preset=True, stored=_E, privileged=True
    ),
    ("webhook", "PUT", "/endpoints/{id}/filters"): Guard(stored=_E, privileged=True),
    ("webhook", "POST", "/endpoints/{id}/rotate"): Guard(stored=_E, privileged=True),
    ("webhook", "GET", "/events"): Guard(list_filter="preset_id"),
    ("webhook", "GET", "/events/{id}"): Guard(answer="preset_id"),
    ("webhook", "GET", "/meta"): Guard(),
    ("webhook", "POST", "/templates/preview"): Guard(),
    ("schedule", "GET", "/runs"): Guard(list_filter="schedule_id"),
    ("schedule", "GET", "/runs/{run_id}"): Guard(answer="schedule_id"),
    ("schedule", "GET", "/schedules"): Guard(list_filter="preset_id"),
    ("schedule", "POST", "/schedules"): Guard(body_preset=True, privileged=True),
    ("schedule", "POST", "/schedules/preview"): Guard(body_preset=True),
    ("schedule", "GET", "/schedules/{schedule_id}"): Guard(stored=_S),
    ("schedule", "PUT", "/schedules/{schedule_id}"): Guard(
        body_preset=True, stored=_S, privileged=True
    ),
    ("schedule", "DELETE", "/schedules/{schedule_id}"): Guard(stored=_S),
    ("schedule", "POST", "/schedules/{schedule_id}/run"): Guard(stored=_S, privileged=True),
    ("schedule", "GET", "/schedules/{schedule_id}/runs"): Guard(stored=_S),
    ("connector", "GET", "/rooms"): Guard(list_filter="preset_id"),
    ("connector", "PUT", "/rooms/{room_id}/settings"): Guard(body_preset=True, stored=_R, privileged=True),
    ("connector", "GET", "/rules"): Guard(),
    ("connector", "POST", "/rules"): Guard(),
    ("connector", "DELETE", "/rules/{rule_id}"): Guard(),
    ("history", "GET", "/sessions"): Guard(list_filter="preset_id", cursor=True),
    ("history", "GET", "/sessions/{session_id}"): Guard(answer="preset_id"),
    ("history", "GET", "/sessions/{session_id}/records"): Guard(stored="history_session"),
}
# stored object kind -> (upstream list path below the service prefix, id field, path parameter)
_STORED = {
    "endpoint": ("/endpoints", "id", "id"),
    "schedule": ("/schedules", "id", "schedule_id"),
    "room": ("/rooms", "room_id", "room_id"),
}
# stored object kind -> (upstream path of the one object below the service prefix, path
# parameter that fills it): kinds whose list is paged, so the object is looked up on its own
_STORED_ONE = {"history_session": ("/sessions/{session_id}", "session_id")}
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


async def _visible(request: Request) -> set[str]:
    """The ids of the presets the superleader may manage, read once per request."""
    cached = getattr(request.state, "visible_presets", None)
    if cached is None:
        async with request.app.state.pool.connection() as conn:
            cached = {str(i) for i in await presets_repo.manageable_ids(conn)}
        request.state.visible_presets = cached
    return cached


def _target(settings: Settings, service: str) -> forward_core.Target:
    upstream = settings.admin_upstream(service)
    if upstream is None:
        raise ApiError(503, "upstream_unconfigured", f"no {service} upstream is configured")
    return forward_core.Target(
        SERVICES[service][2], upstream[0], upstream[1], STATE_ATTR, "upstream_unavailable"
    )


async def _upstream_json(request: Request, settings: Settings, service: str, path: str) -> Any:
    """GET `path` (below the service prefix) as the caller and return its JSON answer."""
    _, session = caller_of(request)
    status, content = await forward_core.forward(
        request,
        _target(settings, service),
        "GET",
        SERVICES[service][1] + path,
        None,
        str(session.id),
        session.slug,
    )
    if status == 404:
        raise not_found("object")
    if not 200 <= status < 300:
        raise ApiError(503, "upstream_unavailable", f"the {service} upstream answered {status}")
    return json.loads(content)


def _bare(request: Request) -> Request:
    """The request without its query string, so a lookup never inherits the caller's filters."""
    scope = {**request.scope, "query_string": b""}
    return Request(scope, request.receive)


async def _schedule_presets(request: Request, settings: Settings) -> dict[str, Any]:
    items = await _upstream_json(_bare(request), settings, "schedule", "/schedules")
    return {str(i.get("id")): i.get("preset_id") for i in items if isinstance(i, dict)}


async def _privileged(request: Request) -> dict[str, list[str]]:
    """Manageable preset id -> its privileged flags, for presets with any on; read once per
    request."""
    cached = getattr(request.state, "privileged_presets", None)
    if cached is None:
        async with request.app.state.pool.connection() as conn:
            specs = await presets_repo.manageable_specs(conn)
        cached = {str(i): f for i, spec in specs.items() if (f := privileged(spec))}
        request.state.privileged_presets = cached
    return cached


async def _stored_preset(
    request: Request, settings: Settings, service: str, guard: Guard
) -> str | None:
    """The preset id of the object a route acts on (None when it has none); an object the
    upstream does not list is 404."""
    if guard.stored in _STORED_ONE:
        template, param = _STORED_ONE[guard.stored]
        path = _fill(template, {param: request.path_params[param]})
        one = await _upstream_json(_bare(request), settings, service, path)
        preset = one.get("preset_id") if isinstance(one, dict) else None
        return None if preset is None else str(preset)
    list_path, id_field, param = _STORED[guard.stored]
    wanted = request.path_params[param]
    items = await _upstream_json(_bare(request), settings, service, list_path)
    for item in items:
        if isinstance(item, dict) and str(item.get(id_field)) == wanted:
            preset = item.get("preset_id")
            return None if preset is None else str(preset)
    raise not_found("object")


def _body_object(body: bytes | None) -> dict[str, Any]:
    """The JSON object a body holds; {} when absent, not JSON (the upstream answers 422) or
    not an object."""
    try:
        data = json.loads(body) if body else None
    except ValueError:
        return {}
    return data if isinstance(data, dict) else {}


def _body_preset(body: bytes | None) -> str | None:
    """The body's `preset_id` as a string; None when absent or null."""
    preset = _body_object(body).get("preset_id")
    return None if preset is None else str(preset)


def _body_hidden(body: bytes | None, visible: set[str]) -> bool:
    """True when a JSON body names a preset outside the visible set (null counts as hidden)."""
    if "preset_id" not in _body_object(body):
        return False
    preset = _body_preset(body)
    return preset is None or preset not in visible


async def _filter_answer(
    request: Request, settings: Settings, guard: Guard, status: int, content: bytes
) -> bytes:
    """Apply the list filter or single-object check of `guard` to a 2xx JSON answer."""
    if not (200 <= status < 300 and content and (guard.list_filter or guard.answer)):
        return content
    visible = await _visible(request)
    payload = json.loads(content)
    field = guard.list_filter or guard.answer
    mapping = None
    if field == "schedule_id":
        mapping = await _schedule_presets(request, settings)

    def shown(item: Any) -> bool:
        if not isinstance(item, dict):
            return False
        preset = mapping.get(str(item.get("schedule_id"))) if mapping is not None else None
        if mapping is None:
            preset = item.get("preset_id")
        return preset is not None and str(preset) in visible

    if guard.list_filter:
        if not isinstance(payload, list):
            raise ApiError(502, "upstream_shape", "upstream list answer is not a list")
        return json.dumps([i for i in payload if shown(i)]).encode()
    if not shown(payload):
        raise not_found("object")
    return content


def _next_cursor(request: Request, guard: Guard, status: int, content: bytes) -> dict[str, str]:
    """`X-Next-Before-Id` for a list route with a cursor: the smallest id of the upstream page,
    taken before the preset filter, when that page was full (more may follow)."""
    if not (guard.cursor and 200 <= status < 300 and content):
        return {}
    page = json.loads(content)
    if not isinstance(page, list):
        return {}
    limit = request.query_params.get("limit", "")
    size = int(limit) if limit.isascii() and limit.isdigit() else HISTORY_DEFAULT_LIMIT
    ids = [i["id"] for i in page if isinstance(i, dict) and type(i.get("id")) is int]
    if not ids or len(page) < size:
        return {}
    return {NEXT_CURSOR_HEADER: str(min(ids))}


def _handler(service: str, method: str, template: str) -> Callable[..., Awaitable[Response]]:
    _, upstream_prefix, _, _ = SERVICES[service]
    issuing = service == "webhook" and (method, template) in _ISSUING
    guard = GUARDS[(service, method, template)]

    async def relay(request: Request, settings: Cfg) -> Response:
        _, session = caller_of(request)
        target = _target(settings, service)
        body = None
        if method in _BODY_METHODS:
            body = await forward_core.capped_body(request) or None
        if guard.body_preset and _body_hidden(body, await _visible(request)):
            raise not_found("preset")
        stored = None
        if guard.stored:
            stored = await _stored_preset(request, settings, service, guard)
            if stored is None or stored not in await _visible(request):
                raise not_found("object")
        if guard.privileged:
            flagged = await _privileged(request)
            flags = {f for p in (_body_preset(body), stored) if p for f in flagged.get(p, [])}
            if flags:
                request.state.preset_fields = sorted(flags)
                raise privileged_refused(sorted(flags))
        path = upstream_prefix + _fill(template, dict(request.path_params))
        status, content = await forward_core.forward(
            request, target, method, path, body, str(session.id), session.slug, empty_ok=True
        )
        headers = _next_cursor(request, guard, status, content)
        content = await _filter_answer(request, settings, guard, status, content)
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
        return Response(
            content, status, headers, media_type="application/json" if content else None
        )

    relay.__name__ = "relay_" + service + "_" + method.lower() + _slug(template)
    return relay


def _slug(template: str) -> str:
    return re.sub(r"\W+", "_", template).rstrip("_")


def _cursor_responses(service: str, method: str, template: str) -> dict[int | str, Any]:
    """The OpenAPI entry that documents `X-Next-Before-Id` on a list route with a cursor."""
    if not GUARDS[(service, method, template)].cursor:
        return {}
    header = {
        "description": "Smallest id of the upstream page when it was full: pass it as `before_id`",
        "schema": {"type": "string"},
    }
    return {200: {"description": "Successful Response", "headers": {NEXT_CURSOR_HEADER: header}}}


def _register() -> None:
    for service, (relay_prefix, _, _, routes) in SERVICES.items():
        for method, template, success in routes:
            router.add_api_route(
                relay_prefix + template,
                _handler(service, method, template),
                methods=[method],
                status_code=success,
                response_class=Response,
                responses=_cursor_responses(service, method, template),
                operation_id="admin_" + service + "_" + method.lower() + _slug(template),
                summary=f"{method} {service} {template}",
                tags=["admin"],
            )


_register()
