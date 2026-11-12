"""Admin relay routes of the agent port: webhooks, schedules, chats and history (D10).

Each route maps 1:1 to an upstream admin route and is registered on the admin router, so the
superleader gate and the audit cover it. The gate runs first, then the upstream must be configured
(503 `upstream_unconfigured`), then the body is capped at 256 KiB (413 `payload_too_large`). The
forwarded request carries only the upstream's service bearer and the caller's `X-Orch-Session`,
`X-Orch-Slug` and `X-Orch-Projects` (see `forward`). The relay is superleader-only, so
`X-Orch-Projects` is always `*` (`scope.scope_of`, projects plan 3.3). The upstream status and JSON
body come back unchanged, except for webhook credentials (D9, `credentials` below). A failed call or
a non-JSON answer is 503 `upstream_unavailable`. The upstream validates the bodies.

The superleader READS every object (decision 388, T-120): GET routes are forwarded as is and
lists are not filtered (objects without a preset are listed too). WRITES (every method other than
GET) are guarded: `GUARDS` holds one `Guard` per route, and a body naming a preset the superleader
may not manage (`superleader_manageable` false, null or unknown), or a stored object (endpoint,
schedule) on such a preset or on none, is 403 `not_manageable` and never forwarded. A body
without `preset_id` passes: the upstream requires it on create and PUT, and the stored check covers
PATCH and clone. A stored object the upstream does not list is 404.

A preset with `kube_cluster_access`, `session_messaging` or `docker` on is a valid target of a
relayed trigger object (endpoint, schedule, room, rule) as long as the superleader may
manage it: the superleader starts sessions from such presets, so it may point triggers at them.
Editing a privileged preset itself stays human-only (`api/presets.py`).

Chat rooms are the exception (user decision Q1, matrix-integration, 2026-11-04): the superleader
manages ALL rooms whatever their preset, so the room list, room settings and room rules carry no
preset check. A chat routing rule that opens routing (an allow rule created, or a deny rule
removed) is checked by kind (`_rule_room`): only a `room` rule on a room the upstream lists
passes. A rule of another kind covers rooms on any preset, present and future, and is 403
`human_only_field` (`fields: ["kind"]`); an unlisted room, or an unknown rule, is 404. A deny rule
created or an allow rule removed only narrows routing and is forwarded as is. Rule values are
personal identifiers and never appear in an error.

Room requests (`POST /chats/room-requests`) never queue without the user's yes: the relay
checks the task on the taskboard, forwards, then asks and attaches a decision as the calling
session, and `POST /chats/room-requests/{request_id}/confirm` settles the request from the
answer (`api/chat_requests.py`).

History is read-only for the superleader: three GET routes and no DELETE (deleting a stored
session is for the user, through the web UI), so the router answers any other method 405 and
nothing reaches the upstream. The session list is paged upstream by `before_id`; when a page was
full the relay sends the upstream's next cursor in `X-Next-Before-Id`.

Usage is read-only too (`GET /usage/history`, `GET /usage/sessions` of agents-usage, plan usage
windows and session marks); any other method is 405.

The vault list (`GET /agent/v1/admin/vault`, `vault_list`) is read-only too (any other method is
405) and answers every entry, metadata only for other projects: an entry whose labels do not match
the caller session's projects (`labels_match` against `project:<project>`; an entry without a
`project:` label counts as the caller's, as attach allows it) has every env `value: null`. An
entry's `attachments` list every preset, each with `privileged` and `manageable`; an env of the
caller's own entry shows its readable value only when attached to a manageable preset, otherwise
its name and `protected` flag with `value: null`. `to_reattach` lists, per preset, the env names a
user still has to re-attach, with `manageable`.
"""

from __future__ import annotations

import json
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal
from urllib.parse import quote

from agents_common.labels import PROJECT_PREFIX, labels_match
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from pydantic import SecretStr

from agents_orchestrator.api import chat_requests, secret_store, vault_client, vault_refs
from agents_orchestrator.api import forward as forward_core
from agents_orchestrator.api.admin import FORBIDDEN, caller_of, not_manageable, router, touched
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.scope import projects_header
from agents_orchestrator.api.sessions import Cfg
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
    ("GET", "/rooms/{room_id}", 200),
    ("PUT", "/rooms/{room_id}/settings", 200),
    ("POST", "/rooms/{room_id}/allow", 200),
    ("GET", "/users", 200),
    ("POST", "/room-requests", 201),
    ("GET", "/room-requests/{request_id}", 200),
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
# Read only: plan usage windows and session marks (agents-usage).
_USAGE = [("GET", "/history", 200), ("GET", "/sessions", 200)]
# name -> (relay prefix, upstream prefix, label, routes)
SERVICES: dict[str, tuple[str, str, str, list[tuple[str, str, int]]]] = {
    "webhook": ("/webhooks", "/webhooks/api/v1", "webhook admin relay", _WEBHOOKS),
    "schedule": ("/schedules", "/schedules/api/v1", "schedule admin relay", _SCHEDULES),
    "connector": ("/chats", "/chat/v1/admin", "chat admin relay", _CHATS),
    "history": ("/history", "/history/v1", "history admin relay", _HISTORY),
    "usage": ("/usage", "/usage/v1", "usage admin relay", _USAGE),
}
STATE_ATTR = "admin_http"


@dataclass(frozen=True)
class Guard:
    """What the superleader's remit changes on one relay route (see the module docstring). The
    preset checks apply to writes only (a method other than GET); reads are never filtered."""

    body_preset: bool = False
    stored: Literal["endpoint", "schedule"] | None = None
    cursor: bool = False  # a full list page also answers the upstream's next `before_id`
    rule: Literal["body", "stored"] | None = None  # a chat routing rule, see `_rule_room`
    ask_decision: bool = False  # a 201 room request asks the user's yes (`chat_requests`)


_E, _S = "endpoint", "schedule"
GUARDS: dict[tuple[str, str, str], Guard] = {
    ("webhook", "GET", "/endpoints"): Guard(),
    ("webhook", "POST", "/endpoints"): Guard(body_preset=True),
    ("webhook", "PATCH", "/endpoints/{id}"): Guard(body_preset=True, stored=_E),
    ("webhook", "DELETE", "/endpoints/{id}"): Guard(stored=_E),
    ("webhook", "POST", "/endpoints/{id}/clone"): Guard(body_preset=True, stored=_E),
    ("webhook", "PUT", "/endpoints/{id}/filters"): Guard(stored=_E),
    ("webhook", "POST", "/endpoints/{id}/rotate"): Guard(stored=_E),
    ("webhook", "GET", "/events"): Guard(),
    ("webhook", "GET", "/events/{id}"): Guard(),
    ("webhook", "GET", "/meta"): Guard(),
    ("webhook", "POST", "/templates/preview"): Guard(),
    ("schedule", "GET", "/runs"): Guard(),
    ("schedule", "GET", "/runs/{run_id}"): Guard(),
    ("schedule", "GET", "/schedules"): Guard(),
    ("schedule", "POST", "/schedules"): Guard(body_preset=True),
    ("schedule", "POST", "/schedules/preview"): Guard(body_preset=True),
    ("schedule", "GET", "/schedules/{schedule_id}"): Guard(),
    ("schedule", "PUT", "/schedules/{schedule_id}"): Guard(body_preset=True, stored=_S),
    ("schedule", "DELETE", "/schedules/{schedule_id}"): Guard(stored=_S),
    ("schedule", "POST", "/schedules/{schedule_id}/run"): Guard(stored=_S),
    ("schedule", "GET", "/schedules/{schedule_id}/runs"): Guard(),
    ("connector", "GET", "/rooms"): Guard(),
    ("connector", "GET", "/rooms/{room_id}"): Guard(),
    ("connector", "PUT", "/rooms/{room_id}/settings"): Guard(),
    ("connector", "POST", "/rooms/{room_id}/allow"): Guard(),
    ("connector", "GET", "/users"): Guard(),
    ("connector", "POST", "/room-requests"): Guard(ask_decision=True),
    ("connector", "GET", "/room-requests/{request_id}"): Guard(),
    ("connector", "GET", "/rules"): Guard(),
    ("connector", "POST", "/rules"): Guard(rule="body"),
    ("connector", "DELETE", "/rules/{rule_id}"): Guard(rule="stored"),
    ("history", "GET", "/sessions"): Guard(cursor=True),
    ("history", "GET", "/sessions/{session_id}"): Guard(),
    ("history", "GET", "/sessions/{session_id}/records"): Guard(),
    ("usage", "GET", "/history"): Guard(),
    ("usage", "GET", "/sessions"): Guard(),
}
# stored object kind -> (upstream list path below the service prefix, id field, path parameter)
_STORED = {
    "endpoint": ("/endpoints", "id", "id"),
    "schedule": ("/schedules", "id", "schedule_id"),
}
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


async def _writable(request: Request) -> set[str]:
    """The ids of the presets the superleader may manage (write), read once per request."""
    cached = getattr(request.state, "writable_presets", None)
    if cached is None:
        async with request.app.state.pool.connection() as conn:
            cached = {str(i) for i in await presets_repo.manageable_ids(conn)}
        request.state.writable_presets = cached
    return cached


def upstream_target(settings: Settings, service: str) -> forward_core.Target:
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
        upstream_target(settings, service),
        "GET",
        SERVICES[service][1] + path,
        None,
        str(session.id),
        session.slug,
        extra_headers=projects_header(session),
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


async def _stored_preset(
    request: Request, settings: Settings, service: str, guard: Guard
) -> str | None:
    """The preset id of the object a route writes (None when it has none); an object the
    upstream does not list is 404."""
    list_path, id_field, param = _STORED[guard.stored]
    wanted = request.path_params[param]
    items = await _upstream_json(_bare(request), settings, service, list_path)
    for item in items:
        if isinstance(item, dict) and str(item.get(id_field)) == wanted:
            preset = item.get("preset_id")
            return None if preset is None else str(preset)
    raise not_found("object")


def human_only_rule() -> ApiError:
    return ApiError(
        403,
        "human_only_field",
        "only a human can open chat routing beyond one room: an allow rule, or the removal of a "
        "deny rule, of a kind other than room",
        extra={"fields": ["kind"]},
    )


async def _stored_rule(request: Request, settings: Settings) -> dict[str, Any]:
    """The rule a route acts on, its `list` taken from the upstream list that holds it; an
    unknown rule is 404."""
    rules = await _upstream_json(_bare(request), settings, "connector", "/rules")
    wanted = request.path_params["rule_id"]
    for list_ in ("allow", "deny"):
        items = rules.get(list_) if isinstance(rules, dict) else None
        for item in items if isinstance(items, list) else []:
            if isinstance(item, dict) and str(item.get("id")) == wanted:
                return {**item, "list": list_}
    raise not_found("object")


async def _rule_room(
    request: Request, settings: Settings, guard: Guard, body: bytes | None
) -> None:
    """Check a chat routing rule that opens routing (a deny rule created or an allow rule removed
    only narrows it and passes). Any list other than exactly `deny` counts as allow and any kind
    other than exactly `room` as a wider one (fail closed); the room must be listed (any preset)."""
    if guard.rule == "body":
        rule = _body_object(body)
        if rule.get("list") == "deny":
            return
    else:
        rule = await _stored_rule(request, settings)
        if rule["list"] != "deny":
            return
    if rule.get("kind") != "room":
        request.state.preset_fields = ["kind"]
        raise human_only_rule()
    rooms = await _upstream_json(_bare(request), settings, "connector", "/rooms")
    for room in rooms if isinstance(rooms, list) else []:
        if isinstance(room, dict) and room.get("room_id") == rule.get("value"):
            return
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


def _body_hidden(body: bytes | None, writable: set[str]) -> bool:
    """True when a JSON body names a preset outside the writable set (null counts as outside)."""
    if "preset_id" not in _body_object(body):
        return False
    preset = _body_preset(body)
    return preset is None or preset not in writable


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
        target = upstream_target(settings, service)
        body = None
        if method in _BODY_METHODS:
            body = await forward_core.capped_body(request) or None
        if method != "GET":
            if guard.body_preset and _body_hidden(body, await _writable(request)):
                raise not_manageable("preset")
            if guard.stored:
                stored = await _stored_preset(request, settings, service, guard)
                if stored is None or stored not in await _writable(request):
                    raise not_manageable("object")
        if guard.rule:
            await _rule_room(request, settings, guard, body)
        if guard.ask_decision:
            await chat_requests.check_task(request, settings, body)
        path = upstream_prefix + _fill(template, dict(request.path_params))
        status, content = await forward_core.forward(
            request,
            target,
            method,
            path,
            body,
            str(session.id),
            session.slug,
            empty_ok=True,
            extra_headers=projects_header(session),
        )
        if guard.ask_decision and status == 201:
            row = await chat_requests.on_created(request, settings, json.loads(content))
            return JSONResponse(row, status)
        headers = _next_cursor(request, guard, status, content)
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


def _relay_responses(service: str, method: str, template: str) -> dict[int | str, Any]:
    """The OpenAPI entries of one relay route: the cursor header, and for the usage reads the
    gate's 403 and the 503 of an unconfigured or unreachable upstream."""
    responses = _cursor_responses(service, method, template)
    if service == "usage":
        responses[403] = FORBIDDEN
        responses[503] = {
            "description": "upstream_unconfigured or upstream_unavailable: the usage service"
        }
    return responses


def _register() -> None:
    for service, (relay_prefix, _, _, routes) in SERVICES.items():
        for method, template, success in routes:
            router.add_api_route(
                relay_prefix + template,
                _handler(service, method, template),
                methods=[method],
                status_code=success,
                response_class=Response,
                responses=_relay_responses(service, method, template),
                operation_id="admin_" + service + "_" + method.lower() + _slug(template),
                summary=f"{method} {service} {template}",
                tags=["admin"],
            )


_VAULT_SHAPE = ApiError(502, "upstream_shape", "the vault upstream answered an unexpected shape")


def _scoped_entry(entry: Any, own_labels: list[str], writable: set[str], marked: set[str]) -> dict:
    """The entry as the superleader may see it; none is dropped. Attachments list every preset,
    each marked `privileged` (in `marked`) and `manageable` (in `writable`). An entry whose
    labels do not match `own_labels` (the caller's own projects) belongs to another project, so
    every env value is None; a matching entry keeps a readable value only when it is attached to
    a manageable preset."""
    if not isinstance(entry, dict):
        raise _VAULT_SHAPE
    labels = entry.get("labels")
    own = isinstance(labels, list) and labels_match(
        (x for x in labels if isinstance(x, str)), own_labels
    )
    attachments = entry.get("attachments")
    manageable_attached = False
    if isinstance(attachments, list):
        attachments = [
            {
                **a,
                "privileged": str(a.get("preset_id")) in marked,
                "manageable": str(a.get("preset_id")) in writable,
            }
            for a in attachments
            if isinstance(a, dict)
        ]
        manageable_attached = any(a["manageable"] for a in attachments)
        entry = {**entry, "attachments": attachments}
    elif attachments is not None:
        raise _VAULT_SHAPE
    if not own:
        envs = [{**e, "value": None} if isinstance(e, dict) else e for e in entry.get("envs") or []]
        entry = {**entry, "envs": envs}
    elif not manageable_attached:
        entry = {
            **entry,
            "envs": [
                {**e, "value": None} if isinstance(e, dict) and not e.get("protected") else e
                for e in entry.get("envs") or []
            ],
        }
    return entry


@router.get(
    "/vault",
    response_class=Response,
    operation_id="admin_vault_list",
    summary="GET vault entries",
    tags=["admin"],
)
async def vault_list(request: Request, q: str | None = None) -> Response:
    """Every vault entry (values only for the caller's projects) and the env names to
    re-attach."""
    _, session = caller_of(request)
    client = request.app.state.vault
    if client is None:
        raise ApiError(503, "upstream_unconfigured", "no vault upstream is configured")
    try:
        listed = await client.entries(q)
    except vault_client.VaultShape:
        raise _VAULT_SHAPE from None
    except vault_client.VaultUnavailable:
        raise ApiError(503, "upstream_unavailable", "the vault upstream is unavailable") from None
    except vault_client.VaultRefused:
        raise vault_refs.unavailable() from None
    writable = await _writable(request)
    project_labels = [f"{PROJECT_PREFIX}{p}" for p in session.projects]
    async with request.app.state.pool.connection() as conn:
        presets = await presets_repo.list_presets(conn)
    marked = {str(p.id) for p in presets if privileged(p.spec)}
    entries = [_scoped_entry(x, project_labels, writable, marked) for x in listed]
    to_reattach = [
        {
            "preset_id": str(p.id),
            "preset_name": p.name,
            "envs": sorted(p.vault_reattach),
            "manageable": str(p.id) in writable,
        }
        for p in presets
        if p.vault_reattach
    ]
    return JSONResponse({"entries": entries, "to_reattach": to_reattach})


_register()
