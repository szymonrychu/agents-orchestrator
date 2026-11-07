"""ASGI fake of the connector's `/chat/v1` agent listener, held to its committed OpenAPI document.

`chat_openapi.json` is the connector's `openapi-agent.json` vendored at the commit named in
`chat_openapi.source`. The fake validates every request body against the document's request
schemas (a body that fails is answered 422 `invalid_request`, as the real listener refuses it)
and every answer it sends against the document's response schemas for that route and status; an
answer or a route the document does not describe raises `AssertionError` inside the fake, so
drift fails the test that triggered it instead of passing silently. It records every call and
can be told to refuse a route with a documented error status.

Use it as the upstream of the relay: `httpx.AsyncClient(transport=httpx.ASGITransport(app=
fake.app))`. The helpers `request_errors` and `response_errors` check any body against the same
document from the outside.
"""

from __future__ import annotations

import hmac
import json
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from referencing.jsonschema import DRAFT202012
from starlette.types import Receive, Scope, Send

HERE = Path(__file__).resolve().parent
DOC_PATH = HERE / "chat_openapi.json"
SOURCE_PATH = HERE / "chat_openapi.source"
DOC: dict[str, Any] = json.loads(DOC_PATH.read_text(encoding="utf-8"))
PREFIX = "/chat/v1"
# the vendored document also holds the connector's admin and internal routes; the fake serves
# only the agent listener's routes
NOT_AGENT_PREFIXES = (PREFIX + "/admin/", PREFIX + "/internal/")
DEFAULT_ROOM = "!dm:hs"
BASE_URI = "urn:chat-openapi"
_REGISTRY = Registry().with_resource(
    BASE_URI, Resource.from_contents(DOC, default_specification=DRAFT202012)
)
_SESSION_HEADER = "x-orch-session"


def operations() -> dict[tuple[str, str], dict[str, Any]]:
    """The document's chat operations, keyed by (METHOD, path template)."""
    return {
        (method.upper(), path): op
        for path, item in DOC["paths"].items()
        if path.startswith(PREFIX + "/") and not path.startswith(NOT_AGENT_PREFIXES)
        for method, op in item.items()
    }


def _template_regex(template: str) -> re.Pattern[str]:
    return re.compile("^" + re.sub(r"\{[^}]+\}", "([^/]+)", template) + "$")


def _validator(schema: dict[str, Any]) -> Draft202012Validator:
    # a $ref into the whole document, so nested $refs resolve against it
    ref = schema["$ref"].replace("#", BASE_URI + "#", 1) if "$ref" in schema else None
    wrapped = {"$ref": ref} if ref else schema
    return Draft202012Validator(wrapped, registry=_REGISTRY, format_checker=FormatChecker())


def _errors(schema: dict[str, Any], body: Any) -> list[str]:
    return [
        f"{'/'.join(str(p) for p in e.absolute_path) or '<body>'}: {e.message}"
        for e in sorted(_validator(schema).iter_errors(body), key=lambda e: list(e.path))
    ]


def _operation(method: str, template: str) -> dict[str, Any]:
    try:
        return operations()[(method, template)]
    except KeyError:
        raise AssertionError(f"the vendored document has no {method} {template}") from None


def request_errors(method: str, template: str, body: Any) -> list[str]:
    """Why `body` is not a valid request body of the route; empty when it is."""
    content = _operation(method, template).get("requestBody", {}).get("content", {})
    schema = content.get("application/json", {}).get("schema")
    if schema is None:
        return [f"{method} {template} takes no JSON body"]
    return _errors(schema, body)


def _response_schema(method: str, template: str, status: int) -> dict[str, Any] | None:
    response = _operation(method, template)["responses"].get(str(status))
    if response is None:
        return None
    return response["content"]["application/json"]["schema"]


def response_errors(method: str, template: str, status: int, body: Any) -> list[str]:
    """Why `body` is not the documented answer of the route at `status`; empty when it is."""
    schema = _response_schema(method, template, status)
    if schema is None:
        return [f"{method} {template} documents no {status} answer"]
    return _errors(schema, body)


@dataclass
class Call:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes
    query: str = ""

    def json(self) -> Any:
        return json.loads(self.body)


@dataclass
class _Refusal:
    status: int
    code: str
    message: str
    extra: dict[str, Any] | None = None


@dataclass
class FakeChat:
    """The chat upstream: `.app` (ASGI), `.calls`, `.refuse(route, status, code)`.

    A route is `"METHOD /chat/v1/path"` with the template of the document, for example
    `"GET /chat/v1/messages/{id}"`. `.rooms` is the answer of the room listing; `.sends` maps the
    ids it handed out to the send each one queued.
    """

    token: str
    calls: list[Call] = field(default_factory=list)
    rooms: list[dict[str, Any]] = field(
        default_factory=lambda: [
            {
                "room_id": DEFAULT_ROOM,
                "name": "Owner",
                "kind": "dm",
                "bound": False,
                "agents_may_initiate": True,
            }
        ]
    )
    sends: dict[str, dict[str, Any]] = field(default_factory=dict)
    _refusals: dict[tuple[str, str, str | None], _Refusal] = field(default_factory=dict)

    # ---------------------------------------------------------------------------- control

    def _key(self, route: str) -> tuple[str, str]:
        method, _, template = route.partition(" ")
        key = (method.upper(), template)
        if key not in operations():
            raise ValueError(f"the vendored document has no route {route!r}")
        return key

    def refuse(
        self,
        route: str,
        status: int,
        code: str,
        message: str = "refused",
        *,
        extra: dict[str, Any] | None = None,
        session: str | None = None,
    ) -> None:
        """Answer every authenticated call of `route` with the documented error `status`.

        `extra` is merged into the error object (for example `{"occupier": "<uuid>"}`). With
        `session` only calls whose `X-Orch-Session` equals it are refused; a refusal for a session
        wins over the route-wide one.
        """
        key = self._key(route)
        if not 400 <= status <= 599 or _response_schema(*key, status) is None:
            raise ValueError(f"{route} documents no {status} error answer")
        self._refusals[(*key, session)] = _Refusal(status, code, message, extra)

    def allow(self, route: str, session: str | None = None) -> None:
        """Lift the refusal of `route` (the one for `session` when given)."""
        self._refusals.pop((*self._key(route), session), None)

    # --------------------------------------------------------------------------- the app

    async def app(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            return
        body = b""
        while True:
            message = await receive()
            body += message.get("body", b"")
            if not message.get("more_body"):
                break
        headers = {k.decode("latin-1").lower(): v.decode("latin-1") for k, v in scope["headers"]}
        call = Call(
            scope["method"],
            scope["path"],
            headers,
            body,
            scope.get("query_string", b"").decode("latin-1"),
        )
        self.calls.append(call)
        status, payload = self._handle(call)
        raw = json.dumps(payload).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(raw)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": raw})

    def _route(self, call: Call) -> tuple[tuple[str, str], re.Match[str]]:
        for (method, template), _ in operations().items():
            match = _template_regex(template).match(call.path)
            if method == call.method and match:
                return (method, template), match
        raise AssertionError(f"the vendored document has no {call.method} {call.path}")

    def _handle(self, call: Call) -> tuple[int, Any]:
        key, match = self._route(call)
        if not self._authorised(call):
            return self._error(key, 401, "unauthorized", "a valid agent token is required")
        try:
            session = uuid.UUID(call.headers.get(_SESSION_HEADER, ""))
        except ValueError:
            return self._error(
                key, 401, "unauthorized", "X-Orch-Session: a session uuid is required"
            )
        refusal = self._refusals.get((*key, str(session))) or self._refusals.get((*key, None))
        if refusal is not None:
            return self._error(key, refusal.status, refusal.code, refusal.message, refusal.extra)
        method, template = key
        if method == "POST":
            return self._post(key, call, session)
        if template.endswith("/rooms"):
            return self._answer(key, 200, {"rooms": self.rooms})
        return self._status(key, match.group(1), session)

    def _authorised(self, call: Call) -> bool:
        scheme, _, token = call.headers.get("authorization", "").partition(" ")
        return scheme.lower() == "bearer" and hmac.compare_digest(
            token.strip().encode(), self.token.encode()
        )

    def _post(self, key: tuple[str, str], call: Call, session: uuid.UUID) -> tuple[int, Any]:
        try:
            body = call.json()
        except ValueError:
            return self._error(key, 422, "invalid_request", "the body is not JSON")
        problems = request_errors(*key, body)
        if problems:
            return self._error(key, 422, "invalid_request", "; ".join(problems))
        kind = "message" if key[1].endswith("/messages") else "reaction"
        send_id = f"s{len(self.sends) + 1}"
        self.sends[send_id] = {
            "session": session,
            "kind": kind,
            "room_id": body.get("room") or DEFAULT_ROOM,
            "created_at": datetime.now(UTC).isoformat(),
        }
        return self._answer(key, 202, {"id": send_id, "status": "queued"})

    def _status(self, key: tuple[str, str], send_id: str, session: uuid.UUID) -> tuple[int, Any]:
        found = self.sends.get(send_id)
        if found is None or found["session"] != session:
            return self._error(key, 404, "not_found", f"no send {send_id!r} of this session")
        return self._answer(
            key,
            200,
            {
                "id": send_id,
                "kind": found["kind"],
                "status": "queued",
                "reason": None,
                "room_id": found["room_id"],
                "created_at": found["created_at"],
                "sent_at": None,
            },
        )

    def _error(
        self,
        key: tuple[str, str],
        status: int,
        code: str,
        message: str,
        extra: dict[str, Any] | None = None,
    ) -> tuple[int, Any]:
        """An error answer in whichever documented shape the route gives this status."""
        schema = _response_schema(*key, status)
        if schema is not None and schema["$ref"].endswith("/HTTPValidationError"):
            body: Any = {"detail": [{"loc": ["path", "id"], "msg": message, "type": code}]}
        else:
            body = {"error": {"code": code, "message": message, **(extra or {})}}
        return self._answer(key, status, body)

    def _answer(self, key: tuple[str, str], status: int, body: Any) -> tuple[int, Any]:
        """Check the answer against the document before it leaves the fake."""
        problems = response_errors(*key, status, body)
        if problems:
            raise AssertionError(f"{key[0]} {key[1]} {status} breaks the document: {problems}")
        return status, body
