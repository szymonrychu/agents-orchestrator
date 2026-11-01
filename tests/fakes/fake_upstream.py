"""ASGI fake of an admin upstream (agents-webhook, agents-schedule, the connector's agent
listener), held to the committed OpenAPI document vendored beside it.

Pass the document path and a bearer token. The fake answers 401 to a missing or wrong bearer,
validates every request body against the document (422 `{"detail": [...]}` when it fails), takes
the answer from `answers` (a `(METHOD, template)` key mapped to `(status, body)` or a callable
`(Call, params) -> (status, body)`), and raises `AssertionError` when it would send an answer the
document does not describe, so drift fails the test that triggered it. It records every call.
"""

from __future__ import annotations

import hmac
import json
import re
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from starlette.types import Receive, Scope, Send

HERE = Path(__file__).resolve().parent
HEALTH = ("/healthz", "/readyz")


def load(name: str) -> dict[str, Any]:
    return json.loads((HERE / f"{name}_openapi.json").read_text(encoding="utf-8"))


def source_line(name: str) -> str:
    return (HERE / f"{name}_openapi.source").read_text(encoding="utf-8")


def operations(doc: dict[str, Any]) -> dict[tuple[str, str], dict[str, Any]]:
    """The document's operations without the health probes, keyed by (METHOD, template)."""
    return {
        (method.upper(), path): op
        for path, item in doc["paths"].items()
        if path not in HEALTH
        for method, op in item.items()
    }


@dataclass
class Call:
    method: str
    path: str
    headers: dict[str, str]
    body: bytes
    query: str = ""

    def json(self) -> Any:
        return json.loads(self.body)


Answer = tuple[int, Any] | Callable[[Call, dict[str, str]], tuple[int, Any]]


@dataclass
class FakeUpstream:
    doc: dict[str, Any]
    token: str
    answers: dict[tuple[str, str], Answer] = field(default_factory=dict)
    calls: list[Call] = field(default_factory=list)

    def __post_init__(self) -> None:
        self._ops = operations(self.doc)

    def _errors(self, schema: dict[str, Any], body: Any) -> list[str]:
        # the document's components ride along, so "#/components/..." refs resolve at any depth
        rooted = {**schema, "components": self.doc["components"]}
        validator = Draft202012Validator(rooted, format_checker=FormatChecker())
        return [e.message for e in validator.iter_errors(body)]

    def request_errors(self, key: tuple[str, str], body: Any) -> list[str]:
        content = self._ops[key].get("requestBody", {}).get("content", {})
        schema = content.get("application/json", {}).get("schema")
        return [f"{key} takes no JSON body"] if schema is None else self._errors(schema, body)

    def response_errors(self, key: tuple[str, str], status: int, body: Any) -> list[str]:
        response = self._ops[key]["responses"].get(str(status))
        if response is None:
            return [f"{key} documents no {status} answer"]
        content = response.get("content", {}).get("application/json")
        if content is None:
            return [] if body is None else [f"{key} {status} documents no body"]
        return self._errors(content["schema"], body)

    def _route(self, call: Call) -> tuple[tuple[str, str], dict[str, str]]:
        for (method, template), _ in self._ops.items():
            names = re.findall(r"\{([^}]+)\}", template)
            match = re.fullmatch(re.sub(r"\{[^}]+\}", "([^/]+)", template), call.path)
            if method == call.method and match:
                return (method, template), dict(zip(names, match.groups(), strict=True))
        raise AssertionError(f"the vendored document has no {call.method} {call.path}")

    def _authorised(self, call: Call) -> bool:
        scheme, _, token = call.headers.get("authorization", "").partition(" ")
        return scheme.lower() == "bearer" and hmac.compare_digest(
            token.strip().encode(), self.token.encode()
        )

    def _handle(self, call: Call) -> tuple[int, Any]:
        if not self._authorised(call):
            return 401, {"detail": "unauthorized"}
        key, params = self._route(call)
        if call.body:
            try:
                body = call.json()
            except ValueError:
                return 422, {"detail": [{"msg": "the body is not JSON"}]}
            problems = self.request_errors(key, body)
            if problems:
                return 422, {"detail": [{"msg": "; ".join(problems)}]}
        answer = self.answers.get(key)
        if answer is None:
            raise AssertionError(f"the test gave no answer for {key}")
        status, payload = answer(call, params) if callable(answer) else answer
        problems = self.response_errors(key, status, payload)
        if problems:
            raise AssertionError(f"{key} {status} breaks the document: {problems}")
        return status, payload

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
        query = scope.get("query_string", b"").decode("latin-1")
        call = Call(scope["method"], scope["path"], headers, body, query)
        self.calls.append(call)
        status, payload = self._handle(call)
        raw = b"" if payload is None else json.dumps(payload).encode()
        out = [(b"content-length", str(len(raw)).encode())]
        if raw:
            out.append((b"content-type", b"application/json"))
        await send({"type": "http.response.start", "status": status, "headers": out})
        await send({"type": "http.response.body", "body": raw})


def sample(doc: dict[str, Any], schema: dict[str, Any]) -> Any:
    """A minimal instance of `schema` (required properties only), for canned valid answers."""
    if "$ref" in schema:
        node: Any = doc
        for part in schema["$ref"].removeprefix("#/").split("/"):
            node = node[part]
        return sample(doc, node)
    for combiner in ("anyOf", "oneOf"):
        if combiner in schema:
            options = [o for o in schema[combiner] if o.get("type") != "null"]
            return sample(doc, options[0] if options else schema[combiner][0])
    if "enum" in schema:
        return schema["enum"][0]
    if "const" in schema:
        return schema["const"]
    kind = schema.get("type")
    if kind == "object" or "properties" in schema:
        props = schema.get("properties", {})
        return {k: sample(doc, props[k]) for k in schema.get("required", []) if k in props}
    if kind == "array":
        return []
    if kind == "integer":
        return max(schema.get("minimum", 1), 1)
    if kind == "number":
        return 1.0
    if kind == "boolean":
        return True
    if kind == "null":
        return None
    fmt = schema.get("format")
    if fmt == "date-time":
        return "2026-11-01T00:00:00Z"
    if fmt == "uuid":
        return "00000000-0000-4000-8000-000000000001"
    return "x" * max(schema.get("minLength", 1), 1)


def success_sample(fake: FakeUpstream, key: tuple[str, str]) -> tuple[int, Any]:
    """The lowest 2xx answer of the operation with a minimal valid body."""
    responses = fake._ops[key]["responses"]
    status = min(int(s) for s in responses if s.startswith("2"))
    content = responses[str(status)].get("content", {}).get("application/json")
    return status, (sample(fake.doc, content["schema"]) if content else None)
