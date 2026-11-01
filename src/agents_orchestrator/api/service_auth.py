"""Service-to-service bearer tokens on the dedicated service port (D10).

In-cluster trigger services (agents-webhook, agents-schedule) call the orchestrator on
ORCH_SERVICE_PORT with `Authorization: Bearer <token>`. Only the sha256 of each token is
configured (ORCH_SERVICE_TOKENS), compared in constant time against every entry. The port
serves a fixed list of method and path pairs and never a WebSocket; the token value never
reaches a log line or an error body.
"""

from __future__ import annotations

import hashlib
import hmac
import logging
import re
from collections.abc import Mapping

from starlette.requests import Request
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from agents_orchestrator.api.allowlist import EXEMPT_PATHS, _port
from agents_orchestrator.api.errors import error_response

log = logging.getLogger(__name__)

# Segments match only the id format each route accepts, so `..`, a stray separator or a
# trailing newline never reaches the router; patterns are applied with fullmatch.
_UUID = r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}"
_INT = r"[0-9]+"

ALLOWED_ROUTES: tuple[tuple[str, re.Pattern[str]], ...] = tuple(
    (method, re.compile(path.replace("{uuid}", _UUID).replace("{int}", _INT)))
    for method, path in (
        ("GET", "/api/v1/meta"),
        ("GET", "/api/v1/presets"),
        ("GET", "/api/v1/presets/{uuid}"),
        ("GET", "/api/v1/sessions"),
        ("POST", "/api/v1/sessions"),
        ("GET", "/api/v1/sessions/{uuid}"),
        ("GET", "/api/v1/sessions/{uuid}/messages"),
        ("POST", "/api/v1/sessions/{uuid}/messages"),
        ("GET", "/api/v1/messages/{int}"),
        ("GET", "/api/v1/stream"),
    )
)

HEALTH_METHODS = frozenset({"GET", "HEAD"})

# Request-state key under which the gate hands the authenticated service name to the routes.
STATE_KEY = "service"


def calling_service(request: Request) -> str | None:
    """The service name the gate authenticated for this request; None for any request that did not
    come through the service port (a user on the main port)."""
    return getattr(request.state, STATE_KEY, None)


def route_allowed(method: str, path: str) -> bool:
    return any(m == method and p.fullmatch(path) for m, p in ALLOWED_ROUTES)


def _bearer(scope: Scope) -> str | None:
    for name, value in scope.get("headers", []):
        if name.lower() == b"authorization":
            scheme, _, token = value.decode("latin-1").partition(" ")
            token = token.strip()
            return token if scheme.lower() == "bearer" and token else None
    return None


class ServiceGate:
    """Bearer service-token gate, active only for connections accepted on `service_port`."""

    def __init__(self, app: ASGIApp, service_port: int, token_hashes: Mapping[str, str]):
        self.app = app
        self.service_port = service_port
        self.hashes = [(name, digest.encode()) for name, digest in token_hashes.items()]

    def authenticate(self, token: str) -> str | None:
        """The service name whose hash matches; every entry is compared, no early exit."""
        presented = hashlib.sha256(token.encode()).hexdigest().encode()
        found = None
        for name, digest in self.hashes:
            if hmac.compare_digest(presented, digest) and found is None:
                found = name
        return found

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] not in ("http", "websocket") or _port(scope) != self.service_port:
            await self.app(scope, receive, send)
            return
        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 1008})
            return
        method, path = scope["method"], scope.get("path", "")
        if path in EXEMPT_PATHS and method in HEALTH_METHODS:
            await self.app(scope, receive, send)
            return
        token = _bearer(scope)
        service = self.authenticate(token) if token is not None else None
        if service is None:
            log.info("service request service=- method=%s path=%r status=401", method, path)
            response = error_response(
                401,
                "unauthorized",
                "a valid service token is required",
                {"WWW-Authenticate": "Bearer"},
            )
            await response(scope, receive, send)
            return
        if not route_allowed(method, path):
            log.info(
                "service request service=%s method=%s path=%r status=403", service, method, path
            )
            response = error_response(403, "forbidden", "route is not served on the service port")
            await response(scope, receive, send)
            return

        scope.setdefault("state", {})[STATE_KEY] = service

        async def logged_send(message: Message) -> None:
            if message["type"] == "http.response.start":
                log.info(
                    "service request service=%s method=%s path=%r status=%s",
                    service,
                    method,
                    path,
                    message["status"],
                )
            await send(message)

        await self.app(scope, receive, logged_send)
