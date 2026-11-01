"""The forwarding core shared by the chat relay and the admin relay (superleader-crud D10).

One call to an upstream carries only the service bearer, `X-Orch-Session` and `X-Orch-Slug` of
the caller and, for a body, `Content-Type: application/json`; never an inbound header and never
the agent token. The upstream status and body come back unchanged. A connection error, a
timeout or a non-JSON answer is a 503 with the caller's `unavailable` code. Log lines carry the
label, method, path and status only, never a token or a body.
"""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass

import httpx
from fastapi import FastAPI, Request

from agents_orchestrator.api.errors import ApiError

log = logging.getLogger(__name__)

MAX_BODY_BYTES = 256 * 1024
TIMEOUT_SECONDS = 10.0


def new_client() -> httpx.AsyncClient:
    return httpx.AsyncClient(timeout=TIMEOUT_SECONDS)


def client(app: FastAPI, attr: str) -> httpx.AsyncClient:
    """The upstream client kept in `app.state.<attr>`, created on first use (tests set it)."""
    http = getattr(app.state, attr, None)
    if http is None:
        http = new_client()
        setattr(app.state, attr, http)
    return http


async def close_client(app: FastAPI, attr: str) -> None:
    http = getattr(app.state, attr, None)
    if http is not None:
        setattr(app.state, attr, None)
        await http.aclose()


def too_large() -> ApiError:
    return ApiError(413, "payload_too_large", f"the request body exceeds {MAX_BODY_BYTES} bytes")


async def capped_body(request: Request) -> bytes:
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > MAX_BODY_BYTES:
        raise too_large()
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            raise too_large()
        chunks.append(chunk)
    return b"".join(chunks)


@dataclass(frozen=True)
class Target:
    """One upstream: where it is, which bearer it takes, and how its failures are named."""

    label: str  # the log prefix, for example "chat relay"
    base: str  # the URL the path is appended to
    token: str
    state_attr: str  # the `app.state` name of its httpx client
    unavailable: str  # the 503 code of a failed call
    timeout: float = TIMEOUT_SECONDS


async def forward(
    request: Request,
    target: Target,
    method: str,
    path: str,
    body: bytes | None,
    session: str,
    slug: str,
    *,
    empty_ok: bool = False,
) -> tuple[int, bytes]:
    """Send one call and return the upstream status and raw body.

    `path` is appended to `target.base`; the request's query string is passed on. With
    `empty_ok` an empty body is accepted (a 204), otherwise the answer must be JSON.
    """
    url = target.base.rstrip("/") + path
    query = request.scope.get("query_string", b"").decode("latin-1")
    if query:
        url += "?" + query
    headers = {
        "Authorization": f"Bearer {target.token}",
        "X-Orch-Session": session,
        "X-Orch-Slug": slug,
    }
    if body is not None:
        headers["Content-Type"] = "application/json"
    http = client(request.app, target.state_attr)
    unavailable = ApiError(503, target.unavailable, f"{target.label} upstream is unavailable")
    try:
        async with asyncio.timeout(target.timeout):
            upstream = await http.request(method, url, content=body, headers=headers)
    except (httpx.HTTPError, TimeoutError) as exc:
        log.warning("%s %s %s unavailable: %s", target.label, method, path, type(exc).__name__)
        raise unavailable from None
    if not (empty_ok and not upstream.content):
        try:
            json.loads(upstream.content)
        except ValueError:
            log.warning(
                "%s %s %s status %s: not JSON", target.label, method, path, upstream.status_code
            )
            raise unavailable from None
    log.info("%s %s %s status %s", target.label, method, path, upstream.status_code)
    return upstream.status_code, upstream.content
