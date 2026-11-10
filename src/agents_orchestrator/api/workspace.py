"""The one seam through which the api calls wrapper pods (user decision N1, amends D5).

This is the only api module allowed to import `agents_orchestrator.wrapper` or
`agents_orchestrator.tokens` (tests/test_api_sessions.py pins it). The file and terminal proxy
routes resolve an agent with `resolve_agent`, validate a terminal id with
`validate_terminal_id`, and call the wrapper with `forward`; the file listing, read and write
share `_file_call`, and the write and the terminal POST and DELETE carry the Origin guard
(`guarded`). A written file's content travels only in the request body and is never logged
(the `forward` debug lines carry the method and path only). The file download relays the
wrapper's raw byte stream (`download_file`); the terminal attach route relays a WebSocket to
the wrapper with `websockets` (`attach_terminal`). `forward` and the download share one
wrapper error map (`_wrapper_errors`). There is no Kubernetes import:
agents come from the database and the token is derived from the master key.

Token hygiene: the derived bearer lives in `WrapperTarget.token` (excluded from repr) and
travels only as the `Authorization` header set by `WrapperClient` or on the upstream
WebSocket handshake. It is never put in a URL, a query string, a response, a frame, a close
reason, an error message or a log record.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import uuid
from collections.abc import AsyncIterator, Callable, Coroutine, Iterator
from dataclasses import dataclass, field
from typing import Annotated, Any
from urllib.parse import quote, urlsplit

import httpx
import psycopg
from fastapi import APIRouter, Depends, FastAPI, Query, Request, Response, WebSocket
from fastapi.routing import APIRoute
from pydantic import BaseModel, ValidationError
from starlette.responses import StreamingResponse
from starlette.types import Receive, Scope, Send
from starlette.websockets import WebSocketDisconnect, WebSocketState
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError, not_found
from agents_orchestrator.api.origin import require_allowed_origin
from agents_orchestrator.api.sessions import Cfg, Pool
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper import WrapperClient, WrapperError, WrapperUnreachable

log = logging.getLogger(__name__)

# the wrapper terminal registry's id format; the one place the api pins it
TERMINAL_ID_RE = re.compile(r"^term_[0-9a-f]{12}$")

_DEAD = "dead"
_MASK = "***"


def validate_terminal_id(value: str) -> str:
    """The id unchanged when it is a wrapper terminal id, else 422 `invalid_request`.

    Called by every terminal route and the WebSocket relay before any upstream request, so a
    crafted id such as `..` can never steer the derived bearer to another upstream path.
    `fullmatch` semantics: a trailing newline does not slip past `$`.
    """
    if not isinstance(value, str) or not TERMINAL_ID_RE.fullmatch(value):
        raise ApiError(422, "invalid_request", "invalid terminal id")
    return value


@dataclass(frozen=True)
class WrapperTarget:
    """One reachable wrapper pod and the bearer for its session."""

    agent_id: uuid.UUID
    session_id: uuid.UUID
    pod_name: str
    url: str
    wrapper_session_id: str
    token: str = field(repr=False)


def _usable(agent: agents_repo.Agent, state: str | None) -> bool:
    return (
        agent.ready
        and agent.gone_at is None
        and bool(agent.url)
        and bool(agent.wrapper_session_id)
        and state != _DEAD
    )


async def resolve_agent(
    conn: psycopg.AsyncConnection,
    settings: Settings,
    *,
    session_id: uuid.UUID | None = None,
    agent_id: uuid.UUID | None = None,
) -> WrapperTarget:
    """The wrapper pod to call.

    With `agent_id` that agent (404 `not_found` when unknown or when `session_id` is given and
    differs); with only `session_id` the first agent by ordinal that is usable. Usable is the
    dispatcher's notion (db/queue.py `_PICK`) minus idleness, since files and terminals work
    on a busy pod: the pod is ready, the agent is not gone, it has a url and a wrapper
    session, its stored state is not `dead`, and its orchestrator session is `desired = 'active'` (a
    discarding session is being torn down). None usable, or the named agent not usable, is 409
    `agent_not_ready`; an unknown session is 404; an unset master key is 503 `unavailable`.
    """
    if agent_id is None and session_id is None:
        raise ApiError(422, "invalid_request", "an agent or a session is required")
    if agent_id is not None:
        agent = await agents_repo.get_agent(conn, agent_id)
        if agent is None or (session_id is not None and agent.session_id != session_id):
            raise not_found("agent")
        session_id = agent.session_id
    assert session_id is not None
    session = await sessions_repo.get(conn, session_id)
    if session is None:
        raise not_found("session")
    states = await agents_repo.list_states(conn, session_id)
    if agent_id is not None:
        pool = [agent]
    else:
        pool = await agents_repo.list_agents(conn, session_id)
    candidates = [a for a in pool if _usable(a, states.get(a.id))]
    if session.desired != "active" or not candidates:
        raise ApiError(409, "agent_not_ready", "no agent of the session is ready")
    chosen = candidates[0]
    return _target(chosen, session_id, _session_token(settings, session_id))


def _session_token(settings: Settings, session_id: uuid.UUID) -> str:
    try:
        return tokens_for(settings, session_id).auth
    except SettingsError:
        raise ApiError(503, "unavailable", "wrapper access is not configured") from None


def _target(agent: agents_repo.Agent, session_id: uuid.UUID, token: str) -> WrapperTarget:
    assert agent.url is not None and agent.wrapper_session_id is not None
    return WrapperTarget(
        agent_id=agent.id,
        session_id=session_id,
        pod_name=agent.pod_name,
        url=agent.url,
        wrapper_session_id=agent.wrapper_session_id,
        token=token,
    )


async def resolve_agents(
    conn: psycopg.AsyncConnection, settings: Settings, session_id: uuid.UUID
) -> list[WrapperTarget]:
    """Every usable agent of the session by ordinal (the fan-out of `resolve_agent`'s rule).

    An unknown session is 404. A discarding session or one with no usable agent yields an empty
    list, so a fan-out answers with nothing to report; the master key is only needed (503
    `unavailable` when unset) once there is an agent to call.
    """
    session = await sessions_repo.get(conn, session_id)
    if session is None:
        raise not_found("session")
    if session.desired != "active":
        return []
    states = await agents_repo.list_states(conn, session_id)
    usable = [
        a for a in await agents_repo.list_agents(conn, session_id) if _usable(a, states.get(a.id))
    ]
    if not usable:
        return []
    token = _session_token(settings, session_id)
    return [_target(a, session_id, token) for a in usable]


# -------------------------------------------------------------------------- shared client


def new_client() -> httpx.AsyncClient:
    # pod addresses are cluster-internal: never route them through an environment proxy
    return httpx.AsyncClient(trust_env=False)


def get_client(app: FastAPI) -> httpx.AsyncClient:
    """The app's shared wrapper client, created on first use and kept on `app.state`.

    Created lazily so an app driven through `httpx.ASGITransport` (which never runs the
    lifespan) works; `close_client` runs in the lifespan shutdown. Tests may preset
    `app.state.wrapper_http` with their own client.
    """
    client = getattr(app.state, "wrapper_http", None)
    if client is None:
        client = app.state.wrapper_http = new_client()
    return client


async def close_client(app: FastAPI) -> None:
    client = getattr(app.state, "wrapper_http", None)
    if client is not None:
        app.state.wrapper_http = None
        await client.aclose()


# -------------------------------------------------------------------------------- forward


def _scrub(text: str, token: str) -> str:
    return text.replace(token, _MASK) if token else text


@contextlib.contextmanager
def _wrapper_errors(target: WrapperTarget, method: str, path: str) -> Iterator[None]:
    """The one map from a wrapper failure inside the block to the api's error.

    A wrapper 4xx is re-raised as `ApiError` with the wrapper's status and code (the bearer
    scrubbed); a wrapper 3xx or 5xx is 502 `wrapper_error` (its body is not relayed); a
    transport failure is 502 `wrapper_unreachable`.
    """
    try:
        yield
    except WrapperError as exc:
        log.debug("wrapper answered %s %s on %s %s", exc.status, exc.code, method, path)
        if exc.status >= 500 or exc.status < 400:
            raise ApiError(
                502, "wrapper_error", f"wrapper answered {exc.status} on {target.pod_name}"
            ) from None
        raise ApiError(
            exc.status, _scrub(exc.code, target.token), _scrub(exc.message, target.token)
        ) from None
    except WrapperUnreachable as exc:
        log.debug("wrapper unreachable on %s %s: %s", method, path, type(exc.__cause__).__name__)
        raise ApiError(
            502, "wrapper_unreachable", f"wrapper of {target.pod_name} is unreachable"
        ) from None


async def forward(
    app: FastAPI,
    target: WrapperTarget,
    method: str,
    path: str,
    *,
    params: dict[str, Any] | None = None,
    json: Any = None,
    timeout: float | None = None,
) -> Any:
    """Call the wrapper `/v1{path}` of `target`: 2xx returns the wrapper's JSON, every
    failure is mapped by `_wrapper_errors`."""
    client = WrapperClient(target.url, target.token, http=get_client(app))
    extra = {} if timeout is None else {"timeout": timeout}
    log.debug(
        "wrapper request %s %s agent=%s pod=%s", method, path, target.agent_id, target.pod_name
    )
    with _wrapper_errors(target, method, path):
        status, body = await client.request_json(method, path, params=params, json=json, **extra)
    log.debug("wrapper answered %s on %s %s", status, method, path)
    return body


# ------------------------------------------------------------------------------ file routes

router = APIRouter(prefix="/api/v1")
# the Origin rule (403 forbidden_origin) on every mutating route a browser can reach
guarded = [Depends(require_allowed_origin)]


async def _file_call[M: BaseModel](
    request: Request,
    pool: Pool,
    settings: Cfg,
    session_id: uuid.UUID,
    agent_id: uuid.UUID | None,
    method: str,
    tail: str,
    params: dict[str, Any],
    model: type[M],
    json: Any = None,
) -> M:
    """Forward one file call (with its JSON body, if any) to the agent chosen by
    `resolve_agent` and add the ids to the wrapper's answer."""
    async with pool.connection() as conn:
        target = await resolve_agent(conn, settings, session_id=session_id, agent_id=agent_id)
    sid = quote(target.wrapper_session_id, safe="")
    path = f"/sessions/{sid}/{tail}"
    body = await forward(request.app, target, method, path, params=params, json=json)
    try:
        return model.model_validate(
            {**body, "session_id": target.session_id, "agent_id": target.agent_id}
        )
    except (ValidationError, TypeError):
        raise ApiError(
            502, "wrapper_error", f"wrapper of {target.pod_name} answered an unexpected body"
        ) from None


@router.get("/sessions/{session_id}/files", response_model=schemas.FileListing)
async def list_files(
    request: Request,
    pool: Pool,
    settings: Cfg,
    session_id: uuid.UUID,
    path: str = "",
    offset: Annotated[int, Query(ge=0)] = 0,
    agent_id: uuid.UUID | None = None,
) -> schemas.FileListing:
    """One directory of the session workdir, read by the first ready agent (or `agent_id`)."""
    return await _file_call(
        request,
        pool,
        settings,
        session_id,
        agent_id,
        "GET",
        "files",
        {"path": path, "offset": offset},
        schemas.FileListing,
    )


@router.get("/sessions/{session_id}/files/content", response_model=schemas.FileContent)
async def file_content(
    request: Request,
    pool: Pool,
    settings: Cfg,
    session_id: uuid.UUID,
    path: str = "",
    agent_id: uuid.UUID | None = None,
) -> schemas.FileContent:
    """One file of the session workdir: text, or kind binary / too_large without content."""
    return await _file_call(
        request,
        pool,
        settings,
        session_id,
        agent_id,
        "GET",
        "files/content",
        {"path": path},
        schemas.FileContent,
    )


def _too_large_body() -> ApiError:
    return ApiError(
        413,
        "too_large",
        f"request body is over the {schemas.WRITE_BODY_MAX_BYTES} byte limit of a file write",
    )


class _CappedRequest(Request):
    """A request whose body stream stops at `schemas.WRITE_BODY_MAX_BYTES`: a declared
    Content-Length over it is refused unread, and a chunked body as soon as it passes it."""

    async def stream(self) -> AsyncIterator[bytes]:
        declared = self.headers.get("content-length", "")
        if declared.isdigit() and int(declared) > schemas.WRITE_BODY_MAX_BYTES:
            raise _too_large_body()
        seen = 0
        async for chunk in super().stream():
            seen += len(chunk)
            if seen > schemas.WRITE_BODY_MAX_BYTES:
                raise _too_large_body()
            yield chunk


class _CappedBodyRoute(APIRoute):
    """Route class of the file write: FastAPI reads the whole body before it validates it, so the
    capped read happens first and the handler reuses the cached body (413 `too_large`, the
    API's envelope)."""

    def get_route_handler(self) -> Callable[[Request], Coroutine[Any, Any, Response]]:
        handler = super().get_route_handler()

        async def capped(request: Request) -> Response:
            capped_request = _CappedRequest(request.scope, request.receive)
            # read here, not inside FastAPI, which turns any error while reading into a 400
            await capped_request.body()
            return await handler(capped_request)

        return capped


async def write_file_content(
    request: Request,
    pool: Pool,
    settings: Cfg,
    session_id: uuid.UUID,
    body: schemas.WriteFile,
    path: str = "",
    agent_id: uuid.UUID | None = None,
) -> schemas.FileWritten:
    """Replace one existing file of the session workdir with `content`; the wrapper answers
    409 `file_changed` when the file no longer matches `expected` (null: overwrite)."""
    return await _file_call(
        request,
        pool,
        settings,
        session_id,
        agent_id,
        "PUT",
        "files/content",
        {"path": path},
        schemas.FileWritten,
        json=body.model_dump(mode="json"),
    )


router.add_api_route(
    "/sessions/{session_id}/files/content",
    write_file_content,
    methods=["PUT"],
    response_model=schemas.FileWritten,
    dependencies=guarded,
    responses={413: {"description": "The request body is over the limit of a file write"}},
    route_class_override=_CappedBodyRoute,
)


# ---------------------------------------------------------------------------- file download

# headers of the wrapper's download answer that are relayed; the rest are the orchestrator's
_RELAYED = ("content-type", "content-length", "content-disposition")
_DOWNLOAD_OPENAPI: dict[int | str, dict[str, Any]] = {
    200: {
        "description": "The file's raw bytes, with download headers",
        "content": {"application/octet-stream": {"schema": {"type": "string", "format": "binary"}}},
    }
}


class DownloadAborted(RuntimeError):
    """The wrapper stream failed after the download started; raised so the server aborts the
    connection and the browser marks the download failed instead of saving a short file."""


class _DownloadRelay(StreamingResponse):
    """Relays an open wrapper download; the upstream is closed on every exit.

    `__call__`'s `finally` covers a complete relay, a failed one, a browser that went away
    (either ASGI disconnect style) and cancellation, so no exit path leaves it open.
    """

    media_type = "application/octet-stream"

    def __init__(self, upstream: httpx.Response, pod_name: str) -> None:
        self._upstream = upstream
        self._pod_name = pod_name
        self._chunks = self._relay()
        headers = {k: upstream.headers[k] for k in _RELAYED if k in upstream.headers}
        headers["X-Content-Type-Options"] = "nosniff"
        headers["Cache-Control"] = "no-store"
        super().__init__(self._chunks, headers=headers)

    async def _relay(self) -> AsyncIterator[bytes]:
        try:
            # raw chunks: no decoding, so the bytes and Content-Length stay the wrapper's
            async for chunk in self._upstream.aiter_raw():
                yield chunk
        except httpx.HTTPError as exc:
            log.warning(
                "download from %s failed mid-stream: %s", self._pod_name, type(exc).__name__
            )
            raise DownloadAborted(f"download from {self._pod_name} failed mid-stream") from None

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        try:
            await super().__call__(scope, receive, send)
        finally:
            await self._chunks.aclose()
            await self._upstream.aclose()


@router.get(
    "/sessions/{session_id}/files/download",
    status_code=200,
    response_class=_DownloadRelay,
    responses=_DOWNLOAD_OPENAPI,
)
async def download_file(
    request: Request,
    pool: Pool,
    settings: Cfg,
    session_id: uuid.UUID,
    path: str = "",
    agent_id: uuid.UUID | None = None,
) -> _DownloadRelay:
    """One file of the session workdir as raw bytes, streamed from the wrapper (any size).

    The wrapper's status is mapped before any byte is sent, so every refusal is a JSON error.
    """
    async with pool.connection() as conn:
        target = await resolve_agent(conn, settings, session_id=session_id, agent_id=agent_id)
    client = WrapperClient(target.url, target.token, http=get_client(request.app))
    sid = quote(target.wrapper_session_id, safe="")
    log.debug("wrapper download agent=%s pod=%s", target.agent_id, target.pod_name)
    with _wrapper_errors(target, "GET", f"/sessions/{sid}/files/download"):
        upstream = await client.open_download(sid, path)
    return _DownloadRelay(upstream, target.pod_name)


# -------------------------------------------------------------------------- terminal routes


def _terminal_view(target: WrapperTarget, body: Any) -> schemas.TerminalView:
    """The wrapper's terminal record with the agent identity added (its session id dropped).

    The wrapper reports `attached` as its subscriber count; the view says whether any is attached.
    """
    try:
        view = {
            **body,
            "agent_id": target.agent_id,
            "session_id": target.session_id,
            "pod_name": target.pod_name,
        }
        if type(view.get("attached")) is int:
            view["attached"] = view["attached"] > 0
        return schemas.TerminalView.model_validate(view)
    except (ValidationError, TypeError):
        raise ApiError(
            502, "wrapper_error", f"wrapper of {target.pod_name} answered an unexpected body"
        ) from None


def _terminals_path(target: WrapperTarget, tail: str = "") -> str:
    return f"/sessions/{quote(target.wrapper_session_id, safe='')}/terminals{tail}"


@router.post(
    "/agents/{agent_id}/terminals",
    status_code=201,
    response_model=schemas.TerminalView,
    dependencies=guarded,
)
async def create_terminal(
    request: Request,
    pool: Pool,
    settings: Cfg,
    agent_id: uuid.UUID,
    body: schemas.CreateTerminal | None = None,
) -> schemas.TerminalView:
    """Start a shell in the agent's session workdir; the wrapper's limits pass through."""
    async with pool.connection() as conn:
        target = await resolve_agent(conn, settings, agent_id=agent_id)
    sizes = body.model_dump(exclude_none=True) if body else {}
    made = await forward(request.app, target, "POST", _terminals_path(target), json=sizes)
    return _terminal_view(target, made)


async def _agent_terminals(
    app: FastAPI, target: WrapperTarget
) -> list[schemas.TerminalView] | None:
    """One agent's shells, or None when the agent cannot answer."""
    try:
        body = await forward(app, target, "GET", _terminals_path(target))
        return [_terminal_view(target, t) for t in body["terminals"]]
    except (ApiError, KeyError, TypeError):
        log.debug("terminals of agent %s unavailable", target.agent_id)
        return None


@router.get("/sessions/{session_id}/terminals", response_model=schemas.TerminalList)
async def list_terminals(
    request: Request, pool: Pool, settings: Cfg, session_id: uuid.UUID
) -> schemas.TerminalList:
    """Every ready agent's shells, queried concurrently; failing agents are reported, not fatal."""
    async with pool.connection() as conn:
        targets = await resolve_agents(conn, settings, session_id)
    answers = await asyncio.gather(*(_agent_terminals(request.app, t) for t in targets))
    terminals = [view for found in answers if found for view in found]
    down = [t.agent_id for t, found in zip(targets, answers, strict=True) if found is None]
    return schemas.TerminalList(terminals=terminals, unavailable_agents=down)


@router.delete("/agents/{agent_id}/terminals/{terminal_id}", status_code=204, dependencies=guarded)
async def delete_terminal(
    request: Request, pool: Pool, settings: Cfg, agent_id: uuid.UUID, terminal_id: str
) -> Response:
    """Close one shell; an unknown id is the wrapper's 404 `terminal_not_found`."""
    validate_terminal_id(terminal_id)
    async with pool.connection() as conn:
        target = await resolve_agent(conn, settings, agent_id=agent_id)
    await forward(request.app, target, "DELETE", _terminals_path(target, f"/{terminal_id}"))
    return Response(status_code=204)


# ------------------------------------------------------------------ dialog relay (T-98, O5)
# Human-only: registered here on the main port, never on the agent port nor the service
# allowlist, so an agent cannot answer its own permission prompt.


def _dialog_path(target: WrapperTarget) -> str:
    return f"/sessions/{quote(target.wrapper_session_id, safe='')}/dialog"


def _dialog_model[M: BaseModel](target: WrapperTarget, model: type[M], body: Any, **ids: Any) -> M:
    try:
        return model.model_validate({**body, **ids})
    except (ValidationError, TypeError):
        raise ApiError(
            502, "wrapper_error", f"wrapper of {target.pod_name} answered an unexpected body"
        ) from None


_DIALOG_ERRORS: dict[int | str, dict[str, Any]] = {
    404: {"description": "Unknown agent"},
    409: {"description": "Agent not ready"},
    502: {"description": "Wrapper unreachable or answered an unexpected body"},
}


@router.get(
    "/agents/{agent_id}/dialog", response_model=schemas.DialogView, responses=_DIALOG_ERRORS
)
async def get_dialog(
    request: Request, pool: Pool, settings: Cfg, agent_id: uuid.UUID
) -> schemas.DialogView:
    """What the agent's claude is waiting on (a permission request, a dialog, a usage wait)."""
    async with pool.connection() as conn:
        target = await resolve_agent(conn, settings, agent_id=agent_id)
    body = await forward(request.app, target, "GET", _dialog_path(target))
    return _dialog_model(
        target, schemas.DialogView, body, agent_id=target.agent_id, session_id=target.session_id
    )


@router.post(
    "/agents/{agent_id}/dialog",
    response_model=schemas.DialogAnswerResult,
    responses={
        **_DIALOG_ERRORS,
        403: {"description": "Origin not allowed"},
        409: {
            "description": "Agent not ready, or the wrapper's no_dialog, dialog_changed, "
            "permission_resolved or not_dismissable"
        },
    },
)
async def answer_dialog(
    request: Request, pool: Pool, settings: Cfg, agent_id: uuid.UUID, body: schemas.DialogAnswer
) -> schemas.DialogAnswerResult:
    """Allow, deny or dismiss the open dialog; the wrapper's 4xx (409 `no_dialog`,
    `dialog_changed`, `permission_resolved`, `not_dismissable`) pass through. The log line
    carries no message text and no tool input. The main port has no user identity (the
    source allowlist is the auth), so the caller is the peer address. The origin guard runs
    inside the audited block so refused answers (403, 404, 409) are logged too."""
    sent = body.model_dump(exclude_none=True)
    outcome: str | int = "-"
    target: WrapperTarget | None = None
    try:
        await require_allowed_origin(request)
        async with pool.connection() as conn:
            target = await resolve_agent(conn, settings, agent_id=agent_id)
        answered = await forward(request.app, target, "POST", _dialog_path(target), json=sent)
        outcome = answered.get("status", "-") if isinstance(answered, dict) else "-"
    except ApiError as exc:
        outcome = exc.status
        raise
    finally:
        log.info(
            "dialog answer peer=%s agent=%s pod=%s action=%s request_id=%s status=%s",
            request.client.host if request.client else "-",
            target.agent_id if target else agent_id,
            target.pod_name if target else "-",
            body.action,
            body.request_id or "-",
            outcome,
        )
    return _dialog_model(target, schemas.DialogAnswerResult, answered, agent_id=target.agent_id)


# ------------------------------------------------------------------- terminal relay (D2, D6)

CLOSE_UNKNOWN = 4404  # unknown agent, or the wrapper does not know the session or terminal
CLOSE_NOT_READY = 4409  # the agent (or its wrapper session) is not ready
CLOSE_POLICY = 1008  # malformed terminal id
CLOSE_INTERNAL = 1011  # unreachable wrapper, rejected bearer, any other upstream failure
RELAY_TASK_PREFIX = "terminal-relay"
# how long a client that vanished mid-send gets to report its close code before 1011 is used
_CLIENT_CLOSE_GRACE = 1.0

# `websockets` logs every handshake header, the bearer included, at DEBUG. The upstream
# connection logs here instead: held at WARNING, and a filter drops anything below even if
# someone lowers the level, so no DEBUG record of it is ever created or propagated.
_upstream_log = logging.getLogger(f"{__name__}.upstream")
_upstream_log.setLevel(logging.WARNING)
_upstream_log.addFilter(lambda record: record.levelno >= logging.WARNING)

# codes a close frame may carry (RFC 6455 7.4); 1004, 1005, 1006 and 1015 are not among them
_SENDABLE = frozenset({1000, 1001, 1002, 1003, 1007, 1008, 1009, 1010, 1011, 1012, 1013, 1014})


def relay_close_code(code: int | None) -> int:
    """The code the relay sends on one side when the other side closed with `code`.

    1000, 1003, 1008, 1009, 1011, 1013 and 4000-4999 (and every other code a close frame may
    carry) pass through; 1005 (no status received) becomes 1000; 1006 (abnormal), 1015 (TLS
    failure), no code at all and anything unsendable become 1011. The reserved 1005, 1006 and
    1015 are therefore never sent.
    """
    if code == 1005:
        return 1000
    if code is not None and (code in _SENDABLE or 3000 <= code <= 4999):
        return code
    return CLOSE_INTERNAL


@dataclass(frozen=True)
class _Closed:
    """Which side of a relay closed first, and with what (None: unknown, or no close frame)."""

    side: str  # "client" or "upstream"
    code: int | None
    reason: str = ""


def _upstream_closed(exc: ConnectionClosed) -> _Closed:
    rcvd = exc.rcvd
    return _Closed("upstream", rcvd.code if rcvd else None, rcvd.reason if rcvd else "")


async def _client_to_upstream(ws: WebSocket, upstream: ClientConnection) -> _Closed:
    while True:
        msg = await ws.receive()
        if msg["type"] == "websocket.disconnect":
            return _Closed("client", msg.get("code"), msg.get("reason") or "")
        data = msg.get("bytes")
        try:
            await upstream.send(data if data is not None else msg.get("text") or "")
        except ConnectionClosed as exc:
            return _upstream_closed(exc)


async def _upstream_to_client(ws: WebSocket, upstream: ClientConnection) -> _Closed:
    while True:
        try:
            data = await upstream.recv()
        except ConnectionClosed as exc:
            return _upstream_closed(exc)
        try:
            if isinstance(data, bytes):
                await ws.send_bytes(data)
            else:
                await ws.send_text(data)
        except (WebSocketDisconnect, OSError, RuntimeError):
            # the client is gone; its reader pump learns the code from the disconnect message
            return _Closed("client", None)


async def _close_client(ws: WebSocket, code: int, reason: str = "") -> None:
    if WebSocketState.DISCONNECTED in (ws.application_state, ws.client_state):
        return
    with contextlib.suppress(WebSocketDisconnect, OSError, RuntimeError):
        await ws.close(code, reason)


async def _relay(ws: WebSocket, upstream: ClientConnection) -> None:
    """Pump frames both ways until either side closes, then close the other side.

    Both pumps are cancelled and awaited before this returns, whatever happens (including
    cancellation of the handler itself), so a relay never outlives its connections.
    """
    reader = asyncio.create_task(
        _client_to_upstream(ws, upstream), name=f"{RELAY_TASK_PREFIX}:client-to-upstream"
    )
    writer = asyncio.create_task(
        _upstream_to_client(ws, upstream), name=f"{RELAY_TASK_PREFIX}:upstream-to-client"
    )
    pumps = (reader, writer)
    closed = _Closed("upstream", None)
    try:
        done, _ = await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        if writer in done and reader not in done and writer.result().code is None:
            # a failed send says the client is gone but not how: give the reader the close
            await asyncio.wait({reader}, timeout=_CLIENT_CLOSE_GRACE)
        finished = [t for t in pumps if t.done() and not t.cancelled() and not t.exception()]
        if finished:
            # prefer the result that knows a code
            closed = max((t.result() for t in finished), key=lambda c: c.code is not None)
    finally:
        for task in pumps:
            task.cancel()
        await asyncio.gather(*pumps, return_exceptions=True)
        for task in pumps:
            if not task.cancelled() and task.exception() is not None:
                log.warning("terminal relay pump failed: %s", type(task.exception()).__name__)
    code = relay_close_code(closed.code)
    reason = closed.reason if code == closed.code else ""
    log.debug("terminal relay ended: %s closed with %s, sending %s", closed.side, closed.code, code)
    if closed.side == "client":
        await upstream.close(code, reason)
    else:
        await _close_client(ws, code, reason)


def _upstream_url(target: WrapperTarget, terminal_id: str) -> str:
    base = urlsplit(target.url)
    scheme = "wss" if base.scheme == "https" else "ws"
    return f"{scheme}://{base.netloc}/v1{_terminals_path(target, f'/{terminal_id}/attach')}"


async def _open_upstream(target: WrapperTarget, terminal_id: str) -> ClientConnection | int:
    """The connection to the wrapper's attach socket, or the close code for the client."""
    try:
        return await connect(
            _upstream_url(target, terminal_id),
            additional_headers={"Authorization": f"Bearer {target.token}"},
            proxy=None,  # pod addresses are cluster-internal, as with `new_client`
            logger=_upstream_log,
        )
    except InvalidStatus as exc:
        status = exc.response.status_code
        log.debug("wrapper of %s refused the attach with %s", target.pod_name, status)
        if status == 404:
            return CLOSE_UNKNOWN
        if status == 409:
            return CLOSE_NOT_READY
        return CLOSE_INTERNAL
    except Exception as exc:  # unreachable, timed out, not a WebSocket: all 1011
        log.debug("wrapper of %s unreachable for attach: %s", target.pod_name, type(exc).__name__)
        return CLOSE_INTERNAL


@router.websocket("/agents/{agent_id}/terminals/{terminal_id}/attach")
async def attach_terminal(ws: WebSocket, agent_id: str, terminal_id: str) -> None:
    """Relay one terminal attach between a client and the agent's wrapper.

    The Origin rule has already run (middleware, 1008 before accept). After accept: a malformed
    terminal id closes 1008 before anything else, an unknown agent or a wrapper 404 closes
    4404, an agent that is not ready (or a wrapper 409) 4409, and every other failure 1011.
    Then frames flow unchanged both ways until either side closes (see `relay_close_code`).
    """
    await ws.accept()
    try:
        validate_terminal_id(terminal_id)
    except ApiError:
        await _close_client(ws, CLOSE_POLICY, "invalid_terminal_id")
        return
    try:
        agent = uuid.UUID(agent_id)
    except ValueError:
        await _close_client(ws, CLOSE_UNKNOWN, "not_found")
        return
    try:
        async with ws.app.state.pool.connection() as conn:
            target = await resolve_agent(conn, ws.app.state.settings, agent_id=agent)
    except ApiError as exc:
        code = {404: CLOSE_UNKNOWN, 409: CLOSE_NOT_READY}.get(exc.status, CLOSE_INTERNAL)
        await _close_client(ws, code, exc.code)
        return
    upstream = await _open_upstream(target, terminal_id)
    if isinstance(upstream, int):
        reason = {CLOSE_UNKNOWN: "not_found", CLOSE_NOT_READY: "agent_not_ready"}
        await _close_client(ws, upstream, reason.get(upstream, "wrapper_unavailable"))
        return
    try:
        await _relay(ws, upstream)
    finally:
        await upstream.close()
        await _close_client(ws, CLOSE_INTERNAL)
