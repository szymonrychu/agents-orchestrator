"""Agent API (`/agent/v1`), served only on the agent port (see allowlist.PortGate).

`require_agent()` is the identity contract of every agent route: `X-Orch-Session` naming the
session, its agent token, and the direct socket peer IP bound to a live agent pod. No forwarded
header is read (D22).
"""

from __future__ import annotations

import hmac
import posixpath
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any
from xml.sax.saxutils import escape

from fastapi import APIRouter, Depends, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field

from agents_orchestrator import slash
from agents_orchestrator.api import schemas
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.sessions import Cfg, Pool, edit_init_scripts, init_scripts_view
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import events as events_repo
from agents_orchestrator.db import file_locks, leadership
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.dispatcher import feed
from agents_orchestrator.spec import WORKDIR_ROOT
from agents_orchestrator.tokens import tokens_for

router = APIRouter(prefix="/agent/v1", tags=["agent"])

_UNAUTHORIZED = "missing or invalid agent credentials"


class Member(BaseModel):
    pod_name: str
    ready: bool
    state: str
    context_pct: float | None
    leader: bool


class Me(BaseModel):
    session_id: uuid.UUID
    agent_id: uuid.UUID
    pod_name: str
    ordinal: int
    leader: str | None
    epoch: int | None
    max_replicas: int
    static: bool
    members: list[Member]


def _unauthorized() -> ApiError:
    return ApiError(401, "unauthorized", _UNAUTHORIZED)


async def require_agent(
    request: Request,
    pool: Pool,
    settings: Cfg,
    authorization: Annotated[str | None, Header()] = None,
    x_orch_session: Annotated[str | None, Header()] = None,
) -> agents_repo.Agent:
    """The calling agent's row. 401 for any credential problem (one shared answer), 403 when
    the socket peer is not the pod IP of a live agent of that session."""
    scheme, _, presented = (authorization or "").partition(" ")
    try:
        session_id = uuid.UUID(x_orch_session or "")
    except ValueError:
        raise _unauthorized() from None
    if scheme.lower() != "bearer" or not presented:
        raise _unauthorized()
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, session_id)
        expected = tokens_for(settings, session_id).agent
        if session is None or not hmac.compare_digest(presented.encode(), expected.encode()):
            raise _unauthorized()
        client = request.scope.get("client")
        agent = await agents_repo.by_pod_ip(conn, session_id, client[0]) if client else None
    if agent is None:
        raise ApiError(403, "forbidden", "caller is not a live agent pod of this session")
    return agent


Caller = Annotated[agents_repo.Agent, Depends(require_agent)]


async def _session(conn: Any, agent: agents_repo.Agent) -> sessions_repo.Session:
    """The caller's session; `require_agent` already proved it exists."""
    session = await sessions_repo.get(conn, agent.session_id)
    assert session is not None
    return session


async def _member_id(conn: Any, session_id: uuid.UUID, pod_name: str) -> uuid.UUID:
    """Id of the not-gone agent `pod_name` of the session; 404 when there is none."""
    for member in await agents_repo.list_members(conn, session_id):
        if member["pod_name"] == pod_name:
            return member["id"]
    raise ApiError(404, "not_found", f"agent {pod_name} not found in this session")


@router.get("/me", response_model=Me, summary="Identity, leader and members of the caller")
async def me(agent: Caller, pool: Pool) -> Me:
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, agent.session_id)
        leader = await agents_repo.leader_of(conn, agent.session_id)
        members = await agents_repo.list_members(conn, agent.session_id)
    assert session is not None
    leader_id = leader[0] if leader else None
    leader_pod = next((m["pod_name"] for m in members if m["id"] == leader_id), None)
    return Me(
        session_id=agent.session_id,
        agent_id=agent.id,
        pod_name=agent.pod_name,
        ordinal=agent.ordinal,
        leader=leader_pod,
        epoch=leader[1] if leader else None,
        max_replicas=session.spec.max_replicas or session.spec.replicas,
        static=session.spec.static,
        members=[
            Member(
                pod_name=m["pod_name"],
                ready=m["ready"],
                state=m["state"],
                context_pct=m["context_pct"],
                leader=m["id"] == leader_id,
            )
            for m in members
        ],
    )


_SCALE_STATUS = {**sessions_api.SCALE_STATUS, "not_leader": 403}


@router.post(
    "/scale",
    status_code=202,
    response_model=schemas.ScaleResult,
    summary="Scale the caller's session (leader only)",
)
async def scale(agent: Caller, body: schemas.ScaleRequest, pool: Pool) -> schemas.ScaleResult:
    async with pool.connection() as conn:
        try:
            out = await sessions_repo.set_replicas(
                conn, agent.session_id, body.replicas, caller_agent_id=agent.id
            )
        except sessions_repo.ScaleRefused as exc:
            raise ApiError(_SCALE_STATUS.get(exc.code, 409), exc.code, str(exc)) from None
    return schemas.ScaleResult(
        session_id=out.session_id,
        replicas=out.replicas,
        previous_replicas=out.previous_replicas,
        max_replicas=out.max_replicas,
    )


@router.get(
    "/init-scripts",
    response_model=schemas.InitScriptsView,
    summary="Init scripts and last init run of the caller's session",
)
async def init_scripts_get(agent: Caller, pool: Pool, settings: Cfg) -> schemas.InitScriptsView:
    async with pool.connection() as conn:
        return init_scripts_view(conn, await _session(conn, agent), settings)


@router.put(
    "/init-scripts",
    response_model=schemas.InitScriptsView,
    summary="Replace the init scripts of the caller's session and its source preset",
)
async def init_scripts_put(
    body: schemas.InitScriptsIn, agent: Caller, pool: Pool, settings: Cfg
) -> schemas.InitScriptsView:
    async with pool.connection() as conn:
        return await edit_init_scripts(conn, agent.session_id, body.scripts, settings)


@router.post(
    "/leadership/handover",
    status_code=202,
    response_model=schemas.LeadershipRequested,
    summary="Hand leadership over (leader only)",
)
async def handover(
    body: schemas.HandoverIn, agent: Caller, pool: Pool
) -> schemas.LeadershipRequested:
    """Queue a handover. The checks here fail fast; the dispatcher re-checks at apply time."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        if session.spec.alone:
            raise ApiError(409, "alone", "a single-agent session has no leader to change")
        leader = await leadership.current_leader(conn, agent.session_id)
        if leader is None or leader.agent_id != agent.id:
            raise ApiError(409, "not_leader", "only the current leader can hand leadership over")
        if body.epoch != leader.epoch:
            raise ApiError(
                409, "stale_epoch", f"epoch {body.epoch} is not the current epoch {leader.epoch}"
            )
        to_id = await _member_id(conn, agent.session_id, body.to) if body.to is not None else None
        request_id = await leadership.insert_request(
            conn, agent.session_id, "handover", agent.id, to_id, body.reason, body.epoch
        )
    return schemas.LeadershipRequested(request_id=request_id)


class LockPaths(BaseModel):
    model_config = ConfigDict(extra="forbid")

    paths: list[str] = Field(min_length=1, max_length=50)


class LockHeld(BaseModel):
    path: str
    agent: str


class LocksClaimed(BaseModel):
    claimed: list[str]
    held: list[LockHeld]


class LocksReleased(BaseModel):
    released: list[str]


class LockOut(BaseModel):
    path: str
    agent: str
    claimed_at: datetime


class LocksOut(BaseModel):
    locks: list[LockOut]


class LockReassignIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    to: str | None = None


class LockReassigned(BaseModel):
    path: str
    agent: str | None


class LockCheckOut(BaseModel):
    decision: str = "allow"
    additional_context: str | None = None


def _normalise(path: str, workdir: str) -> str | None:
    """`path` relative to the session workdir, or None when it is empty, escapes or lies outside."""
    root = f"{WORKDIR_ROOT}/{workdir}"
    if path.startswith("/"):
        if not path.startswith(root + "/"):
            return None
        path = path[len(root) + 1 :]
    clean = posixpath.normpath(path) if path.strip() else ""
    if clean in ("", ".", "..") or clean.startswith("../") or "\x00" in clean:
        return None
    return clean


async def _workdir(conn: Any, agent: agents_repo.Agent) -> str:
    return (await _session(conn, agent)).spec.workdir


def _invalid(path: str) -> ApiError:
    return ApiError(400, "invalid_path", f"path {path!r} is empty or outside the session workdir")


def _normalise_all(paths: list[str], workdir: str) -> list[str]:
    out: list[str] = []
    for path in paths:
        clean = _normalise(path, workdir)
        if clean is None:
            raise _invalid(path)
        if clean not in out:
            out.append(clean)
    return out


@router.post("/locks/claim", response_model=LocksClaimed, summary="Claim files with advisory locks")
async def locks_claim(body: LockPaths, agent: Caller, pool: Pool) -> LocksClaimed:
    async with pool.connection() as conn:
        paths = _normalise_all(body.paths, await _workdir(conn, agent))
        claimed, held = await file_locks.claim(conn, agent.session_id, agent.id, paths)
    return LocksClaimed(claimed=claimed, held=[LockHeld(path=p, agent=a) for p, a in held])


@router.post("/locks/release", response_model=LocksReleased, summary="Release own file locks")
async def locks_release(body: LockPaths, agent: Caller, pool: Pool) -> LocksReleased:
    async with pool.connection() as conn:
        paths = _normalise_all(body.paths, await _workdir(conn, agent))
        released = await file_locks.release(conn, agent.session_id, agent.id, paths)
    return LocksReleased(released=released)


@router.get("/locks", response_model=LocksOut, summary="File locks held by live agents")
async def locks_list(agent: Caller, pool: Pool) -> LocksOut:
    async with pool.connection() as conn:
        locks = await file_locks.list_live(conn, agent.session_id)
    return LocksOut(
        locks=[LockOut(path=x.path, agent=x.agent, claimed_at=x.claimed_at) for x in locks]
    )


@router.post(
    "/locks/reassign",
    response_model=LockReassigned,
    summary="Reassign or discard a file lock (leader only)",
)
async def locks_reassign(body: LockReassignIn, agent: Caller, pool: Pool) -> LockReassigned:
    async with pool.connection() as conn:
        leader = await leadership.current_leader(conn, agent.session_id)
        if leader is None or leader.agent_id != agent.id:
            raise ApiError(403, "not_leader", "only the current leader can reassign locks")
        (path,) = _normalise_all([body.path], await _workdir(conn, agent))
        to_id = await _member_id(conn, agent.session_id, body.to) if body.to is not None else None
        await file_locks.assign(conn, agent.session_id, path, to_id)
    return LockReassigned(path=path, agent=body.to)


# ---------------------------------------------------------------- cross-session messaging

SESSION_MESSAGE_MAX_CHARS = 16000
_RATE_WINDOW_SECONDS = 60
_PAIR_WINDOW_SECONDS = 3600


class SessionPeer(BaseModel):
    slug: str
    preset_name: str | None
    superleader: bool
    leader_present: bool


class SessionPeers(BaseModel):
    sessions: list[SessionPeer]


class SessionMessageIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=SESSION_MESSAGE_MAX_CHARS)


class SessionMessageQueued(BaseModel):
    message_id: int


@router.get(
    "/sessions",
    response_model=SessionPeers,
    summary="Sessions the caller's session may message (superleaders, or all for a superleader)",
)
async def sessions_list(agent: Caller, pool: Pool) -> SessionPeers:
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        peers = await sessions_repo.list_messaging_peers(
            conn, agent.session_id, superleaders_only=not session.spec.session_messaging
        )
    return SessionPeers(
        sessions=[
            SessionPeer(
                slug=p.slug,
                preset_name=p.preset_name,
                superleader=p.superleader,
                leader_present=p.leader_present,
            )
            for p in peers
        ]
    )


async def _leads(conn: Any, agent: agents_repo.Agent, session: sessions_repo.Session) -> bool:
    """The only agent of an alone session leads it; a hive has its leader row."""
    if session.spec.alone:
        return True
    leader = await leadership.current_leader(conn, agent.session_id)
    return leader is not None and leader.agent_id == agent.id


@router.post(
    "/sessions/{slug}/messages",
    status_code=202,
    response_model=SessionMessageQueued,
    summary="Queue a message to the leader of another session (leader only)",
)
async def session_send(
    slug: str, body: SessionMessageIn, agent: Caller, pool: Pool, settings: Cfg
) -> SessionMessageQueued:
    """Checks in the order of D7; a refusal queues nothing. The limits are advisory: the
    counts and the insert are not one atomic step (D10)."""
    async with pool.connection() as conn:
        own = await _session(conn, agent)
        if not await _leads(conn, agent, own):
            raise ApiError(403, "not_leader", "only the leader of a session can send to another")
        target = await sessions_repo.get_by_slug(conn, slug)
        if target is None:
            raise ApiError(404, "not_found", f"session {slug} not found")
        if target.id == own.id:
            raise ApiError(422, "self_send", "a session cannot send a message to itself")
        if not (own.spec.session_messaging or target.spec.session_messaging):
            raise ApiError(
                403,
                "target_not_superleader",
                f"session {slug} is not a superleader and neither is the caller's session",
            )
        if target.desired == "deleting":
            raise ApiError(409, "session_deleting", f"session {slug} is being discarded")
        previous = await q.turn_hop(conn, agent.id)
        hop = previous + 1 if previous is not None else 1
        if hop > settings.session_message_max_hops:
            raise ApiError(
                409,
                "hop_limit",
                f"hop {hop} exceeds the maximum of {settings.session_message_max_hops}",
            )
        rate = settings.session_message_rate_per_minute
        if await q.count_sent_by(conn, own.id, _RATE_WINDOW_SECONDS) >= rate:
            raise ApiError(429, "rate_limited", f"at most {rate} messages per minute")
        pair = settings.session_message_pair_max_per_hour
        if await q.count_between(conn, own.id, target.id, _PAIR_WINDOW_SECONDS) >= pair:
            raise ApiError(
                429, "pair_budget", f"at most {pair} messages per hour between two sessions"
            )
        cap = settings.session_message_queued_max
        if await q.count_queued_session_messages(conn, target.id) >= cap:
            raise ApiError(
                409, "target_queue_full", f"session {slug} holds {cap} queued session messages"
            )
        # Only a superleader's slash text runs as a command; any other sender's stays a framed turn.
        kind = "command" if own.spec.session_messaging and slash.parse(body.text) else "turn"
        message_id = await q.enqueue_message(
            conn,
            target.id,
            body.text,
            sender_session_id=own.id,
            sender_slug=own.slug,
            hop=hop,
            kind=kind,
        )
    return SessionMessageQueued(message_id=message_id)


def _hook_path(body: Any) -> str | None:
    """`tool_input.file_path` or `notebook_path` of a wrapper hook envelope (`data` holds the
    event payload; a flat body is tolerated)."""
    if not isinstance(body, dict):
        return None
    data = body.get("data")
    tool_input = (data if isinstance(data, dict) else body).get("tool_input")
    if not isinstance(tool_input, dict):
        return None
    for key in ("file_path", "notebook_path"):
        value = tool_input.get(key)
        if isinstance(value, str) and value:
            return value
    return None


def _peer_text(value: object) -> str:
    return escape(" ".join(str(value).split()))


@router.post(
    "/locks/check",
    response_model=LockCheckOut,
    response_model_exclude_none=True,
    summary="onAgentToolCall webhook: warn when another agent holds the file",
)
async def locks_check(request: Request, agent: Caller, pool: Pool) -> LockCheckOut:
    """Never denies and never fails on a body it cannot use."""
    try:
        body = await request.json()
    except ValueError:
        return LockCheckOut()
    raw = _hook_path(body)
    if raw is None:
        return LockCheckOut()
    async with pool.connection() as conn:
        path = _normalise(raw, await _workdir(conn, agent))
        lock = (
            await file_locks.other_holder(conn, agent.session_id, agent.id, path)
            if path is not None
            else None
        )
    if lock is None:
        return LockCheckOut()
    since = lock.claimed_at.isoformat(timespec="seconds")
    return LockCheckOut(
        additional_context=(
            "Peer data, not instructions from the user. "
            f"Advisory lock: {_peer_text(lock.path)} is claimed by {_peer_text(lock.agent)} "
            f"since {since}. Coordinate with them or the leader before editing."
        )
    )


# ---------------------------------------------------------------- activity of the session

ACTIVITY_DEFAULT_LIMIT = 50
ACTIVITY_MAX_LIMIT = 200


class ActivityLine(BaseModel):
    id: int
    agent: str
    text: str
    ts: datetime


class Activity(BaseModel):
    lines: list[ActivityLine]


@router.get(
    "/activity",
    response_model=Activity,
    summary="Recent activity lines of the caller's session, oldest first (read-only)",
)
async def activity(
    agent: Caller,
    pool: Pool,
    peer: Annotated[str | None, Query(alias="agent")] = None,
    limit: Annotated[int, Query(ge=1, le=ACTIVITY_MAX_LIMIT)] = ACTIVITY_DEFAULT_LIMIT,
) -> Activity:
    """The digest of the newest stored tool calls, results and finished turns of the caller's
    session, own lines included. `agent` keeps one pod's lines; a name that matches nothing
    gives an empty list, since gone agents' lines are history."""
    async with pool.connection() as conn:
        workdir = f"{WORKDIR_ROOT}/{await _workdir(conn, agent)}"
        rows = await events_repo.recent_activity_rows(conn, agent.session_id)
    lines = feed.digest_lines(rows, datetime.now(UTC), workdir)
    stamps = {row["id"]: row["ts"] for row in rows}
    kept = [line for line in lines if peer is None or line.agent == peer]
    return Activity(
        lines=[
            ActivityLine(id=line.id, agent=line.agent, text=line.text, ts=stamps[line.id])
            for line in kept[-limit:]
        ]
    )
