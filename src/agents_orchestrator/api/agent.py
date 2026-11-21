"""Agent API (`/agent/v1`), served only on the agent port (see allowlist.PortGate).

`require_agent()` is the identity contract of every agent route: `X-Orch-Session` naming the
session, its agent token, and the direct socket peer IP bound to a live agent pod. No forwarded
header is read (D22).
"""

from __future__ import annotations

import hmac
import posixpath
import uuid
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from xml.sax.saxutils import escape

import psycopg.errors
from fastapi import APIRouter, Depends, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field, StringConstraints

from agents_orchestrator import (
    handoff_gate,
    handoff_profile,
    handoff_quality,
    pressure_policy,
    taskboard_client,
)
from agents_orchestrator.api import projects as projects_api
from agents_orchestrator.api import schemas, session_limits, workspace
from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.scope import ALL_PROJECTS, scope_of
from agents_orchestrator.api.sessions import (
    Cfg,
    Pool,
    Usage,
    edit_init_scripts,
    gate_scale_up,
    init_scripts_view,
)
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import events as events_repo
from agents_orchestrator.db import (
    file_locks,
    handoffs,
    leader_state,
    leadership,
    project_posts,
    session_messaging,
)
from agents_orchestrator.db import projects as projects_repo
from agents_orchestrator.db import queue as q
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db import volumes as volumes_repo
from agents_orchestrator.dispatcher import feed
from agents_orchestrator.dispatcher import hive as hive_push
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


class ProjectRoom(BaseModel):
    project: str
    last_post_id: int | None
    unhandled: int


class PressureView(BaseModel):
    """The context pressure of a solo agent or a hive leader (spec 5.3)."""

    level: int
    context_pct: float | None
    used_tokens: int | None
    window_tokens: int | None
    l1_tokens: int
    l2_tokens: int
    l3_tokens: int
    compact_tokens: int
    #: Phase of the open handoff row; None while none is open.
    phase: str | None
    profile: str | None
    waiting_on: list[str]
    rejects: int
    escalated: bool


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
    project: str
    # the other projects the session fully belongs to (T-120), sorted
    extra_projects: list[str] = []
    # "*" for a superleader session (every project), else `project` then `extra_projects`,
    # comma-separated (projects plan 3.2, T-120)
    projects_scope: str
    # the live generated state (same sections as the handover file); only for the current leader
    leader_state: str | None = None
    # one room cursor per project of a (non-superleader) current leader, home first then the
    # extra projects (T-120); empty for a member or a superleader
    project_rooms: list[ProjectRoom] = []
    # context pressure; only for a solo agent or the current leader (None for a member)
    pressure: PressureView | None = None


class CloseRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    reason: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1, max_length=500)]


class CloseRequested(BaseModel):
    session_id: uuid.UUID
    close_requested_at: datetime
    close_reason: str
    already_requested: bool
    grace_seconds: int


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


async def member_id(conn: Any, session_id: uuid.UUID, pod_name: str) -> uuid.UUID:
    """Id of the not-gone agent `pod_name` of the session; 404 when there is none."""
    for member in await agents_repo.list_members(conn, session_id):
        if member["pod_name"] == pod_name:
            return member["id"]
    raise ApiError(404, "not_found", f"agent {pod_name} not found in this session")


async def _pressure_view(
    conn: Any, session: sessions_repo.Session, agent: agents_repo.Agent, settings: Any
) -> PressureView | None:
    subject = await hive_push.pressure_subject(conn, session.id, agent.id, settings)
    if subject is None:
        return None
    window = subject.window_tokens or pressure_policy.DEFAULT_WINDOW_TOKENS
    lv = subject.levels
    l2 = hive_push.strong_tokens(lv, window)
    row = await handoffs.open_row(conn, agent.id)
    return PressureView(
        level=max(subject.level, row.level) if row else subject.level,
        context_pct=subject.context_pct,
        used_tokens=subject.used_tokens,
        window_tokens=subject.window_tokens,
        l1_tokens=hive_push.l1_tokens(lv, window, l2),
        l2_tokens=l2,
        l3_tokens=hive_push.critical_tokens(lv, window),
        compact_tokens=int(lv.compact_pct / 100 * window),
        phase=row.phase if row else None,
        profile=row.profile if row else None,
        waiting_on=pressure_policy.waiting_on(row),
        rejects=row.rejects if row else 0,
        escalated=bool(row and row.escalated_at),
    )


@router.get("/me", response_model=Me, summary="Identity, leader and members of the caller")
async def me(agent: Caller, pool: Pool, settings: Cfg) -> Me:
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, agent.session_id)
        leader = await agents_repo.leader_of(conn, agent.session_id)
        members = await agents_repo.list_members(conn, agent.session_id)
        leads = leader is not None and leader[0] == agent.id
        state = await leader_state.collect(conn, agent.session_id) if leads else None
        pressure = None
        if session is not None and (leads or session.spec.alone):
            pressure = await _pressure_view(conn, session, agent, settings)
        rooms: list[ProjectRoom] = []
        if leads and session is not None and not session.spec.session_messaging:
            unhandled = await leader_state.unhandled_room_counts(conn, session.id)
            rooms = [
                ProjectRoom(
                    project=project,
                    last_post_id=await project_posts.last_post_id(conn, project),
                    unhandled=unhandled.get(project, 0),
                )
                for project in session.projects
            ]
    assert session is not None
    if state is not None:  # the taskboard call happens after the connection is released
        decisions = await taskboard_client.open_decisions(
            settings, session.id, session.slug, scope_of(session)
        )
        state = replace(state, decisions=decisions)
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
        project=session.project,
        extra_projects=list(session.extra_projects),
        projects_scope=scope_of(session),
        leader_state=leader_state.render_state(state) if state is not None else None,
        project_rooms=rooms,
        pressure=pressure,
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


@router.get(
    "/projects",
    response_model=list[schemas.ProjectView],
    summary="The caller's projects, or every project for a superleader",
)
async def projects(agent: Caller, pool: Pool) -> list[schemas.ProjectView]:
    """Scope `project` (projects plan 3.3): the caller's own projects, home first then its extra
    projects (T-120), archived or not; a superleader session gets every project, archived ones
    included. No counts, as on the service port."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        if scope_of(session) == ALL_PROJECTS:
            rows = await projects_repo.list_projects(conn, archived=None)
        else:
            rows = []
            for slug in session.projects:
                own = await projects_repo.get_project(conn, slug)
                if own is None:  # projects are never deleted: a broken invariant
                    raise ApiError(500, "internal", "a project of the session is missing")
                rows.append(own)
    return [projects_api.view(p) for p in rows]


_SCALE_STATUS = {**sessions_api.SCALE_STATUS, "not_leader": 403}


@router.post(
    "/scale",
    status_code=202,
    response_model=schemas.ScaleResult,
    summary="Scale the caller's session (leader only)",
)
async def scale(
    agent: Caller, body: schemas.ScaleRequest, pool: Pool, settings: Cfg, usage: Usage
) -> schemas.ScaleResult:
    """A scale-up inside the credential refuse margin is refused 409 `credential_expiring`, over
    the usage quota 409 `usage_quota`, but only for the leader: a non-leader is always answered
    403 `not_leader`."""
    await gate_scale_up(
        pool, usage, settings, agent.session_id, body.replicas, caller_agent_id=agent.id
    )
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


@router.post(
    "/close",
    status_code=202,
    response_model=CloseRequested,
    summary="Close the caller's session once idle (leader only)",
)
async def close(agent: Caller, body: CloseRequest, pool: Pool, settings: Cfg) -> CloseRequested:
    """Checks in order: leader (403 `not_leader`), not a superleader session (422 `own_session`),
    not being discarded (409 `session_deleting`). The sweeper discards the session once it is idle
    for `ORCH_CLOSE_GRACE_SECONDS`; a repeat call keeps the first request."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        if not await _leads(conn, agent, session):
            raise ApiError(403, "not_leader", "only the leader can close its session")
        if session.spec.session_messaging:
            raise ApiError(
                422,
                "own_session",
                "a superleader session cannot close itself; the user discards it",
            )
        try:
            out = await sessions_repo.request_close(conn, agent.session_id, body.reason)
        except q.SessionBeingDeleted:
            raise ApiError(
                409, "session_deleting", "the session is already being discarded"
            ) from None
        except sessions_repo.SessionNotFound:
            raise ApiError(404, "not_found", "session not found") from None
    assert out.session.close_requested_at is not None and out.session.close_reason is not None
    return CloseRequested(
        session_id=out.session.id,
        close_requested_at=out.session.close_requested_at,
        close_reason=out.session.close_reason,
        already_requested=out.already,
        grace_seconds=settings.close_grace_seconds,
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
        return await edit_init_scripts(
            conn, agent.session_id, body.scripts, settings, same_project_preset=True
        )


async def _volume_view(conn: Any, att: Any, vol: volumes_repo.Volume) -> schemas.AgentVolumeView:
    current = await backups_repo.current(conn, f"{backups_repo.RESERVED_PREFIX}{vol.name}")
    return schemas.AgentVolumeView(
        name=vol.name,
        id=vol.id,
        mount_path=att.mount_path if att.mount_path is not None else f"/volumes/{vol.name}",
        mode=att.mode,
        backup_restore=att.backup_restore,
        state=vol.state,
        current_version=current.id if current else None,
        last_save_at=current.created_at if current else None,
    )


@router.get(
    "/volumes",
    response_model=list[schemas.AgentVolumeView],
    summary="The volumes the caller's session mounts",
)
async def volumes_status(agent: Caller, pool: Pool) -> list[schemas.AgentVolumeView]:
    """The attachments of the caller's session (its own spec, set from the preset when it was
    created) joined with each volume's state, current version and last save time. Another
    session's volumes, and a deleted volume, never appear."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        out = []
        for att in session.spec.volumes or []:
            vol = await volumes_repo.get(conn, att.volume)
            if vol is not None:
                out.append(await _volume_view(conn, att, vol))
    return out


@router.post(
    "/volumes/{name}/save",
    status_code=202,
    response_model=schemas.AgentVolumeView,
    summary="Save a volume of the caller's session now",
    responses={
        403: {"description": "backup_not_enabled: the session's attachment has backup_restore off"},
        404: {"description": "volume_not_found: the session does not attach a volume of that name"},
        409: {"description": "volume_busy: the volume is not ready (saving, restoring, resizing)"},
    },
)
async def volume_save(name: str, agent: Caller, pool: Pool) -> schemas.AgentVolumeView:
    """On-demand save of a volume the session mounts, only when that attachment has
    `backup_restore` (Q12: any mounter with backup on). The save is recorded under the volume
    row lock and run by the volume reconciler; a volume that is not `ready` is 409."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        found = await volumes_repo.by_name(conn, name)
        att = next(
            (a for a in session.spec.volumes or [] if found is not None and a.volume == found.id),
            None,
        )
        if found is None or att is None:
            raise ApiError(404, "volume_not_found", "this session does not attach that volume")
        if not att.backup_restore:
            raise ApiError(
                403, "backup_not_enabled", "this attachment has backup_restore turned off"
            )
        try:
            saved = await volumes_repo.request_save(
                conn,
                found.id,
                trigger="agent",
                session_slug=session.slug,
                actor=session.slug,
                actor_kind="agent",
            )
        except volumes_repo.VolumeBusy as exc:
            raise ApiError(409, "volume_busy", str(exc)) from None
        assert saved is not None  # only the release trigger returns None
        return await _volume_view(conn, att, saved)


@router.post(
    "/leadership/handover",
    status_code=202,
    response_model=schemas.LeadershipRequested,
    summary="Hand leadership over (hive leader only)",
)
async def handover(
    body: schemas.HandoverIn, agent: Caller, pool: Pool
) -> schemas.LeadershipRequested:
    """Queue a handover. The checks here fail fast; the dispatcher re-checks at apply time. Only
    the leader of a hive may hand leadership over; a solo agent submits a handoff instead."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        if session.spec.alone:
            raise ApiError(
                409,
                "not_hive",
                "a solo agent has no leader to change: use orchestrator_handoff_submit",
            )
        leader = await leadership.current_leader(conn, agent.session_id)
        if leader is None or leader.agent_id != agent.id:
            raise ApiError(409, "not_leader", "only the current leader can hand leadership over")
        request_id = await _hive_handover(conn, agent, leader, body)
        handled = await session_messaging.mark_handled(conn, agent.session_id, body.handled)
    return schemas.LeadershipRequested(request_id=request_id, handled=handled)


def _check_epoch(leader: Any, epoch: int | None) -> None:
    if epoch is None:
        raise ApiError(422, "epoch_required", "a hive leader's handover needs the leader epoch")
    if epoch != leader.epoch:
        raise ApiError(409, "stale_epoch", f"epoch {epoch} is not the current epoch {leader.epoch}")


async def _hive_handover(
    conn: Any, agent: agents_repo.Agent, leader: Any, body: schemas.HandoverIn
) -> int:
    _check_epoch(leader, body.epoch)
    to_id = await member_id(conn, agent.session_id, body.to) if body.to is not None else None
    assert body.epoch is not None
    return await leadership.insert_request(
        conn, agent.session_id, "handover", agent.id, to_id, body.reason, body.epoch
    )


# ---------------------------------------------------------------- handoff submit

_REJECT_WORDS = {
    "too_short": "the handoff is too short to carry a context over",
    "not_text": "the handoff is mostly not text (emoji or punctuation)",
}
_REUSABLE = ("pressuring", "submitted")


def _reason_words(reason: str) -> str:
    kind, _, what = reason.partition(":")
    if kind == "missing":
        return f"the field {what} is empty"
    if kind == "unreferenced":
        return f"{what} is still running but no in_flight entry names it (list it or abandon it)"
    return _REJECT_WORDS.get(reason, reason)


def _rejected(reasons: list[str], rejects: int) -> ApiError:
    return ApiError(
        422,
        "handoff_rejected",
        "the handoff was not accepted: "
        + "; ".join(_reason_words(r) for r in reasons)
        + ". Fix it and call orchestrator_handoff_submit again.",
        extra={"missing": reasons, "rejects": rejects},
    )


@router.post(
    "/handoff",
    status_code=202,
    response_model=schemas.HandoffAccepted,
    summary="Submit your handoff once your work is safe to hand over",
)
async def handoff_submit(
    body: schemas.HandoffSubmitIn, request: Request, agent: Caller, pool: Pool, settings: Cfg
) -> schemas.HandoffAccepted:
    """Check order: hive leader and epoch, an open row (reused; a voluntary one is opened only
    once the text passes), the quality of the text (422 `handoff_rejected`), then the in-flight
    gate (409 `in_flight`, the handoff stored). A gate-clear submit is accepted 202; the
    dispatcher then moves leadership (hive) or clears the context and returns the handoff (solo)
    once the agent is idle."""
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        if not session.spec.alone:
            leader = await leadership.current_leader(conn, agent.session_id)
            if leader is None or leader.agent_id != agent.id:
                raise ApiError(409, "not_leader", "only the current leader can submit a handoff")
            _check_epoch(leader, body.epoch)
            if body.to is not None:
                await member_id(conn, agent.session_id, body.to)
        row = await handoffs.open_row(conn, agent.id)
        if row is not None and row.phase not in _REUSABLE:
            raise ApiError(409, "handoff_open", f"a handoff is already {row.phase}")
        subject = await hive_push.pressure_subject(conn, session.id, agent.id, settings)
        profile = row.profile if row is not None else None
        if profile is None:
            profile = await handoff_profile.detect(
                conn,
                {"id": session.id, "spec": {"handoff_profile": session.spec.handoff_profile}},
                agent.id,
                None
                if subject is None
                else datetime.now(UTC) - timedelta(seconds=subject.tenure_seconds),
            )
        used = None if subject is None else subject.used_tokens
    inflight = await workspace.fetch_inflight(request.app, settings, agent)
    reasons = handoff_quality.check(body, profile, inflight, body.abandon, settings)
    gate = handoff_gate.evaluate(inflight, body.abandon)
    snapshot = inflight.model_dump(mode="json") if inflight else {"unknown": True}
    snapshot = {**snapshot, "waiting_on": gate.waiting_on, "abandoned": gate.abandoned}
    stored = body.model_dump(mode="json", exclude={"handled", "epoch"})
    stored["abandoned"] = gate.abandoned
    refusal: ApiError | None = None
    handled: list[int] = []
    async with pool.connection() as conn:
        if reasons and row is None:  # a voluntary submit that fails the check opens no row
            raise _rejected(reasons, 0)
        if row is None:
            try:
                async with conn.transaction():  # savepoint: a concurrent open is no failure
                    row = await handoffs.open_pressure(
                        conn, session.id, agent.id, 0, profile, "voluntary"
                    )
            except psycopg.errors.UniqueViolation:
                row = await handoffs.open_row(conn, agent.id)
            if row is None or row.phase not in _REUSABLE:
                phase = row.phase if row else "open"
                raise ApiError(409, "handoff_open", f"a handoff is already {phase}")
        if reasons:
            rejects = await handoffs.record_reject(conn, row.id, reasons)
            if rejects == 0:
                refusal = ApiError(409, "handoff_open", "the handoff was just closed")
            else:
                refusal = _rejected(reasons, rejects)
        elif not await handoffs.submit(conn, row.id, stored, snapshot, used):
            refusal = ApiError(409, "handoff_open", "the handoff just moved on")
        else:
            handled = await session_messaging.mark_handled(conn, agent.session_id, body.handled)
            if not gate.clear:
                await handoffs.record_gate_block(conn, row.id, snapshot)
                hint = (
                    f" For scratch files: {handoff_gate.SCRATCH_HINT}."
                    if any(w.startswith("scratch:") for w in gate.waiting_on)
                    else ""
                )
                refusal = ApiError(
                    409,
                    "in_flight",
                    "the handoff is stored but work is still in flight: "
                    + ", ".join(gate.waiting_on)
                    + ". Finish each, or abandon it with a reason, then submit again."
                    + hint,
                    extra={"waiting_on": gate.waiting_on},
                )
    if refusal is not None:
        raise refusal
    return schemas.HandoffAccepted(handoff_id=row.id, handled=handled)


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


def normalise_all(paths: list[str], workdir: str) -> list[str]:
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
        paths = normalise_all(body.paths, await _workdir(conn, agent))
        claimed, held = await file_locks.claim(conn, agent.session_id, agent.id, paths)
    return LocksClaimed(claimed=claimed, held=[LockHeld(path=p, agent=a) for p, a in held])


@router.post("/locks/release", response_model=LocksReleased, summary="Release own file locks")
async def locks_release(body: LockPaths, agent: Caller, pool: Pool) -> LocksReleased:
    async with pool.connection() as conn:
        paths = normalise_all(body.paths, await _workdir(conn, agent))
        released = await file_locks.release(conn, agent.session_id, agent.id, paths)
    return LocksReleased(released=released)


async def locks_of(conn: Any, session_id: uuid.UUID) -> LocksOut:
    """The file locks held by the live agents of a session."""
    locks = await file_locks.list_live(conn, session_id)
    return LocksOut(
        locks=[LockOut(path=x.path, agent=x.agent, claimed_at=x.claimed_at) for x in locks]
    )


@router.get("/locks", response_model=LocksOut, summary="File locks held by live agents")
async def locks_list(agent: Caller, pool: Pool) -> LocksOut:
    async with pool.connection() as conn:
        return await locks_of(conn, agent.session_id)


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
        (path,) = normalise_all([body.path], await _workdir(conn, agent))
        to_id = await member_id(conn, agent.session_id, body.to) if body.to is not None else None
        await file_locks.assign(conn, agent.session_id, path, to_id)
    return LockReassigned(path=path, agent=body.to)


# ---------------------------------------------------------------- cross-session messaging

SESSION_MESSAGE_MAX_CHARS = 16000


class SessionPeer(BaseModel):
    slug: str
    preset_name: str | None
    superleader: bool
    leader_present: bool
    # booting | ready | bootstrap_failed, and a one-line reason unless ready
    start_state: Literal["booting", "ready", "bootstrap_failed"]
    start_reason: str | None


class SessionPeers(BaseModel):
    sessions: list[SessionPeer]


class SessionMessageIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1, max_length=SESSION_MESSAGE_MAX_CHARS)
    lane: schemas.Lane = "regular"
    #: Ids of incoming session messages the leader answered or that need no answer.
    handled: list[int] = Field(default_factory=list, max_length=50)


class SessionMessageQueued(BaseModel):
    message_id: int
    handled: list[int] = []


@router.get(
    "/sessions",
    response_model=SessionPeers,
    summary="Sessions the caller may message (superleaders, own projects; all for a superleader)",
)
async def sessions_list(agent: Caller, pool: Pool) -> SessionPeers:
    async with pool.connection() as conn:
        session = await _session(conn, agent)
        peers = await sessions_repo.list_messaging_peers(
            conn,
            agent.session_id,
            projects=None if session.spec.session_messaging else session.projects,
        )
    return SessionPeers(
        sessions=[
            SessionPeer(
                slug=p.slug,
                preset_name=p.preset_name,
                superleader=p.superleader,
                leader_present=p.leader_present,
                start_state=p.start_state,
                start_reason=p.start_reason,
            )
            for p in peers
        ]
    )


async def _leads(conn: Any, agent: agents_repo.Agent, session: sessions_repo.Session) -> bool:
    """The only agent of an alone session leads it; a hive has its leader row."""
    return await leadership.leads(conn, agent.session_id, agent.id, alone=session.spec.alone)


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
        if not await session_limits.may_message(conn, own, target, settings):
            # may_message allows every same-project pair, so a refusal is always cross-project
            window = settings.session_reply_window_seconds
            message = f"session {slug} is in another project and neither session is a superleader"
            if window > 0:
                message = (
                    f"session {slug} is in another project, neither session is a superleader,"
                    f" and it has not messaged the caller's session in the last"
                    f" {window // 3600} hours"
                )
            raise ApiError(403, "project_forbidden", message)
        if target.desired == "deleting":
            raise ApiError(409, "session_deleting", f"session {slug} is being discarded")
        hop = await session_limits.check_limits(conn, agent.id, own, target, settings)
        try:
            message_id = await q.enqueue_message(
                conn,
                target.id,
                body.text,
                sender_session_id=own.id,
                sender_slug=own.slug,
                hop=hop,
                lane=body.lane,
            )
        except q.SessionBeingDeleted as exc:
            raise ApiError(409, "session_deleting", "target session is being discarded") from exc
        handled = await session_messaging.mark_handled(conn, own.id, body.handled)
    return SessionMessageQueued(message_id=message_id, handled=handled)


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


async def activity_lines(
    conn: Any, session: sessions_repo.Session, peer: str | None, limit: int
) -> Activity:
    """The newest `limit` digest lines of `session`, oldest first; `peer` keeps one pod's."""
    workdir = f"{WORKDIR_ROOT}/{session.spec.workdir}"
    rows = await events_repo.recent_activity_rows(conn, session.id)
    lines = feed.digest_lines(rows, datetime.now(UTC), workdir)
    stamps = {row["id"]: row["ts"] for row in rows}
    kept = [line for line in lines if peer is None or line.agent == peer]
    return Activity(
        lines=[
            ActivityLine(id=line.id, agent=line.agent, text=line.text, ts=stamps[line.id])
            for line in kept[-limit:]
        ]
    )


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
        return await activity_lines(conn, await _session(conn, agent), peer, limit)
