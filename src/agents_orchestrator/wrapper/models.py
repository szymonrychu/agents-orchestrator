"""Typed views of the wrapper /v1 API. Unknown fields are ignored."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


class _Model(BaseModel):
    model_config = ConfigDict(extra="ignore")


class Context(_Model):
    used_tokens: int | None = None
    window_tokens: int | None = None
    used_percentage: float | None = None
    model: str | None = None
    source: str | None = None
    updated_at: datetime | None = None


class Waiting(_Model):
    reason: str
    since: datetime | None = None


class BootstrapStep(_Model):
    step: str
    status: str
    message: str | None = None
    started_at: datetime | None = None
    finished_at: datetime | None = None


class Bootstrap(_Model):
    steps: list[BootstrapStep] = Field(default_factory=list)


class SnapshotError(_Model):
    """The wrapper's failure of a session: the step it failed at and a redacted message."""

    step: str | None = None
    message: str | None = None


class Snapshot(_Model):
    # the wrapper sends `session_id`; the orchestrator calls it wrapper_session_id (D3)
    wrapper_session_id: str = Field(validation_alias="session_id")
    name: str | None = None
    state: str
    # the bootstrap step the session is at, and why it failed (state "failed")
    step: str | None = None
    error: SnapshotError | None = None
    current_turn_id: str | None = None
    idle_since: datetime | None = None
    context: Context | None = None
    model: str | None = None
    workdir: str | None = None
    created_at: datetime | None = None
    turns_completed: int = 0
    # wrapper 0.10.0, additive: running subagents; absent on older wrappers
    outstanding_subagents: int = 0
    # wrapper 0.7.0, additive: absent on older wrappers
    waiting: Waiting | None = None
    # wrapper 0.7.1, additive: "unavailable" while the requested permission mode cannot be used
    permission_mode_state: str | None = None
    permission_mode_reason: str | None = None
    # wrapper T-225 release, additive: "failed" while claude shows a re-login screen
    auth_state: str | None = None
    auth_reason: str | None = None
    bootstrap: Bootstrap | None = None
    # The wrapper reports no transcript field (its resume check tests the file on disk). A
    # completed turn proves a transcript exists (True); zero completed turns proves nothing,
    # since an interrupted or failed first turn can still leave one, so that stays None
    # (unknown) and callers must attempt resume rather than skip it. An explicit field wins.
    has_transcript: bool | None = None

    def model_post_init(self, _ctx: Any) -> None:
        if self.has_transcript is None and self.turns_completed > 0:
            self.has_transcript = True


class TurnSummary(_Model):
    turn_id: str
    origin: str | None = None
    state: str
    started_at: datetime | None = None
    completed_at: datetime | None = None
    stop_reason: str | None = None


class TurnRecord(TurnSummary):
    session_id: str | None = None
    text: str | None = None
    final_text: str | None = None
    error: Any = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class Envelope(_Model):
    seq: int
    ts: str
    session_id: str
    type: str
    data: dict[str, Any] = Field(default_factory=dict)


class PeerSession(_Model):
    session_id: str
    name: str | None = None
    cwd: str | None = None


class PeerSelf(_Model):
    instance: str
    url: str


class PeerMember(_Model):
    instance: str
    url: str
    sessions: list[PeerSession] = Field(default_factory=list)
    last_seen_at: datetime | None = None
    missed: int = 0


class PeersView(_Model):
    enabled: bool
    self: PeerSelf | None = None
    members: list[PeerMember] = Field(default_factory=list)


@dataclass(frozen=True)
class TurnAccepted:
    turn_id: str


@dataclass(frozen=True)
class Busy:
    pass


@dataclass(frozen=True)
class DialogOpen:
    """409 dialog_open: a dialog is waiting for the user, the typed message was refused."""

    reason: str = ""


class DialogPermission(_Model):
    """A held PermissionRequest relay (wrapper 0.11.0)."""

    request_id: str
    tool_name: str
    tool_input: str
    truncated: bool
    suggestions: list[Any] = Field(default_factory=list)
    agent_id: str | None = None
    since: str
    expires_at: str


class DialogState(_Model):
    """GET /v1/sessions/{id}/dialog: what the session is waiting on, if anything."""

    kind: Literal["permission", "dialog", "usage_wait", "none"]
    since: str | None = None
    permission: DialogPermission | None = None
    tail: str | None = None


class DialogResult(_Model):
    """POST /v1/sessions/{id}/dialog: the outcome of an allow, deny or dismiss."""

    status: Literal["answered", "dismissed", "still_open"]
    request_id: str | None = None
    behavior: str | None = None
    escape_sent: bool | None = None


@dataclass(frozen=True)
class Gone:
    reason: str = ""


@dataclass(frozen=True)
class ConfigError:
    reason: str = ""


@dataclass(frozen=True)
class Rejected:
    reason: str = ""


@dataclass(frozen=True)
class Unavailable:
    """Nothing was delivered: safe to retry."""

    reason: str = ""


@dataclass(frozen=True)
class Ambiguous:
    """The request may have been delivered: verify before any retry."""

    reason: str = ""


SubmitOutcome = (
    TurnAccepted | Busy | DialogOpen | Gone | ConfigError | Rejected | Unavailable | Ambiguous
)


class WrapperError(Exception):
    """The wrapper answered with an error."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


class WrapperUnreachable(Exception):
    """No usable answer: connect error, timeout or reset."""


class WrapperTimeout(WrapperUnreachable):
    """The wrapper did not answer within `timeout` seconds."""

    def __init__(self, message: str, timeout: float) -> None:
        super().__init__(message)
        self.timeout = timeout


def describe_exception(exc: BaseException, context: str = "") -> str:
    """The text of `exc`, never empty: its type name (and `context`) when it carries none.

    httpx timeouts have an empty `str()`, which used to surface as "ReadTimeout:".
    """
    text = str(exc).strip()
    if text:
        return text
    return f"{type(exc).__name__} ({context})" if context else type(exc).__name__


class PressureView(_Model):
    """GET /v1/sessions/{id}/pressure: the policy version in force and the live evaluation."""

    version: int | None = None
    level: int = 0
    context: Context | None = None
    handoff_state: Literal["none", "submitted", "blocked", "accepted"] = "none"
    deliveries: int = 0
    stop_blocks_this_turn: int = 0


class Inflight(_Model):
    """GET /v1/sessions/{id}/inflight: work that makes a clear or a move unsafe.

    Blocking: running subagents, background tasks, worktrees that are dirty or ahead of their
    upstream, pod-local scratch files, and a git scan that timed out (`unknown`). Session crons
    are carried to the successor and never block."""

    subagents: int = 0
    background_tasks: list[dict[str, Any]] = Field(default_factory=list)
    crons: list[dict[str, Any]] = Field(default_factory=list)
    worktrees: list[dict[str, Any]] = Field(default_factory=list)
    scratch: list[dict[str, Any]] = Field(default_factory=list)
    unknown: bool = False

    def blocking_items(self) -> list[str]:
        return blocking_items(self.model_dump(mode="json"))


def _label(item: dict[str, Any], *keys: str) -> str:
    return " ".join(str(item[k]) for k in keys if item.get(k)) or "?"


def blocking_items(inflight: dict[str, Any] | None) -> list[str]:
    """Short names of what blocks, from an Inflight dump or a stored `agent_handoffs.inflight`."""
    if not inflight:
        return []
    out: list[str] = []
    if inflight.get("subagents"):
        out.append(f"{inflight['subagents']} subagent(s)")
    for t in inflight.get("background_tasks") or []:
        out.append("background task " + _label(t, "id", "description"))
    for w in inflight.get("worktrees") or []:
        if w.get("dirty") or w.get("ahead"):
            what = "dirty" if w.get("dirty") else "unpushed"
            out.append(f"worktree {_label(w, 'path')} {what}")
    for s in inflight.get("scratch") or []:
        out.append("scratch " + _label(s, "path"))
    if inflight.get("unknown"):
        out.append("in-flight scan incomplete")
    return out
