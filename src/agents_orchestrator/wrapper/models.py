"""Typed views of the wrapper /v1 API. Unknown fields are ignored."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any

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


class Snapshot(_Model):
    # the wrapper sends `session_id`; the orchestrator calls it wrapper_session_id (D3)
    wrapper_session_id: str = Field(validation_alias="session_id")
    name: str | None = None
    state: str
    current_turn_id: str | None = None
    idle_since: datetime | None = None
    context: Context | None = None
    model: str | None = None
    workdir: str | None = None
    created_at: datetime | None = None
    turns_completed: int = 0
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
class CommandAccepted:
    """202: the wrapper queued the command; `record` is its CommandRecord as sent."""

    command_id: str
    record: dict[str, Any]


@dataclass(frozen=True)
class Busy:
    """Not now, nothing was taken: `code` is the wrapper's envelope code ("" when none)."""

    code: str = ""
    reason: str = ""


@dataclass(frozen=True)
class Gone:
    reason: str = ""


@dataclass(frozen=True)
class ConfigError:
    reason: str = ""


@dataclass(frozen=True)
class Rejected:
    """Refused for good: `code` is the wrapper's envelope code ("" when none)."""

    code: str = ""
    reason: str = ""


@dataclass(frozen=True)
class Unavailable:
    """Nothing was delivered: safe to retry."""

    reason: str = ""


@dataclass(frozen=True)
class Ambiguous:
    """The request may have been delivered: verify before any retry."""

    reason: str = ""


SubmitOutcome = TurnAccepted | Busy | Gone | ConfigError | Rejected | Unavailable | Ambiguous
CommandOutcome = CommandAccepted | Busy | ConfigError | Rejected | Gone | Unavailable | Ambiguous


class WrapperError(Exception):
    """The wrapper answered with an error."""

    def __init__(self, status: int, code: str, message: str) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


class WrapperUnreachable(Exception):
    """No usable answer: connect error, timeout or reset."""
