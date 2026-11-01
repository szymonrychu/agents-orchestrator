"""Request and response models for the sessions, agents and meta routes."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agents_orchestrator.spec import SessionSpec


class CreateSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str | None = None
    spec: SessionSpec | None = None
    preset_id: uuid.UUID | None = None


class PresetIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    spec: SessionSpec = SessionSpec()


class CopyPreset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None


class PresetView(BaseModel):
    id: uuid.UUID
    name: str
    is_default: bool
    spec: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class SessionSummary(BaseModel):
    state: Literal["working", "waiting", "booting", "error", "deleting"]
    ready: int
    replicas: int
    context_pct_max: float | None
    auto_compact_pct: int
    queued: int
    failed: int
    oldest_queued_at: datetime | None  # created_at of the oldest queued message
    # agents waiting on a person (agent.attention set) and agents with a failed bootstrap step;
    # `state` "waiting" above still means all agents idle and is not related to these
    attention_agents: int
    bootstrap_failed_agents: int


class BackupRun(BaseModel):
    """A finished transcripts save: when it finished and the archive size."""

    finished_at: datetime
    bytes: int


class SessionBackup(BaseModel):
    """What a discard would save now; each flag is true only when that save would really run.

    `reason` is null when both run, else `no_session_cache` (no `spec.cache`),
    `cache_not_configured` (no S3 cache configured) or the save's skip reason, the workdir's first.
    """

    workdir: bool
    transcripts: bool
    reason: str | None
    last_transcripts_backup: BackupRun | None


class SessionView(BaseModel):
    id: uuid.UUID
    slug: str
    spec: SessionSpec
    desired: Literal["active", "deleting"]
    status: dict[str, Any]
    generation: int
    observed_generation: int
    created_at: datetime
    preset_id: uuid.UUID | None
    preset_name: str | None
    summary: SessionSummary
    hive: bool
    max_replicas: int
    static: bool
    leader_agent_id: uuid.UUID | None
    leader_epoch: int | None
    leader_since: datetime | None
    leader_reason: str | None
    closes_at: datetime | None
    init_edited_since_run: bool
    backup: SessionBackup


class InitScriptsIn(BaseModel):
    scripts: list[str]


class InitStatus(BaseModel):
    state: Literal["skipped", "running", "succeeded", "failed"]
    scripts_hash: str
    started_at: datetime | None = None
    finished_at: datetime | None = None
    exit_code: int | None = None
    log_tail: str | None = None
    reason: str | None = None


class InitScriptsView(BaseModel):
    session_id: uuid.UUID
    scripts: list[str]
    preset_id: uuid.UUID | None
    preset_name: str | None
    status: InitStatus | None
    edited_since_run: bool
    script_path: str
    log_path: str
    preset_updated: bool


class AgentContext(BaseModel):
    used_tokens: int | None
    window_tokens: int | None
    used_percentage: float | None


class AgentAttention(BaseModel):
    """The agent's claude is waiting on a person; `state` is unchanged by it."""

    reason: str  # permission | dialog | question
    since: datetime | None


class AgentBootstrapStep(BaseModel):
    """A wrapper bootstrap step. `message` is set on failure only: redacted by the wrapper and
    truncated here."""

    step: str
    status: str  # running | succeeded | failed
    message: str | None
    started_at: datetime | None
    finished_at: datetime | None


class AgentView(BaseModel):
    id: uuid.UUID
    ordinal: int
    pod_name: str
    ready: bool
    state: str
    free_since: datetime | None
    current_turn_id: str | None
    context: AgentContext | None
    context_pct: float | None
    leader: bool
    peers: int
    gone: bool
    attention: AgentAttention | None  # null unless the agent waits on a person
    bootstrap: AgentBootstrapStep | None  # the latest bootstrap step
    bootstrap_failure: AgentBootstrapStep | None  # a failed bootstrap step, else null


class MetaResources(BaseModel):
    requests: dict[str, str]
    limits: dict[str, str]


class MetaDefaults(BaseModel):
    model: str
    effort: str
    workdir: str
    resources: MetaResources
    any_mcp_config: dict[str, Any]
    discard_hook: Literal["none", "command", "url"]
    replicas: int
    auto_compact_pct: int
    pvc_size: str
    storage_class: str
    max_attempts: int


class MetaCaps(BaseModel):
    max_replicas: int
    max_pvc_size: str
    max_sessions: int


class MetaCache(BaseModel):
    available: bool
    max_bytes: int


class MetaKubeAccess(BaseModel):
    available: bool


class MetaHookEvent(BaseModel):
    name: str
    can_veto: bool
    supports_matcher: bool
    supports_when: bool
    supports_interval: bool
    interval_required: bool


class Meta(BaseModel):
    defaults: MetaDefaults
    agent_image: str
    agent_images: list[str]
    models: list[str]
    efforts: list[str]
    caps: MetaCaps
    discard_hook: Literal["none", "command", "url"]
    cache: MetaCache
    kube_access: MetaKubeAccess
    hook_events: list[MetaHookEvent]


Lane = Literal["idle", "regular", "priority"]
MessageStatus = Literal["queued", "dispatching", "verifying", "sent", "failed", "cancelled"]


class PostMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1)
    target_agent_id: uuid.UUID | None = None
    lane: Lane = "regular"


class MessageAccepted(BaseModel):
    message_id: int
    status: Literal["queued"] = "queued"


class TurnView(BaseModel):
    status: str | None
    started_at: datetime
    completed_at: datetime | None
    final_text: str | None
    usage: dict[str, Any] | None = None


class MessageView(BaseModel):
    id: int
    session_id: uuid.UUID
    text: str
    status: MessageStatus
    lane: Lane
    target_agent_id: uuid.UUID | None
    agent_id: uuid.UUID | None
    turn_id: str | None
    attempts: int
    max_attempts: int
    next_attempt_at: datetime | None
    error: str | None
    created_at: datetime
    dispatched_at: datetime | None
    turn: TurnView | None


class CommandAccepted(BaseModel):
    command_id: int


class CommandView(BaseModel):
    id: int
    agent_id: uuid.UUID
    kind: str
    status: str
    result: Any | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class PostCheck(BaseModel):
    model_config = ConfigDict(extra="forbid")

    command: str = Field(min_length=1)


class CheckAccepted(BaseModel):
    check_id: int


class CheckView(BaseModel):
    id: int
    session_id: uuid.UUID
    command: str
    status: str
    exit_code: int | None
    log_tail: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class FileEntry(BaseModel):
    name: str
    type: Literal["dir", "file", "symlink", "other"]
    size: int | None
    mtime: str
    # a symlink's followed target inside the workdir; null otherwise (and for links leaving it)
    target_type: Literal["dir", "file", "other"] | None


class FileListing(BaseModel):
    session_id: uuid.UUID
    agent_id: uuid.UUID
    root: str
    path: str
    entries: list[FileEntry]
    next_offset: int | None


class FileContent(BaseModel):
    session_id: uuid.UUID
    agent_id: uuid.UUID
    path: str
    size: int
    mtime: str
    mime: str | None
    kind: Literal["text", "binary", "too_large"]
    content: str | None


class FileStamp(BaseModel):
    """The `mtime` and `size` of a file as the content route (or the last save) reported them."""

    model_config = ConfigDict(extra="forbid")

    mtime: str
    size: int = Field(ge=0)


# The wrapper refuses content over 1 MiB of UTF-8 (`limits.file_preview_max_bytes`, 413
# `too_large`). A JSON body spends at most 6 bytes per content byte (every byte written as a
# `\u00XX` escape) plus the key framing and the `expected` stamp, so a body over this size
# cannot hold an acceptable content and the orchestrator refuses it before parsing it.
WRITE_CONTENT_MAX_BYTES = 1024 * 1024
WRITE_BODY_MAX_BYTES = 6 * WRITE_CONTENT_MAX_BYTES + 16 * 1024


class WriteFile(BaseModel):
    """Body of a file save: both keys required; `expected: null` overwrites unconditionally.

    A body over 6 MiB + 16 KiB (1 MiB of content, every byte escaped) is 413 `too_large`
    before it is read in full."""

    model_config = ConfigDict(extra="forbid")

    content: str
    expected: FileStamp | None

    @field_validator("content")
    @classmethod
    def _utf8(cls, value: str) -> str:
        value.encode("utf-8")  # a lone surrogate is not text a file can hold (the wrapper's rule)
        return value


class FileWritten(BaseModel):
    session_id: uuid.UUID
    agent_id: uuid.UUID
    path: str
    size: int
    mtime: str


class CreateTerminal(BaseModel):
    """Both sizes are optional; the wrapper validates the 1..1000 range (`invalid_size`)."""

    model_config = ConfigDict(extra="forbid")

    rows: int | None = None
    cols: int | None = None


class TerminalView(BaseModel):
    agent_id: uuid.UUID
    session_id: uuid.UUID
    pod_name: str
    terminal_id: str
    created_at: datetime
    alive: bool
    exit_code: int | None
    attached: bool
    rows: int
    cols: int


class TerminalList(BaseModel):
    terminals: list[TerminalView]
    unavailable_agents: list[uuid.UUID]


class HandoverIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    to: str | None = None
    reason: str = Field(min_length=1, max_length=500)
    epoch: int


class MakeLeaderIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    agent_id: uuid.UUID


class ScaleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replicas: int


class ScaleResult(BaseModel):
    session_id: uuid.UUID
    replicas: int
    previous_replicas: int
    max_replicas: int


class LeadershipRequested(BaseModel):
    request_id: int


class ArchiveView(BaseModel):
    key: str
    name: str
    size: int
    last_modified: datetime
    referenced_by: list[str]
    protected: bool


class ArchivePage(BaseModel):
    items: list[ArchiveView]
    next_cursor: str | None


class CleanupItem(BaseModel):
    key: str
    size: int
    reason: str


class CleanupPreview(BaseModel):
    would_delete: list[CleanupItem]
    count: int
    total_bytes: int
    retention_days: int
    keep_newest: int
    grace_seconds: int
    sweep_enabled: bool
    sweep_dry_run: bool
    last_sweep: dict[str, Any] | None


class DeleteArchives(BaseModel):
    model_config = ConfigDict(extra="forbid")

    keys: list[str] = Field(min_length=1, max_length=1000)


class ArchiveRefusal(BaseModel):
    key: str
    reason: str


class ArchiveDeleteError(BaseModel):
    key: str
    code: str
    message: str


class ArchiveDeleteResult(BaseModel):
    deleted: list[str]
    refused: list[ArchiveRefusal]
    errors: list[ArchiveDeleteError]
