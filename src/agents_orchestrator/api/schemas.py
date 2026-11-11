"""Request and response models for the sessions, agents and meta routes."""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, SecretStr, field_validator

from agents_orchestrator.labels import normalize_labels
from agents_orchestrator.settings import (
    SWEEP_GRACE_SECONDS_MAX,
    SWEEP_INTERVAL_SECONDS_MAX,
    SWEEP_KEEP_NEWEST_MAX,
    SWEEP_RETENTION_DAYS_MAX,
)
from agents_orchestrator.spec import SessionSpec


class CreateSession(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str | None = None
    spec: SessionSpec | None = None
    preset_id: uuid.UUID | None = None
    # None: the preset's project, or the default project for a spec-only session
    project: str | None = None


class PresetIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    spec: SessionSpec = SessionSpec()
    superleader_manageable: bool | None = None
    # None keeps the stored labels on PUT and means none on POST
    labels: list[str] | None = None
    # None keeps the stored project on PUT and means the default project on POST
    project: str | None = None

    @field_validator("labels")
    @classmethod
    def _labels(cls, value: list[str] | None) -> list[str] | None:
        return None if value is None else normalize_labels(value)


class VaultAttachIn(BaseModel):
    """Attach a vault entry to a preset: `envs` maps entry env to preset env (every env of the
    entry under its own name when omitted); `replace` lets it overwrite an existing preset env."""

    model_config = ConfigDict(extra="forbid")

    envs: dict[str, str] | None = Field(default=None, min_length=1)
    replace: bool = False

    @field_validator("envs")
    @classmethod
    def _distinct(cls, value: dict[str, str] | None) -> dict[str, str] | None:
        if value is not None and len(set(value.values())) != len(value):
            raise ValueError("two entry envs map to the same preset env")
        return value


class PresetPatch(BaseModel):
    """Admin partial edit: `spec` is an RFC 7396 merge patch of the stored spec. A `project`
    other than the stored one moves the preset."""

    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    labels: list[str] | None = None
    project: str | None = None
    spec: dict[str, Any] | None = None

    @field_validator("labels")
    @classmethod
    def _labels(cls, value: list[str] | None) -> list[str] | None:
        return None if value is None else normalize_labels(value)


class SessionSpecPatch(BaseModel):
    """Agent edit of a live session: `spec` is an RFC 7396 merge patch of the stored spec."""

    model_config = ConfigDict(extra="forbid")

    spec: dict[str, Any]


class SessionProjectIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    project: str


class SessionLabelsIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    labels: list[str]

    @field_validator("labels")
    @classmethod
    def _labels(cls, value: list[str]) -> list[str]:
        return normalize_labels(value)


class AgentTokenIn(BaseModel):
    """An agent token a service was handed by one of its sessions' agents, to be checked."""

    model_config = ConfigDict(extra="forbid")

    token: SecretStr = Field(min_length=1, max_length=512)


class AgentTokenVerified(BaseModel):
    session_id: uuid.UUID


class CopyPreset(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    # None keeps the source's project
    project: str | None = None


class ProjectIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    slug: str
    name: str
    description: str = ""


class ProjectPatch(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = None
    description: str | None = None
    archived: bool | None = None


class ProjectView(BaseModel):
    slug: str
    name: str
    description: str
    archived: bool
    created_at: datetime
    updated_at: datetime
    # None on the service port, which does not read counts
    presets: int | None = None
    sessions: int | None = None


class PresetView(BaseModel):
    id: uuid.UUID
    name: str
    is_default: bool
    superleader_manageable: bool
    labels: list[str]
    project: str
    # the privileged grants on (preset_policy.privileged), so a client never names the fields
    privileged: list[str]
    # env names to re-attach as vault references (set by migration 0041)
    vault_reattach: list[str]
    spec: dict[str, Any]
    created_at: datetime
    updated_at: datetime


class VaultAttachView(PresetView):
    """The preset view an admin vault attach answers. `attachment_privileged` marks a preset
    whose session can read every Secret in the namespace (`privileged` is not empty); marked,
    never blocked."""

    attachment_privileged: bool


class ProposalIn(BaseModel):
    """A change proposal (plan 3.4): an RFC 7396 merge `patch` of the target's spec, which
    may carry vault references (never a value)."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["preset", "schedule", "session"]
    target: str  # preset id or name, schedule id, session slug
    patch: dict[str, Any]
    reason: str = Field(min_length=1, max_length=4000)
    client_key: str | None = Field(default=None, max_length=200)


class ScheduleIn(BaseModel):
    """agents-schedule's `ScheduleIn` (v0.6.0): the full body of its `PUT /schedules/{id}`, which
    a schedule proposal is merged into, validated as and applied with."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    cron: str
    timezone: str | None = None  # None: the schedule service's default timezone
    preset_id: uuid.UUID
    prompt: str = Field(min_length=1, max_length=16384)
    enabled: bool = True


class ProposalRevise(BaseModel):
    """A new revision: a full proposal against the target as it is now, not a delta."""

    model_config = ConfigDict(extra="forbid")

    patch: dict[str, Any]
    reason: str | None = Field(default=None, min_length=1, max_length=4000)


class ReviewItemIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str
    decision: Literal["approve", "rework", "deny"]
    note: str | None = Field(default=None, max_length=4000)


class ReviewIn(BaseModel):
    """A human review of the proposal's current revision: one decision per item (plan 3.4)."""

    model_config = ConfigDict(extra="forbid")

    revision: int
    items: list[ReviewItemIn]


class ProposalItemView(BaseModel):
    path: str
    before: Any
    after: Any
    security: bool
    grant: bool
    decision: Literal["pending", "approved", "rework", "denied"]
    note: str | None


class ProposalRevisionView(BaseModel):
    revision: int
    reason: str
    created_at: datetime
    reviewed_at: datetime | None
    outcome: str | None
    items: list[ProposalItemView]


class ProposalView(BaseModel):
    id: int
    kind: Literal["preset", "schedule", "session"]
    target_id: str
    target_name: str
    proposer_slug: str
    proposer_superleader: bool
    status: Literal[
        "open", "rework", "stale", "applied", "denied", "withdrawn", "expired", "target_gone"
    ]
    revision: int
    created_at: datetime
    updated_at: datetime
    expires_at: datetime
    closed_at: datetime | None
    stale: bool  # the live target differs from the current revision's base (read-only check)
    revisions: list[ProposalRevisionView]


class BootstrapFailure(BaseModel):
    """Why one agent's wrapper session failed to start: the step, a one-line message and the
    redacted log tail (cut like check logs). `step` is "create" when the create itself failed."""

    pod: str
    step: str
    message: str
    log_tail: str | None = None


class SessionSummary(BaseModel):
    state: Literal["working", "waiting", "booting", "error", "deleting"]
    # booting: no pod ready yet; ready: a pod is ready and no agent failed to start; a recorded
    # start failure of any live agent makes it bootstrap_failed until that agent has a session
    start_state: Literal["booting", "ready", "bootstrap_failed"]
    bootstrap_failures: list[BootstrapFailure]
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
    # agents whose wrapper reported the requested permission mode (auto) unavailable
    permission_unavailable_agents: int


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


CloseKind = Literal["self", "inactivity"]


class VaultPinView(BaseModel):
    """What a session was pinned to for one vault env variable; never a value."""

    entry: uuid.UUID | None
    entry_env: str | None
    version: int | None
    protected: bool


class SessionView(BaseModel):
    id: uuid.UUID
    slug: str
    spec: SessionSpec
    # the session's vault pins by env name (empty without vault refs)
    vault: dict[str, VaultPinView] = {}
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
    close_requested_at: datetime | None = None
    close_reason: str | None = None
    # who asked for the pending close: the agent (`self`) or the inactivity limit
    close_kind: CloseKind | None = None
    # an inactivity close is forced at this time even mid-turn
    close_deadline_at: datetime | None = None
    inactivity_reminded_at: datetime | None = None
    # the quiet clock: the last user-visible update of the session
    last_update_at: datetime | None = None
    # when the reminder is due; null when it is off, for a superleader or with a close pending
    reminder_at: datetime | None = None
    # when the inactivity limit requests the close; null when it is off or for a superleader
    inactivity_closes_at: datetime | None = None
    created_by_service: str | None = None
    created_by_session: uuid.UUID | None = None
    labels: list[str] = []
    # the project the session belongs to; moved by PATCH /sessions/{id}/project
    project: str


class InitScriptsIn(BaseModel):
    scripts: list[str]


class PermissionModeIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    permission_mode: Literal["bypassPermissions", "auto"]


class PermissionModeView(BaseModel):
    session_id: uuid.UUID
    permission_mode: Literal["bypassPermissions", "auto"]
    preset_id: uuid.UUID | None = None
    preset_name: str | None = None
    preset_updated: bool = False


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


class DialogPermissionView(BaseModel):
    """A held PermissionRequest of the agent's claude (wrapper 0.11.0)."""

    request_id: str
    tool_name: str
    tool_input: str
    truncated: bool
    suggestions: list[Any] = Field(default_factory=list)
    agent_id: str | None = None
    since: str
    expires_at: str


class DialogView(BaseModel):
    """What the agent's claude waits on: the wrapper's dialog state plus the agent identity."""

    kind: Literal["permission", "dialog", "usage_wait", "none"]
    since: str | None = None
    permission: DialogPermissionView | None = None
    tail: str | None = None
    agent_id: uuid.UUID
    session_id: uuid.UUID


class DialogAnswer(BaseModel):
    """A person's answer to the open dialog; forwarded to the wrapper unchanged."""

    model_config = ConfigDict(extra="forbid")

    action: Literal["allow", "deny", "dismiss"]
    request_id: str | None = Field(None, pattern=r"^perm_[0-9a-f]{12}$")
    expect_since: str | None = None
    message: str | None = Field(None, max_length=2000)


class DialogAnswerResult(BaseModel):
    status: Literal["answered", "dismissed", "still_open"]
    request_id: str | None = None
    behavior: str | None = None
    escape_sent: bool | None = None
    agent_id: uuid.UUID


class AgentBootstrapStep(BaseModel):
    """A wrapper bootstrap step. `message` is set on failure only: redacted by the wrapper and
    truncated here."""

    step: str
    status: str  # running | succeeded | failed
    message: str | None
    started_at: datetime | None
    finished_at: datetime | None
    # redacted tail of the failing step's log; set on a start failure the orchestrator recorded
    log_tail: str | None = None


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
    # "unavailable" when the wrapper could not use the requested permission mode, else null
    permission_mode_state: Literal["unavailable"] | None = None
    permission_mode_reason: str | None = None


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


class MetaKubeClusterAccess(BaseModel):
    available: bool


class MetaKubeNamespaceAccess(BaseModel):
    available: bool
    namespaces: list[str]
    tiers: list[str]


class MetaDocker(BaseModel):
    available: bool
    storage: str
    max_storage: str
    max_cpu: str
    max_memory: str


class MetaHookEvent(BaseModel):
    name: str
    can_veto: bool
    supports_matcher: bool
    supports_when: bool
    supports_interval: bool
    interval_required: bool


class MetaPermissionMode(BaseModel):
    value: Literal["bypassPermissions", "auto"]
    label: str


class MetaUsage(BaseModel):
    configured: bool
    quota_default_pct: int


class UsageWindowView(BaseModel):
    utilization_pct: float
    resets_at: datetime
    stale: bool
    observed_at: datetime | None
    source: str | None


class UsageView(BaseModel):
    """The account's usage windows as the usage gate reads them (read-only)."""

    configured: bool
    available: bool
    source: str
    updated_at: datetime | None
    five_hour: UsageWindowView | None
    seven_day: UsageWindowView | None


class MetaInactivity(BaseModel):
    reminder_seconds: int
    close_seconds: int
    handoff_seconds: int


class Meta(BaseModel):
    defaults: MetaDefaults
    agent_image: str
    agent_images: list[str]
    models: list[str]
    efforts: list[str]
    caps: MetaCaps
    discard_hook: Literal["none", "command", "url"]
    cache: MetaCache
    kube_cluster_access: MetaKubeClusterAccess
    kube_namespace_access: MetaKubeNamespaceAccess
    docker: MetaDocker
    hook_events: list[MetaHookEvent]
    permission_modes: list[MetaPermissionMode]
    usage: MetaUsage
    inactivity: MetaInactivity


Lane = Literal["idle", "regular", "priority"]
MessageStatus = Literal["queued", "dispatching", "verifying", "sent", "failed", "cancelled"]
# an orchestrator-written message; null for every other message
Notice = Literal["inactivity_reminder", "inactivity_close", "handover", "proposal", "project_grant"]


class MessageAttachment(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    mime: str
    data_b64: str


class MessageAttachmentView(BaseModel):
    name: str
    mime: str
    size: int
    path: str


class PostMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    text: str = Field(min_length=1)
    target_agent_id: uuid.UUID | None = None
    lane: Lane = "regular"
    attachments: list[MessageAttachment] = []


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
    notice: Notice | None = None
    attachments: list[MessageAttachmentView]


class CommandAccepted(BaseModel):
    command_id: int


class SessionRestartAccepted(BaseModel):
    command_ids: list[int]


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
    reason: str = Field(min_length=1, max_length=2000)
    epoch: int
    #: Ids of incoming session messages the leader answered or that need no answer.
    handled: list[int] = Field(default_factory=list, max_length=50)


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
    status: Literal["pending"] = "pending"
    applies: str = "when your turn has ended and your background agents have reported"
    handled: list[int] = []


class WithdrawIn(BaseModel):
    model_config = ConfigDict(extra="forbid")

    request_id: int | None = None


class Withdrawn(BaseModel):
    withdrawn: list[int]


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


class SweepPolicyIn(BaseModel):
    """The whole sweep policy; every field is required (a PUT replaces the saved policy)."""

    model_config = ConfigDict(extra="forbid")

    enabled: bool
    dry_run: bool
    retention_days: int = Field(ge=1, le=SWEEP_RETENTION_DAYS_MAX)
    keep_newest: int = Field(ge=1, le=SWEEP_KEEP_NEWEST_MAX)
    interval_seconds: int = Field(ge=60, le=SWEEP_INTERVAL_SECONDS_MAX)
    grace_seconds: int = Field(ge=0, le=SWEEP_GRACE_SECONDS_MAX)


class SweepPolicyView(BaseModel):
    enabled: bool
    dry_run: bool
    retention_days: int
    keep_newest: int
    interval_seconds: int
    grace_seconds: int


class CleanupSettingsView(BaseModel):
    policy: SweepPolicyView
    defaults: SweepPolicyView
    source: Literal["database", "environment"]
    updated_at: datetime | None
    run_requested_at: datetime | None
    run_pending: bool
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
