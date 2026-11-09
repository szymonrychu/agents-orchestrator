"""Validated configuration for every orchestrator process (env prefix ORCH_)."""

from __future__ import annotations

import ipaddress
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Annotated, Any, Literal
from urllib.parse import urlparse

import yaml
from pydantic import Field, ValidationError, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict

from agents_orchestrator.spec import (
    CACHE_MAX_BYTES_LIMIT,
    INACTIVITY_MAX_SECONDS,
    INACTIVITY_MIN_SECONDS,
    KUBE_CUSTOM_ROLE_TIERS,
    KUBE_TIERS,
    parse_quantity,
)

PREFIX = "ORCH_"
MIN_MASTER_KEY_BYTES = 32
# Upper bounds of the archive sweep policy; also literals in the CHECKs of migration 0025.
SWEEP_RETENTION_DAYS_MAX = 3650
SWEEP_KEEP_NEWEST_MAX = 1000
SWEEP_INTERVAL_SECONDS_MAX = 30 * 86400
SWEEP_GRACE_SECONDS_MAX = 7 * 86400
_TAG_RE = re.compile(r"^v\d+\.\d+\.\d+$")
_DOCKER_IMAGE_RE = re.compile(r"^[a-z0-9][a-z0-9._:/-]*:\d+\.\d+\.\d+-dind$")
_SERVICE_TOKEN_RE = re.compile(r"^([a-z0-9][a-z0-9-]{0,62}):([0-9a-f]{64})$")

CsvList = Annotated[list[str], NoDecode]


class SettingsError(ValueError):
    """One error naming every bad ORCH_* variable."""

    def __init__(self, problems: dict[str, str]):
        self.problems = problems
        super().__init__(
            "invalid configuration: " + "; ".join(f"{k}: {v}" for k, v in problems.items())
        )


@dataclass(frozen=True)
class DiscardHook:
    """A pre-discard hook: the settings one (command or URL) or a session's own URL.

    `token` is the bearer sent with a URL hook; only the settings URL ever carries one.
    """

    kind: Literal["command", "url"]
    target: str
    name: Literal["settings", "session"] = "settings"
    token: str = field(default="", repr=False)


def _var(field: str) -> str:
    return PREFIX + field.upper()


def _csv(value: object) -> object:
    if isinstance(value, str):
        return [p.strip() for p in value.split(",") if p.strip()]
    return value


_SUBDOMAIN = re.compile(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*")


class _Bad(ValueError):
    """Validation failure that already names its variable(s)."""


def normalise_origin(value: str) -> str | None:
    """Canonical `scheme://host[:port]` (lower-cased, default port dropped), or None.

    Rejects a path, query, fragment, userinfo, a missing host and any scheme but http(s), so the
    literal `null` Origin of sandboxed pages never matches.
    """
    value = value.strip().lower()
    try:
        parsed = urlparse(value)
        port = parsed.port
    except ValueError:
        return None
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    if parsed.path or parsed.params or parsed.query or parsed.fragment:
        return None
    host = f"[{parsed.hostname}]" if ":" in parsed.hostname else parsed.hostname
    default = {"http": 80, "https": 443}[parsed.scheme]
    origin = f"{parsed.scheme}://{host}" + (f":{port}" if port not in (None, default) else "")
    # Anything urlparse tolerated beyond the bare origin (stray separators, spaces) is rejected.
    explicit = f"{parsed.scheme}://{host}" + (f":{port}" if port is not None else "")
    return origin if value == explicit else None


#: The fixed part of the wrapper's priority hold: four spaced ESCs (1 s apart at most) and two
#: runner drains after an interrupt (5 s each), see `priority_hold_seconds`.
PRIORITY_HOLD_SLACK_SECONDS = 4 * 1.0 + 2 * 5.0


def priority_hold_seconds(interrupt_settle_seconds: float) -> float:
    """The longest a wrapper holds a priority send before its 202 (claude-code-wrapper
    `SessionRouter._priority`): it interrupts the running turn (an ESC, up to one settle wait
    for the interrupt line, a second ESC), waits up to one settle for the turn claude starts
    for the still-queued messages and interrupts that one too, then submits. Three settle
    waits plus the fixed slack; onTurn and onInterrupt hooks are extra."""
    return 3 * interrupt_settle_seconds + PRIORITY_HOLD_SLACK_SECONDS


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix=PREFIX, extra="ignore")

    database_url: str
    master_key_file: str = ""
    master_key: str | None = None
    namespace: str = "agents"
    agent_image: str = ""
    check_image: str = ""

    default_model: str = ""  # empty = the first entry of models
    default_effort: str = ""
    models: CsvList = [
        "claude-opus-5-5",
        "claude-sonnet-5-5",
        "claude-haiku-5-5",
        "claude-fable-5-1",
    ]
    efforts: CsvList = ["low", "medium", "high", "xhigh", "max"]
    default_cpu_request: str = "500m"
    default_cpu_limit: str = "4"
    default_memory_request: str = "1Gi"
    default_memory_limit: str = "8Gi"
    default_replicas: int = 1
    default_auto_compact_pct: int = 95
    default_restart_continue_prompt: str = (
        "Claude was restarted. Continue the task where you left off."
    )

    pvc_storage_class: str = "rook-ceph-rwx"
    pvc_size: str = "20Gi"
    claude_secret_name: str = "agents-orchestrator-claude"
    image_pull_secret: str = "regcred"
    any_mcp_config_file: str = ""
    wrapper_port: int = 8080
    cluster_domain: str = "cluster.local"
    agent_termination_grace_seconds: int = 90

    http_host: str = "0.0.0.0"
    http_port: int = 8000
    agent_port: int = 8001
    service_port: int = 8002
    service_tokens_file: str = ""
    service_tokens: CsvList = Field(default=[], validate_default=True)
    # JSON object: service name to the route names ("<METHOD> <path template>") it may call on
    # the service port; a service without an entry may call every route of the service list
    service_scopes: dict[str, list[str]] = {}
    agent_api_url: str | None = None
    dispatch_poll_seconds: float = 2
    reconcile_poll_seconds: float = 10
    ingest_refresh_seconds: float = 5
    shutdown_grace_seconds: float = 20
    log_level: str = "INFO"

    allowed_cidrs: CsvList = ["192.168.1.0/24"]
    allowed_origins: CsvList = []
    agent_images: CsvList = []
    max_replicas: int = 16
    max_pvc_size: str = "100Gi"
    max_sessions: int = 20
    autoclose_min_seconds: int = Field(default=300, ge=1)
    attachments_max_bytes: int = Field(default=6291456, ge=1)
    attachments_max_total_bytes: int = Field(default=8388608, ge=1)
    attachments_max_count: int = Field(default=8, ge=0)
    #: Seconds a session with a requested self-close stays idle before the sweeper discards it,
    #: so readers of the final answer (matrix connector, trigger runs) get it first. The floor of
    #: 10 s keeps a session from being discarded between a message turning `sent` and its turn
    #: being recorded.
    close_grace_seconds: int = Field(default=30, ge=10, le=3600)
    #: Quiet time (no user-visible update) before the leader gets one reminder, and before the
    #: orchestrator requests the close itself; each 0 (off) or 60 to 2592000, reminder first.
    #: A session's `spec.inactivity` overrides either one.
    inactivity_reminder_seconds: int = 0
    inactivity_close_seconds: int = 0
    #: Seconds the leader has to hand off after the hard-limit notice; 0 means no window.
    inactivity_handoff_seconds: int = Field(default=300, ge=0, le=3600)
    #: Size budget of the predecessor's rendered transcript attached to a handover notice (the
    #: oldest lines are dropped first), and the cap on each tool output in it.
    handover_transcript_bytes: int = Field(default=65536, ge=8192, le=1048576)
    handover_output_chars: int = Field(default=500, ge=100, le=4000)
    #: Rollout: seconds the leader has, once only its pod is outdated, to hand over at a natural
    #: boundary before the reconciler queues the forced `rollout` handover; and the cap after
    #: which background work stops holding a rollout (members since the rollout began, the
    #: leader since its forced request was queued). The cap must exceed the window.
    rollout_handover_seconds: int = Field(default=1800, ge=0, le=86400)
    rollout_max_defer_seconds: int = Field(default=14400, ge=600, le=86400)
    #: The deadline of every other `handover` request: queued longer than this, background
    #: work stops holding it, and one the leader's turn still holds is rejected `expired`.
    handover_max_wait_seconds: int = Field(default=1800, ge=60, le=86400)

    wrapper_send_timeout_seconds: float = 15
    #: Rendered into every wrapper as `sessions.interrupt_settle_seconds`; the priority lane's
    #: send timeout is derived from it (`priority_send_timeout_seconds`).
    wrapper_interrupt_settle_seconds: float = Field(default=20, ge=0)
    dispatching_stale_seconds: float = 60
    dispatching_heartbeat_seconds: float = 5
    lock_heartbeat_seconds: float = 5
    lock_tcp_user_timeout_seconds: float = 15

    retry_backoff_base_seconds: float = 2
    retry_backoff_max_seconds: float = 60
    ambiguous_settle_seconds: float = 60
    ambiguous_deadline_seconds: float = 600

    discard_hook_command: str = ""
    discard_hook_url: str = ""
    # bearer for the settings URL hook only, never sent to a session's own discard_hook_url
    discard_hook_token: str = Field(default="", repr=False)
    discard_hook_timeout_seconds: float = 120
    # a URL hook server that stalls fails the call after these, well inside the overall timeout
    discard_hook_connect_timeout_seconds: float = Field(default=5, gt=0, le=60)
    discard_hook_read_timeout_seconds: float = Field(default=30, gt=0, le=300)
    # hook time one reconcile pass may spend; spent, later deleting sessions wait for the next pass
    discard_hook_pass_budget_seconds: float = Field(default=60, gt=0, le=600)
    # an owed hook still failing this long after its first failure is abandoned and the discard
    # goes on (the workspace PVC is kept); 30 min rides out a restart or short outage (T-26)
    discard_hook_give_up_seconds: float = Field(default=1800, gt=0, le=7 * 86400)
    discard_quiesce_timeout_seconds: float = 120
    discard_drain_seconds: float = 5
    scale_drain_seconds: float = 900

    any_mcp_repo: str = "https://github.com/szymonrychu/any-mcp"
    any_mcp_ref: str = "v0.5.0"
    any_mcp_plugins_repo: str = "https://github.com/szymonrychu/any-mcp-tatara-plugins"
    any_mcp_plugins_ref: str = "v0.28.0"
    on_start_timeout_seconds: float = 600

    cache_s3_endpoint: str = ""
    cache_s3_region: str = "us-east-1"
    cache_s3_bucket: str = ""
    cache_s3_prefix: str = "workspace-cache/"
    cache_s3_path_style: bool = False
    cache_s3_verify_tls: bool = True
    cache_s3_access_key_id: str = ""
    cache_s3_secret_access_key: str = Field(default="", repr=False)
    cache_image: str = ""
    cache_max_bytes: int = Field(default=CACHE_MAX_BYTES_LIMIT, ge=1, le=CACHE_MAX_BYTES_LIMIT)
    cache_timeout_seconds: int = Field(default=1800, ge=60, le=86400)
    # Archive cleanup sweep (reconciler leader). The grace period, when unset, is the cache Job
    # deadline plus the presign margin: an archive younger than that may still be mid-save.
    # Seconds between two restarts an agent asks for on its own session (429 restart_cooldown).
    self_restart_cooldown_seconds: int = Field(default=600, ge=1)
    cache_retention_days: int = Field(default=30, ge=1, le=SWEEP_RETENTION_DAYS_MAX)
    cache_keep_newest: int = Field(default=3, ge=1, le=SWEEP_KEEP_NEWEST_MAX)
    cache_sweep_interval_seconds: int = Field(
        default=6 * 3600, ge=60, le=SWEEP_INTERVAL_SECONDS_MAX
    )
    cache_sweep_grace_seconds: int | None = Field(default=None, ge=0, le=SWEEP_GRACE_SECONDS_MAX)
    cache_sweep_enabled: bool = True
    cache_sweep_dry_run: bool = True
    # The api pod's own list/get/delete key pair (the reconciler keeps cache_s3_access_key_id).
    cache_s3_api_access_key_id: str = ""
    cache_s3_api_secret_access_key: str = Field(default="", repr=False)

    # Cluster-wide Kubernetes access (spec.kube_cluster_access): each granted session gets its own
    # ServiceAccount bound to cluster-admin, so the setting is a plain switch.
    kube_cluster_access_enabled: bool = False
    # Namespace-scoped Kubernetes access (spec.kube_namespace_access): the allowlisted
    # namespaces, the enabled tiers (subset of KUBE_TIERS) and the prefix of the custom
    # ClusterRoles of the port-forward and exec tiers (required when either tier is enabled).
    kube_namespace_access_namespaces: CsvList = []
    kube_namespace_access_tiers: CsvList = []
    kube_namespace_access_role_prefix: str = Field(default="", validate_default=True)
    # Opt-in privileged Docker-in-Docker sidecar for agent pods (spec.docker). Empty image = off.
    # Pinned by exact semver tag (docker:X.Y.Z-dind), never latest or a sha.
    docker_image: str = ""
    docker_storage: str = "30Gi"
    docker_cpu_request: str = "500m"
    docker_cpu_limit: str = "4"
    docker_memory_request: str = "1Gi"
    docker_memory_limit: str = "8Gi"
    docker_ephemeral_request: str = "5Gi"
    max_docker_storage: str = "100Gi"
    max_docker_cpu: str = "8"
    max_docker_memory: str = "16Gi"
    agent_service_account: str = "default"

    session_message_rate_per_minute: int = Field(default=10, ge=1)
    session_message_queued_max: int = Field(default=20, ge=1)
    session_message_max_hops: int = Field(default=8, ge=1)
    session_message_pair_max_per_hour: int = Field(default=30, ge=1)
    session_reply_window_seconds: int = Field(default=86400, ge=0)

    # change proposals (preset-change-proposals 3.4)
    proposal_ttl_seconds: int = Field(default=604800, ge=3600)
    proposals_open_max: int = Field(default=20, ge=1)
    proposal_sweep_seconds: int = Field(default=60, ge=10)

    chat_url: str = ""
    chat_token: str = Field(default="", repr=False)

    # taskboard relay (agents-taskboard): url and token come as a pair, or neither
    taskboard_url: str = ""
    taskboard_token: str = Field(default="", repr=False)

    # usage gate (agents-usage): url and service token come as a pair, or neither
    usage_url: str = ""
    usage_token: str = Field(default="", repr=False)
    usage_quota_default: int = Field(default=100, ge=1, le=100)
    usage_cache_seconds: float = Field(default=30, ge=1, le=600)
    usage_timeout_seconds: float = Field(default=3, gt=0, le=30)

    # usage forwarding from agent pods (claude-code-wrapper >= 0.8.0 `usage` section): the
    # agents-usage ingest URL and the Secret name (key `token`) holding the shared ingest token.
    # Pod-side only; the token value never reaches the orchestrator. Pair, or neither.
    agent_usage_url: str = ""
    agent_usage_secret: str = ""

    # superleader admin relay upstreams (url and service token come as a pair, or neither)
    webhook_url: str = ""
    webhook_token: str = Field(default="", repr=False)
    schedule_url: str = ""
    schedule_token: str = Field(default="", repr=False)
    connector_url: str = ""
    connector_token: str = Field(default="", repr=False)
    vault_url: str = ""
    vault_token: str = Field(default="", repr=False)
    history_url: str = ""
    history_token: str = Field(default="", repr=False)

    def __init__(self, **values: object) -> None:
        try:
            super().__init__(**values)
        except ValidationError as exc:
            problems: dict[str, str] = {}
            for err in exc.errors():
                loc = err["loc"]
                msg = err["msg"].removeprefix("Value error, ")
                if loc:
                    # "ORCH_X::msg" names a different variable than the failing field
                    var, sep, rest = msg.partition("::")
                    if sep and var.startswith(PREFIX):
                        problems.setdefault(var, rest)
                    else:
                        problems.setdefault(_var(str(loc[0])), msg)
                else:
                    problems.setdefault("ORCH_*", msg)
            raise SettingsError(problems) from None

    _split = field_validator(
        "models",
        "efforts",
        "allowed_cidrs",
        "allowed_origins",
        "agent_images",
        "kube_namespace_access_namespaces",
        "kube_namespace_access_tiers",
        mode="before",
    )(lambda cls, v: _csv(v))

    # Checks are field validators (not one mode="after" model validator) so they still run
    # when an unrelated field fails type or required checks; cross-field checks skip
    # silently when a dependency already failed (that failure is reported on its own).

    @field_validator("service_port")
    @classmethod
    def _check_service_port(cls, v: int, info: ValidationInfo) -> int:
        clashes = [
            _var(other) for other in ("http_port", "agent_port") if info.data.get(other) == v
        ]
        if clashes:
            raise _Bad("must differ from " + " and ".join(clashes))
        return v

    @field_validator("service_tokens", mode="before")
    @classmethod
    def _read_service_tokens(cls, v: object, info: ValidationInfo) -> object:
        path = info.data.get("service_tokens_file")
        if path:
            try:
                v = Path(path).read_text()
            except OSError as exc:
                raise _Bad(
                    f"ORCH_SERVICE_TOKENS_FILE::cannot read {path}: {type(exc).__name__}"
                ) from None
        if isinstance(v, str):
            return [p for p in re.split(r"[,\s]+", v) if p]
        return v

    @field_validator("service_tokens")
    @classmethod
    def _check_service_tokens(cls, v: list[str]) -> list[str]:
        # Entries are never echoed: a typo can put the raw token where the hash belongs.
        seen: set[str] = set()
        for n, entry in enumerate(v, start=1):
            match = _SERVICE_TOKEN_RE.match(entry)
            if match is None:
                raise _Bad(f"entry {n} is not <name>:<64 lowercase hex sha256>")
            if match.group(1) in seen:
                raise _Bad(f"entry {n} repeats a service name")
            seen.add(match.group(1))
        return v

    @field_validator("service_scopes")
    @classmethod
    def _check_service_scopes(
        cls, v: dict[str, list[str]], info: ValidationInfo
    ) -> dict[str, list[str]]:
        from agents_orchestrator.api.service_auth import ROUTE_NAMES

        tokened = {entry.split(":", 1)[0] for entry in info.data.get("service_tokens", [])}
        untokened = sorted(set(v) - tokened)
        if untokened:
            raise _Bad("names a service without a token: " + ", ".join(untokened))
        for name, routes in v.items():
            unknown = [r for r in routes if r not in ROUTE_NAMES]
            if unknown:
                raise _Bad(f"scope of {name!r} lists routes the service port does not serve")
        return v

    @field_validator("allowed_cidrs")
    @classmethod
    def _check_cidrs(cls, v: list[str]) -> list[str]:
        bad = []
        for entry in v:
            try:
                ipaddress.ip_network(entry, strict=False)
            except ValueError:
                bad.append(entry)
        if bad:
            raise _Bad("unparsable CIDR " + ", ".join(repr(c) for c in bad))
        return v

    @field_validator("allowed_origins")
    @classmethod
    def _check_origins(cls, v: list[str]) -> list[str]:
        out = [normalise_origin(entry) for entry in v]
        bad = [entry for entry, norm in zip(v, out, strict=True) if norm is None]
        if bad:
            raise _Bad(
                "not an origin (scheme://host[:port], no path or trailing slash): "
                + ", ".join(repr(o) for o in bad)
            )
        return [o for o in out if o is not None]

    @field_validator(
        "default_cpu_request",
        "default_cpu_limit",
        "default_memory_request",
        "default_memory_limit",
    )
    @classmethod
    def _check_quantity(cls, v: str) -> str:
        try:
            positive = parse_quantity(v) > 0
        except ValueError as exc:
            raise _Bad(str(exc)) from None
        if not positive:
            raise _Bad(f"{v!r} must be a positive quantity")
        return v

    @field_validator("efforts")
    @classmethod
    def _check_default_effort(cls, v: list[str], info: ValidationInfo) -> list[str]:
        effort = info.data.get("default_effort")
        if effort and effort not in v:
            raise _Bad(f"ORCH_DEFAULT_EFFORT::{effort!r} is not one of ORCH_EFFORTS")
        return v

    @field_validator("models")
    @classmethod
    def _check_default_model(cls, v: list[str], info: ValidationInfo) -> list[str]:
        if not v:
            raise _Bad("must list at least one model")
        model = info.data.get("default_model")
        if model and model not in v:
            raise _Bad(f"ORCH_DEFAULT_MODEL::{model!r} is not one of ORCH_MODELS")
        return v

    @field_validator("default_memory_limit")
    @classmethod
    def _check_request_within_limit(cls, v: str, info: ValidationInfo) -> str:
        data = info.data
        for res, req_key, lim in (
            ("CPU", "default_cpu_request", data.get("default_cpu_limit")),
            ("MEMORY", "default_memory_request", v),
        ):
            req = data.get(req_key)
            if req is None or lim is None:
                continue
            try:
                over = parse_quantity(req) > parse_quantity(lim)
            except ValueError:
                continue  # reported on its own
            if over:
                raise _Bad(f"ORCH_DEFAULT_{res}_REQUEST::must not exceed ORCH_DEFAULT_{res}_LIMIT")
        return v

    @field_validator("any_mcp_ref", "any_mcp_plugins_ref")
    @classmethod
    def _check_ref(cls, v: str) -> str:
        if not _TAG_RE.match(v):
            raise _Bad(f"{v!r} must be a pinned release tag like v1.2.3, not a branch")
        return v

    @field_validator("kube_namespace_access_tiers")
    @classmethod
    def _check_kube_tiers(cls, v: list[str]) -> list[str]:
        unknown = [t for t in v if t not in KUBE_TIERS]
        if unknown:
            raise _Bad(f"{', '.join(map(repr, unknown))} not in {', '.join(KUBE_TIERS)}")
        return v

    @field_validator("kube_namespace_access_role_prefix")
    @classmethod
    def _check_kube_role_prefix(cls, v: str, info: ValidationInfo) -> str:
        tiers = info.data.get("kube_namespace_access_tiers") or []
        if not v and any(t in tiers for t in KUBE_CUSTOM_ROLE_TIERS):
            raise _Bad("required when ORCH_KUBE_NAMESPACE_ACCESS_TIERS has port-forward or exec")
        return v

    @field_validator("docker_image")
    @classmethod
    def _check_docker_image(cls, v: str) -> str:
        if v and not _DOCKER_IMAGE_RE.match(v):
            raise _Bad(f"{v!r} must be pinned by semver tag like docker:27.3.1-dind")
        return v

    @field_validator(
        "docker_storage",
        "docker_cpu_request",
        "docker_cpu_limit",
        "docker_memory_request",
        "docker_memory_limit",
        "docker_ephemeral_request",
        "max_docker_storage",
        "max_docker_cpu",
        "max_docker_memory",
    )
    @classmethod
    def _check_docker_quantity(cls, v: str) -> str:
        try:
            if parse_quantity(v) <= 0:
                raise ValueError("must be positive")
        except ValueError as exc:
            raise _Bad(f"{v!r}: {exc}") from None
        return v

    @field_validator("docker_cpu_limit", "docker_memory_limit")
    @classmethod
    def _check_docker_request_within_limit(cls, v: str, info: ValidationInfo) -> str:
        res = info.field_name.removeprefix("docker_").removesuffix("_limit")  # type: ignore[union-attr]
        req = info.data.get(f"docker_{res}_request")
        if req is not None and parse_quantity(req) > parse_quantity(v):
            raise _Bad(f"must not be below ORCH_DOCKER_{res.upper()}_REQUEST")
        return v

    @field_validator("max_docker_storage", "max_docker_cpu", "max_docker_memory")
    @classmethod
    def _check_docker_defaults_within_caps(cls, v: str, info: ValidationInfo) -> str:
        res = info.field_name.removeprefix("max_docker_")  # type: ignore[union-attr]
        default = f"docker_{res}" if res == "storage" else f"docker_{res}_limit"
        value = info.data.get(default)
        if value is not None and parse_quantity(value) > parse_quantity(v):
            raise _Bad(f"{_var(default)}::must not exceed {_var(info.field_name)}")  # type: ignore[arg-type]
        return v

    @field_validator("master_key_file")
    @classmethod
    def _check_key_file(cls, v: str) -> str:
        if v:
            try:
                key = Path(v).read_text().strip()
            except OSError as exc:
                raise _Bad(f"cannot read {v}: {exc}") from None
            if len(key.encode()) < MIN_MASTER_KEY_BYTES:
                raise _Bad(f"master key must be at least {MIN_MASTER_KEY_BYTES} bytes")
        return v

    @field_validator("master_key")
    @classmethod
    def _check_key(cls, v: str | None, info: ValidationInfo) -> str | None:
        if v is None or info.data.get("master_key_file"):
            return v  # the file wins and is validated on its own
        v = v.strip()
        if len(v.encode()) < MIN_MASTER_KEY_BYTES:
            raise _Bad(f"master key must be at least {MIN_MASTER_KEY_BYTES} bytes")
        return v

    @field_validator("inactivity_reminder_seconds", "inactivity_close_seconds")
    @classmethod
    def _check_inactivity(cls, v: int, info: ValidationInfo) -> int:
        if v != 0 and not INACTIVITY_MIN_SECONDS <= v <= INACTIVITY_MAX_SECONDS:
            raise _Bad(f"must be 0 (off) or {INACTIVITY_MIN_SECONDS} to {INACTIVITY_MAX_SECONDS}")
        reminder = info.data.get("inactivity_reminder_seconds")
        if info.field_name == "inactivity_close_seconds" and v and reminder and reminder >= v:
            raise _Bad(
                "ORCH_INACTIVITY_REMINDER_SECONDS::must be below ORCH_INACTIVITY_CLOSE_SECONDS"
            )
        return v

    @field_validator("dispatching_stale_seconds")
    @classmethod
    def _check_stale(cls, v: float, info: ValidationInfo) -> float:
        send = info.data.get("wrapper_send_timeout_seconds")
        if send is not None and 2 * send > v:
            raise _Bad(
                "ORCH_WRAPPER_SEND_TIMEOUT_SECONDS::must be at most half of "
                "ORCH_DISPATCHING_STALE_SECONDS"
            )
        return v

    @field_validator("ambiguous_settle_seconds")
    @classmethod
    def _check_settle(cls, v: float, info: ValidationInfo) -> float:
        send = info.data.get("wrapper_send_timeout_seconds")
        if send is not None and v < 2 * send:
            raise _Bad("must be at least twice ORCH_WRAPPER_SEND_TIMEOUT_SECONDS")
        return v

    @field_validator("ambiguous_deadline_seconds")
    @classmethod
    def _check_deadline(cls, v: float, info: ValidationInfo) -> float:
        settle = info.data.get("ambiguous_settle_seconds")
        if settle is not None and v <= settle:
            raise _Bad("must exceed ORCH_AMBIGUOUS_SETTLE_SECONDS")
        return v

    @field_validator("rollout_max_defer_seconds")
    @classmethod
    def _check_rollout_cap(cls, v: int, info: ValidationInfo) -> int:
        window = info.data.get("rollout_handover_seconds")
        if window is not None and v <= window:
            raise _Bad("must exceed ORCH_ROLLOUT_HANDOVER_SECONDS")
        return v

    @field_validator("discard_hook_read_timeout_seconds")
    @classmethod
    def _check_hook_read(cls, v: float, info: ValidationInfo) -> float:
        overall = info.data.get("discard_hook_timeout_seconds")
        if overall is not None and v > overall:
            raise _Bad("must be at most ORCH_DISCARD_HOOK_TIMEOUT_SECONDS")
        return v

    @field_validator("retry_backoff_base_seconds", "scale_drain_seconds")
    @classmethod
    def _check_positive(cls, v: float) -> float:
        if v <= 0:
            raise _Bad("must be greater than 0")
        return v

    @field_validator("retry_backoff_max_seconds")
    @classmethod
    def _check_max(cls, v: float, info: ValidationInfo) -> float:
        base = info.data.get("retry_backoff_base_seconds")
        if base is not None and v < base:
            raise _Bad("must be at least ORCH_RETRY_BACKOFF_BASE_SECONDS")
        return v

    @field_validator("cache_s3_endpoint")
    @classmethod
    def _check_cache_endpoint(cls, v: str) -> str:
        if v:
            parsed = urlparse(v)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise _Bad("must be an http or https URL")
        return v

    @field_validator("discard_hook_url")
    @classmethod
    def _check_hook_url(cls, v: str, info: ValidationInfo) -> str:
        if info.data.get("discard_hook_command") and v:
            raise _Bad("set at most one of ORCH_DISCARD_HOOK_COMMAND and ORCH_DISCARD_HOOK_URL")
        if v:
            parsed = urlparse(v)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise _Bad("must be an http or https URL")
        return v

    @field_validator("discard_hook_token")
    @classmethod
    def _check_hook_token(cls, v: str, info: ValidationInfo) -> str:
        if v and info.data.get("discard_hook_command"):
            raise _Bad("not used with ORCH_DISCARD_HOOK_COMMAND (a bearer for the URL hook only)")
        if v and info.data.get("discard_hook_url") == "":
            raise _Bad("needs ORCH_DISCARD_HOOK_URL")
        if not re.fullmatch(r"[\x21-\x7e]*", v):
            # an illegal header value would echo the token into the failure reason and the log
            raise _Bad("must be printable ASCII without whitespace")
        return v

    @field_validator("chat_url", "taskboard_url")
    @classmethod
    def _check_relay_url(cls, v: str) -> str:
        if v:
            parsed = urlparse(v)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise _Bad("must be an http or https URL")
            if parsed.query or parsed.fragment:
                raise _Bad("must not carry a query or fragment (the relay appends the API path)")
        return v

    @field_validator(
        "webhook_url", "schedule_url", "connector_url", "history_url", "usage_url", "vault_url"
    )
    @classmethod
    def _check_admin_url(cls, v: str) -> str:
        if v:
            parsed = urlparse(v)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise _Bad("must be an http or https URL")
            if parsed.query or parsed.fragment:
                raise _Bad("must not carry a query or fragment (the relay appends the path)")
        return v

    @field_validator("agent_usage_url")
    @classmethod
    def _check_agent_usage_url(cls, v: str) -> str:
        # Written into the agents' wrapper config as usage.url, whose own check also refuses
        # a trailing slash and a missing host (a pod would fail config load).
        if v:
            try:
                parsed = urlparse(v)
                valid = parsed.scheme in ("http", "https") and parsed.hostname is not None
            except ValueError:
                valid = False
            if not valid:
                raise _Bad("must be an http or https URL with a host")
            if parsed.query or parsed.fragment:
                raise _Bad("must not carry a query or fragment (the relay appends the path)")
            if v.endswith("/"):
                raise _Bad("must not end with a slash (the wrapper refuses it)")
        return v

    @field_validator(
        "webhook_token",
        "schedule_token",
        "connector_token",
        "history_token",
        "usage_token",
        "vault_token",
    )
    @classmethod
    def _check_admin_pair(cls, v: str, info: ValidationInfo) -> str:
        name = (info.field_name or "").removesuffix("_token")
        url = info.data.get(f"{name}_url")
        if url is not None and bool(url) != bool(v):
            raise _Bad(
                f"set ORCH_{name.upper()}_URL and ORCH_{name.upper()}_TOKEN together, or neither"
            )
        return v

    @field_validator("cache_s3_api_secret_access_key")
    @classmethod
    def _check_api_pair(cls, v: str, info: ValidationInfo) -> str:
        key_id = info.data.get("cache_s3_api_access_key_id")
        if key_id is not None and bool(key_id) != bool(v):
            raise _Bad(
                "set ORCH_CACHE_S3_API_ACCESS_KEY_ID and ORCH_CACHE_S3_API_SECRET_ACCESS_KEY "
                "together, or neither"
            )
        return v

    @field_validator("agent_usage_secret")
    @classmethod
    def _check_agent_usage_pair(cls, v: str, info: ValidationInfo) -> str:
        if v and (len(v) > 253 or not _SUBDOMAIN.fullmatch(v)):
            raise _Bad("must be a Kubernetes Secret name (a DNS-1123 subdomain)")
        url = info.data.get("agent_usage_url")
        if url is not None and bool(url) != bool(v):
            raise _Bad("set ORCH_AGENT_USAGE_URL and ORCH_AGENT_USAGE_SECRET together, or neither")
        return v

    @field_validator("chat_token")
    @classmethod
    def _check_chat_pair(cls, v: str, info: ValidationInfo) -> str:
        url = info.data.get("chat_url")
        if url is not None and bool(url) != bool(v):
            raise _Bad("set ORCH_CHAT_URL and ORCH_CHAT_TOKEN together, or neither")
        return v

    @field_validator("taskboard_token")
    @classmethod
    def _check_taskboard_pair(cls, v: str, info: ValidationInfo) -> str:
        url = info.data.get("taskboard_url")
        if url is not None and bool(url) != bool(v):
            raise _Bad("set ORCH_TASKBOARD_URL and ORCH_TASKBOARD_TOKEN together, or neither")
        return v

    @model_validator(mode="after")
    def _resolve(self) -> Settings:
        if self.master_key_file:
            self.master_key = Path(self.master_key_file).read_text().strip()
        if not self.default_model:
            self.default_model = self.models[0]
        if not self.check_image:
            self.check_image = self.agent_image
        if self.cache_sweep_grace_seconds is None:
            self.cache_sweep_grace_seconds = self.cache_timeout_seconds + 600
        unknown = sorted(set(self.service_scopes) - set(self.service_token_hashes()))
        if unknown:
            raise _Bad(
                "ORCH_SERVICE_SCOPES::names a service without a token: " + ", ".join(unknown)
            )
        return self

    def require_master_key(self) -> str:
        if not self.master_key:
            raise SettingsError({"ORCH_MASTER_KEY": "required (or ORCH_MASTER_KEY_FILE)"})
        return self.master_key

    def require_agent_image(self) -> str:
        if not self.agent_image:
            raise SettingsError({"ORCH_AGENT_IMAGE": "required"})
        return self.agent_image

    @property
    def priority_send_timeout_seconds(self) -> float:
        """The send timeout of a priority message: the plain send timeout plus the longest the
        wrapper holds a priority request before it answers (`priority_hold_seconds`)."""
        return self.wrapper_send_timeout_seconds + priority_hold_seconds(
            self.wrapper_interrupt_settle_seconds
        )

    @property
    def cache_available(self) -> bool:
        return bool(self.cache_s3_bucket)

    @property
    def reconciler_s3_configured(self) -> bool:
        """The bucket and the reconciler's key pair are set (presign, list resolution, sweep)."""
        return self.cache_available and bool(
            self.cache_s3_access_key_id and self.cache_s3_secret_access_key
        )

    @property
    def api_s3_configured(self) -> bool:
        """The bucket and the api pod's own key pair are set (archives endpoints answer 503
        otherwise)."""
        return self.cache_available and bool(
            self.cache_s3_api_access_key_id and self.cache_s3_api_secret_access_key
        )

    def reconciler_s3_credentials(self) -> tuple[str, str]:
        return self.cache_s3_access_key_id, self.cache_s3_secret_access_key

    def api_s3_credentials(self) -> tuple[str, str]:
        return self.cache_s3_api_access_key_id, self.cache_s3_api_secret_access_key

    def admin_upstream(self, name: str) -> tuple[str, str] | None:
        """(url, token) of an admin relay upstream (webhook, schedule, connector, history, ...)."""
        url = getattr(self, f"{name}_url")
        return (url, getattr(self, f"{name}_token")) if url else None

    @property
    def chat_available(self) -> bool:
        return bool(self.chat_url)

    @property
    def taskboard_available(self) -> bool:
        return bool(self.taskboard_url)

    @property
    def vault_available(self) -> bool:
        return bool(self.vault_url)

    @property
    def usage_available(self) -> bool:
        return bool(self.usage_url)

    @property
    def agent_usage_enabled(self) -> bool:
        return bool(self.agent_usage_url)

    @property
    def kube_cluster_access_available(self) -> bool:
        return self.kube_cluster_access_enabled

    @property
    def kube_namespace_access_available(self) -> bool:
        return bool(self.kube_namespace_access_namespaces and self.kube_namespace_access_tiers)

    def kube_tier_role(self, tier: str) -> str:
        """The ClusterRole bound for a tier: the built-in view, edit or admin, or the custom
        `<role prefix><tier>` role of port-forward and exec."""
        if tier in KUBE_CUSTOM_ROLE_TIERS:
            return f"{self.kube_namespace_access_role_prefix}{tier}"
        return {"read": "view"}.get(tier, tier)

    @property
    def docker_available(self) -> bool:
        return bool(self.docker_image)

    def require_cache(self) -> None:
        """Refuse a bucket without the credentials and image the cache Jobs need."""
        if not self.cache_available:
            return
        missing = {
            _var(name): "required when ORCH_CACHE_S3_BUCKET is set"
            for name in ("cache_s3_access_key_id", "cache_s3_secret_access_key", "cache_image")
            if not getattr(self, name)
        }
        if missing:
            raise SettingsError(missing)

    def retry_delay_seconds(self, attempts: int) -> float:
        return min(
            self.retry_backoff_base_seconds * 2 ** (attempts - 1), self.retry_backoff_max_seconds
        )

    def service_token_hashes(self) -> dict[str, str]:
        """Service name to the sha256 hex of its bearer token."""
        return dict(entry.split(":", 1) for entry in self.service_tokens)

    def allowed_networks(self) -> list[ipaddress.IPv4Network | ipaddress.IPv6Network]:
        return [ipaddress.ip_network(c, strict=False) for c in self.allowed_cidrs]

    def image_allowlist(self) -> frozenset[str]:
        extra = set(self.agent_images)
        if self.agent_image:
            extra.add(self.agent_image)
        return frozenset(extra)

    def default_any_mcp_config(self) -> dict[str, Any]:
        """The any-mcp config sessions get when their spec carries none."""
        if self.any_mcp_config_file:
            return yaml.safe_load(Path(self.any_mcp_config_file).read_text()) or {}
        return {}

    def default_resources(self) -> dict[str, dict[str, str]]:
        return {
            "requests": {"cpu": self.default_cpu_request, "memory": self.default_memory_request},
            "limits": {"cpu": self.default_cpu_limit, "memory": self.default_memory_limit},
        }

    def discard_hook(self) -> DiscardHook | None:
        if self.discard_hook_command:
            return DiscardHook("command", self.discard_hook_command)
        if self.discard_hook_url:
            return DiscardHook("url", self.discard_hook_url, token=self.discard_hook_token)
        return None
