"""SessionSpec model and the validation that needs server settings."""

from __future__ import annotations

import hashlib
import json
import re
import secrets
import string
import uuid
from collections.abc import Callable
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Annotated, Any, Literal
from urllib.parse import urlparse

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from agents_orchestrator.slash import NAME_PATTERN

if TYPE_CHECKING:
    from agents_orchestrator.settings import Settings

SLUG_MAX = 40
RESERVED_PLUGIN = "orchestrator"
_LABEL_RE = re.compile(r"^[a-z0-9]([-a-z0-9]*[a-z0-9])?$")
_ENV_KEY_RE = re.compile(r"^[A-Z_][A-Z0-9_]*$")
_RESERVED_ENV_PREFIXES = ("CCW_", "CLAUDE_", "ANTHROPIC_", "ORCH_")
_RESERVED_ENV_SUFFIXES = ("_PROXY", "_BASE_URL")
_QUANTITY_RE = re.compile(
    r"([+-]?(?:\d+\.?\d*|\.\d+))(Ki|Mi|Gi|Ti|Pi|Ei|[numkMGTPE]|[eE][+-]?\d+)?"
)
_SUFFIX: dict[str, Decimal] = {
    "n": Decimal("1e-9"),
    "u": Decimal("1e-6"),
    "m": Decimal("1e-3"),
    "": Decimal(1),
    "k": Decimal("1e3"),
    "M": Decimal("1e6"),
    "G": Decimal("1e9"),
    "T": Decimal("1e12"),
    "P": Decimal("1e15"),
    "E": Decimal("1e18"),
    "Ki": Decimal(2**10),
    "Mi": Decimal(2**20),
    "Gi": Decimal(2**30),
    "Ti": Decimal(2**40),
    "Pi": Decimal(2**50),
    "Ei": Decimal(2**60),
}


class SpecInvalid(ValueError):
    """A spec that breaks server-side rules; `errors` names every violation."""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("invalid session spec: " + "; ".join(errors))


def slugify(text: str, max_len: int = SLUG_MAX) -> str:
    """Lowercase `text` to a DNS-1123 label body of at most `max_len`; "" if nothing is usable."""
    body = re.sub(r"[^a-z0-9]+", "-", text.lower()).strip("-")
    return body[:max_len].rstrip("-")


def _random_suffix() -> str:
    return "".join(secrets.choice(string.ascii_lowercase) for _ in range(4))


def generated_slug(name: str, rand: Callable[[], str] | None = None) -> str:
    """`<base>-<4 random letters>` from a display name, always valid and at most SLUG_MAX."""
    room = SLUG_MAX - 5
    base = slugify(name, room) or "session"
    if base == "orchestrator" or base.startswith("orchestrator-"):
        base = slugify(f"p-{base}", room)
    return f"{base}-{(rand or _random_suffix)()}"


def validate_slug(slug: str) -> None:
    if not isinstance(slug, str) or len(slug) > SLUG_MAX or not _LABEL_RE.fullmatch(slug):
        raise ValueError(f"slug {slug!r} must be a DNS-1123 label of at most {SLUG_MAX} characters")
    if slug == "orchestrator" or slug.startswith("orchestrator-"):
        raise ValueError(f"slug {slug!r} is reserved (orchestrator, orchestrator-*)")


def parse_quantity(value: str) -> Decimal:
    """Parse a Kubernetes quantity (e.g. 500m, 20Gi, 1e3) into a Decimal."""
    m = _QUANTITY_RE.fullmatch(value) if isinstance(value, str) else None
    if not m:
        raise ValueError(f"{value!r} is not a Kubernetes quantity")
    number, suffix = m.group(1), m.group(2) or ""
    try:
        if suffix[:1] in ("e", "E") and suffix not in _SUFFIX:
            return Decimal(number + suffix)
        return Decimal(number) * _SUFFIX[suffix]
    except (InvalidOperation, KeyError):
        raise ValueError(f"{value!r} is not a Kubernetes quantity") from None


def _quantity(value: str) -> str:
    if parse_quantity(value) <= 0:
        raise ValueError(f"{value!r} must be a positive quantity")
    return value


Quantity = Annotated[str, Field(min_length=1)]


class Resources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    requests: dict[str, str] = {}
    limits: dict[str, str] = {}

    @field_validator("requests", "limits")
    @classmethod
    def _cpu_memory(cls, v: dict[str, str]) -> dict[str, str]:
        for key, qty in v.items():
            if key not in ("cpu", "memory"):
                raise ValueError(f"only cpu and memory are allowed, got {key!r}")
            _quantity(qty)
        return v


# Mount point of the shared workspace; a session works in WORKDIR_ROOT/<spec.workdir>.
WORKDIR_ROOT = "/workdir"
# Directory under the workdir where init scripts and their log are surfaced to the agent.
INIT_DIR = ".agent-init"
ANY_MCP_SOURCES_MAX = 20
ANY_MCP_SOURCE_MAX_CHARS = 500
_PLUGIN_NAME = re.compile(r"^[a-z][a-z0-9_-]{0,19}$")
_PLUGIN_NAME_MAX = 20


def remote_source_problem(source: str) -> str | None:
    """Why a plugin source is refused, or None: only remote https git sources are accepted."""
    from any_mcp.errors import ConfigError
    from any_mcp.sources import parse_source

    try:
        parsed = parse_source(source)
    except ConfigError as exc:
        return str(exc)
    if parsed.kind != "git" or parsed.url is None or not parsed.url.startswith("https://"):
        return (
            f"plugin source '{source}': only https git sources are accepted "
            "(git::https://<host>/<path> or github.com/<owner>/<repo>, with optional "
            "//<subdir> and ?ref=<ref>)"
        )
    authority = parsed.url[len("https://") :].split("/", 1)[0]
    if "@" in authority:
        redacted = source.replace(authority, "***@" + authority.rsplit("@", 1)[1], 1)
        return f"plugin source '{redacted}': credentials in the URL are not allowed"
    return None


def source_plugin_name(source: str) -> str:
    """The plugin name of a parsed source: last subdir segment, else the repo name without .git.

    Lower-cased, characters outside [a-z0-9_-] replaced by '-', cut to 20. The result is not
    validated here; `validate_spec` reports names that do not fit.
    """
    from any_mcp.sources import parse_source

    parsed = parse_source(source)
    if parsed.subdir:
        base = parsed.subdir.rsplit("/", 1)[-1]
    else:
        base = (parsed.url or "").rstrip("/").rsplit("/", 1)[-1].removesuffix(".git")
    return re.sub(r"[^a-z0-9_-]", "-", base.lower())[:_PLUGIN_NAME_MAX]


INIT_SCRIPTS_MAX = 20

CACHE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
CACHE_EXCLUDE_MAX = 50
CACHE_EXCLUDE_ENTRY_MAX = 256
# Always left out of a cache archive, besides spec.cache.exclude.
CACHE_ALWAYS_EXCLUDE = (INIT_DIR,)
# Left out of the /sessions transcript archive (fnmatch on the path relative to /sessions; `*`
# crosses `/`, so the second entry covers `.credentials.json` at any depth). Claude credential
# files must never reach the object store.
SESSIONS_EXCLUDE = (".credentials.json", "*/.credentials.json")
# S3 and RGW cap one PUT at 5 GiB.
CACHE_MAX_BYTES_LIMIT = 5368709120
INIT_SCRIPT_MAX_CHARS = 16384


def init_scripts_hash(scripts: list[str]) -> str:
    """sha256 hex of the compact JSON list; the identity of an init script set."""
    return hashlib.sha256(json.dumps(scripts, separators=(",", ":")).encode()).hexdigest()


# The claude-code-wrapper 0.2.0 hook events (config.py:HOOK_EVENTS); the wrapper is only a dev
# dependency, so the orchestrator keeps its own copy of the shapes it accepts.
HOOK_EVENTS: tuple[str, ...] = (
    "onCreate",
    "onResume",
    "onDestroy",
    "onTurn",
    "onTurnComplete",
    "onStop",
    "onTick",
    "onAgentResponse",
    "onAgentToolCall",
    "onToolResult",
    "onSubagentStart",
    "onSubagentStop",
    "onInterrupt",
    "onCommand",
    "onCommandComplete",
    "onError",
    "preJoin",
    "postJoin",
    "preLeave",
    "postLeave",
    "onStart",
)
TOOL_EVENTS: tuple[str, ...] = ("onAgentToolCall", "onToolResult")
HOOK_ACTIONS_MAX = 20
HOOK_RUN_MAX_CHARS = 16384
INSTRUCTIONS_MAX = 2000
SYSTEM_APPEND_MAX = 16384
RESERVED_MCP_SERVERS = ("any-mcp", "ccw")
WELL_KNOWN_MARKETPLACES = frozenset({"claude-plugins-official"})
# A Claude Code slash command name (design D2), or "*" for any command.
AgentCommand = Annotated[str, Field(pattern=rf"^(\*|{NAME_PATTERN})$")]
_MCP_NAME_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")
_PLUGIN_RE = re.compile(r"^[^@\s]+@[^@\s]+$")
_INSTRUCTIONS_CONTROL = re.compile(r"[\x00-\x1f\x7f-\x9f\u2028\u2029]")
_HTTP_URL_RE = re.compile(r"^https?://[^/\s]+")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class _HookAction(_Strict):
    timeout_seconds: float | None = Field(default=None, gt=0)
    blocking: bool | None = None
    on_error: Literal["allow", "deny"] | None = None
    matcher: str | None = None
    interval_seconds: float | None = Field(default=None, gt=0)
    when: Literal["busy", "always"] | None = None

    @field_validator("matcher")
    @classmethod
    def _compiles(cls, v: str | None) -> str | None:
        if v is not None:
            try:
                re.compile(v)
            except re.error as exc:
                raise ValueError(f"invalid regex: {exc}") from exc
        return v


class CommandHook(_HookAction):
    type: Literal["command"]
    run: str = Field(min_length=1, max_length=HOOK_RUN_MAX_CHARS)
    env: dict[str, str] | None = None

    @field_validator("env")
    @classmethod
    def _env_keys(cls, v: dict[str, str] | None) -> dict[str, str] | None:
        bad = [key for key in v or {} if not _ENV_KEY_RE.fullmatch(key)]
        if bad:
            raise ValueError("env keys must match [A-Z_][A-Z0-9_]*: " + ", ".join(sorted(bad)))
        return v


class WebhookHook(_HookAction):
    type: Literal["webhook"]
    url: str
    secret: str | None = None
    headers: dict[str, str] | None = None
    retries: int | None = Field(default=None, ge=0)

    @field_validator("url")
    @classmethod
    def _http(cls, v: str) -> str:
        if not _HTTP_URL_RE.match(v):
            raise ValueError("must be an http or https URL")
        return v


HookAction = Annotated[CommandHook | WebhookHook, Field(discriminator="type")]


class McpStdio(_Strict):
    type: Literal["stdio"]
    command: str = Field(min_length=1)
    args: list[str] | None = None
    env: dict[str, str] | None = None


class McpRemote(_Strict):
    type: Literal["http", "sse"]
    url: str
    headers: dict[str, str] | None = None


McpServer = Annotated[McpStdio | McpRemote, Field(discriminator="type")]


class AutoCompact(_Strict):
    idle_seconds: float | None = Field(default=None, ge=0)
    instructions: str | None = None
    min_context_tokens: int | None = Field(default=None, ge=0)

    @field_validator("instructions")
    @classmethod
    def _one_line(cls, v: str | None) -> str | None:
        if v is not None:
            if _INSTRUCTIONS_CONTROL.search(v):
                raise ValueError("must be one line without control characters")
            if len(v) > INSTRUCTIONS_MAX:
                raise ValueError(f"must not exceed {INSTRUCTIONS_MAX} characters")
        return v


class Permissions(_Strict):
    allow: list[str] | None = None
    deny: list[str] | None = None


class Marketplace(_Strict):
    name: str = Field(min_length=1)
    source: str = Field(min_length=1)


class PluginInstall(_Strict):
    plugin: str
    scope: Literal["user", "project", "local"] | None = None
    required: bool | None = None

    @field_validator("plugin")
    @classmethod
    def _shape(cls, v: str) -> str:
        if not _PLUGIN_RE.match(v):
            raise ValueError("must look like name@marketplace")
        return v


class Plugins(_Strict):
    marketplaces: list[Marketplace] | None = None
    install: list[PluginInstall] | None = None

    @model_validator(mode="after")
    def _marketplaces_known(self) -> Plugins:
        known = {m.name for m in self.marketplaces or []} | WELL_KNOWN_MARKETPLACES
        for i, entry in enumerate(self.install or []):
            market = entry.plugin.rsplit("@", 1)[1]
            if market not in known:
                raise ValueError(
                    f"wrapper.plugins.install.{i}.plugin: marketplace {market!r} is not listed in "
                    "marketplaces or claude-plugins-official"
                )
        return self


class WrapperSettings(_Strict):
    """Typed claude-code-wrapper settings a session may carry (create-time only)."""

    hooks: dict[str, list[HookAction]] | None = None
    auto_compact: AutoCompact | None = None
    system_append: str | None = Field(default=None, max_length=SYSTEM_APPEND_MAX)
    permissions: Permissions | None = None
    mcp_servers: dict[str, McpServer] | None = None
    plugins: Plugins | None = None
    turn_stall_seconds: float | None = Field(default=None, ge=0)
    max_restarts: int | None = Field(default=None, ge=0, le=10)
    restart_continue_prompt: str | None = None
    turn_max_seconds: float | None = Field(default=None, ge=0)
    agent_commands: list[AgentCommand] | None = None

    @field_validator("hooks")
    @classmethod
    def _hooks(cls, v: dict[str, list[CommandHook | WebhookHook]] | None):
        errors: list[str] = []
        for event, actions in (v or {}).items():
            if event not in HOOK_EVENTS:
                errors.append(f"wrapper.hooks.{event}: unknown hook event")
                continue
            if len(actions) > HOOK_ACTIONS_MAX:
                errors.append(
                    f"wrapper.hooks.{event}: has {len(actions)} actions, at most {HOOK_ACTIONS_MAX}"
                )
            for i, action in enumerate(actions):
                at = f"wrapper.hooks.{event}.{i}"
                if action.matcher is not None and event not in TOOL_EVENTS:
                    errors.append(f"{at}.matcher: only allowed on {', '.join(TOOL_EVENTS)}")
                for key in ("interval_seconds", "when"):
                    if event != "onTick" and getattr(action, key) is not None:
                        errors.append(f"{at}.{key}: only allowed on onTick")
                if event == "onTick" and action.interval_seconds is None:
                    errors.append(f"{at}.interval_seconds: required on onTick")
        if errors:
            raise ValueError("; ".join(errors))
        return v

    @field_validator("mcp_servers")
    @classmethod
    def _mcp_names(cls, v: dict[str, McpServer] | None):
        for name in v or {}:
            if name in RESERVED_MCP_SERVERS:
                raise ValueError(f"wrapper.mcp_servers.{name}: the name is reserved")
            if not _MCP_NAME_RE.fullmatch(name):
                raise ValueError(
                    f"wrapper.mcp_servers.{name!r}: name must match [A-Za-z0-9_-]{{1,64}}"
                )
        return v


def default_cache_key(preset_id: uuid.UUID) -> str:
    """Stable cache key derived from the immutable preset id (a rename keeps the cache)."""
    digest = hashlib.sha256(b"workspace-cache:" + preset_id.bytes).hexdigest()
    return "preset-" + digest[:16]


CACHE_KEY_RESERVED_PREFIX = "preset-"


def check_caller_cache_key(spec: SessionSpec, derived_key: str | None = None) -> None:
    """Reject a caller-supplied cache key in the prefix reserved for preset-derived keys.

    `derived_key` is the one reserved key the spec may carry (its own preset's). Applied at the
    API input boundary only: derived specs the server writes never pass through it.
    """
    key = spec.cache.key if spec.cache is not None else None
    if key and key.startswith(CACHE_KEY_RESERVED_PREFIX) and key != derived_key:
        raise ValueError(
            f"cache.key prefix {CACHE_KEY_RESERVED_PREFIX} is reserved for preset-derived keys"
        )


class WorkspaceCache(_Strict):
    key: str | None = None
    exclude: list[str] = []

    @field_validator("key")
    @classmethod
    def _check_key(cls, v: str | None) -> str | None:
        if v is not None and not CACHE_KEY_RE.match(v):
            raise ValueError(f"cache key must match {CACHE_KEY_RE.pattern}")
        return v

    @field_validator("exclude")
    @classmethod
    def _check_exclude(cls, v: list[str]) -> list[str]:
        if len(v) > CACHE_EXCLUDE_MAX:
            raise ValueError(f"at most {CACHE_EXCLUDE_MAX} exclude entries")
        for entry in v:
            if not entry or len(entry) > CACHE_EXCLUDE_ENTRY_MAX:
                raise ValueError(f"exclude entries are 1 to {CACHE_EXCLUDE_ENTRY_MAX} characters")
            if entry.startswith("/") or ".." in entry.split("/"):
                raise ValueError(f"exclude {entry!r} must be relative with no '..' segment")
        return v


class SessionSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")

    replicas: int = Field(default=1, ge=1, le=16)
    max_replicas: int | None = Field(default=None, ge=1, le=16)
    static: bool = False
    image: str | None = None
    model: str | None = None
    effort: str | None = None
    peers: bool = True
    workdir: str = Field(default="workdir", max_length=63)
    pvc_size: Quantity | None = None
    resources: Resources | None = None
    env: dict[str, str] = {}
    any_mcp_config: dict[str, Any] | None = None
    max_attempts: int = Field(default=5, ge=1, le=10)
    auto_compact_pct: int | None = Field(default=None, ge=1, le=100)
    discard_hook_url: str | None = None
    init_scripts: list[str] = []
    any_mcp_sources: list[str] | None = None
    wrapper: WrapperSettings | None = None
    leader_lease_seconds: int | None = Field(default=None, ge=60, le=604800)
    autoclose_after_seconds: int | None = Field(default=None, ge=1, le=2592000)
    # Read only by trigger services (agents-webhook, agents-schedule): send a new event to
    # the session still running for it. The orchestrator never acts on it, so a session a
    # user creates ignores it. Sparse: unset is not stored or dumped in a preset.
    reuse_running_agent: bool | None = Field(default=None, strict=True)
    # Superleader: the leader may message the leaders of all sessions, and every other
    # session's leader may message it. Sparse: unset is not stored or dumped in a preset.
    session_messaging: bool | None = Field(default=None, strict=True)
    # Full read-write Kubernetes access in every namespace for the agent pods; refused until
    # ORCH_KUBE_ACCESS_SERVICE_ACCOUNT is set. Sparse: unset is not stored or dumped in a preset.
    kube_access: bool | None = Field(default=None, strict=True)
    # Workspace cache in S3; presence enables it. Sparse: unset is not stored or dumped.
    cache: WorkspaceCache | None = None

    @model_validator(mode="after")
    def _cap(self) -> SessionSpec:
        if self.max_replicas is None:
            # resolved without marking the field set, so presets stay sparse
            self.__dict__["max_replicas"] = self.replicas
        elif self.replicas > self.max_replicas:
            raise ValueError(f"replicas {self.replicas} exceeds max_replicas {self.max_replicas}")
        if self.static and self.max_replicas != self.replicas:
            raise ValueError("static requires max_replicas to equal replicas")
        return self

    @model_validator(mode="after")
    def _lease_needs_hive(self) -> SessionSpec:
        if self.leader_lease_seconds is not None and not self.hive:
            raise ValueError("leader_lease_seconds requires a hive session (max_replicas > 1)")
        return self

    @property
    def hive(self) -> bool:
        """Every session that may run more than one agent is a hive."""
        return (self.max_replicas or self.replicas) > 1

    @property
    def alone(self) -> bool:
        return not self.hive

    @property
    def scalable(self) -> bool:
        return self.hive and not self.static

    @field_validator("workdir")
    @classmethod
    def _workdir(cls, v: str) -> str:
        if not _LABEL_RE.fullmatch(v):
            raise ValueError("workdir must be a DNS-1123 label")
        return v

    @field_validator("init_scripts")
    @classmethod
    def _init_scripts(cls, v: list[str]) -> list[str]:
        if len(v) > INIT_SCRIPTS_MAX:
            raise ValueError(f"init_scripts has {len(v)} entries, at most {INIT_SCRIPTS_MAX}")
        for i, script in enumerate(v):
            if not script.strip():
                raise ValueError(f"init_scripts[{i}] must not be empty")
            if len(script) > INIT_SCRIPT_MAX_CHARS:
                raise ValueError(
                    f"init_scripts[{i}] has {len(script)} characters, max {INIT_SCRIPT_MAX_CHARS}"
                )
        return v

    @field_validator("any_mcp_sources")
    @classmethod
    def _any_mcp_sources(cls, v: list[str] | None) -> list[str] | None:
        if v is None:
            return v
        if len(v) > ANY_MCP_SOURCES_MAX:
            raise ValueError(f"any_mcp_sources has {len(v)} entries, at most {ANY_MCP_SOURCES_MAX}")
        for i, source in enumerate(v):
            if not source.strip():
                raise ValueError(f"any_mcp_sources[{i}] must not be empty")
            if len(source) > ANY_MCP_SOURCE_MAX_CHARS:
                raise ValueError(
                    f"any_mcp_sources[{i}] has {len(source)} characters, "
                    f"max {ANY_MCP_SOURCE_MAX_CHARS}"
                )
            if (problem := remote_source_problem(source)) is not None:
                raise ValueError(f"any_mcp_sources[{i}]: {problem}")
        return v

    @field_validator("pvc_size")
    @classmethod
    def _pvc_size(cls, v: str | None) -> str | None:
        if v is not None:
            _quantity(v)
        return v

    @field_validator("env")
    @classmethod
    def _env(cls, v: dict[str, str]) -> dict[str, str]:
        bad = []
        for key in v:
            if (
                not _ENV_KEY_RE.fullmatch(key)
                or key == "POD_NAME"
                or key.startswith(_RESERVED_ENV_PREFIXES)
                or key.endswith(_RESERVED_ENV_SUFFIXES)
            ):
                bad.append(key)
        if bad:
            raise ValueError(
                "env keys must match [A-Z_][A-Z0-9_]* and must not be POD_NAME, CCW_*, CLAUDE_*, "
                "ANTHROPIC_*, ORCH_*, *_PROXY or *_BASE_URL: " + ", ".join(sorted(bad))
            )
        return v

    @field_validator("discard_hook_url")
    @classmethod
    def _hook_url(cls, v: str | None) -> str | None:
        if v is not None:
            parsed = urlparse(v)
            if parsed.scheme not in ("http", "https") or not parsed.netloc:
                raise ValueError("discard_hook_url must be an http or https URL")
        return v


def fill_resource_defaults(spec: SessionSpec, settings: Settings) -> SessionSpec:
    """Fill each empty resources key from the settings defaults; explicit values win per key.

    A default limit is skipped when below the explicit request of the same resource, and a
    default request is skipped when above the explicit limit.
    """
    current = spec.resources or Resources()
    requests, limits = dict(current.requests), dict(current.limits)
    defaults = settings.default_resources()
    for key in ("cpu", "memory"):
        if key not in limits:
            default = defaults["limits"][key]
            if key not in requests or _not_above(requests[key], default):
                limits[key] = default
        if key not in requests:
            default = defaults["requests"][key]
            if key not in limits or _not_above(default, limits[key]):
                requests[key] = default
    return spec.model_copy(update={"resources": Resources(requests=requests, limits=limits)})


def _not_above(low: str, high: str) -> bool:
    try:
        return parse_quantity(low) <= parse_quantity(high)
    except ValueError:
        return True


def _config_plugin_names(config: dict[str, Any]) -> set[str]:
    plugins = config.get("plugins")
    if not isinstance(plugins, list):
        return set()
    return {e["name"] for e in plugins if isinstance(e, dict) and isinstance(e.get("name"), str)}


def validate_spec(spec: SessionSpec, settings: Settings, stored: SessionSpec | None = None) -> None:
    """Validate a spec.

    `stored` stays for the preset PUT: a model, effort or image that has since left the settings
    lists, and a cache or kube_access whose setting has since been unset, is accepted only when
    unchanged from the stored preset spec. Scheduling a session passes
    no `stored` and validates fully.
    """
    from any_mcp.config import AnyMcpConfig
    from pydantic import ValidationError

    errors: list[str] = []
    if (
        spec.image is not None
        and spec.image not in settings.image_allowlist()
        and (stored is None or spec.image != stored.image)
    ):
        errors.append(f"image {spec.image!r} is not in the allowed agent images")
    if spec.discard_hook_url is not None:
        parsed = urlparse(spec.discard_hook_url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            errors.append("discard_hook_url must be an http or https URL")
    if (
        spec.model is not None
        and spec.model not in settings.models
        and (stored is None or spec.model != stored.model)
    ):
        errors.append(f"model {spec.model!r} is not one of: {', '.join(settings.models)}")
    if (
        spec.effort
        and spec.effort not in settings.efforts
        and (stored is None or spec.effort != stored.effort)
    ):
        errors.append(f"effort {spec.effort!r} is not one of: {', '.join(settings.efforts)}")
    if (
        spec.cache is not None
        and not settings.cache_available
        and (stored is None or spec.cache != stored.cache)
    ):
        errors.append("workspace cache is not configured (ORCH_CACHE_S3_BUCKET is unset)")
    if (
        spec.kube_access
        and not settings.kube_access_available
        and (stored is None or spec.kube_access != stored.kube_access)
    ):
        errors.append(
            "Kubernetes access is not configured (ORCH_KUBE_ACCESS_SERVICE_ACCOUNT is unset)"
        )
    if spec.resources is not None:
        for key in ("cpu", "memory"):
            request = spec.resources.requests.get(key)
            limit = spec.resources.limits.get(key)
            if request is None or limit is None:
                continue
            if parse_quantity(request) > parse_quantity(limit):
                errors.append(f"resources: {key} request {request} exceeds limit {limit}")
    if spec.replicas > settings.max_replicas:
        errors.append(f"replicas {spec.replicas} exceeds the maximum {settings.max_replicas}")
    if spec.max_replicas is not None and spec.max_replicas > settings.max_replicas:
        errors.append(
            f"max_replicas {spec.max_replicas} exceeds the maximum {settings.max_replicas}"
        )
    if spec.pvc_size is not None:
        try:
            too_big = parse_quantity(spec.pvc_size) > parse_quantity(settings.max_pvc_size)
        except ValueError as exc:
            errors.append(f"pvc_size: {exc}")
        else:
            if too_big:
                errors.append(
                    f"pvc_size {spec.pvc_size} exceeds the maximum {settings.max_pvc_size}"
                )
    if (
        spec.autoclose_after_seconds is not None
        and spec.autoclose_after_seconds < settings.autoclose_min_seconds
    ):
        errors.append(
            f"autoclose_after_seconds {spec.autoclose_after_seconds} is below the minimum "
            f"{settings.autoclose_min_seconds}"
        )
    if spec.any_mcp_config is not None:
        try:
            config = AnyMcpConfig.model_validate(spec.any_mcp_config)
        except ValidationError as exc:
            for err in exc.errors():
                loc = ".".join(str(p) for p in err["loc"])
                errors.append(f"any_mcp_config.{loc}: {err['msg']}".replace(".: ", ": "))
        else:
            for entry in config.plugins:
                if RESERVED_PLUGIN in (entry.name, entry.namespace):
                    errors.append(
                        f"any_mcp_config.plugins: {RESERVED_PLUGIN!r} is reserved for the "
                        f"rendered plugin (entry {entry.name!r})"
                    )
            for i, entry in enumerate(config.plugins):
                if entry.source is not None and (problem := remote_source_problem(entry.source)):
                    errors.append(f"any_mcp_config.plugins[{i}].source: {problem}")
    if spec.any_mcp_sources:
        taken = _config_plugin_names(
            spec.any_mcp_config
            if spec.any_mcp_config is not None
            else settings.default_any_mcp_config()
        )
        derived: dict[str, int] = {}
        for i, source in enumerate(spec.any_mcp_sources):
            name = source_plugin_name(source)
            if not _PLUGIN_NAME.match(name):
                reason = f"derived plugin name {name!r} must match {_PLUGIN_NAME.pattern}"
            elif name == RESERVED_PLUGIN:
                reason = f"derived plugin name {name!r} is reserved for the rendered plugin"
            elif name in derived:
                reason = f"derived plugin name {name!r} repeats any_mcp_sources[{derived[name]}]"
            elif name in taken:
                reason = f"derived plugin name {name!r} is already a plugin of the any-mcp config"
            else:
                derived[name] = i
                continue
            errors.append(f"any_mcp_sources[{i}]: {reason}")
    if errors:
        raise SpecInvalid(errors)
