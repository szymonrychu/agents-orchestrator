"""Pure rules for human-only preset fields.

An agent (the superleader) may change these preset fields only toward safe;
anything else is refused. Compared against the stored spec, with absent, None
and false equal:

- kube_cluster_access, session_messaging, docker: may be turned off or removed, never on.
- kube_namespace_access (namespace -> tiers map): may shrink or be removed, never grow: no
  namespace or tier may appear that the stored map lacks.
- discard_hook_url: may be removed, never set to a value other than the stored one.
- wrapper.permission_mode: unset means bypassPermissions; may become "auto",
  never any other change.
- wrapper.permissions.deny: entries may be added, never removed.
- wrapper.permissions.allow: entries may be removed, never added.
- env.<NAME>: on a privileged target an `env.<NAME>` path is still security, except when the
  stored and the new entry are each a vault reference or absent (attach, detach, re-point).

`HUMAN_ONLY_COLUMNS` are preset columns outside the spec that an agent may not change at all.

A live session adds two rules of its own: `fixed_changes` (fields that cannot change on a
running session) and `tighten_changes` (limits a regular leader may only lower: usage quota,
inactivity, autoclose, pod resources and docker sizes).

All functions work on JSON dicts as stored (model_dump(mode="json",
exclude_unset=True)) and never mutate their inputs. The main port never calls
them: only the agent-facing admin routes do.
"""

import copy
from collections.abc import Iterable
from decimal import Decimal
from typing import Any

from agents_orchestrator.spec import parse_quantity

GRANTS: tuple[str, ...] = (
    "kube_cluster_access",
    "kube_namespace_access",
    "session_messaging",
    "docker",
    "discard_hook_url",
)
DOCKER_SIZES: tuple[str, ...] = ("docker_storage", "docker_cpu", "docker_memory")
# discard_hook_url is human-only to set but grants no reach to a started session.
PRIVILEGED: tuple[str, ...] = (
    "kube_cluster_access",
    "kube_namespace_access",
    "session_messaging",
    "docker",
)
# The human-only fields in the `changed_paths` spelling (one level under `wrapper`): marked
# `grant` in a review; `human_only_changes` reports `wrapper.permissions` as `.allow`/`.deny`.
HUMAN_ONLY_PATHS: frozenset[str] = frozenset(
    {*GRANTS, "wrapper.permission_mode", "wrapper.permissions"}
)

# Spec paths an agent may change directly on a privileged target (preset-change-proposals plan
# 3.1): sizes, timers, budgets and choices bounded by deploy-time allowlists and caps; nothing
# executes new code or text in the granted pod. Anything else is security (fail closed). The
# columns (name, labels, project) are not spec paths and are not classified here.
NON_SECURITY: frozenset[str] = frozenset(
    {
        "replicas",
        "max_replicas",
        "static",
        "model",
        "effort",
        "image",
        "peers",
        "workdir",
        "pvc_size",
        "resources",
        "max_attempts",
        "auto_compact_pct",
        "autoclose_after_seconds",
        "reuse_running_agent",
        "usage_quota",
        "inactivity",
        *DOCKER_SIZES,
        "wrapper.turn_stall_seconds",
        "wrapper.max_restarts",
        "wrapper.turn_max_seconds",
        "wrapper.agent_commands",
    }
)
# Preset columns an agent may not change: `project` decides which project secrets the preset's
# sessions get (projects plan 3.16), so only a human moves a preset between projects.
HUMAN_ONLY_COLUMNS: tuple[str, ...] = ("project",)

# Fields of a live session that a spec edit may not change.
SESSION_FIXED: tuple[str, ...] = (
    "replicas",
    "max_replicas",
    "static",
    "workdir",
    "pvc_size",
    "cache",
)
_FIXED_DEFAULTS: dict[str, Any] = {"replicas": 1, "static": False, "workdir": "workdir"}

# Limits a regular leader may only tighten on its own session: (block, key).
_QUOTA_PATHS: tuple[tuple[str, str], ...] = (
    ("usage_quota", "five_hour_pct"),
    ("usage_quota", "seven_day_pct"),
)
_INACTIVITY_PATHS: tuple[tuple[str, str], ...] = (
    ("inactivity", "reminder_seconds"),
    ("inactivity", "close_seconds"),
)

_DEFAULT_PERMISSION_MODE = "bypassPermissions"
_SAFE_PERMISSION_MODE = "auto"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _entries(value: Any) -> set[str]:
    return {str(v) for v in value} if isinstance(value, list) else set()


def _grant_grows(stored: Any, new: Any) -> bool:
    """True when `new` holds a namespace or a tier of a namespace that `stored` lacks."""
    old = _dict(stored)
    return any(not _entries(tiers) <= _entries(old.get(ns)) for ns, tiers in _dict(new).items())


def classify(paths: Iterable[str]) -> dict[str, tuple[bool, bool]]:
    """Each spec path (as `changed_paths` spells it) to (security, grant). Fail closed: a path
    not in `NON_SECURITY` is security (every `env.*`, any unknown name or `wrapper.*`); grant
    is a human-only path."""
    return {p: (p not in NON_SECURITY, p in HUMAN_ONLY_PATHS) for p in paths}


# Schedule fields (agents-schedule `ScheduleIn`) a reviewer may wave through (plan 3.1); `prompt`
# (text the started session runs) and `preset_id` (which grants it runs with) are security.
SCHEDULE_NON_SECURITY: frozenset[str] = frozenset({"name", "cron", "timezone", "enabled"})


def classify_schedule(fields: Iterable[str]) -> dict[str, tuple[bool, bool]]:
    """Each schedule field to (security, grant), failing closed like `classify`; no schedule
    field is a grant."""
    return {f: (f not in SCHEDULE_NON_SECURITY, False) for f in fields}


def privileged(spec: dict[str, Any]) -> list[str]:
    return sorted(f for f in PRIVILEGED if bool(spec.get(f)))


def human_only_changes(stored: dict[str, Any], new: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for f in PRIVILEGED:
        if f == "kube_namespace_access":
            grows = _grant_grows(stored.get(f), new.get(f))
        else:
            grows = bool(new.get(f)) and not bool(stored.get(f))
        if grows:
            out.append(f)

    hook = new.get("discard_hook_url")
    if hook is not None and hook != stored.get("discard_hook_url"):
        out.append("discard_hook_url")

    s_wrap, n_wrap = _dict(stored.get("wrapper")), _dict(new.get("wrapper"))
    old_mode = s_wrap.get("permission_mode") or _DEFAULT_PERMISSION_MODE
    new_mode = n_wrap.get("permission_mode") or _DEFAULT_PERMISSION_MODE
    if new_mode != old_mode and new_mode != _SAFE_PERMISSION_MODE:
        out.append("wrapper.permission_mode")

    s_perm, n_perm = _dict(s_wrap.get("permissions")), _dict(n_wrap.get("permissions"))
    if not _entries(n_perm.get("allow")) <= _entries(s_perm.get("allow")):
        out.append("wrapper.permissions.allow")
    if not _entries(s_perm.get("deny")) <= _entries(n_perm.get("deny")):
        out.append("wrapper.permissions.deny")

    return sorted(out)


def _is_ref_or_absent(entry: Any) -> bool:
    return entry is None or (isinstance(entry, dict) and entry.get("vault") is not None)


def privileged_target_changes(
    stored: dict[str, Any], new: dict[str, Any], *, plain_to_ref: bool = False
) -> list[str]:
    """The changed spec paths an agent may not touch on a stored preset that is privileged.

    Empty when the stored preset has no privileged flag on. Otherwise only the `NON_SECURITY`
    paths, the human-only fields (which `human_only_changes` already limits to toward safe) and
    the `env.<NAME>` paths whose stored and new entries are each a vault reference or absent
    may change; name and labels are not spec paths. The stored preset counts, so turning a flag
    off together with another edit is refused. With `plain_to_ref` (the vault attach) an
    `env.<NAME>` may also go from a plain value to a vault reference, never the other way."""
    if not privileged(stored):
        return []
    allowed = NON_SECURITY | HUMAN_ONLY_PATHS
    old_env, new_env = _dict(stored.get("env")), _dict(new.get("env"))

    def refs_only(path: str) -> bool:
        name = path.removeprefix("env.")
        return path.startswith("env.") and all(
            _is_ref_or_absent(e.get(name))
            for e in ((new_env,) if plain_to_ref else (old_env, new_env))
        )

    return sorted(p for p in changed_paths(stored, new) if p not in allowed and not refs_only(p))


def _fixed_value(spec: dict[str, Any], field: str) -> Any:
    """The effective value of a fixed field: a missing or null key reads as its default, and
    `max_replicas` unset means `replicas`."""
    if field == "max_replicas":
        value = spec.get("max_replicas")
        return value if value is not None else _fixed_value(spec, "replicas")
    value = spec.get(field)
    return value if value is not None else _FIXED_DEFAULTS.get(field)


def fixed_changes(stored: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """The `SESSION_FIXED` fields whose effective value differs between two session specs
    (sorted). Absent, null and the default are equal; `max_replicas` absent equals
    `replicas`; a key present on one side only counts unless both normalise equal."""
    return sorted(f for f in SESSION_FIXED if _fixed_value(stored, f) != _fixed_value(new, f))


def _effective(
    spec: dict[str, Any], block: str, key: str, default: int, *, off_is_zero: bool
) -> float:
    value = _dict(spec.get(block)).get(key)
    if value is None:
        value = default
    # Inactivity: 0 is off, which is the loosest limit; a quota has no such value.
    return float("inf") if off_is_zero and value == 0 else value


def _autoclose(spec: dict[str, Any]) -> float:
    """Unset means the session never autocloses, the loosest value."""
    value = spec.get("autoclose_after_seconds")
    return float("inf") if value is None else value


def _resource_changes(stored: dict[str, Any], new: dict[str, Any]) -> list[str]:
    """`resources.<requests|limits>.<key>` paths that got looser, quantities compared parsed
    (1Gi equals 1024Mi). A removed key is looser, a raised one too. A limit added where none
    was is tighter. An added request is compared with what the stored pod gets: the stored
    limit of the same key (Kubernetes defaults a request to its limit), else zero."""
    s_res, n_res = _dict(stored.get("resources")), _dict(new.get("resources"))
    s_lim = _dict(s_res.get("limits"))
    out: list[str] = []
    for section in ("requests", "limits"):
        old, cur = _dict(s_res.get(section)), _dict(n_res.get(section))
        for key in set(old) | set(cur):
            if key not in cur:
                looser = True
            elif key in old:
                looser = parse_quantity(cur[key]) > parse_quantity(old[key])
            elif section == "requests":
                floor = parse_quantity(s_lim[key]) if key in s_lim else Decimal(0)
                looser = parse_quantity(cur[key]) > floor
            else:
                looser = False
            if looser:
                out.append(f"resources.{section}.{key}")
    return out


def _docker_changes(
    stored: dict[str, Any], new: dict[str, Any], defaults: dict[str, str]
) -> list[str]:
    """`docker_cpu`, `docker_memory` and `docker_storage` whose effective size got looser. The
    effective size is the spec value, else the deploy default the renderer uses (`defaults`);
    quantities are compared parsed (1Gi equals 1024Mi). Only when both specs have docker on:
    turning it off drops the sizes (tighter) and turning it on is the human-only grant guard's."""
    if not (stored.get("docker") and new.get("docker")):
        return []
    return [
        field
        for field, default in defaults.items()
        if parse_quantity(new.get(field) or default) > parse_quantity(stored.get(field) or default)
    ]


def tighten_changes(
    stored: dict[str, Any],
    new: dict[str, Any],
    *,
    quota_default: int,
    reminder_default: int,
    close_default: int,
    docker_cpu_default: str,
    docker_memory_default: str,
    docker_storage_default: str,
) -> list[str]:
    """The limit paths whose effective value got looser from `stored` to `new` (sorted):
    `usage_quota.five_hour_pct` and `.seven_day_pct` (unset is `quota_default`, higher is
    looser), `inactivity.reminder_seconds` and `.close_seconds` (unset is the default, 0 is
    off and so infinite, longer is looser), `autoclose_after_seconds` (unset is never, so
    clearing or raising is looser) and `resources.requests.*` / `resources.limits.*` (see
    `_resource_changes`) and `docker_cpu` / `docker_memory` / `docker_storage` (unset is the
    deploy default, higher is looser; see `_docker_changes`). Lowering, or keeping, a limit is
    never reported."""
    defaults = {
        "five_hour_pct": quota_default,
        "seven_day_pct": quota_default,
        "reminder_seconds": reminder_default,
        "close_seconds": close_default,
    }
    out: list[str] = []
    for block, key in (*_QUOTA_PATHS, *_INACTIVITY_PATHS):
        off_is_zero = block == "inactivity"
        old_v = _effective(stored, block, key, defaults[key], off_is_zero=off_is_zero)
        new_v = _effective(new, block, key, defaults[key], off_is_zero=off_is_zero)
        if new_v > old_v:
            out.append(f"{block}.{key}")
    if _autoclose(new) > _autoclose(stored):
        out.append("autoclose_after_seconds")
    out.extend(_resource_changes(stored, new))
    out.extend(
        _docker_changes(
            stored,
            new,
            {
                "docker_cpu": docker_cpu_default,
                "docker_memory": docker_memory_default,
                "docker_storage": docker_storage_default,
            },
        )
    )
    return sorted(out)


def strip_grants(spec: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    drop = set(GRANTS)
    if "docker" in spec:
        drop |= set(DOCKER_SIZES)
    removed = sorted(k for k in spec if k in drop)
    return {k: copy.deepcopy(v) for k, v in spec.items() if k not in drop}, removed


def merge_patch(target: Any, patch: Any) -> Any:
    """RFC 7396 JSON merge patch."""
    if not isinstance(patch, dict):
        return copy.deepcopy(patch)
    out = copy.deepcopy(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = merge_patch(out.get(key), value)
    return out


def changed_paths(stored: dict[str, Any], new: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for key in set(stored) | set(new):
        old_v, new_v = stored.get(key), new.get(key)
        if old_v == new_v:
            continue
        if key in ("env", "wrapper"):
            old_d, new_d = _dict(old_v), _dict(new_v)
            out.extend(
                f"{key}.{sub}"
                for sub in set(old_d) | set(new_d)
                if old_d.get(sub) != new_d.get(sub)
            )
        else:
            out.append(key)
    return sorted(out)
