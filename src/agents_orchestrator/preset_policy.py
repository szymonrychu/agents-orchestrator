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
- env.<NAME> of a stored protected variable: may not be removed or unprotected
  (decision 7 b); adding a protected variable and setting its value stay allowed.

A live session adds two rules of its own: `fixed_changes` (fields that cannot change on a
running session) and `tighten_changes` (limits a regular leader may only lower: usage quota,
inactivity, autoclose and pod resources).

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
HUMAN_ONLY: tuple[str, ...] = (
    "kube_cluster_access",
    "kube_namespace_access",
    "session_messaging",
    "docker",
    "discard_hook_url",
    "wrapper.permission_mode",
    "wrapper.permissions.allow",
    "wrapper.permissions.deny",
)

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

    new_env = _dict(new.get("env"))
    for name, entry in _dict(stored.get("env")).items():
        if _dict(entry).get("protected") and not _dict(new_env.get(name)).get("protected"):
            out.append(f"env.{name}")
    return sorted(out)


def privileged_target_changes(
    stored: dict[str, Any], new: dict[str, Any], secret_values: Iterable[str] = ()
) -> list[str]:
    """The changed spec paths an agent may not touch on a stored preset that is privileged.

    Empty when the stored preset has no privileged flag on. Otherwise only `usage_quota` and
    the human-only fields (which `human_only_changes` already limits to toward safe) may
    change; name and labels are not spec paths. The stored preset counts, so turning a flag
    off together with another edit is refused. `secret_values` names the protected variables
    the call gives a value: each is refused as `env.<NAME>`, because a new value would swap a
    credential the granted pod runs with. A stored protected entry resent unchanged (or
    differing only in form) passes; removing or unprotecting it is human-only (7 b)."""
    if not privileged(stored):
        return []
    protected = {
        f"env.{n}" for n, e in _dict(stored.get("env")).items() if _dict(e).get("protected")
    }
    allowed = {"usage_quota", *GRANTS, *DOCKER_SIZES, *protected}
    allowed |= {"wrapper.permission_mode", "wrapper.permissions"}
    refused = {p for p in changed_paths(stored, new) if p not in allowed}
    return sorted(refused | {f"env.{n}" for n in secret_values})


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


def tighten_changes(
    stored: dict[str, Any],
    new: dict[str, Any],
    *,
    quota_default: int,
    reminder_default: int,
    close_default: int,
) -> list[str]:
    """The limit paths whose effective value got looser from `stored` to `new` (sorted):
    `usage_quota.five_hour_pct` and `.seven_day_pct` (unset is `quota_default`, higher is
    looser), `inactivity.reminder_seconds` and `.close_seconds` (unset is the default, 0 is
    off and so infinite, longer is looser), `autoclose_after_seconds` (unset is never, so
    clearing or raising is looser) and `resources.requests.*` / `resources.limits.*` (see
    `_resource_changes`). Lowering, or keeping, a limit is never reported."""
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
