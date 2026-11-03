"""Pure rules for human-only preset fields.

An agent (the superleader) may change these preset fields only toward safe;
anything else is refused. Compared against the stored spec, with absent, None
and false equal:

- kube_access, session_messaging, docker: may be turned off or removed, never on.
- discard_hook_url: may be removed, never set to a value other than the stored one.
- wrapper.permission_mode: unset means bypassPermissions; may become "auto",
  never any other change.
- wrapper.permissions.deny: entries may be added, never removed.
- wrapper.permissions.allow: entries may be removed, never added.
- env.<NAME> of a stored protected variable: may not be removed or unprotected
  (decision 7 b); adding a protected variable and setting its value stay allowed.

All functions work on JSON dicts as stored (model_dump(mode="json",
exclude_unset=True)) and never mutate their inputs. The main port never calls
them: only the agent-facing admin routes do.
"""

import copy
from collections.abc import Iterable
from typing import Any

GRANTS: tuple[str, ...] = ("kube_access", "session_messaging", "docker", "discard_hook_url")
DOCKER_SIZES: tuple[str, ...] = ("docker_storage", "docker_cpu", "docker_memory")
# discard_hook_url is human-only to set but grants no reach to a started session.
PRIVILEGED: tuple[str, ...] = ("kube_access", "session_messaging", "docker")
HUMAN_ONLY: tuple[str, ...] = (
    "kube_access",
    "session_messaging",
    "docker",
    "discard_hook_url",
    "wrapper.permission_mode",
    "wrapper.permissions.allow",
    "wrapper.permissions.deny",
)

_DEFAULT_PERMISSION_MODE = "bypassPermissions"
_SAFE_PERMISSION_MODE = "auto"


def _dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _entries(value: Any) -> set[str]:
    return {str(v) for v in value} if isinstance(value, list) else set()


def privileged(spec: dict[str, Any]) -> list[str]:
    return sorted(f for f in PRIVILEGED if bool(spec.get(f)))


def human_only_changes(stored: dict[str, Any], new: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for f in PRIVILEGED:
        if bool(new.get(f)) and not bool(stored.get(f)):
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
