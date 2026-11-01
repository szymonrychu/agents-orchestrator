"""Redaction of protected env variables on every read of a spec (superleader-crud D7).

A protected variable is rebuilt from scratch as `{protected, expand, set, version, updated_at}`;
nothing of the stored entry is copied, so a value that reached a spec by any other route is still
never read back. Plain variables pass through unchanged.
"""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from agents_orchestrator.spec import EnvVar, SessionSpec

_VERSION_RE = re.compile(r"-v(\d+)$")


@dataclass(frozen=True)
class EnvMeta:
    """What is known of a protected variable's stored value."""

    version: int
    updated_at: datetime


def session_metas(secret_versions: Mapping[str, str], created_at: datetime) -> dict[str, EnvMeta]:
    """A session's metadata comes from its snapshot: the version it was created with, bound at
    `created_at` (a later rotation of the preset does not change what the session runs)."""
    out = {}
    for name, secret in secret_versions.items():
        match = _VERSION_RE.search(secret)
        if match:
            out[name] = EnvMeta(int(match.group(1)), created_at)
    return out


def _entry(expand: Any, meta: EnvMeta | None) -> dict[str, Any]:
    entry: dict[str, Any] = {"protected": True, "expand": bool(expand), "set": meta is not None}
    if meta is not None:
        entry["version"] = meta.version
        entry["updated_at"] = meta.updated_at
    return entry


def redact_spec_dict(spec: Mapping[str, Any], metas: Mapping[str, EnvMeta]) -> dict[str, Any]:
    """A stored spec dict with each protected variable reduced to its metadata."""
    out = dict(spec)
    env = out.get("env")
    if isinstance(env, dict):
        out["env"] = {
            name: _entry(e.get("expand"), metas.get(name))
            if isinstance(e, dict) and e.get("protected")
            else e
            for name, e in env.items()
        }
    return out


def redact_spec(spec: SessionSpec, metas: Mapping[str, EnvMeta]) -> SessionSpec:
    """A copy of `spec` for output; its protected variables carry metadata, never a value."""
    env = {
        name: EnvVar.model_construct(
            value=None,
            protected=True,
            expand=var.expand,
            set=name in metas,
            version=metas[name].version if name in metas else None,
            updated_at=metas[name].updated_at if name in metas else None,
        )
        if var.protected
        else var
        for name, var in spec.env.items()
    }
    return spec.model_copy(update={"env": env})
