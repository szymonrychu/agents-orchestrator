"""Preset volume attachments: the save-time check against the volumes table, the default mount
path and the attach/detach audit. Names, ids and modes only, never content."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any

import psycopg

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import volumes
from agents_orchestrator.spec import SessionSpec, VolumeAttachment, effective_mount_path

__all__ = [
    "attachments_in",
    "audit_volume_attachments",
    "check_volume_refs",
    "effective_mount_path",
    "prepare_volumes",
]


def attachments_in(spec: dict[str, Any]) -> list[dict[str, Any]]:
    """The attachments of a stored spec dict; a malformed stored spec is not trusted."""
    raw = spec.get("volumes")
    return [a for a in raw if isinstance(a, dict)] if isinstance(raw, list) else []


async def check_volume_refs(
    conn: psycopg.AsyncConnection, spec: SessionSpec, preset_scope: Iterable[str]
) -> list[str]:
    """Problems of the attachments against the volumes table: the volume exists and is not
    deleted, its `projects` intersect `preset_scope` (the preset's home project plus approved
    extra projects), and the effective mount paths (default `/volumes/<name>`) are unique."""
    scope = set(preset_scope)
    problems: list[str] = []
    paths: dict[str, int] = {}
    for i, att in enumerate(spec.volumes or []):
        found = await volumes.get(conn, att.volume)
        if found is None:
            problems.append(f"volumes[{i}]: unknown volume {att.volume}")
            continue
        if found.state == "deleting":
            problems.append(f"volumes[{i}]: volume {found.name!r} is being deleted")
            continue
        if not scope & set(found.projects):
            problems.append(
                f"volumes[{i}]: volume {found.name!r} belongs to other projects than the"
                " preset's project scope"
            )
        path = effective_mount_path(att, found.name)
        if path in paths:
            problems.append(
                f"volumes[{i}]: mount path {path} is already used by volumes[{paths[path]}]"
            )
        paths.setdefault(path, i)
    return problems


async def prepare_volumes(
    conn: psycopg.AsyncConnection, spec: dict[str, Any], preset_scope: Iterable[str]
) -> None:
    """Check the stored-shape `spec`'s attachments (422 `volumes_invalid`) and fill the default
    mount path of each in place."""
    atts = attachments_in(spec)
    if not atts:
        return
    problems = await check_volume_refs(conn, SessionSpec.model_validate(spec), preset_scope)
    if problems:
        raise ApiError(422, "volumes_invalid", "; ".join(problems), extra={"problems": problems})
    for att in atts:
        if att.get("mount_path") is None:
            found = await volumes.get(conn, uuid.UUID(str(att["volume"])))
            assert found is not None  # checked just above
            att["mount_path"] = effective_mount_path(
                VolumeAttachment.model_validate(att), found.name
            )


def _key(att: dict[str, Any]) -> tuple[str, str, str | None, bool]:
    return (
        str(att.get("volume")),
        str(att.get("mode")),
        att.get("mount_path"),
        bool(att.get("backup_restore", False)),
    )


async def audit_volume_attachments(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    old: list[dict[str, Any]] | None,
    new: list[dict[str, Any]] | None,
    actor_kind: str,
    actor: str,
) -> None:
    """One `attached` or `detached` volume event per attachment that differs between two saves
    (a changed mode, path or restore flag is a detach of the old one and an attach of the new)."""
    old_by, new_by = {_key(a): a for a in old or []}, {_key(a): a for a in new or []}
    for kind, atts in (
        ("detached", [a for k, a in old_by.items() if k not in new_by]),
        ("attached", [a for k, a in new_by.items() if k not in old_by]),
    ):
        for att in atts:
            try:
                vid = uuid.UUID(str(att["volume"]))
            except (KeyError, ValueError):
                continue
            if await volumes.get(conn, vid) is None:
                continue
            await volumes.record_event(
                conn,
                vid,
                kind,
                actor_kind=actor_kind,
                actor=actor,
                detail={
                    "preset_id": str(preset_id),
                    "mode": att.get("mode"),
                    "mount_path": att.get("mount_path"),
                    "backup_restore": bool(att.get("backup_restore", False)),
                },
            )
