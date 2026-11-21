"""Preset volume attachments: the save-time check against the volumes table, the default mount
path and the attach/detach audit. Names, ids and modes only, never content."""

from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any

import psycopg

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import volumes
from agents_orchestrator.preset_policy import volume_attach_is_security
from agents_orchestrator.spec import SessionSpec, VolumeAttachment, effective_mount_path

__all__ = [
    "attachments_in",
    "audit_volume_attachments",
    "check_volume_refs",
    "cross_trust_problems",
    "other_attaching_specs",
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


async def other_attaching_specs(
    conn: psycopg.AsyncConnection, volume_id: uuid.UUID, preset_id: uuid.UUID | None
) -> list[Any]:
    """The specs of the presets other than `preset_id` that attach the volume, plus those of the
    sessions that still mount it (a detached preset's live session keeps its mount). A vanished
    row reads as None, which the policy counts as privileged."""
    ids = sorted({a.preset_id for a in await volumes.attaching_presets(conn, volume_id)})
    out: list[Any] = []
    for other_id in ids:
        if other_id != preset_id:
            other = await presets_repo.get(conn, other_id)
            out.append(other.spec if other is not None else None)
    out.extend(await volumes.claim_specs(conn, volume_id, except_preset=preset_id))
    return out


async def cross_trust_problems(
    conn: psycopg.AsyncConnection,
    spec: dict[str, Any],
    old: list[dict[str, Any]] | None,
    preset_id: uuid.UUID | None,
) -> list[str]:
    """The attachments of `spec` that a superleader may not save (Q7, `volume_attach_is_security`):
    only attachments whose (volume, mode) pair is new against `old` (the preset's stored ones)
    count, so an unrelated edit of a preset the user already shared stays possible. Each volume
    is locked FOR UPDATE first, so two concurrent attaches of one volume see each other. Fail
    closed: an unreadable attachment is a problem."""
    kept = {(str(a.get("volume")), str(a.get("mode"))) for a in old or []}
    problems: list[str] = []
    for i, att in enumerate(attachments_in(spec)):
        if (str(att.get("volume")), str(att.get("mode"))) in kept:
            continue
        try:
            vid = uuid.UUID(str(att["volume"]))
        except (KeyError, ValueError):
            problems.append(f"volumes[{i}]: unreadable attachment")
            continue
        found = await volumes.get_locked(conn, vid)
        if found is None:
            continue  # check_volume_refs reports an unknown volume
        others = await other_attaching_specs(conn, vid, preset_id)
        if volume_attach_is_security(spec, str(att.get("mode")), others):
            problems.append(
                f"volumes[{i}]: volume {found.name!r} would be shared with a privileged preset"
                " or attached read-write to one; only the user approves that (proposal)"
            )
    return problems


async def prepare_volumes(
    conn: psycopg.AsyncConnection,
    spec: dict[str, Any],
    preset_scope: Iterable[str],
    *,
    superleader: bool = False,
    old: list[dict[str, Any]] | None = None,
    preset_id: uuid.UUID | None = None,
) -> None:
    """Check the stored-shape `spec`'s attachments (422 `volumes_invalid`) and fill the default
    mount path of each in place. For a superleader (`superleader`) the cross-trust rule runs
    too (403 `volume_cross_trust`) against `old`, the preset's stored attachments, and
    `preset_id`, excluded from the other attaching presets. Every save path of a preset goes
    through here, so no superleader route can skip the rule; a human save does not run it."""
    atts = attachments_in(spec)
    if not atts:
        return
    # every referenced row in id order, before any check: FOR SHARE so an attach cannot race a
    # delete or a project change (they hold FOR UPDATE); FOR UPDATE for a superleader, whose
    # cross-trust check needs concurrent attaches of one volume to see each other (taking SHARE
    # first and upgrading later would deadlock two of them)
    await volumes.lock_rows(
        conn,
        [uuid.UUID(str(a["volume"])) for a in atts if _is_uuid(a.get("volume"))],
        exclusive=superleader,
    )
    problems = await check_volume_refs(conn, SessionSpec.model_validate(spec), preset_scope)
    if problems:
        raise ApiError(422, "volumes_invalid", "; ".join(problems), extra={"problems": problems})
    if superleader and (trust := await cross_trust_problems(conn, spec, old, preset_id)):
        raise ApiError(403, "volume_cross_trust", "; ".join(trust), extra={"problems": trust})
    for att in atts:
        if att.get("mount_path") is None:
            found = await volumes.get(conn, uuid.UUID(str(att["volume"])))
            assert found is not None  # checked just above
            att["mount_path"] = effective_mount_path(
                VolumeAttachment.model_validate(att), found.name
            )


def _is_uuid(value: Any) -> bool:
    try:
        uuid.UUID(str(value))
    except ValueError:
        return False
    return True


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
