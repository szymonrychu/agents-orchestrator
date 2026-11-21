"""volumes, volume_mounts and volume_events: named shared volumes (Postgres).

Writer of all three tables: this module. Every mutation takes the `volumes` row lock
(`SELECT ... FOR UPDATE`) in the caller's transaction, so claims, resizes and state changes of
one volume serialise; the caller must hold a transaction (for example `conn.transaction()`).
Each change NOTIFYs `volumes_changed {id}` (ids only; consumers re-read the table). Event
detail holds names, ids and versions, never file names or content.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db.backups import Page
from agents_orchestrator.db.pool import notify
from agents_orchestrator.spec import parse_quantity

CHANGED_CHANNEL = "volumes_changed"  # NOTIFY {"id"}: a volume or its claims changed
NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$")
DEFAULT_MAX_SIZE = "200Gi"
MAX_LIMIT = 200
STATES = ("provisioning", "ready", "resizing", "restoring", "saving", "deleting", "failed")
_COLUMNS = (
    "id, name, projects, labels, description, size, storage_class, pvc_name, state,"
    " state_reason, content_version, created_by, created_at, updated_at, deleted_at"
)
_CLAIM_COLUMNS = (
    "id, volume_id, session_id, preset_id, mode, backup_restore, claimed_at, released_at"
)
_EVENT_COLUMNS = "id, volume_id, at, actor_kind, actor, kind, detail"


class VolumeExists(Exception):
    """A live volume already has this name."""


class ShrinkRefused(Exception):
    """A volume only grows."""


class OverCap(Exception):
    """The size is above the configured maximum."""


class VolumeNotFound(Exception):
    pass


@dataclass
class Volume:
    id: uuid.UUID
    name: str
    projects: list[str]
    labels: list[str]
    description: str
    size: str
    storage_class: str
    pvc_name: str
    state: str
    state_reason: str | None
    content_version: uuid.UUID | None
    created_by: str
    created_at: datetime
    updated_at: datetime
    deleted_at: datetime | None


@dataclass
class Claim:
    id: int
    volume_id: uuid.UUID
    session_id: uuid.UUID
    preset_id: uuid.UUID | None
    mode: str
    backup_restore: bool
    claimed_at: datetime
    released_at: datetime | None


@dataclass
class Event:
    id: int
    volume_id: uuid.UUID
    at: datetime
    actor_kind: str
    actor: str
    kind: str
    detail: dict[str, Any]


def quantity_bytes(value: str) -> int:
    """A Kubernetes quantity (binary or decimal suffix) as whole bytes; ValueError when bad."""
    return int(parse_quantity(value))


def _check_size(size: str, max_size: str) -> int:
    n = quantity_bytes(size)
    if n <= 0:
        raise ValueError(f"{size!r} must be a positive quantity")
    if n > quantity_bytes(max_size):
        raise OverCap(f"{size} is above the maximum {max_size}")
    return n


async def _rows(conn: psycopg.AsyncConnection, sql: str, params: tuple, cls: type) -> list:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, params)
        return [cls(**r) for r in await cur.fetchall()]


async def _lock(conn: psycopg.AsyncConnection, id: uuid.UUID) -> Volume:
    rows = await _rows(
        conn,
        f"SELECT {_COLUMNS} FROM volumes WHERE id = %s AND deleted_at IS NULL FOR UPDATE",
        (id,),
        Volume,
    )
    if not rows:
        raise VolumeNotFound(str(id))
    return rows[0]


async def _touch(conn: psycopg.AsyncConnection, id: uuid.UUID) -> None:
    await conn.execute("UPDATE volumes SET updated_at = now() WHERE id = %s", (id,))
    await notify(conn, CHANGED_CHANNEL, {"id": str(id)})


async def record_event(
    conn: psycopg.AsyncConnection,
    id: uuid.UUID,
    kind: str,
    *,
    actor_kind: str,
    actor: str,
    detail: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        "INSERT INTO volume_events (volume_id, actor_kind, actor, kind, detail)"
        " VALUES (%s, %s, %s, %s, %s)",
        (id, actor_kind, actor, kind, Jsonb(detail or {})),
    )


async def create(
    conn: psycopg.AsyncConnection,
    *,
    name: str,
    projects: list[str],
    size: str,
    description: str = "",
    labels: list[str] | None = None,
    storage_class: str,
    created_by: str,
    actor_kind: str,
    actor: str,
    max_size: str = DEFAULT_MAX_SIZE,
) -> Volume:
    """Insert a `provisioning` volume (PVC name `vol-<name>`); VolumeExists on a live name."""
    if not NAME_RE.match(name):
        raise ValueError(f"{name!r} is not a valid volume name")
    if not projects:
        raise ValueError("a volume needs at least one project")
    _check_size(size, max_size)
    # serialise creators of one name: the partial unique index alone would abort the transaction
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"volume:{name}",))
    if await by_name(conn, name) is not None:
        raise VolumeExists(name)
    vid = uuid.uuid4()
    rows = await _rows(
        conn,
        f"INSERT INTO volumes (id, name, projects, labels, description, size, storage_class,"
        f" pvc_name, state, created_by) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'provisioning',%s)"
        f" RETURNING {_COLUMNS}",
        (
            vid,
            name,
            projects,
            labels or [],
            description,
            size,
            storage_class,
            f"vol-{name}",
            created_by,
        ),
        Volume,
    )
    await record_event(
        conn,
        vid,
        "created",
        actor_kind=actor_kind,
        actor=actor,
        detail={"name": name, "size": size, "projects": projects},
    )
    await notify(conn, CHANGED_CHANNEL, {"id": str(vid)})
    return rows[0]


async def get(conn: psycopg.AsyncConnection, id: uuid.UUID) -> Volume | None:
    rows = await _rows(
        conn, f"SELECT {_COLUMNS} FROM volumes WHERE id = %s AND deleted_at IS NULL", (id,), Volume
    )
    return rows[0] if rows else None


async def by_name(conn: psycopg.AsyncConnection, name: str) -> Volume | None:
    rows = await _rows(
        conn,
        f"SELECT {_COLUMNS} FROM volumes WHERE name = %s AND deleted_at IS NULL",
        (name,),
        Volume,
    )
    return rows[0] if rows else None


async def list(conn: psycopg.AsyncConnection, *, project: str | None = None) -> list[Volume]:  # noqa: A001
    """Live volumes by name; `project` keeps those holding it among their projects."""
    where = "deleted_at IS NULL" + (" AND %s = ANY(projects)" if project is not None else "")
    params = (project,) if project is not None else ()
    return await _rows(
        conn, f"SELECT {_COLUMNS} FROM volumes WHERE {where} ORDER BY name", params, Volume
    )


async def update(
    conn: psycopg.AsyncConnection,
    id: uuid.UUID,
    *,
    size: str | None = None,
    description: str | None = None,
    labels: list[str] | None = None,
    projects: list[str] | None = None,
    actor_kind: str,
    actor: str,
    max_size: str = DEFAULT_MAX_SIZE,
) -> Volume:
    """Change a volume. Size is grow-only (ShrinkRefused) up to `max_size` (OverCap); a size
    equal to the current one is a no-op, a larger one sets `resizing`."""
    cur = await _lock(conn, id)
    sets: dict[str, Any] = {}
    events: list[tuple[str, dict[str, Any]]] = []
    if size is not None:
        new = _check_size(size, max_size)
        old = quantity_bytes(cur.size)
        if new < old:
            raise ShrinkRefused(f"{size} is below the current {cur.size}")
        if new > old:
            sets.update(size=size, state="resizing", state_reason=None)
            events.append(("resized", {"from": cur.size, "to": size}))
    if description is not None and description != cur.description:
        sets["description"] = description
    if labels is not None and labels != cur.labels:
        sets["labels"] = labels
        events.append(("labels_updated", {"labels": labels}))
    if projects is not None and projects != cur.projects:
        if not projects:
            raise ValueError("a volume needs at least one project")
        sets["projects"] = projects
        events.append(("projects_updated", {"from": cur.projects, "to": projects}))
    if sets:
        cols = ", ".join(f"{k} = %s" for k in sets)
        await conn.execute(f"UPDATE volumes SET {cols} WHERE id = %s", (*sets.values(), id))
        for kind, detail in events:
            await record_event(conn, id, kind, actor_kind=actor_kind, actor=actor, detail=detail)
        await _touch(conn, id)
    got = await get(conn, id)
    assert got is not None
    return got


async def set_state(
    conn: psycopg.AsyncConnection,
    id: uuid.UUID,
    state: str,
    *,
    reason: str | None = None,
    content_version: uuid.UUID | None = None,
) -> None:
    """Set the state and its failure reason (cleared otherwise); `content_version` is set only
    when given (a save or restore)."""
    if state not in STATES:
        raise ValueError(f"unknown volume state {state!r}")
    await _lock(conn, id)
    await conn.execute(
        "UPDATE volumes SET state = %s, state_reason = %s,"
        " content_version = COALESCE(%s, content_version) WHERE id = %s",
        (state, reason, content_version, id),
    )
    await _touch(conn, id)


async def mark_deleting(conn: psycopg.AsyncConnection, id: uuid.UUID) -> None:
    await set_state(conn, id, "deleting")


async def finish_delete(conn: psycopg.AsyncConnection, id: uuid.UUID) -> None:
    """Set `deleted_at` (the name becomes reusable) and write the `deleted` event."""
    vol = await _lock(conn, id)
    await conn.execute("UPDATE volumes SET deleted_at = now() WHERE id = %s", (id,))
    await record_event(
        conn, id, "deleted", actor_kind="system", actor="reconciler", detail={"name": vol.name}
    )
    await notify(conn, CHANGED_CHANNEL, {"id": str(id)})


async def claim(
    conn: psycopg.AsyncConnection,
    id: uuid.UUID,
    *,
    session_id: uuid.UUID,
    preset_id: uuid.UUID | None,
    mode: str,
    backup_restore: bool,
) -> Claim:
    """Claim the volume for a session (idempotent per volume and session); writes `mounted`."""
    await _lock(conn, id)
    rows = await _rows(
        conn,
        f"INSERT INTO volume_mounts (volume_id, session_id, preset_id, mode, backup_restore)"
        f" VALUES (%s,%s,%s,%s,%s) ON CONFLICT (volume_id, session_id) DO UPDATE"
        f" SET volume_id = EXCLUDED.volume_id RETURNING {_CLAIM_COLUMNS}",
        (id, session_id, preset_id, mode, backup_restore),
        Claim,
    )
    await record_event(
        conn,
        id,
        "mounted",
        actor_kind="system",
        actor="reconciler",
        detail={"session_id": str(session_id), "mode": mode, "backup_restore": backup_restore},
    )
    await notify(conn, CHANGED_CHANNEL, {"id": str(id)})
    return rows[0]


async def unreleased_claims(conn: psycopg.AsyncConnection, volume_id: uuid.UUID) -> list[Claim]:
    return await _rows(
        conn,
        f"SELECT {_CLAIM_COLUMNS} FROM volume_mounts WHERE volume_id = %s AND released_at IS NULL"
        " ORDER BY id",
        (volume_id,),
        Claim,
    )


async def release_claims(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, *, actor_kind: str, actor: str
) -> list[Claim]:
    """Release every unreleased claim of a session; one `released` event per volume."""
    released = await _rows(
        conn,
        f"UPDATE volume_mounts SET released_at = now() WHERE session_id = %s"
        f" AND released_at IS NULL RETURNING {_CLAIM_COLUMNS}",
        (session_id,),
        Claim,
    )
    for c in released:
        await record_event(
            conn,
            c.volume_id,
            "released",
            actor_kind=actor_kind,
            actor=actor,
            detail={"session_id": str(session_id)},
        )
        await notify(conn, CHANGED_CHANNEL, {"id": str(c.volume_id)})
    return released


async def events(
    conn: psycopg.AsyncConnection, id: uuid.UUID, *, before: int | None = None, limit: int = 50
) -> Page[Event]:
    """Newest first; `before` is an event id, `next_cursor` the id to pass for the next page."""
    limit = max(1, min(limit, MAX_LIMIT))
    where, params = "volume_id = %s", [id]
    if before is not None:
        where += " AND id < %s"
        params.append(before)
    rows = await _rows(
        conn,
        f"SELECT {_EVENT_COLUMNS} FROM volume_events WHERE {where} ORDER BY id DESC LIMIT %s",
        (*params, limit + 1),
        Event,
    )
    more = len(rows) > limit
    items = rows[:limit]
    return Page(items=items, next_cursor=str(items[-1].id) if more else None)
