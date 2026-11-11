"""presets table repository. The api is the only caller and the only writer.

A preset is a named SessionSpec template. The spec is stored exactly as given: the repository
never fills defaults, and it does not NOTIFY because nothing listens for preset changes.
"""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db.projects import DEFAULT

NAME_MAX = 40
_NAME = re.compile(r"[A-Za-z0-9 ._-]{1,40}")


class PresetExists(Exception):
    """Another preset already has this name (compared case-insensitively)."""


class PresetNotFound(Exception):
    pass


class PresetChanged(Exception):
    """The row changed since the caller read it (compare-and-swap on updated_at)."""


class PresetDefault(Exception):
    """The default preset cannot be renamed or deleted."""


class LastPreset(Exception):
    """The only remaining preset cannot be deleted."""


@dataclass(frozen=True)
class Preset:
    id: uuid.UUID
    name: str
    spec: dict[str, Any]
    is_default: bool
    superleader_manageable: bool
    labels: list[str]
    created_at: datetime
    updated_at: datetime
    project: str = DEFAULT
    vault_reattach: list[str] = field(default_factory=list)
    # the other projects its sessions fully belong to (T-120), sorted; copied at session create
    extra_projects: tuple[str, ...] = ()


def validate_preset_name(name: str) -> str:
    """Return name unchanged when valid; a name is never trimmed, so edge spaces are rejected."""
    if not _NAME.fullmatch(name):
        raise ValueError(
            f"preset name must be 1 to {NAME_MAX} characters of letters, digits, spaces,"
            " dots, dashes and underscores"
        )
    if name != name.strip(" "):
        raise ValueError("preset name must not start or end with a space")
    return name


def _from_row(row: dict[str, Any]) -> Preset:
    return Preset(
        id=row["id"],
        name=row["name"],
        spec=row["spec"],
        is_default=row["is_default"],
        superleader_manageable=row["superleader_manageable"],
        labels=row["labels"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        project=row["project"],
        vault_reattach=row["vault_reattach"],
        extra_projects=tuple(row["extra_projects"]),
    )


async def _fetch(conn: psycopg.AsyncConnection, query: str, params: tuple) -> list[Preset]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return [_from_row(r) for r in await cur.fetchall()]


async def list_presets(
    conn: psycopg.AsyncConnection, *, project: str | None = None
) -> list[Preset]:
    """Every preset; with `project`, only that project's."""
    where = "" if project is None else " WHERE project = %s"
    return await _fetch(
        conn,
        f"SELECT * FROM presets{where} ORDER BY is_default DESC, lower(name), name",
        () if project is None else (project,),
    )


async def get(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> Preset | None:
    rows = await _fetch(conn, "SELECT * FROM presets WHERE id = %s", (preset_id,))
    return rows[0] if rows else None


async def get_for_share(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> Preset | None:
    """Read with a row share lock so the preset cannot be deleted until the caller commits."""
    rows = await _fetch(conn, "SELECT * FROM presets WHERE id = %s FOR SHARE", (preset_id,))
    return rows[0] if rows else None


async def get_for_update(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> Preset | None:
    """The row locked FOR UPDATE until the caller's transaction ends (a proposal apply)."""
    rows = await _fetch(conn, "SELECT * FROM presets WHERE id = %s FOR UPDATE", (preset_id,))
    return rows[0] if rows else None


async def create(
    conn: psycopg.AsyncConnection,
    name: str,
    spec: dict[str, Any],
    superleader_manageable: bool = False,
    labels: list[str] | None = None,
    project: str = DEFAULT,
) -> Preset:
    validate_preset_name(name)
    try:
        async with conn.transaction():
            rows = await _fetch(
                conn,
                "INSERT INTO presets (id, name, spec, superleader_manageable, labels, project)"
                " VALUES (%s, %s, %s, %s, %s, %s) RETURNING *",
                (uuid.uuid4(), name, Jsonb(spec), superleader_manageable, labels or [], project),
            )
    except psycopg.errors.UniqueViolation:
        raise PresetExists(name) from None
    return rows[0]


async def update(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    name: str,
    spec: dict[str, Any],
    superleader_manageable: bool | None = None,
    labels: list[str] | None = None,
    expected_updated_at: datetime | None = None,
    project: str | None = None,
    recheck: Callable[[Preset], None] | None = None,
) -> Preset:
    """Replace name and spec; a None flag, None labels or a None project keep the stored values.

    With `expected_updated_at`, the row must still carry that timestamp (checked under the row
    lock) or `PresetChanged` is raised and nothing is written. `recheck(row)` runs on the locked
    row before the write and raises to refuse."""
    validate_preset_name(name)
    try:
        async with conn.transaction():
            current = await _fetch(
                conn, "SELECT * FROM presets WHERE id = %s FOR UPDATE", (preset_id,)
            )
            if not current:
                raise PresetNotFound(str(preset_id))
            if expected_updated_at is not None and current[0].updated_at != expected_updated_at:
                raise PresetChanged(str(preset_id))
            if current[0].is_default and name != current[0].name:
                raise PresetDefault(current[0].name)
            if recheck is not None:
                recheck(current[0])
            rows = await _fetch(
                conn,
                "UPDATE presets SET name = %s, spec = %s,"
                " superleader_manageable = COALESCE(%s, superleader_manageable),"
                " labels = COALESCE(%s, labels), project = COALESCE(%s, project),"
                " extra_projects = array_remove(extra_projects, COALESCE(%s, project)),"
                " updated_at = now() WHERE id = %s RETURNING *",
                (name, Jsonb(spec), superleader_manageable, labels, project, project, preset_id),
            )
    except psycopg.errors.UniqueViolation:
        raise PresetExists(name) from None
    return rows[0]


async def set_extra_projects(
    conn: psycopg.AsyncConnection, preset_id: uuid.UUID, projects: list[str]
) -> Preset:
    """Replace the preset's extra projects (stored sorted and distinct); the caller validates the
    slugs (`api.project_grants`). Sessions created before keep theirs. `updated_at` is not
    bumped: it fingerprints the spec for spec proposals, which a grant does not touch."""
    rows = await _fetch(
        conn,
        "UPDATE presets SET extra_projects = %s WHERE id = %s RETURNING *",
        (sorted(set(projects)), preset_id),
    )
    if not rows:
        raise PresetNotFound(str(preset_id))
    return rows[0]


async def manageable_ids(conn: psycopg.AsyncConnection) -> set[uuid.UUID]:
    cur = await conn.execute("SELECT id FROM presets WHERE superleader_manageable")
    return {r[0] for r in await cur.fetchall()}


async def unmanageable_ids(conn: psycopg.AsyncConnection) -> set[uuid.UUID]:
    cur = await conn.execute("SELECT id FROM presets WHERE NOT superleader_manageable")
    return {r[0] for r in await cur.fetchall()}


async def delete(
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    recheck: Callable[[dict[str, Any]], None] | None = None,
) -> None:
    """Delete a preset. `recheck(spec)` runs on the spec read under the row lock, before
    anything is deleted; it raises to refuse."""
    async with conn.transaction():
        # Lock every row in a fixed order: concurrent deletes queue here, so the count below
        # is taken after the earlier delete has committed and the last row cannot go.
        cur = await conn.execute("SELECT id, is_default, spec FROM presets ORDER BY id FOR UPDATE")
        rows = await cur.fetchall()
        match = [r for r in rows if r[0] == preset_id]
        if not match:
            raise PresetNotFound(str(preset_id))
        if recheck is not None:
            recheck(match[0][2])
        if match[0][1]:
            raise PresetDefault(str(preset_id))
        if len(rows) == 1:
            raise LastPreset(str(preset_id))
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset_id,))


async def set_vault_reattach(
    conn: psycopg.AsyncConnection, preset_id: uuid.UUID, names: list[str]
) -> Preset:
    """Replace the env names a user still has to re-attach as vault references (names only)."""
    rows = await _fetch(
        conn,
        "UPDATE presets SET vault_reattach = %s, updated_at = now() WHERE id = %s RETURNING *",
        (sorted(set(names)), preset_id),
    )
    if not rows:
        raise PresetNotFound(str(preset_id))
    return rows[0]


async def insert_vault_event(
    conn: psycopg.AsyncConnection, kind: str, actor: str, data: dict[str, Any]
) -> None:
    """Append a names-only vault attach or detach event (the payload carries no value)."""
    await conn.execute(
        "INSERT INTO preset_vault_events (kind, actor, data) VALUES (%s, %s, %s)",
        (kind, actor, Jsonb(data)),
    )


async def vault_events(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> list[dict[str, Any]]:
    """The vault events of one preset, oldest first."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT kind, actor, data FROM preset_vault_events"
            " WHERE data ->> 'preset_id' = %s ORDER BY id",
            (str(preset_id),),
        )
        return await cur.fetchall()
