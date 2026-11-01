"""presets table repository. The api is the only caller and the only writer.

A preset is a named SessionSpec template. The spec is stored exactly as given: the repository
never fills defaults, and it does not NOTIFY because nothing listens for preset changes.
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

NAME_MAX = 40
_NAME = re.compile(r"[A-Za-z0-9 ._-]{1,40}")


class PresetExists(Exception):
    """Another preset already has this name (compared case-insensitively)."""


class PresetNotFound(Exception):
    pass


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
    created_at: datetime
    updated_at: datetime


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
        created_at=row["created_at"],
        updated_at=row["updated_at"],
    )


async def _fetch(conn: psycopg.AsyncConnection, query: str, params: tuple) -> list[Preset]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return [_from_row(r) for r in await cur.fetchall()]


async def list_presets(conn: psycopg.AsyncConnection) -> list[Preset]:
    return await _fetch(
        conn, "SELECT * FROM presets ORDER BY is_default DESC, lower(name), name", ()
    )


async def get(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> Preset | None:
    rows = await _fetch(conn, "SELECT * FROM presets WHERE id = %s", (preset_id,))
    return rows[0] if rows else None


async def get_for_share(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> Preset | None:
    """Read with a row share lock so the preset cannot be deleted until the caller commits."""
    rows = await _fetch(conn, "SELECT * FROM presets WHERE id = %s FOR SHARE", (preset_id,))
    return rows[0] if rows else None


async def create(
    conn: psycopg.AsyncConnection,
    name: str,
    spec: dict[str, Any],
    superleader_manageable: bool = False,
) -> Preset:
    validate_preset_name(name)
    try:
        async with conn.transaction():
            rows = await _fetch(
                conn,
                "INSERT INTO presets (id, name, spec, superleader_manageable)"
                " VALUES (%s, %s, %s, %s) RETURNING *",
                (uuid.uuid4(), name, Jsonb(spec), superleader_manageable),
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
) -> Preset:
    """Replace name and spec; a None flag keeps the stored superleader_manageable."""
    validate_preset_name(name)
    try:
        async with conn.transaction():
            current = await _fetch(
                conn, "SELECT * FROM presets WHERE id = %s FOR UPDATE", (preset_id,)
            )
            if not current:
                raise PresetNotFound(str(preset_id))
            if current[0].is_default and name != current[0].name:
                raise PresetDefault(current[0].name)
            rows = await _fetch(
                conn,
                "UPDATE presets SET name = %s, spec = %s,"
                " superleader_manageable = COALESCE(%s, superleader_manageable),"
                " updated_at = now() WHERE id = %s RETURNING *",
                (name, Jsonb(spec), superleader_manageable, preset_id),
            )
    except psycopg.errors.UniqueViolation:
        raise PresetExists(name) from None
    return rows[0]


async def manageable_ids(conn: psycopg.AsyncConnection) -> set[uuid.UUID]:
    cur = await conn.execute("SELECT id FROM presets WHERE superleader_manageable")
    return {r[0] for r in await cur.fetchall()}


async def delete(conn: psycopg.AsyncConnection, preset_id: uuid.UUID) -> None:
    async with conn.transaction():
        # Lock every row in a fixed order: concurrent deletes queue here, so the count below
        # is taken after the earlier delete has committed and the last row cannot go.
        cur = await conn.execute("SELECT id, is_default FROM presets ORDER BY id FOR UPDATE")
        rows = await cur.fetchall()
        match = [r for r in rows if r[0] == preset_id]
        if not match:
            raise PresetNotFound(str(preset_id))
        if match[0][1]:
            raise PresetDefault(str(preset_id))
        if len(rows) == 1:
            raise LastPreset(str(preset_id))
        await conn.execute("DELETE FROM presets WHERE id = %s", (preset_id,))
