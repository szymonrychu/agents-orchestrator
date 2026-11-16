"""backups and backup_versions: named, versioned directory backups (Postgres).

Writer of both tables: this module. Plain versioning (decision 766): the later save becomes the
current version, the earlier one stays as a `superseded` version and can be copied back; there
is no merge. Every pointer change runs in the caller's transaction after `SELECT ... FOR UPDATE`
on the `backups` row, so two saves of one name serialise: both rows exist, exactly one is
`current`. The caller must hold a transaction (for example `conn.transaction()`): under
autocommit the row lock is released at once and atomicity is lost. Each function here issues
its statements in order and relies on that lock.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db.pool import notify

CURRENT_CHANNEL = "backups_current"  # NOTIFY {"name"}: a name got a new current version
SaveResult = Literal["advanced", "unchanged", "orphan"]
MAX_LIMIT = 200
_COLUMNS = (
    "id, name, object_key, size, digest, manifest, base_version, origin, session_slug,"
    " preset_id, project, created_by, created_at, state"
)


@dataclass
class Version:
    id: uuid.UUID
    name: str
    object_key: str
    size: int
    digest: str
    manifest: dict[str, Any] | None
    base_version: uuid.UUID | None
    origin: str
    session_slug: str | None
    preset_id: uuid.UUID | None
    project: str | None
    created_by: str
    created_at: datetime
    state: str


@dataclass
class NameRow:
    name: str
    current: Version | None
    version_count: int
    last_used_at: datetime | None
    updated_at: datetime


@dataclass
class Page[T]:
    items: list[T]
    next_cursor: str | None = None


def _limit(limit: int) -> int:
    return max(1, min(limit, MAX_LIMIT))


def _version(row: dict[str, Any]) -> Version:
    return Version(**row)


async def _lock(conn: psycopg.AsyncConnection, name: str, *, create: bool) -> dict | None:
    """Lock the backups row of `name` (created empty when `create`); None when it is missing."""
    if create:
        await conn.execute(
            "INSERT INTO backups (name, updated_at) VALUES (%s, now()) ON CONFLICT DO NOTHING",
            (name,),
        )
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT name, current_version FROM backups WHERE name = %s FOR UPDATE", (name,)
        )
        return await cur.fetchone()


async def record_save(
    conn: psycopg.AsyncConnection,
    *,
    name: str,
    object_key: str,
    size: int,
    digest: str,
    manifest: dict[str, Any] | None,
    base_version: uuid.UUID | None,
    origin: str,
    session_slug: str | None,
    preset_id: uuid.UUID | None,
    project: str | None,
    created_by: str,
    orphan: bool,
) -> tuple[SaveResult, Version | None]:
    """Record a stored object as a version of `name`.

    Not orphan and a digest different from the current one: new `current` row, the previous
    current becomes `superseded` ("advanced"). Equal digest: no row, the caller deletes the
    fresh object ("unchanged", None). Orphan: a row `orphan`, the pointer stays ("orphan").
    """
    locked = await _lock(conn, name, create=True)
    assert locked is not None
    previous = locked["current_version"]
    if not orphan and previous is not None:
        cur = await conn.execute("SELECT digest FROM backup_versions WHERE id = %s", (previous,))
        row = await cur.fetchone()
        if row is not None and row[0] == digest:
            return "unchanged", None
    version_id = uuid.uuid4()
    state = "orphan" if orphan else "current"
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"INSERT INTO backup_versions ({_COLUMNS}) VALUES"
            " (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,clock_timestamp(),%s) RETURNING " + _COLUMNS,
            (
                version_id,
                name,
                object_key,
                size,
                digest,
                Jsonb(manifest) if manifest is not None else None,
                base_version,
                origin,
                session_slug,
                preset_id,
                project,
                created_by,
                state,
            ),
        )
        created = _version(await cur.fetchone())
    if orphan:
        return "orphan", created
    if previous is not None:
        await conn.execute(
            "UPDATE backup_versions SET state = 'superseded' WHERE id = %s", (previous,)
        )
    await conn.execute(
        "UPDATE backups SET current_version = %s, updated_at = now() WHERE name = %s",
        (version_id, name),
    )
    await notify(conn, CURRENT_CHANNEL, {"name": name})  # wakes live delivery (on commit)
    return "advanced", created


async def current(conn: psycopg.AsyncConnection, name: str) -> Version | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT "
            + ", ".join(f"v.{c.strip()}" for c in _COLUMNS.split(","))
            + " FROM backups b JOIN backup_versions v ON v.id = b.current_version"
            " WHERE b.name = %s",
            (name,),
        )
        row = await cur.fetchone()
    return _version(row) if row else None


async def by_object_key(conn: psycopg.AsyncConnection, object_key: str) -> Version | None:
    """The version stored under `object_key` (a save recorded twice after a crash), or None."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM backup_versions WHERE object_key = %s", (object_key,)
        )
        row = await cur.fetchone()
    return _version(row) if row else None


async def touch_used(conn: psycopg.AsyncConnection, name: str) -> None:
    await conn.execute("UPDATE backups SET last_used_at = now() WHERE name = %s", (name,))


def _like(q: str) -> str:
    escaped = q.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{escaped}%"


async def list_names(
    conn: psycopg.AsyncConnection, *, q: str | None, limit: int, cursor: str | None
) -> Page[NameRow]:
    """Names ordered by name, filtered by a substring `q`, keyset-paged after `cursor`."""
    limit = _limit(limit)
    cols = ", ".join(f"v.{c.strip()} AS v_{c.strip()}" for c in _COLUMNS.split(","))
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT b.name, b.last_used_at, b.updated_at, {cols},"
            " (SELECT count(*) FROM backup_versions c WHERE c.name = b.name) AS version_count"
            " FROM backups b LEFT JOIN backup_versions v ON v.id = b.current_version"
            " WHERE (%(q)s::text IS NULL OR b.name ILIKE %(q)s)"
            " AND (%(cursor)s::text IS NULL OR b.name > %(cursor)s)"
            " ORDER BY b.name LIMIT %(n)s",
            {"q": _like(q) if q else None, "cursor": cursor, "n": limit + 1},
        )
        rows = await cur.fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    items = []
    for r in rows:
        cur_v = (
            _version({c.strip(): r[f"v_{c.strip()}"] for c in _COLUMNS.split(",")})
            if r["v_id"] is not None
            else None
        )
        items.append(
            NameRow(r["name"], cur_v, r["version_count"], r["last_used_at"], r["updated_at"])
        )
    return Page(items, rows[-1]["name"] if more else None)


async def versions(
    conn: psycopg.AsyncConnection, name: str, *, limit: int, cursor: str | None
) -> Page[Version]:
    """Versions of `name`, newest first; the cursor is `<created_at iso>|<id>` of the last row."""
    limit = _limit(limit)
    after_ts, after_id = cursor.split("|", 1) if cursor else (None, None)
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM backup_versions WHERE name = %(name)s"
            " AND (%(ts)s::timestamptz IS NULL"
            " OR (created_at, id) < (%(ts)s::timestamptz, %(id)s::uuid))"
            " ORDER BY created_at DESC, id DESC LIMIT %(n)s",
            {"name": name, "ts": after_ts, "id": after_id, "n": limit + 1},
        )
        rows = await cur.fetchall()
    more = len(rows) > limit
    items = [_version(r) for r in rows[:limit]]
    next_cursor = f"{items[-1].created_at.isoformat()}|{items[-1].id}" if more else None
    return Page(items, next_cursor)


async def get_version(conn: psycopg.AsyncConnection, name: str, id: uuid.UUID) -> Version | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COLUMNS} FROM backup_versions WHERE name = %s AND id = %s", (name, id)
        )
        row = await cur.fetchone()
    return _version(row) if row else None


async def delete_version(conn: psycopg.AsyncConnection, name: str, id: uuid.UUID) -> bool:
    """Delete one version row; the current one is refused unless it is the only version.

    The only version going away deletes the name too. The caller deletes the object (read it
    with `get_version` first). False when refused or missing.
    """
    locked = await _lock(conn, name, create=False)
    if locked is None:
        return False
    cur = await conn.execute(
        "SELECT count(*), count(*) FILTER (WHERE id = %s) FROM backup_versions WHERE name = %s",
        (id, name),
    )
    total, found = await cur.fetchone()
    if not found:
        return False
    if locked["current_version"] == id:
        if total > 1:
            return False
        await conn.execute("DELETE FROM backups WHERE name = %s", (name,))
        return True
    await conn.execute("DELETE FROM backup_versions WHERE id = %s", (id,))
    await conn.execute("UPDATE backups SET updated_at = now() WHERE name = %s", (name,))
    return True


async def delete_name(conn: psycopg.AsyncConnection, name: str) -> list[str]:
    """Delete a name and all its versions; returns the object keys the caller deletes."""
    if await _lock(conn, name, create=False) is None:
        return []
    cur = await conn.execute("SELECT object_key FROM backup_versions WHERE name = %s", (name,))
    keys = [r[0] for r in await cur.fetchall()]
    await conn.execute("DELETE FROM backups WHERE name = %s", (name,))
    return keys


async def referenced_versions(conn: psycopg.AsyncConnection) -> set[uuid.UUID]:
    """Versions that stored session statuses name as restore source or base.

    Reads `sessions.status.backup.entries`, a list of entry records that carry `version` (restore
    source) and `base_version` (the version a later save is based on).
    """
    cur = await conn.execute(
        "SELECT DISTINCT e.value ->> k FROM sessions s,"
        " jsonb_array_elements(CASE WHEN jsonb_typeof(s.status #> '{backup,entries}') = 'array'"
        "   THEN s.status #> '{backup,entries}' ELSE '[]'::jsonb END) AS e(value),"
        " unnest(ARRAY['version', 'base_version']) AS k"
        " WHERE jsonb_typeof(e.value) = 'object' AND e.value ->> k IS NOT NULL"
    )
    out: set[uuid.UUID] = set()
    for (text,) in await cur.fetchall():
        try:
            out.add(uuid.UUID(text))
        except ValueError:
            continue
    return out
