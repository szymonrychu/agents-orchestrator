"""projects table repository. The api is the only caller and the only writer.

A project groups presets and sessions. The slug is immutable; name and description are
editable. Projects are archived, never deleted, and `default` can never be archived.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row

DEFAULT = "default"
SLUG_MAX = 40
NAME_MAX = 80
DESCRIPTION_MAX = 4000
_SLUG = re.compile(r"[a-z0-9][a-z0-9-]{0,39}")

# Per-project counts in one grouped query: presets, and sessions not being deleted.
_COUNTS = (
    "LEFT JOIN (SELECT project, count(*) AS n FROM presets GROUP BY project) pc"
    " ON pc.project = p.slug"
    " LEFT JOIN (SELECT project, count(*) AS n FROM sessions WHERE desired = 'active'"
    " GROUP BY project) sc ON sc.project = p.slug"
)


class ProjectExists(Exception):
    """Another project already has this slug."""


class ProjectNotFound(Exception):
    pass


class DefaultProject(Exception):
    """The default project cannot be archived."""


class UnknownProject(Exception):
    """A new object names a project that does not exist."""


class ProjectArchived(Exception):
    """A new object names an archived project."""


@dataclass(frozen=True)
class Project:
    slug: str
    name: str
    description: str
    archived_at: datetime | None
    created_at: datetime
    updated_at: datetime
    # filled only by list_projects(counts=True): presets and active sessions in the project
    presets: int | None = None
    sessions: int | None = None


def validate_project_slug(slug: str) -> str:
    if not _SLUG.fullmatch(slug):
        raise ValueError(
            f"project slug must be 1 to {SLUG_MAX} lowercase letters, digits and dashes,"
            " starting with a letter or digit"
        )
    return slug


def _validate_name(name: str) -> None:
    if not 1 <= len(name) <= NAME_MAX:
        raise ValueError(f"project name must be 1 to {NAME_MAX} characters")


def _validate_description(description: str) -> None:
    if len(description) > DESCRIPTION_MAX:
        raise ValueError(f"project description must be at most {DESCRIPTION_MAX} characters")


def _from_row(row: dict[str, Any]) -> Project:
    return Project(
        slug=row["slug"],
        name=row["name"],
        description=row["description"],
        archived_at=row["archived_at"],
        created_at=row["created_at"],
        updated_at=row["updated_at"],
        presets=row.get("preset_count"),
        sessions=row.get("session_count"),
    )


async def _fetch(conn: psycopg.AsyncConnection, query: str, params: tuple) -> list[Project]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(query, params)
        return [_from_row(r) for r in await cur.fetchall()]


async def list_projects(
    conn: psycopg.AsyncConnection, *, archived: bool | None = False, counts: bool = False
) -> list[Project]:
    """Projects by slug: active ones by default, archived ones with True, all with None.

    With `counts`, each carries its preset and active session counts (one grouped query)."""
    where = {
        None: "",
        False: " WHERE p.archived_at IS NULL",
        True: " WHERE p.archived_at IS NOT NULL",
    }[archived]
    if counts:
        query = (
            "SELECT p.*, coalesce(pc.n, 0) AS preset_count, coalesce(sc.n, 0) AS session_count"
            f" FROM projects p {_COUNTS}{where} ORDER BY p.slug"
        )
    else:
        query = f"SELECT p.* FROM projects p{where} ORDER BY p.slug"
    return await _fetch(conn, query, ())


async def get_project(conn: psycopg.AsyncConnection, slug: str) -> Project | None:
    rows = await _fetch(conn, "SELECT * FROM projects WHERE slug = %s", (slug,))
    return rows[0] if rows else None


async def create_project(
    conn: psycopg.AsyncConnection, slug: str, name: str, description: str = ""
) -> Project:
    validate_project_slug(slug)
    _validate_name(name)
    _validate_description(description)
    try:
        async with conn.transaction():
            rows = await _fetch(
                conn,
                "INSERT INTO projects (slug, name, description) VALUES (%s, %s, %s) RETURNING *",
                (slug, name, description),
            )
    except psycopg.errors.UniqueViolation:
        raise ProjectExists(slug) from None
    return rows[0]


async def update_project(
    conn: psycopg.AsyncConnection,
    slug: str,
    *,
    name: str | None = None,
    description: str | None = None,
    archived: bool | None = None,
) -> Project:
    """Change name, description or the archived state; None keeps the stored value. Archiving an
    archived project keeps its first `archived_at`."""
    if name is not None:
        _validate_name(name)
    if description is not None:
        _validate_description(description)
    if archived and slug == DEFAULT:
        raise DefaultProject(slug)
    rows = await _fetch(
        conn,
        "UPDATE projects SET name = coalesce(%(name)s, name),"
        " description = coalesce(%(description)s, description),"
        " archived_at = CASE WHEN %(archived)s::boolean IS NULL THEN archived_at"
        "  WHEN %(archived)s THEN coalesce(archived_at, now()) END,"
        " updated_at = now() WHERE slug = %(slug)s RETURNING *",
        {"name": name, "description": description, "archived": archived, "slug": slug},
    )
    if not rows:
        raise ProjectNotFound(slug)
    return rows[0]


_LOCKS = {None: "", "share": " FOR SHARE", "update": " FOR UPDATE"}


async def require_active(
    conn: psycopg.AsyncConnection, slug: str, *, lock: Literal["share", "update"] | None = None
) -> Project:
    """The project a new object goes into; raises UnknownProject or ProjectArchived. `lock`
    holds the row until the caller's transaction ends, so it cannot be archived meanwhile."""
    if lock not in _LOCKS:
        raise ValueError(f"unknown lock {lock!r}, expected one of {list(_LOCKS)}")
    rows = await _fetch(conn, f"SELECT * FROM projects WHERE slug = %s{_LOCKS[lock]}", (slug,))
    if not rows:
        raise UnknownProject(slug)
    if rows[0].archived_at is not None:
        raise ProjectArchived(slug)
    return rows[0]


async def project_counts(conn: psycopg.AsyncConnection, slug: str) -> dict[str, int]:
    """Presets and active (not deleting) sessions in the project."""
    cur = await conn.execute(
        "SELECT (SELECT count(*) FROM presets WHERE project = %(s)s),"
        " (SELECT count(*) FROM sessions WHERE project = %(s)s AND desired = 'active')",
        {"s": slug},
    )
    n_presets, n_sessions = await cur.fetchone()
    return {"presets": n_presets, "sessions": n_sessions}
