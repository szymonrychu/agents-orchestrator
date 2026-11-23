"""The projects of a chat room, for the scoped chat admin reads (S-83 decisions 869, 871-873).

The chat connector has no project concept. A room belongs to the projects of the preset it
routes to: the preset of the session bound to it when that session is known and has one, else
the room's configured (effective) preset. Those projects are the preset's `project` plus its
`extra_projects`; a room with no preset belongs to no project, so only a superleader reaches it.

`RoomMap` caches what the upstream says about each room (the bound session and the fallback
preset), built from one upstream rooms list and kept for `TTL_SECONDS` for reads; a relayed room
settings change or allow (`invalidate`) drops it, and the next lookup rebuilds it. A write looks
the room up fresh, so a binding changed on another replica or in the connector itself never lets
the old project write. A room the map does not hold triggers a rebuild at most once per
`MISS_REFRESH_SECONDS`, so reads of unknown room ids cannot make every call an upstream fetch.
The projects are resolved from the database on every lookup with one query, so a preset moved to
another project (patch, copy, delete, a project grant) changes the answer at once, without an
invalidation hook on every path that writes a preset.
"""

from __future__ import annotations

import time
import uuid
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

TTL_SECONDS = 60.0
MISS_REFRESH_SECONDS = 5.0

# room_id -> (bound session id, fallback preset id)
Entry = tuple[uuid.UUID | None, uuid.UUID | None]


def _uuid(value: Any) -> uuid.UUID | None:
    try:
        return None if value is None else uuid.UUID(str(value))
    except ValueError:
        return None


def entry_of(room: dict[str, Any]) -> Entry:
    """What a room object of the connector says about its preset."""
    binding = room.get("binding")
    bound = _uuid(binding.get("session_id")) if isinstance(binding, dict) else None
    return bound, _uuid(room.get("effective_preset_id") or room.get("preset_id"))


def entries_of(rooms: Any) -> dict[str, Entry]:
    """The entries of an upstream rooms list (anything else: none)."""
    return {
        str(room["room_id"]): entry_of(room)
        for room in (rooms if isinstance(rooms, list) else [])
        if isinstance(room, dict) and room.get("room_id") is not None
    }


_PROJECTS = """
SELECT r.room_id, p.project, p.extra_projects
FROM unnest(%s::text[], %s::uuid[], %s::uuid[]) AS r(room_id, session_id, fallback)
LEFT JOIN sessions s ON s.id = r.session_id
JOIN presets p ON p.id = COALESCE(s.preset_id, r.fallback)
"""


async def projects_of(conn: Any, entries: dict[str, Entry]) -> dict[str, frozenset[str]]:
    """Each room's projects; a room with no preset is left out (no project)."""
    if not entries:
        return {}
    ids = list(entries)
    async with conn.cursor() as cur:
        await cur.execute(
            _PROJECTS,
            (ids, [entries[i][0] for i in ids], [entries[i][1] for i in ids]),
        )
        rows = await cur.fetchall()
    return {room: frozenset([project, *extra]) for room, project, extra in rows}


def overlaps(projects: frozenset[str] | None, scope: Iterable[str]) -> bool:
    return projects is not None and not projects.isdisjoint(scope)


class RoomMap:
    """The cached room entries (see the module docstring)."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._entries: dict[str, Entry] = {}
        self._at: float | None = None

    def invalidate(self) -> None:
        self._at = None

    def _younger_than(self, seconds: float) -> bool:
        return self._at is not None and self._clock() - self._at < seconds

    def replace(self, entries: dict[str, Entry]) -> None:
        """Take a full rooms list as the new map."""
        self._entries, self._at = dict(entries), self._clock()

    async def entry(
        self, room_id: str, fetch_rooms: Callable[[], Awaitable[Any]], *, fresh: bool = False
    ) -> Entry | None:
        """The room's entry. The map is rebuilt from one upstream rooms list when `fresh` (a
        write), when it is stale, or when it lacks the room and was built at least
        `MISS_REFRESH_SECONDS` ago. None when the map does not hold the room."""
        missing = room_id not in self._entries
        if (
            fresh
            or not self._younger_than(TTL_SECONDS)
            or (missing and not self._younger_than(MISS_REFRESH_SECONDS))
        ):
            self.replace(entries_of(await fetch_rooms()))
        return self._entries.get(room_id)
