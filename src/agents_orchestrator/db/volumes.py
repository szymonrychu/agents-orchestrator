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
from typing import Any, Literal

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db.backups import Page
from agents_orchestrator.db.pool import notify
from agents_orchestrator.labels import normalize_labels
from agents_orchestrator.spec import check_exclude, parse_quantity

CHANGED_CHANNEL = "volumes_changed"  # NOTIFY {"id"}: a volume or its claims changed
NAME_RE = re.compile(r"^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$")
DEFAULT_MAX_SIZE = "200Gi"
_QUANTITY_MAX_LEN = 32
_MAX_EXPONENT = 18
_EXPONENT_RE = re.compile(r"[eE]([+-]?\d+)$")
MAX_LIMIT = 200
STATES = ("provisioning", "ready", "resizing", "restoring", "saving", "deleting", "failed")
_COLUMNS = (
    "id, name, projects, labels, description, exclude, size, storage_class, pvc_name, state,"
    " state_reason, content_version, save_pending, created_by, created_at, updated_at, deleted_at"
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


class VolumeBusy(Exception):
    """The volume is in a state that does not allow this change."""


class VolumeMounted(Exception):
    """A session holds an unreleased claim on the volume (HARD RULE: no restore)."""


Decision = Literal["proceed", "restore", "wait", "refuse"]


@dataclass
class Volume:
    id: uuid.UUID
    name: str
    projects: list[str]
    labels: list[str]
    description: str
    exclude: list[str]
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
    save_pending: bool = False


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
    if not isinstance(value, str) or len(value) > _QUANTITY_MAX_LEN:
        raise ValueError(f"{value!r} is not a valid size")
    m = _EXPONENT_RE.search(value)
    if m and abs(int(m.group(1))) > _MAX_EXPONENT:
        raise ValueError(f"{value!r} is not a valid size")
    return int(parse_quantity(value))


def _clean_projects(projects: list[str]) -> list[str]:
    """Trimmed, deduplicated (first-seen order) project labels; at least one, none empty."""
    cleaned = [*dict.fromkeys(p.strip() for p in projects)]
    if not cleaned or "" in cleaned:
        raise ValueError("a volume needs at least one non-empty project")
    return cleaned


def _positive_bytes(size: str) -> int:
    n = quantity_bytes(size)
    if n <= 0:
        raise ValueError(f"{size!r} must be a positive quantity")
    return n


def _check_size(size: str, max_size: str) -> int:
    n = _positive_bytes(size)
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
    exclude: list[str] | None = None,
    storage_class: str,
    created_by: str,
    actor_kind: str,
    actor: str,
    max_size: str = DEFAULT_MAX_SIZE,
) -> Volume:
    """Insert a `provisioning` volume (PVC name `vol-<name>`); VolumeExists on a live name."""
    if not NAME_RE.fullmatch(name):
        raise ValueError(f"{name!r} is not a valid volume name")
    projects = _clean_projects(projects)
    labels = normalize_labels(labels or [])
    _check_size(size, max_size)
    check_exclude(exclude or [])
    # serialise creators of one name: the partial unique index alone would abort the transaction
    await conn.execute("SELECT pg_advisory_xact_lock(hashtext(%s))", (f"volume:{name}",))
    if await by_name(conn, name) is not None:
        raise VolumeExists(name)
    vid = uuid.uuid4()
    rows = await _rows(
        conn,
        f"INSERT INTO volumes (id, name, projects, labels, description, exclude, size,"
        f" storage_class, pvc_name, state, created_by)"
        f" VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,'provisioning',%s)"
        f" RETURNING {_COLUMNS}",
        (
            vid,
            name,
            projects,
            labels,
            description,
            exclude or [],
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
    exclude: list[str] | None = None,
    actor_kind: str,
    actor: str,
    max_size: str = DEFAULT_MAX_SIZE,
) -> Volume:
    """Change a volume. Size is grow-only (ShrinkRefused) up to `max_size` (OverCap); a size
    equal to the current one is a no-op, a larger one sets `resizing` and only from `ready`
    (VolumeBusy in any other state)."""
    cur = await _lock(conn, id)
    sets: dict[str, Any] = {}
    events: list[tuple[str, dict[str, Any]]] = []
    if size is not None:
        new = _positive_bytes(size)
        old = quantity_bytes(cur.size)
        if new < old:
            raise ShrinkRefused(f"{size} is below the current {cur.size}")
        if new > old:
            if new > quantity_bytes(max_size):
                raise OverCap(f"{size} is above the maximum {max_size}")
            if cur.state != "ready":
                raise VolumeBusy(f"cannot resize a volume that is {cur.state}")
            sets.update(size=size, state="resizing", state_reason=None)
            events.append(("resized", {"from": cur.size, "to": size}))
    if description is not None and description != cur.description:
        sets["description"] = description
    if labels is not None:
        labels = normalize_labels(labels)
        if labels != cur.labels:
            sets["labels"] = labels
            events.append(("labels_updated", {"labels": labels}))
    if exclude is not None and exclude != cur.exclude:
        sets["exclude"] = check_exclude(exclude)
        events.append(("exclude_updated", {"exclude": exclude}))
    if projects is not None:
        projects = _clean_projects(projects)
        if projects != cur.projects:
            sets["projects"] = projects
            events.append(("projects_updated", {"from": cur.projects, "to": projects}))
    if sets:
        cols = ", ".join(f"{k} = %s" for k in sets)
        await conn.execute(
            f"UPDATE volumes SET {cols} WHERE id = %s AND state = %s",
            (*sets.values(), id, cur.state),
        )
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
    expect: str | None = None,
) -> bool:
    """Set the state and its failure reason (cleared otherwise); `content_version` is set only
    when given (a save or restore). A `deleting` volume leaves that state only through
    `finish_delete` (VolumeBusy otherwise). With `expect` the write is status-guarded: when the
    row is no longer in that state nothing changes and False is returned."""
    if state not in STATES:
        raise ValueError(f"unknown volume state {state!r}")
    cur = await _lock(conn, id)
    if expect is not None and cur.state != expect:
        return False
    if cur.state == "deleting" and state != "deleting":
        raise VolumeBusy("a deleting volume cannot change state")
    await conn.execute(
        "UPDATE volumes SET state = %s, state_reason = %s,"
        " content_version = COALESCE(%s, content_version),"
        " save_pending = save_pending AND %s <> 'deleting' WHERE id = %s AND state = %s",
        (state, reason, content_version, state, id, cur.state),
    )
    await _touch(conn, id)
    return True


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


async def refuse_delete(conn: psycopg.AsyncConnection, id: uuid.UUID, *, reason: str) -> None:
    """A `deleting` volume found mounted by the reconciler goes back to `ready` (the one way out
    of `deleting` besides `finish_delete`); event `delete_refused`."""
    cur = await _lock(conn, id)
    if cur.state != "deleting":
        raise VolumeBusy("only a deleting volume can refuse a delete")
    await conn.execute(
        "UPDATE volumes SET state = 'ready', state_reason = NULL WHERE id = %s", (id,)
    )
    await record_event(
        conn,
        id,
        "delete_refused",
        actor_kind="system",
        actor="reconciler",
        detail={"reason": reason},
    )
    await _touch(conn, id)


@dataclass
class ClaimDecision:
    """What `claim` decided: `proceed` (claimed, mount), `restore` (claimed, volume now
    `restoring`: run the pod check, then the restore Job), `wait` (an unsettled state, nothing
    claimed, retry next reconcile) or `refuse` (deleting, nothing claimed). `volume` is the row
    after the decision; `claim` is None for wait and refuse."""

    decision: Decision
    volume: Volume
    claim: Claim | None


async def _upsert_claim(
    conn: psycopg.AsyncConnection,
    volume_id: uuid.UUID,
    session_id: uuid.UUID,
    preset_id: uuid.UUID | None,
    mode: str,
    backup_restore: bool,
) -> tuple[Claim, bool]:
    """Idempotent while the claim is active (no change, no event, False); a new claim or one
    re-opened after its release writes `mounted` (True). The caller holds the volume row lock."""
    active = await _rows(
        conn,
        f"SELECT {_CLAIM_COLUMNS} FROM volume_mounts WHERE volume_id = %s AND session_id = %s"
        " AND released_at IS NULL",
        (volume_id, session_id),
        Claim,
    )
    if active:
        return active[0], False
    rows = await _rows(
        conn,
        f"INSERT INTO volume_mounts (volume_id, session_id, preset_id, mode, backup_restore)"
        f" VALUES (%s,%s,%s,%s,%s) ON CONFLICT (volume_id, session_id) DO UPDATE"
        f" SET released_at = NULL, claimed_at = now(), preset_id = EXCLUDED.preset_id,"
        f" mode = EXCLUDED.mode, backup_restore = EXCLUDED.backup_restore"
        f" RETURNING {_CLAIM_COLUMNS}",
        (volume_id, session_id, preset_id, mode, backup_restore),
        Claim,
    )
    await record_event(
        conn,
        volume_id,
        "mounted",
        actor_kind="system",
        actor="reconciler",
        detail={"session_id": str(session_id), "mode": mode, "backup_restore": backup_restore},
    )
    return rows[0], True


def _str(v: uuid.UUID | None) -> str | None:
    return str(v) if v is not None else None


async def claim(
    conn: psycopg.AsyncConnection,
    *,
    volume_id: uuid.UUID,
    session_id: uuid.UUID,
    preset_id: uuid.UUID | None,
    mode: str,
    backup_restore: bool,
    session_slug: str,
    current_version: uuid.UUID | None,
) -> ClaimDecision:
    """The HARD RULE guard (spec 8.3): claim the volume for a session and decide whether it is
    restored first. Under the volume row lock: `deleting` refuses, any state but `ready` and
    `failed` (provisioning, resizing, restoring, saving) waits (no claim); else the claim is
    inserted, and a wanted restore (`backup_restore`, a new or re-opened claim, a
    `current_version` that differs from `content_version`, volume `ready`) starts only when no
    other session holds an unreleased claim (sessions in deleting count until released);
    otherwise `restore_skipped_mounted` is recorded and the session proceeds. `current_version`
    is `backups.current(volume-<name>)`, read by the caller.

    Locks one volume row: a caller claiming more than one volume must use `claim_all` (ascending
    id order), or it can deadlock with `release_claims`."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state == "deleting":
            return ClaimDecision("refuse", vol, None)
        if vol.state not in ("ready", "failed"):
            return ClaimDecision("wait", vol, None)
        mine, created = await _upsert_claim(
            conn, volume_id, session_id, preset_id, mode, backup_restore
        )
        decision: Decision = "proceed"
        if (
            backup_restore
            and created  # a re-claim is a running claimant: never restore under its pods
            and vol.state == "ready"
            and current_version is not None
            and current_version != vol.content_version
        ):
            others = [
                str(c.session_id)
                for c in await unreleased_claims(conn, volume_id)
                if c.session_id != session_id
            ]
            if others:  # once per claim, not on every reconcile's re-claim
                await record_event(
                    conn,
                    volume_id,
                    "restore_skipped_mounted",
                    actor_kind="system",
                    actor="reconciler",
                    detail={
                        "session_id": str(session_id),
                        "others": others,
                        "current_version": str(current_version),
                        "content_version": _str(vol.content_version),
                    },
                )
            elif not others:
                await conn.execute(
                    "UPDATE volumes SET state = 'restoring', state_reason = NULL WHERE id = %s",
                    (volume_id,),
                )
                await record_event(
                    conn,
                    volume_id,
                    "restore_started",
                    actor_kind="system",
                    actor="reconciler",
                    detail={
                        "session_id": str(session_id),
                        "session_slug": session_slug,
                        "version": str(current_version),
                        "content_version": _str(vol.content_version),
                    },
                )
                decision = "restore"
        if (
            created or decision == "restore"
        ):  # an idempotent re-claim must not wake NOTIFY listeners every reconcile
            await _touch(conn, volume_id)
        after = await get(conn, volume_id)
        assert after is not None
        return ClaimDecision(decision, after, mine)


@dataclass
class ClaimRequest:
    """One volume of a `claim_all` call (the arguments of `claim`)."""

    volume_id: uuid.UUID
    session_id: uuid.UUID
    preset_id: uuid.UUID | None
    mode: str
    backup_restore: bool
    session_slug: str
    current_version: uuid.UUID | None


async def claim_all(
    conn: psycopg.AsyncConnection, claims: list[ClaimRequest]
) -> list[ClaimDecision]:
    """`claim` for a set of volumes in one transaction, taking the volume row locks in ascending
    volume id order (the order `release_claims` uses), so concurrent claimers and releasers of
    the same volumes cannot deadlock. Callers with more than one volume must use this, not
    repeated `claim` calls. Decisions come back in the order of `claims`."""
    out: dict[int, ClaimDecision] = {}
    async with conn.transaction():
        for i in sorted(range(len(claims)), key=lambda i: claims[i].volume_id):
            c = claims[i]
            out[i] = await claim(
                conn,
                volume_id=c.volume_id,
                session_id=c.session_id,
                preset_id=c.preset_id,
                mode=c.mode,
                backup_restore=c.backup_restore,
                session_slug=c.session_slug,
                current_version=c.current_version,
            )
    return [out[i] for i in range(len(claims))]


async def note_restore_wait(
    conn: psycopg.AsyncConnection, volume_id: uuid.UUID, *, started_id: int, reason: str
) -> bool:
    """The pod check refused a restore Job (reason e.g. `pod_found`): the volume stays
    `restoring` (HARD RULE: no Job while a pod uses it) and the pass retries each tick. Writes
    one `restore_skipped_mounted` event per restore attempt (`started_id` is the id of its
    `restore_started` event); True when written. VolumeBusy when the volume is not restoring."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state != "restoring":
            raise VolumeBusy(f"cannot hold a restore of a volume that is {vol.state}")
        cur = await conn.execute(
            "SELECT 1 FROM volume_events WHERE volume_id = %s AND id > %s"
            " AND kind = 'restore_skipped_mounted'",
            (volume_id, started_id),
        )
        if await cur.fetchone():
            return False
        await record_event(
            conn,
            volume_id,
            "restore_skipped_mounted",
            actor_kind="system",
            actor="reconciler",
            detail={"reason": reason},
        )
        await _touch(conn, volume_id)
        return True


async def begin_explicit_restore(
    conn: psycopg.AsyncConnection,
    volume_id: uuid.UUID,
    *,
    version: uuid.UUID,
    actor: str,
    actor_kind: str = "user",
) -> Volume:
    """A user-triggered restore of `version` (spec 8.3), same lock as `claim`: VolumeBusy unless
    the volume is `ready` or `failed`, VolumeMounted while any claim is unreleased (HARD RULE);
    else `restoring` and `restore_started` (detail carries the version to restore)."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state not in ("ready", "failed"):
            raise VolumeBusy(f"cannot restore a volume that is {vol.state}")
        if await unreleased_claims(conn, volume_id):
            raise VolumeMounted(str(volume_id))
        await conn.execute(
            "UPDATE volumes SET state = 'restoring', state_reason = NULL WHERE id = %s",
            (volume_id,),
        )
        await record_event(
            conn,
            volume_id,
            "restore_started",
            actor_kind=actor_kind,
            actor=actor,
            detail={"version": str(version), "content_version": _str(vol.content_version)},
        )
        await _touch(conn, volume_id)
        after = await get(conn, volume_id)
        assert after is not None
        return after


async def unreleased_claims(conn: psycopg.AsyncConnection, volume_id: uuid.UUID) -> list[Claim]:
    return await _rows(
        conn,
        f"SELECT {_CLAIM_COLUMNS} FROM volume_mounts WHERE volume_id = %s AND released_at IS NULL"
        " ORDER BY id",
        (volume_id,),
        Claim,
    )


async def orphan_claims(conn: psycopg.AsyncConnection) -> list[Claim]:
    """Unreleased claims whose session row no longer exists (a purge without release)."""
    return await _rows(
        conn,
        f"SELECT {_CLAIM_COLUMNS} FROM volume_mounts m WHERE released_at IS NULL"
        " AND NOT EXISTS (SELECT 1 FROM sessions s WHERE s.id = m.session_id) ORDER BY id",
        (),
        Claim,
    )


async def release_claims(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID, *, actor_kind: str, actor: str
) -> list[Claim]:
    """Release every unreleased claim of a session; one `released` event per volume. Takes the
    volume row locks first (id order), like `claim`."""
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT id FROM volumes WHERE id IN (SELECT volume_id FROM volume_mounts"
            " WHERE session_id = %s AND released_at IS NULL) ORDER BY id FOR UPDATE",
            (session_id,),
        )
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


SaveTrigger = Literal["user", "agent", "release"]


async def request_save(
    conn: psycopg.AsyncConnection,
    volume_id: uuid.UUID,
    *,
    trigger: SaveTrigger,
    session_slug: str | None,
    actor: str,
    actor_kind: str = "user",
) -> Volume | None:
    """Start a save under the volume row lock (spec 6.3): `saving` and `save_started`, whose
    detail says whether a claim was unreleased (`live`), who triggered it and the session slug.

    A volume that is not `ready` is busy: VolumeBusy for a user or agent request. The automatic
    `release` trigger sets `save_pending` instead (coalesced; a release during a running save
    too) and returns None, so the last mounter's final writes are saved once the volume is
    `ready` again (`start_pending_save`). `deleting` refuses every trigger (VolumeBusy; None
    for release, nothing pending)."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state != "ready":
            if trigger == "release":
                if vol.state != "deleting" and not vol.save_pending:
                    await conn.execute(
                        "UPDATE volumes SET save_pending = true WHERE id = %s", (volume_id,)
                    )
                    await _touch(conn, volume_id)
                return None
            raise VolumeBusy(f"cannot save a volume that is {vol.state}")
        return await _begin_save(
            conn,
            vol,
            trigger=trigger,
            session_slug=session_slug,
            actor=actor,
            actor_kind=actor_kind,
        )


async def start_pending_save(conn: psycopg.AsyncConnection, volume_id: uuid.UUID) -> Volume | None:
    """Start the save a release left pending: only on a `ready` volume with `save_pending`,
    which is cleared under the row lock as the save starts. None otherwise."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state != "ready" or not vol.save_pending:
            return None
        return await _begin_save(
            conn, vol, trigger="release", session_slug=None, actor="reconciler", actor_kind="system"
        )


async def _begin_save(
    conn: psycopg.AsyncConnection,
    vol: Volume,
    *,
    trigger: SaveTrigger,
    session_slug: str | None,
    actor: str,
    actor_kind: str,
) -> Volume:
    volume_id = vol.id
    live = bool(await unreleased_claims(conn, volume_id))
    await conn.execute(
        "UPDATE volumes SET state = 'saving', state_reason = NULL, save_pending = false"
        " WHERE id = %s",
        (volume_id,),
    )
    await record_event(
        conn,
        volume_id,
        "save_started",
        actor_kind=actor_kind,
        actor=actor,
        detail={"live": live, "trigger": trigger, "session_slug": session_slug},
    )
    await _touch(conn, volume_id)
    after = await get(conn, volume_id)
    assert after is not None
    return after


async def finish_restore(
    conn: psycopg.AsyncConnection,
    volume_id: uuid.UUID,
    *,
    version: uuid.UUID | None,
    reason: str | None = None,
) -> None:
    """End a restore: `ready`; `restored` and the new `content_version` on success, or
    `restore_failed` with the fixed `reason` and `content_version` unchanged (`version` None)."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state != "restoring":
            raise VolumeBusy(f"cannot finish a restore of a volume that is {vol.state}")
        await set_state(conn, volume_id, "ready", content_version=version)
        if version is not None:
            kind, detail = "restored", {"version": str(version)}
        else:
            kind, detail = "restore_failed", {"reason": reason or "failed"}
        await record_event(
            conn, volume_id, kind, actor_kind="system", actor="reconciler", detail=detail
        )


async def finish_save(
    conn: psycopg.AsyncConnection,
    volume_id: uuid.UUID,
    *,
    kind: Literal["saved", "save_unchanged", "save_failed"],
    version: uuid.UUID | None = None,
    reason: str | None = None,
) -> None:
    """End a save: always back to `ready`. `saved` and `save_unchanged` set `content_version` to
    `version` (the new or the current one); `save_failed` leaves it and carries `reason`."""
    async with conn.transaction():
        vol = await _lock(conn, volume_id)
        if vol.state != "saving":
            raise VolumeBusy(f"cannot finish a save of a volume that is {vol.state}")
        await set_state(
            conn, volume_id, "ready", content_version=None if kind == "save_failed" else version
        )
        detail: dict[str, Any] = {"reason": reason or "failed"} if kind == "save_failed" else {}
        if version is not None and kind != "save_failed":
            detail["version"] = str(version)
        await record_event(
            conn, volume_id, kind, actor_kind="system", actor="reconciler", detail=detail
        )


async def claimed_volume_ids(
    conn: psycopg.AsyncConnection, session_id: uuid.UUID
) -> set[uuid.UUID]:
    """The volumes a session holds an unreleased claim on."""
    cur = await conn.execute(
        "SELECT volume_id FROM volume_mounts WHERE session_id = %s AND released_at IS NULL",
        (session_id,),
    )
    return {r[0] for r in await cur.fetchall()}
