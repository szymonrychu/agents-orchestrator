"""Move legacy flat cache archives into versioned backups (S-68, T-523; D9.1).

Legacy objects are `<prefix><name>.tar.zst` outside `b/` and `sessions/` (transcripts are never
touched). Each is server-side copied to `<prefix>b/<name>/<now>-migration-<4 hex>.tar.zst` and
recorded as a version with origin `migration` and `manifest.migrated_from` (the old key, which
makes the run idempotent). A group `x/<ts>` becomes the versions of the name `x`, the newest
current; the old `preset-<hash>` key becomes `<slug(preset name)>/workspace`. When the name
already has a current version from a save, the migrated versions are recorded as orphans so
they never move the pointer. The new key is derived from the old one, so a crash between copy
and row insert repeats the same copy.
"""

from __future__ import annotations

import hashlib
import re
import uuid
from dataclasses import dataclass, field

import psycopg
from psycopg.rows import dict_row

from agents_orchestrator import rendering
from agents_orchestrator.cache.keys import ArchiveNameError, validate_name
from agents_orchestrator.cache.presign import (
    SUFFIX,
    ArchiveObject,
    copy_object,
    delete_keys,
    list_archives,
)
from agents_orchestrator.db import backups
from agents_orchestrator.db.migrate_backup_spec import HASH_KEY, legacy_cache_key
from agents_orchestrator.settings import Settings

TIMESTAMP = re.compile(r"\d{8}t\d{6}z")
ACTOR = "migration"
RESERVED = ("b/", "sessions/")
# one object migration at a time: a second run waits, then skips what the first recorded
LOCK_KEY = 0x6F72636842616B4D  # "orchBakM"


@dataclass
class Result:
    copied: int = 0
    skipped: int = 0  # already migrated
    orphaned: int = 0  # copied, but the name already had a current version from a save
    invalid: list[str] = field(default_factory=list)
    deleted: int = 0
    failures: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Legacy:
    obj: ArchiveObject
    name: str
    stamp: str
    preset_id: uuid.UUID | None


def is_legacy(key: str, prefix: str) -> bool:
    if not key.startswith(prefix) or not key.endswith(SUFFIX):
        return False
    return not key[len(prefix) :].startswith(RESERVED)


def _plan(
    objs: list[ArchiveObject], prefix: str, presets: dict[str, tuple[uuid.UUID, str]], res: Result
) -> list[Legacy]:
    out = []
    for obj in objs:
        if not is_legacy(obj.key, prefix):
            continue
        rel = obj.key[len(prefix) : -len(SUFFIX)]
        preset_id = None
        head, _, tail = rel.rpartition("/")
        if HASH_KEY.fullmatch(rel) and rel in presets:
            preset_id, pname = presets[rel]
            name, stamp = (
                f"{rendering.slug(pname)}/workspace",
                rendering.format_now(obj.last_modified),
            )
        elif head and TIMESTAMP.fullmatch(tail):
            name, stamp = head, tail
        else:
            name, stamp = rel, rendering.format_now(obj.last_modified)
        try:
            validate_name(name, allow_prefix=False)
            if backups.is_reserved(name):  # volume- names are written only by volumes
                raise ArchiveNameError(name)
        except ArchiveNameError:
            res.invalid.append(obj.key)
            continue
        out.append(Legacy(obj, name, stamp, preset_id))
    return sorted(out, key=lambda x: (x.name, x.stamp, x.obj.key))


def new_key(prefix: str, item: Legacy) -> str:
    tail = hashlib.sha256(item.obj.key.encode()).hexdigest()[:4]
    return f"{prefix}b/{item.name}/{item.stamp}-{ACTOR}-{tail}{SUFFIX}"


async def migrated_keys(conn: psycopg.AsyncConnection) -> set[str]:
    cur = await conn.execute(
        "SELECT manifest ->> 'migrated_from' FROM backup_versions"
        " WHERE origin = 'migration' AND manifest ->> 'migrated_from' IS NOT NULL"
    )
    return {r[0] for r in await cur.fetchall()}


async def _presets(conn: psycopg.AsyncConnection) -> dict[str, tuple[uuid.UUID, str]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT id, name FROM presets")
        return {legacy_cache_key(r["id"]): (r["id"], r["name"]) for r in await cur.fetchall()}


async def migrate_objects(conn: psycopg.AsyncConnection, settings: Settings) -> Result:
    """Copy every legacy archive not yet migrated; one transaction per backup name."""
    res = Result()
    prefix = settings.cache_s3_prefix
    await conn.execute("SELECT pg_advisory_lock(%s)", (LOCK_KEY,))
    try:
        return await _migrate(conn, settings, prefix, res)
    finally:
        await conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))


async def _migrate(
    conn: psycopg.AsyncConnection, settings: Settings, prefix: str, res: Result
) -> Result:
    done = await migrated_keys(conn)
    plan = _plan(list_archives(settings, prefix), prefix, await _presets(conn), res)
    names: dict[str, list[Legacy]] = {}
    for item in plan:
        if item.obj.key in done:
            res.skipped += 1
        else:
            names.setdefault(item.name, []).append(item)
    for name, items in names.items():
        async with conn.transaction():
            current = await backups.current(conn, name)
            orphan = current is not None and current.origin != "migration"
            for item in items:
                key = new_key(prefix, item)
                copy_object(settings, item.obj.key, key)
                await backups.record_save(
                    conn,
                    name=name,
                    object_key=key,
                    size=item.obj.size,
                    digest="legacy:" + hashlib.sha256(item.obj.key.encode()).hexdigest()[:32],
                    manifest={"migrated_from": item.obj.key},
                    base_version=None,
                    origin="migration",
                    session_slug=None,
                    preset_id=item.preset_id,
                    project=None,
                    created_by=ACTOR,
                    orphan=orphan,
                )
                res.copied += 1
                res.orphaned += orphan
    return res


async def delete_old(conn: psycopg.AsyncConnection, settings: Settings) -> Result:
    """Delete the legacy objects that have a migration row; transcripts are never listed."""
    res = Result()
    prefix = settings.cache_s3_prefix
    done = await migrated_keys(conn)
    keys = [
        o.key for o in list_archives(settings, prefix) if is_legacy(o.key, prefix) and o.key in done
    ]
    failures = delete_keys(settings, keys)
    res.deleted = len(keys) - len(failures)
    res.failures = [f"{f.key}: {f.code}" for f in failures]
    return res
