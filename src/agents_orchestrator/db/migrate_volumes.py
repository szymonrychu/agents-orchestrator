"""One-shot migration of preset backup entries to volumes (S-77 section 13, owner T-617).

Every preset whose stored spec still lists `backup` entries gets, per distinct mount path, one
volume (name derived from the preset and entry names, made unique; projects = the preset's home
project; default size; the deploy storage class) and one `rw` attachment with `backup_restore`
true at the entry's old path (workspace root: `/workdir/<workdir>/<path>`, claude root:
`/claude/<path>`). Entries of one preset that share a mount path collapse into one volume (a
`restore` entry and a `backup` entry of one tree); a templated (Jinja) name collapses too. The
seed is the NEWEST current version among the collapsed entries' backups (a templated name: every
backup whose name it can render and whose current version this preset saved), copied server-side
into `volume-<name>` (origin `copy`). `exclude` lists are unioned onto the volume; `live` and
`after_init` are dropped (logged per preset as counts).

Seeded volumes keep `content_version` NULL ON PURPOSE: their first `backup_restore` claim
restores the seed (a fresh volume, no claims, so the HARD RULE guard allows it). `volumes.create`
pins `content_version` to an existing `volume-<name>` current version; the migration never picks
a name whose `volume-<name>` backup exists (a pre-existing reserved name, logged), so that pin is
always empty here and the seed is recorded after the create.

It works on the raw stored JSON (no BackupEntry model), one transaction and one audited edit per
preset. The api runs it once at startup as a background task (`startup`) under a
try-advisory-lock, with a per-copy timeout and an overall deadline; a preset with no
entries is skipped, so a second run changes nothing. A preset that cannot be migrated (a seed
object missing, no S3, an invalid result, a concurrent edit) is left untouched and logged, and a
later startup retries it. Logs carry preset and volume names and counts only.
"""

from __future__ import annotations

import asyncio
import logging
import posixpath
import re
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from pydantic import ValidationError

from agents_orchestrator import rendering
from agents_orchestrator.cache import presign as s3
from agents_orchestrator.cache.keys import object_key_for
from agents_orchestrator.db import backups, volumes
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import (
    VOLUME_PATH_MAX,
    WORKDIR_ROOT,
    SessionSpec,
    _volume_problems,
    legacy_context,
)

log = logging.getLogger(__name__)

ACTOR = "migration"
CLAUDE_ROOT = "/claude"  # k8s.render.CLAUDE_ROOT: where the claude root is mounted
LOCK_KEY = 0x6F726368566F6C4D  # "orchVolM": one volume migration at a time
NAME_MAX = 40
_TAG = re.compile(r"\{\{.*?\}\}|\{%.*?%\}|\{#.*?#\}", re.S)
_NON_NAME = re.compile(r"[^a-z0-9]+")


class Skip(Exception):
    """This preset is left untouched (the message names no path or content)."""


@dataclass
class Planned:
    name: str
    mount_path: str
    entry_names: list[str]
    exclude: list[str]
    seed: backups.Version | None = None
    seed_key: str | None = None  # the copied object, set before the write


@dataclass
class PresetPlan:
    id: uuid.UUID
    name: str
    project: str
    spec: dict[str, Any]  # the stored spec the plan was made from
    entries: int
    volumes: list[Planned]
    dropped: dict[str, int]  # live / after_init entry counts


@dataclass
class Result:
    presets: int = 0
    entries: int = 0
    volumes: list[str] = field(default_factory=list)
    seeds: int = 0
    skipped: list[str] = field(default_factory=list)  # preset names left untouched
    reserved: list[str] = field(default_factory=list)  # pre-existing volume- backup names
    stripped: list[str] = field(default_factory=list)  # presets whose empty/null backup key went


# ---------------------------------------------------------------------- pure parts


def name_base(preset_name: str, entry_name: str) -> str:
    """`<preset>-<entry>` in the volume grammar: lowercase, runs of other characters squeezed to
    `-`, Jinja tags dropped, at most 40 characters; `volume` when nothing is left."""
    parts = [
        _NON_NAME.sub("-", s.lower()).strip("-") for s in (preset_name, _TAG.sub("", entry_name))
    ]
    base = "-".join(p for p in parts if p)[:NAME_MAX].strip("-")
    return base or "volume"


def candidate(base: str, n: int) -> str:
    if n == 1:
        return base
    suffix = f"-{n}"
    return base[: NAME_MAX - len(suffix)].rstrip("-") + suffix


def is_template(name: str) -> bool:
    return any(m in name for m in rendering.MARKERS)


def template_pattern(template: str) -> re.Pattern[str] | None:
    """Names a template can render: literal parts kept, every tag matches any text. None when
    the template has no literal part (a wildcard-only pattern reaches every backup)."""
    parts = _TAG.split(template)
    if not any(parts):
        return None
    return re.compile(".*".join(re.escape(p) for p in parts))


def mount_path(spec: dict[str, Any], entry: dict[str, Any]) -> str:
    """The entry's old path as an absolute mount path; an empty path is the root itself."""
    if entry.get("root") == "claude":
        base = CLAUDE_ROOT
    elif entry.get("root") == "workspace":
        base = f"{WORKDIR_ROOT}/{spec.get('workdir') or 'workdir'}"
    else:
        raise Skip("entry with an unknown root")
    path = str(entry.get("path") or ".")
    if path.startswith("/") or ".." in path.split("/"):
        raise Skip("entry path is not relative")
    out = posixpath.normpath(posixpath.join(base, path))
    if len(out) > VOLUME_PATH_MAX:
        raise Skip("mount path too long")
    return out


# ---------------------------------------------------------------------- planning


async def _reserved_names(conn: psycopg.AsyncConnection) -> list[str]:
    """`volume-` backup names that no volume (live or deleted) owns: written before the prefix
    was reserved."""
    cur = await conn.execute(
        "SELECT b.name FROM backups b WHERE starts_with(b.name, %s) AND NOT EXISTS ("
        " SELECT 1 FROM volumes v WHERE %s || v.name = b.name) ORDER BY b.name",
        (backups.RESERVED_PREFIX, backups.RESERVED_PREFIX),
    )
    return [r[0] for r in await cur.fetchall()]


async def _taken(conn: psycopg.AsyncConnection, name: str) -> bool:
    """A live volume has the name, or `volume-<name>` already holds backups (it would seed or
    pin the new volume with data that is not this entry's)."""
    if await volumes.by_name(conn, name) is not None:
        return True
    cur = await conn.execute(
        "SELECT 1 FROM backups WHERE name = %s", (backups.RESERVED_PREFIX + name,)
    )
    return await cur.fetchone() is not None


async def _unique(conn: psycopg.AsyncConnection, base: str, taken: set[str]) -> str:
    n = 1
    while True:
        name = candidate(base, n)
        if name not in taken and not await _taken(conn, name):
            taken.add(name)
            return name
        n += 1


async def _seed(  # noqa: C901
    conn: psycopg.AsyncConnection,
    preset_id: uuid.UUID,
    names: list[str],
    others: list[str],
) -> backups.Version | None:
    """The newest current version among the backups this entry's names reach. `others` are the
    names of the preset's other mount groups: a template never takes their backups, nor any
    `volume-` backup, and a template with no literal part takes nothing (logged by count)."""
    found: list[backups.Version] = []
    templates: list[re.Pattern[str]] = []
    for name in names:
        if not is_template(name):
            if (v := await backups.current(conn, name)) is not None:
                found.append(v)
        elif (pat := template_pattern(name)) is None:
            log.warning("volume migration: a wildcard-only template name seeds nothing")
        else:
            templates.append(pat)
    foreign = [p for p in (template_pattern(n) for n in others if is_template(n)) if p]
    plain = {n for n in others if not is_template(n)}
    if templates:
        cur = await conn.execute(
            "SELECT b.name FROM backups b JOIN backup_versions v ON v.id = b.current_version"
            " WHERE v.preset_id = %s",
            (preset_id,),
        )
        for (name,) in await cur.fetchall():
            if name.startswith(backups.RESERVED_PREFIX) or name in plain:
                continue
            if any(p.fullmatch(name) for p in foreign):
                continue
            if any(p.fullmatch(name) for p in templates):
                v = await backups.current(conn, name)
                if v is not None:
                    found.append(v)
    return max(found, key=lambda v: (v.created_at, str(v.id))) if found else None


async def _plan(conn: psycopg.AsyncConnection, row: dict[str, Any], taken: set[str]) -> PresetPlan:
    spec = row["spec"]
    entries = spec["backup"]
    if not all(isinstance(e, dict) and isinstance(e.get("name"), str) for e in entries):
        raise Skip("malformed backup entry")
    groups: dict[str, list[dict[str, Any]]] = {}
    for entry in entries:
        groups.setdefault(mount_path(spec, entry), []).append(entry)
    planned = []
    for path, group in groups.items():
        names = list(dict.fromkeys(e["name"] for e in group))
        exclude = sorted({str(x) for e in group for x in e.get("exclude") or []})
        others = [e["name"] for p, g in groups.items() if p != path for e in g]
        name = await _unique(conn, name_base(row["name"], names[0]), taken)
        seed = await _seed(conn, row["id"], names, others)
        planned.append(Planned(name, path, names, exclude, seed))
    dropped = {k: sum(1 for e in entries if e.get(k)) for k in ("live", "after_init")}
    return PresetPlan(row["id"], row["name"], row["project"], spec, len(entries), planned, dropped)


def _new_spec(plan: PresetPlan, ids: dict[str, uuid.UUID]) -> dict[str, Any]:
    spec = {k: v for k, v in plan.spec.items() if k != "backup"}
    spec["volumes"] = list(spec.get("volumes") or []) + [
        {
            "volume": str(ids[p.name]),
            "mount_path": p.mount_path,
            "mode": "rw",
            "backup_restore": True,
        }
        for p in plan.volumes
    ]
    return spec


def _check(spec: dict[str, Any], settings: Settings) -> None:
    """The migrated spec must load and pass the attachment shape rules; field names only."""
    try:
        parsed = SessionSpec.model_validate(spec, context=legacy_context(None))
    except ValidationError as exc:
        locs = sorted({".".join(str(p) for p in e["loc"][:2]) for e in exc.errors()})
        raise Skip(f"migrated spec invalid at {', '.join(locs)}") from None
    if _volume_problems(parsed, settings.volume_max_per_preset):
        raise Skip("migrated volumes invalid (count or mount paths)")


# ---------------------------------------------------------------------- writing


def _creds(settings: Settings) -> s3.S3Credentials:
    return s3.S3Credentials(*settings.api_s3_credentials())


async def _copy_seeds(plan: PresetPlan, settings: Settings) -> None:
    seeds = [p for p in plan.volumes if p.seed is not None]
    if not seeds:
        return
    if not settings.api_s3_configured:
        raise Skip("seeds need S3 and the api has no S3 credentials")
    stamp = rendering.format_now(datetime.now(UTC))
    for p in seeds:
        assert p.seed is not None
        key = object_key_for(
            settings.cache_s3_prefix, backups.RESERVED_PREFIX + p.name, stamp, ACTOR
        )
        try:
            await asyncio.wait_for(
                asyncio.to_thread(
                    s3.copy_object, settings, p.seed.object_key, key, credentials=_creds(settings)
                ),
                settings.volume_migration_copy_timeout_seconds,
            )
        except Exception as exc:  # noqa: BLE001 - botocore errors: the preset waits for a retry
            await _drop_copies(plan, settings)
            raise Skip(f"seed copy failed ({type(exc).__name__})") from None
        p.seed_key = key


async def _drop_copies(plan: PresetPlan, settings: Settings) -> None:
    keys = [p.seed_key for p in plan.volumes if p.seed_key]
    if keys:
        try:
            await asyncio.to_thread(s3.delete_keys, settings, keys, credentials=_creds(settings))
        except Exception:  # noqa: BLE001 - a leftover object is an orphan the sweep removes
            log.warning("volume migration: could not delete %d copied seed object(s)", len(keys))


async def _write(conn: psycopg.AsyncConnection, plan: PresetPlan, settings: Settings) -> None:
    """One transaction: create the volumes, record the seeds, swap entries for attachments."""
    async with conn.transaction():
        locked = await presets_repo.get_for_update(conn, plan.id)
        if locked is None or locked.spec != plan.spec:
            raise Skip("preset changed during the migration")
        ids: dict[str, uuid.UUID] = {}
        for p in plan.volumes:
            vol = await volumes.create(
                conn,
                name=p.name,
                projects=[plan.project],
                size=settings.volume_default_size,
                exclude=p.exclude,
                storage_class=settings.volume_storage_class,
                created_by=ACTOR,
                actor_kind="system",
                actor=ACTOR,
                max_size=settings.volume_max_size,
                detail={"migrated_from_preset": str(plan.id)},
            )
            if vol.content_version is not None:  # a volume- backup appeared meanwhile
                raise Skip("volume name gained backups during the migration")
            ids[p.name] = vol.id
            if p.seed is not None and p.seed_key is not None:
                await backups.record_save(
                    conn,
                    name=backups.RESERVED_PREFIX + p.name,
                    object_key=p.seed_key,
                    size=p.seed.size,
                    digest=p.seed.digest,
                    manifest=p.seed.manifest,
                    base_version=None,
                    origin="copy",
                    session_slug=None,
                    preset_id=plan.id,
                    project=plan.project,
                    created_by=ACTOR,
                    orphan=False,
                    volume_writer=True,
                )
        spec = _new_spec(plan, ids)
        _check(spec, settings)
        await presets_repo.update(conn, plan.id, locked.name, spec)
        for p in plan.volumes:
            await volumes.record_event(
                conn,
                ids[p.name],
                "attached",
                actor_kind="system",
                actor=ACTOR,
                detail={
                    "preset_id": str(plan.id),
                    "mode": "rw",
                    "mount_path": p.mount_path,
                    "backup_restore": True,
                },
            )


def _log_plan(plan: PresetPlan, dry_run: bool) -> None:
    log.info(
        "%svolume migration: preset=%s entries=%d volumes=%s seeds=%d",
        "dry run: " if dry_run else "audit: ",
        plan.name,
        plan.entries,
        ",".join(p.name for p in plan.volumes),
        sum(1 for p in plan.volumes if p.seed is not None),
    )
    if any(plan.dropped.values()):
        log.info(
            "volume migration: preset=%s dropped live=%d after_init=%d",
            plan.name,
            plan.dropped["live"],
            plan.dropped["after_init"],
        )


async def _rows(conn: psycopg.AsyncConnection) -> list[dict[str, Any]]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id, name, project, spec FROM presets"
            " WHERE jsonb_typeof(spec -> 'backup') = 'array'"
            " AND jsonb_array_length(spec -> 'backup') > 0 ORDER BY name, id"
        )
        return await cur.fetchall()


def _empty_backup(spec: dict[str, Any]) -> bool:
    return "backup" in spec and not spec["backup"]


async def _strip_empty(conn: psycopg.AsyncConnection, dry_run: bool) -> list[str]:
    """Drop an empty or null `backup` key from stored presets (one audited edit each, logged by
    name); a preset with entries is left to the migration. Idempotent: a second run finds none."""
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT id FROM presets WHERE spec ? 'backup'"
            " AND (jsonb_typeof(spec -> 'backup') = 'null' OR spec -> 'backup' = '[]'::jsonb)"
            " ORDER BY name, id"
        )
        ids = [r["id"] for r in await cur.fetchall()]
    await conn.commit()
    done: list[str] = []
    for pid in ids:
        try:
            async with conn.transaction():
                locked = await presets_repo.get_for_update(conn, pid)
                if locked is None or not _empty_backup(locked.spec):
                    continue
                if not dry_run:
                    spec = {k: v for k, v in locked.spec.items() if k != "backup"}
                    await presets_repo.update(conn, pid, locked.name, spec)
                done.append(locked.name)
                log.info(
                    "%svolume migration: preset=%s empty backup key stripped",
                    "dry run: " if dry_run else "audit: ",
                    locked.name,
                )
        except Exception as exc:  # noqa: BLE001 - one preset must not stop the others
            await conn.rollback()
            log.warning(
                "volume migration: preset=%s empty backup key kept: failed (%s)",
                pid,
                type(exc).__name__,
            )
    await conn.commit()
    return done


async def migrate(
    pool: AsyncConnectionPool, settings: Settings, *, dry_run: bool = False
) -> Result | None:
    """Migrate every preset with backup entries; None when another process holds the lock (it
    is migrating, and its result stands). The dry run plans and logs, and writes nothing."""
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT pg_try_advisory_lock(%s)", (LOCK_KEY,))
        got = (await cur.fetchone())[0]
        await conn.commit()  # the session-level lock outlives the implicit transaction
        if not got:
            log.info("volume migration: another process is running it; skipped")
            return None
        try:
            return await _migrate(conn, settings, dry_run)
        finally:
            # an aborted transaction would make the unlock fail and leave the lock on a pooled
            # connection
            await conn.rollback()
            await conn.execute("SELECT pg_advisory_unlock(%s)", (LOCK_KEY,))
            await conn.commit()


async def _migrate(conn: psycopg.AsyncConnection, settings: Settings, dry_run: bool) -> Result:
    res = Result(reserved=await _reserved_names(conn))
    if res.reserved:
        log.warning(
            "volume migration: backup names with the reserved volume- prefix and no volume: %s;"
            " no volume takes these names, entries naming them seed a fresh volume",
            ",".join(res.reserved),
        )
    res.stripped = await _strip_empty(conn, dry_run)
    taken: set[str] = set()
    rows = await _rows(conn)
    await conn.commit()  # pool connections are not autocommit: each preset commits on its own
    for row in rows:
        plan: PresetPlan | None = None
        try:
            try:
                plan = await _plan(conn, row, taken)
            finally:
                await conn.commit()  # end the planning reads before S3 and the write
            if dry_run:
                _check(_new_spec(plan, {p.name: uuid.uuid4() for p in plan.volumes}), settings)
            else:
                await _copy_seeds(plan, settings)
                try:
                    await _write(conn, plan, settings)
                except Skip:
                    await _drop_copies(plan, settings)
                    raise
                except (psycopg.Error, volumes.VolumeExists, ValueError) as exc:
                    await _drop_copies(plan, settings)
                    raise Skip(f"write failed ({type(exc).__name__})") from None
        except Skip as exc:
            res.skipped.append(row["name"])
            log.warning("volume migration: preset=%s left untouched: %s", row["name"], exc)
            continue
        except Exception as exc:  # noqa: BLE001 - one preset must not stop the others
            await conn.rollback()
            if plan is not None:
                await _drop_copies(plan, settings)
            res.skipped.append(row["name"])
            log.warning(
                "volume migration: preset=%s left untouched: failed (%s)",
                row["name"],
                type(exc).__name__,
            )
            continue
        _log_plan(plan, dry_run)
        res.presets += 1
        res.entries += plan.entries
        res.volumes += [p.name for p in plan.volumes]
        res.seeds += sum(1 for p in plan.volumes if p.seed is not None)
    log.info(
        "%svolume migration: presets=%d entries=%d volumes=%d seeds=%d skipped=%d",
        "dry run: " if dry_run else "",
        res.presets,
        res.entries,
        len(res.volumes),
        res.seeds,
        len(res.skipped),
    )
    return res


async def startup(pool: AsyncConnectionPool, settings: Settings) -> None:
    """The api's one-shot startup run: never raises (a failure is logged and retried at the
    next start)."""
    try:
        await asyncio.wait_for(
            migrate(pool, settings, dry_run=settings.volume_migration_dry_run),
            settings.volume_migration_deadline_seconds,
        )
    except TimeoutError:
        log.error("volume migration passed its deadline; the rest is retried at the next start")
    except Exception as exc:  # noqa: BLE001 - startup must not crash on the migration
        log.error("volume migration failed (%s); retried at the next start", type(exc).__name__)
