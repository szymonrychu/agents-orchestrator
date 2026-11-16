"""Backup cleanup: a pure planner over name and version rows, the references read from the
database, and an executor that deletes the rows first and the objects after.

`plan_sweep` decides from the version rows, the references, the object listing and the clock
alone, so the same function serves the periodic sweep, the dry-run preview and the manual
delete's re-check. Per backup name:

- The current version is never deleted, nor any version a stored session status references
  (`status.backup[*].version` / `base_version`), nor a version younger than the grace period (a
  save may still be recording it).
- A name not restored or saved for the retention period, with no referenced version, no session
  spec rendering to it and nothing inside the grace period, is deleted whole (every version).
- Otherwise the newest K non-current versions stay and older non-current ones go once past the
  retention period.
- An object under `<prefix>b/` that no version row owns is an orphan (a save the reconciler
  never recorded, or an object left after a failed delete): deleted by age (retention and
  grace). Any other key outside `b/` and `sessions/` (the legacy flat archives the migration
  imports) is never planned for deletion, dry run or not.
  Transcripts (`<prefix>sessions/<slug>.tar.zst`) are one object per session, deleted by
  age alone while no session references them.

The executor deletes the database row inside a transaction (re-checking references under the
`backups` row lock), then the object; an object whose delete fails is an orphan the next sweep
removes.
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import psycopg
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import rendering
from agents_orchestrator.cache.keys import ArchiveNameError, validate_name
from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.cache.presign import (
    SUFFIX,
    ArchiveObject,
    DeleteFailure,
    S3Credentials,
    delete_keys,
    list_archives,
)
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import has_workspace_entry

log = logging.getLogger(__name__)

SESSIONS_GROUP = "sessions/"
VERSIONS_GROUP = "b/"
RESULT_KEYS_MAX = 200


@dataclass(frozen=True)
class Decision:
    key: str
    size: int
    delete: bool
    reason: str
    name: str | None = None
    version_id: uuid.UUID | None = None
    whole_name: bool = False


@dataclass(frozen=True)
class SweepPlan:
    """One decision per object, sorted by key."""

    decisions: tuple[Decision, ...]

    @property
    def deletes(self) -> tuple[Decision, ...]:
        return tuple(d for d in self.decisions if d.delete)

    @property
    def keys(self) -> list[str]:
        return [d.key for d in self.deletes]

    @property
    def bytes(self) -> int:
        return sum(d.size for d in self.deletes)

    def reason(self, key: str) -> str | None:
        return next((d.reason for d in self.decisions if d.key == key), None)


@dataclass
class References:
    """What stored sessions point at: version ids and names (with the slugs) and transcripts."""

    versions: dict[uuid.UUID, list[str]] = field(default_factory=dict)
    names: dict[str, list[str]] = field(default_factory=dict)
    transcripts: set[str] = field(default_factory=set)
    planned_keys: set[str] = field(default_factory=set)  # in-flight saves: no version row yet

    def slugs_of(self, name: str, ids: Iterable[uuid.UUID]) -> list[str]:
        found = set(self.names.get(name, ()))
        for vid in ids:
            found.update(self.versions.get(vid, ()))
        return sorted(found)


def plan_sweep(
    *,
    versions: Iterable[backups_repo.Version],
    activity: dict[str, datetime],
    objects: Iterable[ArchiveObject],
    refs: References,
    now: datetime,
    retention_days: int,
    keep_newest: int,
    grace_seconds: int,
    prefix: str,
) -> SweepPlan:
    """Which versions and objects to delete, each with the reason it is kept or deleted."""
    old = now - timedelta(days=retention_days)
    fresh = now - timedelta(seconds=grace_seconds)
    by_name: dict[str, list[backups_repo.Version]] = defaultdict(list)
    for version in versions:
        by_name[version.name].append(version)
    decisions: list[Decision] = []
    for name, group in by_name.items():
        group.sort(key=lambda v: (v.created_at, str(v.id)), reverse=True)
        decisions += _plan_name(name, group, activity.get(name), refs, old, fresh, keep_newest)
    owned = {v.object_key for vs in by_name.values() for v in vs}
    for obj in objects:
        if not obj.key.endswith(SUFFIX) or obj.key in owned:  # not ours to delete, or planned
            continue
        if obj.key in refs.planned_keys:
            decisions.append(Decision(obj.key, obj.size, False, "protected: a backup run plans it"))
            continue
        if obj.key.startswith(f"{prefix}{SESSIONS_GROUP}"):
            if obj.key in refs.transcripts:
                decisions.append(Decision(obj.key, obj.size, False, "protected: a session uses it"))
                continue
            what = "transcripts older than the retention period"
        elif obj.key.startswith(f"{prefix}{VERSIONS_GROUP}"):
            what = "object no backup version owns, older than the retention period"
        else:  # a legacy flat archive: the migration still imports it, never ours to delete
            continue
        decisions.append(_aged(obj, old, fresh, what))
    return SweepPlan(tuple(sorted(decisions, key=lambda d: d.key)))


def _aged(obj: ArchiveObject, old: datetime, fresh: datetime, what: str) -> Decision:
    if obj.last_modified > fresh:
        return Decision(obj.key, obj.size, False, "kept: younger than the grace period")
    if obj.last_modified > old:
        return Decision(obj.key, obj.size, False, "kept: younger than the retention period")
    return Decision(obj.key, obj.size, True, what)


def _plan_name(
    name: str,
    group: list[backups_repo.Version],
    activity: datetime | None,
    refs: References,
    old: datetime,
    fresh: datetime,
    keep_newest: int,
) -> list[Decision]:
    def verdict(v: backups_repo.Version, delete: bool, reason: str, *, whole: bool = False):
        return Decision(v.object_key, v.size, delete, reason, name, v.id, whole)

    last = activity or max(v.created_at for v in group)
    pinned = name in refs.names or any(v.id in refs.versions for v in group)
    if last <= old and not pinned and all(v.created_at <= fresh for v in group):
        why = "name not restored or saved for longer than the retention period"
        return [verdict(v, True, why, whole=True) for v in group]
    out: list[Decision] = []
    rank = 0
    for v in group:
        if v.id in refs.versions:
            out.append(verdict(v, False, "protected: a session references it"))
        elif v.state == "current":
            out.append(verdict(v, False, "kept: the current version"))
        else:
            rank += 1
            if rank <= keep_newest:
                out.append(verdict(v, False, "kept: among the newest non-current versions"))
            elif v.created_at > fresh:
                out.append(verdict(v, False, "kept: younger than the grace period"))
            elif v.created_at > old:
                out.append(verdict(v, False, "kept: younger than the retention period"))
            else:
                out.append(
                    verdict(v, True, "beyond the newest non-current versions and past retention")
                )
    return out


# ------------------------------------------------------------------ the references


def _spec_names(session: Session) -> set[str]:
    """Names a session's spec entries render to before the reconciler resolves them (an exact
    render only; a template needing save-time values renders no name). Blocking (the sandbox is
    a child process): run in a thread."""
    ctx = rendering.session_context("archive_name", session)
    found: set[str] = set()
    for entry in session.spec.backup or []:
        try:
            found.add(
                validate_name(rendering.render_name(entry.name, ctx, dry=True), allow_prefix=False)
            )
        except ArchiveNameError:
            continue
    return found


def _run_plan_keys(status: dict[str, Any]) -> set[str]:
    """Object keys in `status.backup.runs[*].plan[].key`: saves in flight that have no version
    row yet."""
    backup = status.get("backup")
    runs = backup.get("runs") if isinstance(backup, dict) else None
    if not isinstance(runs, dict):
        return set()
    return {
        item["key"]
        for run in runs.values()
        if isinstance(run, dict)
        for item in run.get("plan") or []
        if isinstance(item, dict) and isinstance(item.get("key"), str)
    }


async def references(conn: psycopg.AsyncConnection, settings: Settings) -> References:
    """Everything a stored session references. A session row exists from create until its
    discard finished (its save included), so "present in the table" is non-terminal."""
    found = await sessions_repo.list_sessions(conn)
    prefix = settings.cache_s3_prefix
    per_session = await asyncio.to_thread(lambda: [_spec_names(s) for s in found])
    refs = References()
    versions: dict[uuid.UUID, set[str]] = defaultdict(set)
    for vid, _name, slug in await backups_repo.referencing(conn):
        versions[vid].add(slug)
    refs.versions = {vid: sorted(slugs) for vid, slugs in versions.items()}
    names: dict[str, list[str]] = defaultdict(list)
    for session, rendered in zip(found, per_session, strict=True):
        refs.planned_keys |= _run_plan_keys(session.status)
        if has_workspace_entry(session.spec):
            refs.transcripts.add(f"{prefix}{SESSIONS_GROUP}{session.slug}{SUFFIX}")
        for name in rendered:
            names[name].append(session.slug)
    refs.names = {n: sorted(s) for n, s in names.items()}
    return refs


# ------------------------------------------------------------------ the executor


@dataclass
class SweepResult:
    dry_run: bool
    started_at: datetime
    finished_at: datetime | None = None
    listed: int = 0
    planned: int = 0
    planned_bytes: int = 0
    deleted: int = 0
    deleted_bytes: int = 0
    skipped_protected: int = 0
    failures: list[DeleteFailure] = field(default_factory=list)
    deleted_keys: list[str] = field(default_factory=list)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "dry_run": self.dry_run,
            "started_at": self.started_at.isoformat(),
            "finished_at": self.finished_at.isoformat() if self.finished_at else None,
            "listed": self.listed,
            "planned": self.planned,
            "planned_bytes": self.planned_bytes,
            "deleted": self.deleted,
            "deleted_bytes": self.deleted_bytes,
            "skipped_protected": self.skipped_protected,
            "failed": len(self.failures),
            "failures": [
                {"key": f.key, "code": f.code, "message": f.message}
                for f in self.failures[:RESULT_KEYS_MAX]
            ],
            "deleted_keys": self.deleted_keys[:RESULT_KEYS_MAX],
            "error": self.error,
        }


async def build_plan(
    settings: Settings,
    pool: AsyncConnectionPool,
    objects: list[ArchiveObject],
    policy: SweepPolicy,
) -> tuple[SweepPlan, References, datetime]:
    """Read the rows and references and plan with `policy`; also returns the database clock."""
    async with pool.connection() as conn:
        now = (await (await conn.execute("SELECT now()")).fetchone())[0]
        versions = await backups_repo.all_versions(conn)
        activity = await backups_repo.name_activity(conn)
        refs = await references(conn, settings)
    plan = plan_sweep(
        versions=versions,
        activity=activity,
        objects=objects,
        refs=refs,
        now=now,
        retention_days=policy.retention_days,
        keep_newest=policy.keep_newest,
        grace_seconds=policy.grace_seconds,
        prefix=settings.cache_s3_prefix,
    )
    return plan, refs, now


async def _drop_rows(
    pool: AsyncConnectionPool, plan: SweepPlan, late: References, result: SweepResult
) -> list[Decision]:
    """Delete the planned rows (each name in its own transaction, references re-checked) and
    return the decisions whose object may now be deleted."""
    whole: dict[str, list[Decision]] = defaultdict(list)
    singles: list[Decision] = []
    orphans: list[Decision] = []
    for d in plan.deletes:
        if d.whole_name and d.name:
            whole[d.name].append(d)
        elif d.version_id is not None:
            singles.append(d)
        else:
            orphans.append(d)
    gone: list[Decision] = []
    async with pool.connection() as conn:
        for name, ds in whole.items():
            async with conn.transaction():
                rows = await backups_repo.name_versions(conn, name)
                planned = {d.version_id for d in ds}
                if (
                    {v.id for v in rows} != planned
                    or name in late.names
                    or planned & late.versions.keys()
                ):
                    result.skipped_protected += len(ds)
                    continue
                await backups_repo.delete_name(conn, name)
            gone += ds
        for d in singles:
            assert d.name is not None and d.version_id is not None
            async with conn.transaction():
                row = await backups_repo.get_version(conn, d.name, d.version_id)
                if (
                    row is None
                    or row.state == "current"
                    or row.id in late.versions
                    or not await backups_repo.delete_version(conn, d.name, d.version_id)
                ):
                    result.skipped_protected += 1
                    continue
            gone.append(d)
        owned = await backups_repo.known_keys(conn, [d.key for d in orphans])
    for d in orphans:
        if d.key in owned or d.key in late.transcripts:
            result.skipped_protected += 1
        else:
            gone.append(d)
    return gone


async def delete_planned(
    settings: Settings,
    pool: AsyncConnectionPool,
    plan: SweepPlan,
    late: References,
    result: SweepResult,
    *,
    credentials: S3Credentials | None = None,
) -> SweepResult:
    """Delete the planned rows, then their objects (batches of 1000), recording failures and
    continuing past them."""
    gone = await _drop_rows(pool, plan, late, result)
    keys = [d.key for d in gone]
    failures = await asyncio.to_thread(delete_keys, settings, keys, credentials=credentials)
    failed = {f.key for f in failures}
    sizes = {d.key: d.size for d in gone}
    result.failures += failures
    result.deleted_keys = [k for k in keys if k not in failed]
    result.deleted = len(result.deleted_keys)
    result.deleted_bytes = sum(sizes[k] for k in result.deleted_keys)
    for f in failures:
        log.warning("backup sweep could not delete %s: %s %s", f.key, f.code, f.message)
    return result


async def run_sweep(
    settings: Settings,
    pool: AsyncConnectionPool,
    *,
    policy: SweepPolicy,
    credentials: S3Credentials | None = None,
) -> SweepResult:
    """List, plan with `policy`, re-read the references right before deleting, delete (unless
    dry-run).

    Never raises for S3 or DB trouble on the way: the failure is recorded in `error` and the
    next sweep retries.
    """
    result = SweepResult(dry_run=policy.dry_run, started_at=await _db_now(pool))
    try:
        objects = await asyncio.to_thread(
            list_archives, settings, settings.cache_s3_prefix, credentials=credentials
        )
        result.listed = len(objects)
        plan, _refs, _now = await build_plan(settings, pool, objects, policy)
        result.planned, result.planned_bytes = len(plan.deletes), plan.bytes
        if not policy.dry_run and plan.deletes:
            async with pool.connection() as conn:
                late = await references(conn, settings)
            await delete_planned(settings, pool, plan, late, result, credentials=credentials)
    except Exception as exc:
        log.exception("backup sweep failed")
        result.error = f"{type(exc).__name__}: {exc}"[:500]
    result.finished_at = await _db_now(pool)
    log.info(
        "backup sweep %s: listed %d, planned %d (%d bytes), deleted %d, failed %d, error %s",
        "dry-run" if policy.dry_run else "done",
        result.listed,
        result.planned,
        result.planned_bytes,
        result.deleted,
        len(result.failures),
        result.error,
    )
    return result


async def _db_now(pool: AsyncConnectionPool) -> datetime:
    async with pool.connection() as conn:
        return (await (await conn.execute("SELECT now()")).fetchone())[0]
