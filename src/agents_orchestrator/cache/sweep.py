"""Archive cleanup: a pure planner, the protected set read from the database, an executor.

`plan_sweep` decides from a listing, the protected keys and the clock alone, so the same function
serves the periodic sweep, the dry-run preview and the manual delete's re-check. It never returns a
protected key and never the newest object of a versioned group; it deletes an object only when it
is beyond the newest K of its group AND older than N days AND older than the grace period (an
archive younger than the grace may still be mid-save).

Groups follow `keys.group_of`: everything up to the last `/` of the key below the cache prefix
(`proj/20261031t120000z.tar.zst` groups under `proj/`). Keep-newest-K applies only to these
versioned groups. A flat name (one object per group, e.g. `preset-<sha16>`) is deleted by age alone
when no session references it and it is past the grace period. Transcripts
(`<prefix>sessions/<slug>.tar.zst`) are one object per session: they are deleted by age alone, still
never while protected.

Newest means most recent `last_modified`, ties broken by key; the greatest key of a group (what a
prefix-form restore picks) is additionally never deleted, so the two notions of newest cannot
disagree into deleting the object a restore would pick.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

import psycopg

from agents_orchestrator import rendering
from agents_orchestrator.cache.keys import ArchiveNameError, group_of, object_name_for
from agents_orchestrator.cache.presign import (
    SUFFIX,
    ArchiveObject,
    DeleteFailure,
    S3Credentials,
    delete_keys,
    list_archives,
)
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

SESSIONS_GROUP = "sessions/"
RESULT_KEYS_MAX = 200


@dataclass(frozen=True)
class Decision:
    key: str
    size: int
    delete: bool
    reason: str


@dataclass(frozen=True)
class SweepPlan:
    """One decision per archive in the listing, sorted by key."""

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


def plan_sweep(
    objects: Iterable[ArchiveObject],
    protected: Iterable[str],
    now: datetime,
    *,
    retention_days: int,
    keep_newest: int,
    grace_seconds: int,
    prefix: str,
) -> SweepPlan:
    """Which of `objects` to delete, each with the reason it is kept or deleted."""
    protect = frozenset(protected)
    old = now - timedelta(days=retention_days)
    fresh = now - timedelta(seconds=grace_seconds)
    groups: dict[str, list[ArchiveObject]] = defaultdict(list)
    for obj in objects:
        if obj.key.endswith(SUFFIX):  # anything else in the bucket is not ours to delete
            groups[
                SESSIONS_GROUP if _is_transcript(obj.key, prefix) else group_of(obj.key, prefix)
            ].append(obj)
    decisions: list[Decision] = []
    for group, members in groups.items():
        members.sort(key=lambda o: (o.last_modified, o.key), reverse=True)
        keep_newest_keys: set[str] = set()
        if group.endswith("/") and group != SESSIONS_GROUP:  # versioned groups only
            keep_newest_keys = {o.key for o in members[:keep_newest]}
            keep_newest_keys.add(max(o.key for o in members))
        for obj in members:
            decisions.append(
                _decide(obj, protect, keep_newest_keys, group == SESSIONS_GROUP, old, fresh)
            )
    return SweepPlan(tuple(sorted(decisions, key=lambda d: d.key)))


def _is_transcript(key: str, prefix: str) -> bool:
    return key.startswith(f"{prefix}{SESSIONS_GROUP}")


def _decide(
    obj: ArchiveObject,
    protect: frozenset[str],
    newest: set[str],
    transcript: bool,
    old: datetime,
    fresh: datetime,
) -> Decision:
    def verdict(delete: bool, reason: str) -> Decision:
        return Decision(obj.key, obj.size, delete, reason)

    if obj.key in protect:
        return verdict(False, "protected: a session references it")
    if obj.key in newest:
        return verdict(False, "kept: among the newest of its group")
    if obj.last_modified > fresh:
        return verdict(False, "kept: younger than the grace period")
    if obj.last_modified > old:
        return verdict(False, "kept: younger than the retention period")
    what = (
        "transcripts older than the retention period"
        if transcript
        else ("beyond the newest of its group and older than the retention period")
    )
    return verdict(True, what)


# ------------------------------------------------------------------ the protected set


def _spec_objects(session: Session, prefix: str) -> set[str]:
    """Objects a session will touch that its status does not record yet.

    A plain or exactly-rendering restore name is protected before the reconciler resolves it; a
    save name is rendered at save time, so rendering it with the creation time protects only a
    fixed (unversioned) save target. Prefix-form restores resolve to the newest object of a group,
    which the planner never deletes. Blocking (the sandbox is a child process): run in a thread.
    """
    cache = session.spec.cache
    if cache is None:
        return set()
    ctx = rendering.session_context("archive_name", session)
    found: set[str] = set()
    for template, prefix_form in ((cache.restore_key, True), (cache.save_key, False)):
        if not template:
            continue
        try:
            name = rendering.render_name(template, ctx, allow_prefix=prefix_form)
            if not name.endswith("/"):
                found.add(object_name_for(prefix, name))
        except ArchiveNameError:
            continue
    return found


def _session_keys(session: Session, prefix: str) -> set[str]:
    if session.spec.cache is None:
        return set()
    cache = session.status.get("cache")
    cache = cache if isinstance(cache, dict) else {}
    keys = {f"{prefix}{SESSIONS_GROUP}{session.slug}{SUFFIX}"}
    for field_name in ("restore_resolved", "save_resolved"):
        value = cache.get(field_name)
        if isinstance(value, str) and value:
            keys.add(value)
    return keys | _spec_objects(session, prefix)


async def referenced_keys(
    conn: psycopg.AsyncConnection, settings: Settings
) -> dict[str, list[str]]:
    """Every object a stored session references, with the slugs that reference it.

    A session row exists from create until the discard finished (its save included), so "present
    in the table" is non-terminal: live, deleting with a save in flight, or restoring.
    """
    found = await sessions_repo.list_sessions(conn)
    prefix = settings.cache_s3_prefix
    per_session = await asyncio.to_thread(lambda: [_session_keys(s, prefix) for s in found])
    refs: dict[str, list[str]] = defaultdict(list)
    for session, keys in zip(found, per_session, strict=True):
        for key in keys:
            refs[key].append(session.slug)
    return {k: sorted(v) for k, v in refs.items()}


async def protected_keys(conn: psycopg.AsyncConnection, settings: Settings) -> set[str]:
    return set(await referenced_keys(conn, settings))


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


async def delete_planned(
    settings: Settings,
    plan: SweepPlan,
    result: SweepResult,
    *,
    credentials: S3Credentials | None = None,
) -> SweepResult:
    """Delete the planned keys (batches of 1000), recording failures and continuing past them."""
    keys = plan.keys
    failures = await asyncio.to_thread(delete_keys, settings, keys, credentials=credentials)
    failed = {f.key for f in failures}
    sizes = {d.key: d.size for d in plan.deletes}
    result.failures += failures
    result.deleted_keys = [k for k in keys if k not in failed]
    result.deleted = len(result.deleted_keys)
    result.deleted_bytes = sum(sizes[k] for k in result.deleted_keys)
    for f in failures:
        log.warning("cache sweep could not delete %s: %s %s", f.key, f.code, f.message)
    return result


async def run_sweep(
    settings: Settings,
    protected: Callable[[], Awaitable[set[str]]],
    now: Callable[[], Awaitable[datetime]],
    *,
    dry_run: bool,
    credentials: S3Credentials | None = None,
) -> SweepResult:
    """List, plan, re-check protection right before deleting, delete (unless `dry_run`).

    `protected` is called twice: for the plan, and again after it so a key a session started to
    reference in between is dropped from the deletes. Never raises for S3 or DB trouble on the
    way: the failure is recorded in `error` and the next sweep retries.
    """
    result = SweepResult(dry_run=dry_run, started_at=await now())
    try:
        objects = await asyncio.to_thread(
            list_archives, settings, settings.cache_s3_prefix, credentials=credentials
        )
        result.listed = len(objects)
        plan = plan_sweep(
            objects,
            await protected(),
            await now(),
            retention_days=settings.cache_retention_days,
            keep_newest=settings.cache_keep_newest,
            grace_seconds=settings.cache_sweep_grace_seconds or 0,
            prefix=settings.cache_s3_prefix,
        )
        result.planned, result.planned_bytes = len(plan.deletes), plan.bytes
        if not dry_run and plan.deletes:
            late = await protected()
            safe = SweepPlan(tuple(d for d in plan.decisions if d.delete and d.key not in late))
            result.skipped_protected = len(plan.deletes) - len(safe.decisions)
            await delete_planned(settings, safe, result, credentials=credentials)
    except Exception as exc:
        log.exception("cache sweep failed")
        result.error = f"{type(exc).__name__}: {exc}"[:500]
    result.finished_at = await now()
    log.info(
        "cache sweep %s: listed %d, planned %d (%d bytes), deleted %d, failed %d, error %s",
        "dry-run" if dry_run else "done",
        result.listed,
        result.planned,
        result.planned_bytes,
        result.deleted,
        len(result.failures),
        result.error,
    )
    return result
