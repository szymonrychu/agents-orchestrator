"""Backup entry restore and save Jobs, resolve Jobs and the Claude transcripts Jobs (S-68; D7).

A session's `spec.backup` entries run in phases, each one Job that carries every entry of its
phase (`orch cache restore|save` in the configured cache image, never the session's `spec.image`):

1. transcripts restore (only with a workspace entry; the StatefulSet is held at zero replicas),
2. a resolve Job when a restore name calls `sha256`, then the restore Job of the `after_init:
   false` entries,
3. the init Job (reconciler/init.py),
4. a resolve Job when needed, then the restore Job of the `after_init: true` entries,
5. the pods are bootstrapped.

The discard (decision 765: saves only at discard and through the backup tool) runs one save Job
with every backup-direction entry once the pods are gone, preceded by a resolve Job when a name
calls `sha256`, then the transcripts save. A restore warning never blocks: `missing` (no current
version of the name), `unreadable` (the archive could not be extracted) and a save's
`refused_secret` become a `backup_warning` session event and an entry state; only a Job-level
`failed` degrades the session.

Names render at restore time and at save time. A restore reads `backups.current` per name and
presigns that version's object; a name without a row is `missing` and gets no Job entry. A save
writes a fresh immutable version object (`presign`/`keys.object_key_for`) and every outcome goes
through `db.backups.record_save` (an orphan when the entry's restore ended `failed`; an `unchanged`
digest deletes the fresh object).

Status: `sessions.status.backup` is `{entries, runs, transcripts}`. `entries[i]` is `{index, root,
path, version, base_version, restore: {state, name, version, at}, save: {...}}` (`version` and
`base_version` are what `db.backups.referenced_versions` reads); `runs[<phase>]` and
`transcripts.restore|save` are the crash-safe Job runs with the shape of the init run: the state
(and the plan: names, object keys, versions) is written before the create and `job` only after it,
the outcome is recorded and then the Job deleted. The outcome is the last line of the pod log, a
JSON object (`{"entries": [...]}` or `{"digests": {...}}`); a line that cannot be read is a
failure, never a success.

The S3 credentials stay in the reconciler: the Job gets presigned, expiring URLs in
`CACHE_ENTRIES` and no Secret, `spec.env`, `envFrom` or service account token. The pod is
hardened (uid 1000, non-root, read-only root filesystem with an emptyDir `/tmp` as `HOME`, no
privilege escalation, all capabilities dropped, seccomp RuntimeDefault). A save archives into an
emptyDir `/scratch` sized to `cache_max_bytes`. Only restore Jobs mount the `backup-state` subPath.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Literal

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import rendering
from agents_orchestrator.cache import save_rule
from agents_orchestrator.cache.keys import ArchiveNameError, object_key_for
from agents_orchestrator.cache.presign import (
    delete_keys,
    presign_object,
    sessions_object_name,
)
from agents_orchestrator.cache.save_rule import EntryPlan, save_plan, transcripts_skip
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import events as events_repo
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.db.volumes import Volume
from agents_orchestrator.k8s.kube import KubeApi, KubeError, Obj, is_owned
from agents_orchestrator.k8s.labels import prefix, session_labels, volume_labels, workspace_pvc
from agents_orchestrator.k8s.render import (
    BACKUP_STATE_ROOT,
    BACKUP_STATE_SUBPATH,
    CLAUDE_ROOT,
    CLAUDE_SUBPATH,
    SESSIONS_ROOT,
    WORKSPACE_VOLUME,
    agent_identity,
    workdir_mount,
    workdir_path,
)
from agents_orchestrator.reconciler.checks import (
    DEADLINE_GRACE_SECONDS,
    TAIL_LINES,
    container_exit_code,
    job_condition,
)
from agents_orchestrator.reconciler.init import REASON_MAX, StatusWriter
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import (
    SESSIONS_EXCLUDE,
    BackupEntry,
    SessionSpec,
    backups,
    has_workspace_entry,
    restores,
)

log = logging.getLogger(__name__)

CACHE_LABEL = "agents.szymonrichert.pl/cache"
BACKOFF_LIMIT = 2
TMP_DIR = "/tmp"
BACKUP_PREFIX = backups_repo.RESERVED_PREFIX
VOLUME_ROOT = "/volume"  # where a volume Job pod sees the volume PVC
SCRATCH_DIR = "/scratch"
SESSIONS_SUBPATH = "sessions"  # the PVC subtree holding every agent pod's /sessions
WARNING_EVENT = "backup_warning"
EVENT_SESSION = "backup"  # the events key of the reconciler's own rows (agent_id: the session id)
LIVE_ATTEMPTS = 3  # deliveries of one version tried before it is given up (state `failed`)
LIVE_RETRY_SECONDS = 60  # backoff between two attempts of the same version
# The outcome line carries a per-file manifest (100-200+ KB for a real workspace), far over the
# 16 KiB diagnostic tail of `log_tail`; this is its own cap. A longer line is treated as bad output.
OUTCOME_MAX_BYTES = 8 * 1024 * 1024
WARN_STATES = frozenset({"missing", "unreadable", "refused_secret"})
# a restored claude path that the agents' Claude Code loads at start: they get `reload-skills`
RELOAD_PATHS = frozenset({"skills", "."})

RESOLVE_PRE, RESTORE_PRE = "resolve_pre", "restore_pre"
RESOLVE_POST, RESTORE_POST = "resolve_post", "restore_post"
RESOLVE_SAVE, SAVE = "resolve_save", "save"
# live delivery of a new current version, and the on-demand save of one entry (reconciler/live.py)
LIVE, SAVE_NOW, RESOLVE_NOW = "live", "save_now", "resolve_now"
DIRTY_MESSAGE = (
    "local changes not saved; call orchestrator_backup_save or the next save makes yours current"
)
TX_RESTORE, TX_SAVE = "transcripts_restore", "transcripts_save"
Phase = Literal["pre", "post"]
_JOB_SUFFIX = {
    RESOLVE_PRE: "backup-resolve-pre",
    RESTORE_PRE: "backup-restore-pre",
    RESOLVE_POST: "backup-resolve-post",
    RESTORE_POST: "backup-restore-post",
    RESOLVE_SAVE: "backup-resolve-save",
    SAVE: "backup-save",
    LIVE: "backup-live",
    SAVE_NOW: save_rule.SAVE_NOW_JOB_SUFFIX,
    RESOLVE_NOW: "backup-resolve-now",
    TX_RESTORE: "sessions-restore",
    TX_SAVE: "sessions-save",
}
_TX_FIELD = {TX_RESTORE: "restore", TX_SAVE: "save"}
# a resolve run that failed: names cannot render
_RESOLVE_FAILED: dict[str, str] = {}


def job_name(slug: str, key: str) -> str:
    return f"{prefix(slug)}-{_JOB_SUFFIX[key]}"


def cache_labels(slug: str, key: str) -> dict[str, str]:
    return {**session_labels(slug), CACHE_LABEL: _JOB_SUFFIX[key]}


def _title(key: str) -> str:
    return _JOB_SUFFIX[key].replace("-", " ")


def _is_restore(key: str) -> bool:
    return key in (RESTORE_PRE, RESTORE_POST, TX_RESTORE, LIVE)


def _is_resolve(key: str) -> bool:
    return key in (RESOLVE_PRE, RESOLVE_POST, RESOLVE_SAVE, RESOLVE_NOW)


def root_dir(spec: SessionSpec, root: str) -> str:
    """Where the Job pod sees the root of an entry (`volume` is the fixed `/volume`)."""
    if root == "volume":
        return VOLUME_ROOT
    return workdir_path(spec) if root == "workspace" else CLAUDE_ROOT


def get_run(status: dict[str, Any], key: str) -> dict[str, Any] | None:
    """The run record of `key` in `status.backup`, or None."""
    backup = save_rule.backup_state(status)
    if key in _TX_FIELD:
        return save_rule.transcripts_run(status, _TX_FIELD[key])  # type: ignore[arg-type]
    runs = backup.get("runs")
    run = runs.get(key) if isinstance(runs, dict) else None
    return run if isinstance(run, dict) else None


def _with_run(backup: dict[str, Any], key: str, run: dict[str, Any]) -> dict[str, Any]:
    out = dict(backup)
    if key in _TX_FIELD:
        out["transcripts"] = {**(backup.get("transcripts") or {}), _TX_FIELD[key]: run}
    else:
        out["runs"] = {**(backup.get("runs") or {}), key: run}
    return out


def _entry_record(spec: SessionSpec, index: int) -> dict[str, Any]:
    entry = (spec.backup or [])[index]
    return {
        "index": index,
        "root": entry.root,
        "path": entry.path,
        "version": None,
        "base_version": None,
        "restore": None,
        "save": None,
    }


def phase_pairs(spec: SessionSpec, phase: Phase) -> list[tuple[int, BackupEntry]]:
    """The restore entries of one phase: `after_init` false before the init Job, true after."""
    return [(i, e) for i, e in restores(spec) if e.after_init == (phase == "post")]


@dataclass(frozen=True)
class CacheGate:
    """What a restore run means for this tick's init run, sessions and phase."""

    hold: bool = False  # neither init nor any session is started; phase pending
    degraded: bool = False  # init and the sessions start; phase degraded
    message: str = ""

    @classmethod
    def of_run(cls, run: dict[str, Any] | None, title: str) -> CacheGate:
        state = (run or {}).get("state")
        if run is None or state == "running":
            return cls(hold=True, message=f"{title} running")
        if state == "failed":
            reason = run.get("reason")
            failed = f"{title} failed"
            return cls(degraded=True, message=f"{failed}: {reason}" if reason else failed)
        return cls()

    @classmethod
    def of_restores(cls, spec: SessionSpec, status: dict[str, Any]) -> CacheGate:
        """The gate of the entry restores (both phases) from the stored status."""
        gates = [
            cls.of_run(get_run(status, key), _title(key))
            for phase, key in (("pre", RESTORE_PRE), ("post", RESTORE_POST))
            if phase_pairs(spec, phase)  # type: ignore[arg-type]
        ]
        held = next((g for g in gates if g.hold), None)
        return held or next((g for g in gates if g.degraded), cls())


def outcome_line(log_text: str) -> str | None:
    """The complete last non-empty log line, None when it is over `OUTCOME_MAX_BYTES`."""
    lines = [ln for ln in log_text.splitlines() if ln.strip()]
    if not lines or len(lines[-1].encode()) > OUTCOME_MAX_BYTES:
        return None
    return lines[-1]


def parse_outcome(tail: str | None) -> dict[str, Any] | None:
    """The CLI's outcome object: the last non-empty log line as JSON, else None."""
    lines = [ln for ln in (tail or "").splitlines() if ln.strip()]
    if not lines:
        return None
    try:
        value = json.loads(lines[-1])
    except ValueError:
        return None
    if not isinstance(value, dict):
        return None
    if isinstance(value.get("entries"), list) or isinstance(value.get("digests"), dict):
        return value
    return None


# ---------------------------------------------------------------------------------- rendering


def _hardened(settings: Settings, container: dict[str, Any], volumes: list[Obj]) -> Obj:
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        **agent_identity(settings),
        "securityContext": {
            "runAsUser": 1000,
            "runAsNonRoot": True,
            "fsGroup": 1000,
            "fsGroupChangePolicy": "OnRootMismatch",
            "seccompProfile": {"type": "RuntimeDefault"},
        },
        "containers": [container],
        "volumes": volumes,
    }
    if settings.image_pull_secret:
        pod_spec = {"imagePullSecrets": [{"name": settings.image_pull_secret}], **pod_spec}
    return pod_spec


def _job(
    session: Session, settings: Settings, key: str, pod_spec: Obj, labels: dict[str, str]
) -> Obj:
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name(session.slug, key),
            "namespace": settings.namespace,
            "labels": dict(labels),
        },
        "spec": {
            "backoffLimit": BACKOFF_LIMIT,
            "activeDeadlineSeconds": settings.cache_timeout_seconds,
            "template": {"metadata": {"labels": dict(labels)}, "spec": pod_spec},
        },
    }


def _claude_mount(read_only: bool) -> dict[str, Any]:
    mount: dict[str, Any] = {
        "name": WORKSPACE_VOLUME,
        "mountPath": CLAUDE_ROOT,
        "subPath": CLAUDE_SUBPATH,
    }
    return {**mount, "readOnly": True} if read_only else mount


def _pvc_volume(session: Session) -> Obj:
    return {
        "name": WORKSPACE_VOLUME,
        "persistentVolumeClaim": {"claimName": workspace_pvc(session.slug, session.status)},
    }


def _cache_env(settings: Settings, entries: list[dict[str, Any]]) -> list[dict[str, str]]:
    return [
        {"name": "CACHE_ENTRIES", "value": json.dumps(entries)},
        {"name": "CACHE_MAX_BYTES", "value": str(settings.cache_max_bytes)},
        {"name": "CACHE_SCRATCH", "value": SCRATCH_DIR},
        {"name": "CACHE_VERIFY_TLS", "value": str(settings.cache_s3_verify_tls).lower()},
        {"name": "HOME", "value": TMP_DIR},
    ]


def _cache_container(
    settings: Settings, action: str, env: list[dict[str, str]], mounts: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "name": "cache",
        "image": settings.cache_image,
        "command": ["orch", "cache", action],
        "env": env,
        "securityContext": {
            "readOnlyRootFilesystem": True,
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": mounts,
    }


def volume_job_name(volume_name: str, action: str, n: int) -> str:
    return f"vol-{volume_name}-{action}-{n}"


def render_volume_job(
    volume: Volume,
    settings: Settings,
    *,
    action: Literal["restore", "save"],
    version: uuid.UUID | None,
    n: int,
    url: str,
    annotations: dict[str, str] | None = None,
) -> Obj:
    """The restore or save Job of a volume (S-77): the PVC `vol-<name>` at `/volume`, read-write
    for a restore and read-only for a save, one presigned URL, the cache Job's hardening and
    deadline. No session label (the session prune never matches it). A save carries the volume's
    `exclude` globs as the entry's tar excludes; a restore ignores them."""
    save = action == "save"
    entry: dict[str, Any] = {
        "index": 0,
        "root": "volume",
        "root_dir": VOLUME_ROOT,
        "path": ".",
        "exclude": list(volume.exclude) if save else [],
        "url": url,
        "name": f"{BACKUP_PREFIX}{volume.name}",
    }
    if version is not None and not save:
        entry["version"] = str(version)
    mounts: list[dict[str, Any]] = [
        {"name": "volume", "mountPath": VOLUME_ROOT, **({"readOnly": True} if save else {})},
        {"name": "tmp", "mountPath": TMP_DIR},
    ]
    volumes: list[Obj] = [
        {
            "name": "volume",
            "persistentVolumeClaim": {
                "claimName": volume.pvc_name,
                **({"readOnly": True} if save else {}),
            },
        },
        {"name": "tmp", "emptyDir": {}},
    ]
    if save:
        mounts.append({"name": "scratch", "mountPath": SCRATCH_DIR})
        volumes.append(
            {"name": "scratch", "emptyDir": {"sizeLimit": str(settings.cache_max_bytes)}}
        )
    container = _cache_container(settings, action, _cache_env(settings, [entry]), mounts)
    labels = {**volume_labels(volume.id), CACHE_LABEL: f"volume-{action}"}
    metadata: dict[str, Any] = {
        "name": volume_job_name(volume.name, action, n),
        "namespace": settings.namespace,
        "labels": dict(labels),
    }
    if annotations:
        metadata["annotations"] = dict(annotations)
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": metadata,
        "spec": {
            "backoffLimit": BACKOFF_LIMIT,
            "activeDeadlineSeconds": settings.cache_timeout_seconds,
            "template": {
                "metadata": {"labels": dict(labels)},
                "spec": _hardened(settings, container, volumes),
            },
        },
    }


def render_cache_job(
    session: Session, settings: Settings, key: str, entries: list[dict[str, Any]]
) -> Obj:
    """The restore or save Job of run `key`, given its entries (each with its presigned `url`).

    Entry roots: `workspace` (the session workdir), `claude` (/claude) or, for the transcripts,
    `sessions` (/sessions). Restore Jobs (live deliveries included) mount the backup-state
    subPath, save Jobs do not, except the on-demand save, which records what it saved there so
    the saver's own directory is not dirty for the next live delivery.
    """
    spec = session.spec
    save = not _is_restore(key)
    action = "save" if save else "restore"
    labels = cache_labels(session.slug, key)
    if key in _TX_FIELD:
        sessions_mount: dict[str, Any] = {
            "name": WORKSPACE_VOLUME,
            "mountPath": SESSIONS_ROOT,
            "subPath": SESSIONS_SUBPATH,
        }
        mounts: list[dict[str, Any]] = [{**sessions_mount, **({"readOnly": True} if save else {})}]
    else:
        wd = workdir_mount(spec)
        mounts = [{**wd, "readOnly": True} if save else wd, _claude_mount(save)]
        if not save or key == SAVE_NOW:
            mounts.append(
                {
                    "name": WORKSPACE_VOLUME,
                    "mountPath": BACKUP_STATE_ROOT,
                    "subPath": BACKUP_STATE_SUBPATH,
                }
            )
    mounts.append({"name": "tmp", "mountPath": TMP_DIR})
    volumes: list[Obj] = [_pvc_volume(session), {"name": "tmp", "emptyDir": {}}]
    if save:
        mounts.append({"name": "scratch", "mountPath": SCRATCH_DIR})
        volumes.append(
            {"name": "scratch", "emptyDir": {"sizeLimit": str(settings.cache_max_bytes)}}
        )
    env = _cache_env(settings, entries)
    if (not save or key == SAVE_NOW) and key not in _TX_FIELD:
        env.append({"name": "CACHE_STATE_DIR", "value": BACKUP_STATE_ROOT})
    container = _cache_container(settings, action, env, mounts)
    return _job(session, settings, key, _hardened(settings, container, volumes), labels)


def render_resolve_job(
    session: Session, settings: Settings, key: str, calls: list[tuple[str, int]]
) -> Obj:
    """The resolve Job: digests of the paths a name template asks for, workdir read-only."""
    spec = session.spec
    labels = cache_labels(session.slug, key)
    request = [
        {"root_dir": workdir_path(spec), "path": path, "length": length, "exclude": []}
        for path, length in calls
    ]
    container: dict[str, Any] = {
        "name": "cache",
        "image": settings.cache_image,
        "command": ["orch", "cache", "resolve"],
        "env": [
            {"name": "CACHE_RESOLVE", "value": json.dumps(request)},
            {"name": "HOME", "value": TMP_DIR},
        ],
        "securityContext": {
            "readOnlyRootFilesystem": True,
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"]},
        },
        "volumeMounts": [
            {**workdir_mount(spec), "readOnly": True},
            {"name": "tmp", "mountPath": TMP_DIR},
        ],
    }
    volumes = [_pvc_volume(session), {"name": "tmp", "emptyDir": {}}]
    return _job(session, settings, key, _hardened(settings, container, volumes), labels)


# ---------------------------------------------------------------------------------- name work


def _sha_calls(templates: list[str]) -> list[tuple[str, int]]:
    calls: list[tuple[str, int]] = []
    for template in templates:
        try:
            found = rendering.sha256_calls(template)
        except ArchiveNameError:
            continue
        calls += [c for c in found if c not in calls]
    return calls


def _render_names(
    session: Session,
    templates: list[str],
    digests: dict[str, str] | None,
    now: datetime | None = None,
) -> list[str | ArchiveNameError]:
    """Each template rendered (sandbox: blocking, run in a thread) or the error it raised."""
    ctx = rendering.session_context("archive_name", session, now=now)
    out: list[str | ArchiveNameError] = []
    for template in templates:
        try:
            out.append(rendering.render_name(template, ctx, digests))
        except ArchiveNameError as exc:
            out.append(exc)
        except rendering.NeedsDigests as exc:
            out.append(ArchiveNameError(str(exc)))
    return out


# ---------------------------------------------------------------------------------- the runner


class CacheRunner:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        kube: KubeApi,
        settings: Settings,
        *,
        write_status: StatusWriter,
    ) -> None:
        self.pool, self.kube, self.settings = pool, kube, settings
        self.write_status = write_status

    # ------------------------------------------------------------------ entry restores

    async def restore_step(self, session: Session, phase: Phase) -> tuple[Session, CacheGate]:
        """Advance the restore of one phase by one transition; the gate for init and pods."""
        pairs = phase_pairs(session.spec, phase)
        if not pairs:
            return session, CacheGate()
        key = RESTORE_PRE if phase == "pre" else RESTORE_POST
        run = get_run(session.status, key)
        if run is None:
            session, run = await self._begin_restore(session, key, phase, pairs)
        elif run.get("state") == "running":
            session, run = await self._observe(session, key, run)
        elif run.get("job"):
            await self._delete_job(session, key)  # crashed between the outcome and the delete
        return session, CacheGate.of_run(run, _title(key))

    async def _begin_restore(
        self, session: Session, key: str, phase: Phase, pairs: list[tuple[int, BackupEntry]]
    ) -> tuple[Session, dict[str, Any] | None]:
        now = await self._now()
        if not self.settings.cache_available:
            return await self._fail_entries(session, key, pairs, "not_configured", now)
        templates = [e.name for _, e in pairs]
        calls = await asyncio.to_thread(_sha_calls, templates)
        digests: dict[str, str] | None = None
        if calls:
            rkey = RESOLVE_PRE if phase == "pre" else RESOLVE_POST
            session, found = await self._resolve(session, rkey, calls)
            if found is None:
                return session, None  # the resolve Job is still running: the restore waits
            if found is _RESOLVE_FAILED:
                return await self._fail_entries(session, key, pairs, "resolve_failed", now)
            digests = found
        names = await asyncio.to_thread(_render_names, session, templates, digests)
        entries = self._entries(session)
        plan: list[dict[str, Any]] = []
        warnings: list[dict[str, Any]] = []
        async with self.pool.connection() as conn:
            for (index, _), name in zip(pairs, names, strict=True):
                if isinstance(name, ArchiveNameError):
                    entries[index]["restore"] = _state(
                        "failed", None, None, now, f"bad_name: {name}"
                    )
                    continue
                version = await backups_repo.current(conn, name)
                if version is None:
                    entries[index]["restore"] = _state("missing", name, None, now)
                    warnings.append(_warning(index, name, "missing", "restore"))
                    continue
                await backups_repo.touch_used(conn, name)
                entries[index]["version"] = str(version.id)
                entries[index]["restore"] = _state("running", name, str(version.id), now)
                plan.append(
                    {
                        "index": index,
                        "name": name,
                        "key": version.object_key,
                        "version": str(version.id),
                    }
                )
        run = _new_run(now, plan)
        if not plan:
            run = {**run, "state": "done", "finished_at": now.isoformat()}
            session = await self._write(session, key, run, entries)
            await self._warn(session, warnings)
            return session, run
        session = await self._write(session, key, run, entries)
        await self._warn(session, warnings)
        return await self._start(session, key, run)

    async def _fail_entries(
        self,
        session: Session,
        key: str,
        pairs: list[tuple[int, BackupEntry]],
        reason: str,
        now: datetime,
    ) -> tuple[Session, dict[str, Any]]:
        entries = self._entries(session)
        for index, entry in pairs:
            entries[index]["restore"] = _state("failed", entry.name, None, now, reason)
        run = {**_new_run(now, []), "state": "failed", "reason": reason}
        run["finished_at"] = now.isoformat()
        session = await self._write(session, key, run, entries)
        log.warning("%s of session %s failed: %s", _title(key), session.slug, reason)
        return session, run

    # ------------------------------------------------------------------ resolve

    async def _resolve(
        self,
        session: Session,
        rkey: str,
        calls: list[tuple[str, int]],
        stamp: dict[str, Any] | None = None,
    ) -> tuple[Session, dict[str, str] | None]:
        """The digests of `calls`: None while the resolve Job runs, `_RESOLVE_FAILED` on failure.

        `stamp` is added to a new run (an on-demand save marks its request id).
        """
        run = get_run(session.status, rkey)
        if run is None:
            run = {
                **_new_run(await self._now(), []),
                "calls": [[p, n] for p, n in calls],
                **(stamp or {}),
            }
            session = await self._write(session, rkey, run)
            session, run = await self._start(session, rkey, run)
        elif run.get("state") == "running":
            session, run = await self._observe(session, rkey, run)
        elif run.get("job"):
            await self._delete_job(session, rkey)
        state = run.get("state")
        if state == "running":
            return session, None
        if state == "done" and isinstance(run.get("digests"), dict):
            return session, run["digests"]
        return session, _RESOLVE_FAILED  # type: ignore[return-value]

    # ------------------------------------------------------------------ entry saves

    def save_owed(self, session: Session) -> bool:
        """A backup-direction entry is set and the save run has no outcome yet."""
        run = get_run(session.status, SAVE)
        return bool(backups(session.spec)) and (run is None or run.get("state") == "running")

    async def save_step(self, session: Session) -> tuple[Session, bool]:
        """Advance the one save run by at most one transition; True lets the discard on.

        A failed or timed-out save is logged and recorded, never held.
        """
        if not backups(session.spec):
            return session, True
        run = get_run(session.status, SAVE)
        if run is None:
            session, run = await self._begin_save(session)
        elif run.get("state") == "running":
            session, run = await self._observe(session, SAVE, run)
        elif run.get("job"):
            await self._delete_job(session, SAVE)  # crashed between the outcome and the delete
        return session, run is not None and run.get("state") != "running"

    async def begin_save_now(
        self, session: Session, request_id: str, index: int
    ) -> tuple[Session, dict[str, Any] | None]:
        """Start the on-demand save of entry `index` for request `request_id`.

        The run record carries the request id. A resolve run of an earlier request is dropped
        first, so the digests are fresh. None while a resolve Job runs.
        """
        stale = get_run(session.status, RESOLVE_NOW)
        if stale is not None and stale.get("request_id") != request_id:
            session = await self._drop_run(session, RESOLVE_NOW)
        return await self._begin_save(
            session, SAVE_NOW, only=index, stamp={"request_id": request_id}
        )

    async def _drop_run(self, session: Session, key: str) -> Session:
        backup = dict(save_rule.backup_state(session.status))
        backup["runs"] = {k: v for k, v in (backup.get("runs") or {}).items() if k != key}
        await self.write_status(session, {"backup": backup}, session.observed_generation)
        return replace(session, status={**session.status, "backup": backup})

    async def _begin_save(
        self,
        session: Session,
        key: str = SAVE,
        *,
        only: int | None = None,
        stamp: dict[str, Any] | None = None,
    ) -> tuple[Session, dict[str, Any] | None]:
        now = await self._now()
        plan = save_plan(session, self.settings.cache_available)
        if only is not None:
            plan = [p for p in plan if p.index == only]
        targets = [p for p in plan if p.action == "save"]
        entries = self._entries(session)
        for line in plan:
            if line.action == "skip":
                entries[line.index]["save"] = _state("skipped", None, None, now, line.reason)
        if not targets:
            reason = next((p.reason for p in plan if p.action == "skip"), "nothing_to_save")
            run = {**_new_run(now, []), **(stamp or {}), "state": "skipped", "reason": reason}
            run["finished_at"] = now.isoformat()
            log.info("%s of session %s skipped: %s", _title(key), session.slug, reason)
            return await self._write(session, key, run, entries), run
        templates = [session.spec.backup[p.index].name for p in targets]  # type: ignore[index]
        calls = await asyncio.to_thread(_sha_calls, templates)
        digests: dict[str, str] | None = None
        if calls:
            rkey = RESOLVE_NOW if key == SAVE_NOW else RESOLVE_SAVE
            session, found = await self._resolve(session, rkey, calls, stamp)
            if found is None:
                return session, None
            if found is _RESOLVE_FAILED:
                return await self._fail_save_entries(session, key, targets, entries, now, stamp)
            digests = found
        names = await asyncio.to_thread(_render_names, session, templates, digests, now)
        items: list[dict[str, Any]] = []
        actor = rendering.slug(session.slug)
        for line, name in zip(targets, names, strict=True):
            if isinstance(name, ArchiveNameError):
                entries[line.index]["save"] = _state("failed", None, None, now, f"bad_name: {name}")
                continue
            try:
                object_key = object_key_for(
                    self.settings.cache_s3_prefix, name, rendering.format_now(now), actor
                )
            except ArchiveNameError as exc:
                entries[line.index]["save"] = _state("failed", name, None, now, f"bad_name: {exc}")
                continue
            base = entries[line.index].get("version")
            entries[line.index]["base_version"] = base
            entries[line.index]["save"] = _state("running", name, None, now)
            items.append(
                {
                    "index": line.index,
                    "name": name,
                    "key": object_key,
                    "base_version": base,
                    "orphan": line.reason == save_rule.ORPHAN,
                }
            )
        run = {**_new_run(now, items), **(stamp or {})}
        if not items:
            run = {**run, "state": "failed", "reason": "no_valid_name"}
            run["finished_at"] = now.isoformat()
            return await self._write(session, key, run, entries), run
        session = await self._write(session, key, run, entries)
        return await self._start(session, key, run)

    async def _fail_save_entries(
        self,
        session: Session,
        key: str,
        targets: list[EntryPlan],
        entries: list[dict[str, Any]],
        now,
        stamp: dict[str, Any] | None = None,
    ) -> tuple[Session, dict[str, Any]]:
        for line in targets:
            entries[line.index]["save"] = _state("failed", None, None, now, "resolve_failed")
        run = {**_new_run(now, []), **(stamp or {}), "state": "failed", "reason": "resolve_failed"}
        run["finished_at"] = now.isoformat()
        return await self._write(session, key, run, entries), run

    # ------------------------------------------------------------------ transcripts

    async def sessions_restore_step(
        self, session: Session, agents_exist: Callable[[], Awaitable[bool]]
    ) -> tuple[Session, CacheGate]:
        """Advance the one transcripts restore by at most one transition, before the pods start.

        Returns the session carrying the run and the gate: `hold` keeps the StatefulSet at zero
        replicas, `degraded` starts it after a failure. On the first converge with a workspace
        entry, `agents_exist` decides: the session's StatefulSet or an agent pod already exists,
        so the pods wrote their own transcripts and no restore may replace them
        (`skipped`/`pods_exist`). An unconfigured cache records `skipped`/`not_configured`.
        Otherwise the Job restores `<prefix>sessions/<slug>.tar.zst`.
        """
        if not has_workspace_entry(session.spec):
            return session, CacheGate()
        run = get_run(session.status, TX_RESTORE)
        if run is None:
            skip = save_rule.PODS_EXIST if await agents_exist() else None
            if skip is None and not self.settings.cache_available:
                skip = "not_configured"
            if skip is None:
                session, run = await self._begin_tx(session, TX_RESTORE)
            else:
                run = {"state": "skipped", "reason": skip}
                session = await self._write(session, TX_RESTORE, run)
                log.info("sessions restore of session %s skipped: %s", session.slug, skip)
        elif run.get("state") == "running":
            session, run = await self._observe(session, TX_RESTORE, run)
        elif run.get("job"):
            await self._delete_job(session, TX_RESTORE)
        return session, CacheGate.of_run(run, "sessions restore")

    def transcripts_save_owed(self, session: Session) -> bool:
        run = get_run(session.status, TX_SAVE)
        return has_workspace_entry(session.spec) and (run is None or run.get("state") == "running")

    async def transcripts_save_step(self, session: Session) -> tuple[Session, bool]:
        """Advance the transcripts save; True lets the discard on (a failure is recorded only)."""
        if not has_workspace_entry(session.spec):
            return session, True
        run = get_run(session.status, TX_SAVE)
        if run is None:
            skip = transcripts_skip(self.settings.cache_available, session.status)
            if skip is not None:
                session = await self._write(session, TX_SAVE, {"state": "skipped", "reason": skip})
                log.info("sessions save of session %s skipped: %s", session.slug, skip)
                return session, True
            session, run = await self._begin_tx(session, TX_SAVE)
        elif run.get("state") == "running":
            session, run = await self._observe(session, TX_SAVE, run)
        elif run.get("job"):
            await self._delete_job(session, TX_SAVE)
        return session, run is not None and run.get("state") != "running"

    async def _begin_tx(self, session: Session, key: str) -> tuple[Session, dict[str, Any]]:
        run = {**_new_run(await self._now(), []), "bytes": None}
        session = await self._write(session, key, run)
        return await self._start(session, key, run)

    # ------------------------------------------------------------------ Job lifecycle

    def _job_for(self, session: Session, key: str, run: dict[str, Any]) -> Obj:
        settings, spec = self.settings, session.spec
        if _is_resolve(key):
            calls = [(str(p), int(n)) for p, n in run.get("calls") or []]
            return render_resolve_job(session, settings, key, calls)
        method: Literal["GET", "PUT"] = "GET" if _is_restore(key) else "PUT"
        if key in _TX_FIELD:
            url = presign_object(settings, method, sessions_object_name(settings, session.slug))
            entries = [
                {
                    "index": 0,
                    "root": "sessions",
                    "root_dir": SESSIONS_ROOT,
                    "path": ".",
                    "exclude": list(SESSIONS_EXCLUDE),
                    "url": url,
                }
            ]
            return render_cache_job(session, settings, key, entries)
        entries = []
        for item in run.get("plan") or []:
            entry = (spec.backup or [])[item["index"]]
            record: dict[str, Any] = {
                "index": item["index"],
                "root": entry.root,
                "root_dir": root_dir(spec, entry.root),
                "path": entry.path,
                "exclude": list(entry.exclude),
                "url": presign_object(settings, method, item["key"]),
                "name": item["name"],
            }
            if method == "GET":
                record["version"] = item["version"]
            if key == LIVE:
                record["skip_dirty"] = True  # a live delivery never overwrites unsaved edits
            entries.append(record)
        return render_cache_job(session, settings, key, entries)

    async def _start(
        self, session: Session, key: str, run: dict[str, Any]
    ) -> tuple[Session, dict[str, Any]]:
        ns, name = self.settings.namespace, job_name(session.slug, key)
        if not self.settings.cache_available:
            return await self._finish(session, key, run, "failed", reason="not_configured")
        existing = await self.kube.get_job(ns, name)
        if existing is not None:
            if not is_owned(existing, session.slug):
                return await self._finish(session, key, run, "failed", reason="foreign_object")
        else:
            try:
                await self.kube.create_job(self._job_for(session, key, run))
            except Exception as exc:
                log.exception("creating the %s Job of session %s failed", _title(key), session.slug)
                reason = f"job_create_failed: {type(exc).__name__}"[:REASON_MAX]
                return await self._finish(session, key, run, "failed", reason=reason)
        run = {**run, "job": name}
        return await self._write(session, key, run), run

    async def _observe(
        self, session: Session, key: str, run: dict[str, Any]
    ) -> tuple[Session, dict[str, Any]]:
        ns, name = self.settings.namespace, job_name(session.slug, key)
        job = await self.kube.get_job(ns, name)
        if job is None:
            if run.get("job") is None:  # crashed between the running write and the create
                return await self._start(session, key, run)
            return await self._finish(session, key, run, "failed", reason="job_missing")
        if not is_owned(job, session.slug):
            return await self._finish(session, key, run, "failed", reason="foreign_object")
        if run.get("job") is None:  # crashed between the create and the job write
            run = {**run, "job": name}
            session = await self._write(session, key, run)
        status = job.get("status") or {}
        failed = job_condition(job, "Failed")
        if failed is not None and failed.get("reason") == "DeadlineExceeded":
            return await self._outcome(session, key, run, reason="timeout")
        if job_condition(job, "Complete") is not None or status.get("succeeded"):
            return await self._outcome(session, key, run)
        if failed is not None or status.get("failed"):
            return await self._outcome(session, key, run, failed=True)
        started = datetime.fromisoformat(run["started_at"])
        limit = self.settings.cache_timeout_seconds + DEADLINE_GRACE_SECONDS
        if (await self._now() - started).total_seconds() > limit:
            return await self._outcome(session, key, run, reason="timeout")
        return session, run

    async def _outcome(
        self,
        session: Session,
        key: str,
        run: dict[str, Any],
        *,
        reason: str | None = None,
        failed: bool = False,
    ) -> tuple[Session, dict[str, Any]]:
        """Record the Job's outcome from its pod's log and exit code, then delete it."""
        ns = self.settings.namespace
        pods = [
            p
            for p in await self.kube.list_pods(ns, cache_labels(session.slug, key))
            if is_owned(p, session.slug)
        ]
        pod = pods[-1] if pods else None
        tail: str | None = None
        if pod is not None:
            try:
                tail = outcome_line(
                    await self.kube.read_pod_log(ns, pod["metadata"]["name"], TAIL_LINES)
                )
            except KubeError as exc:
                log.warning(
                    "reading the %s log of session %s failed: %s", _title(key), session.slug, exc
                )
        if reason is not None:
            session, run = await self._finish(session, key, run, "failed", reason=reason)
        else:
            outcome = parse_outcome(tail)
            if outcome is None:
                code = container_exit_code(pod) if pod is not None and failed else None
                bad = f"exit {code}" if code is not None else "failed" if failed else "bad_output"
                session, run = await self._finish(session, key, run, "failed", reason=bad)
            else:
                session, run = await self._finish_outcome(session, key, run, outcome, failed)
        await self._delete_job(session, key)
        return session, run

    async def _finish_outcome(
        self, session: Session, key: str, run: dict[str, Any], outcome: dict[str, Any], failed: bool
    ) -> tuple[Session, dict[str, Any]]:
        if _is_resolve(key):
            digests = outcome.get("digests")
            if failed or not isinstance(digests, dict) or outcome.get("reason"):
                reason = str(outcome.get("reason") or "bad_output")[:REASON_MAX]
                return await self._finish(session, key, run, "failed", reason=reason)
            return await self._finish(session, key, run, "done", extra={"digests": digests})
        results = [r for r in outcome.get("entries") or [] if isinstance(r, dict)]
        if not results and (failed or outcome.get("reason")):
            reason = str(outcome.get("reason") or "failed")[:REASON_MAX]
            return await self._finish(session, key, run, "failed", reason=reason)
        if key in _TX_FIELD:
            return await self._finish_tx(session, key, run, results[0] if results else {})
        by_index = {r.get("index"): r for r in results}
        if key == LIVE:
            return await self._finish_live(session, key, run, by_index)
        if _is_restore(key):
            return await self._finish_restore(session, key, run, by_index)
        return await self._finish_save(session, key, run, by_index)

    async def _finish_tx(
        self, session: Session, key: str, run: dict[str, Any], result: dict[str, Any]
    ) -> tuple[Session, dict[str, Any]]:
        state = result.get("state")
        reason = result.get("reason")
        size = result.get("size")
        if key == TX_RESTORE:
            mapped = {"restored": "restored", "missing": "miss", "unreadable": "unreadable"}
        else:
            mapped = {"uploaded": "saved", "skipped": "skipped"}
        if state not in mapped:
            text = str(reason or state or "bad_output")[:REASON_MAX]
            return await self._finish(session, key, run, "failed", reason=text)
        nbytes = size if isinstance(size, int) and not isinstance(size, bool) else None
        return await self._finish(
            session,
            key,
            run,
            mapped[state],
            nbytes=nbytes,
            reason=str(reason)[:REASON_MAX] if reason else None,
        )

    async def _finish_restore(
        self, session: Session, key: str, run: dict[str, Any], by_index: dict[Any, Any]
    ) -> tuple[Session, dict[str, Any]]:
        now = await self._now()
        entries = self._entries(session)
        warnings: list[dict[str, Any]] = []
        first_failure: str | None = None
        for item in run.get("plan") or []:
            result = by_index.get(item["index"]) or {}
            state = result.get("state") or "failed"
            reason = result.get("reason") if state == "failed" else None
            if state == "failed" and reason is None:
                reason = "no_outcome"
            if state not in ("restored", "missing", "unreadable", "failed"):
                state, reason = "failed", "bad_output"
            entries[item["index"]]["restore"] = _state(
                state, item["name"], item["version"] if state != "missing" else None, now, reason
            )
            if state in WARN_STATES:
                warnings.append(_warning(item["index"], item["name"], state, "restore"))
            if state == "failed" and first_failure is None:
                first_failure = str(reason)[:REASON_MAX]
        session, run = await self._finish(
            session,
            key,
            run,
            "failed" if first_failure else "done",
            reason=first_failure,
            entries=entries,
        )
        await self._warn(session, warnings)
        return session, run

    async def _finish_live(
        self, session: Session, key: str, run: dict[str, Any], by_index: dict[Any, Any]
    ) -> tuple[Session, dict[str, Any]]:
        """Record a live delivery: a restored entry stands on the new version, a skipped one
        (dirty directory, missing or unreadable object) keeps its own and is not retried for the
        same version. Rules and skills restored into `claude` are loaded by `reload-skills`."""
        now = await self._now()
        entries = self._entries(session)
        warnings: list[dict[str, Any]] = []
        first_failure: str | None = None
        reload = False
        for item in run.get("plan") or []:
            index, name, version = item["index"], item["name"], item["version"]
            result = by_index.get(index) or {}
            state = result.get("state") or "failed"
            if state == "restored":
                entries[index]["version"] = version
                entries[index]["restore"] = _state("restored", name, version, now)
                entries[index]["live_delivery"] = _state("delivered", name, version, now)
                spec_entry = (session.spec.backup or [])[index]
                reload = reload or (spec_entry.root == "claude" and spec_entry.path in RELOAD_PATHS)
            elif state in ("skipped_dirty", "missing", "unreadable"):
                entries[index]["live_delivery"] = _state(state, name, version, now)
                warning = _warning(index, name, state, "restore")
                if state == "skipped_dirty":
                    warning["message"] = DIRTY_MESSAGE
                warnings.append(warning)
            else:
                reason = str(result.get("reason") or "no_outcome")[:REASON_MAX]
                entries[index]["live_delivery"] = _failed_delivery(
                    entries[index], name, version, now, reason
                )
                warnings.append(_failed_warning(index, name, reason))
                first_failure = first_failure or reason
        session, run = await self._finish(
            session,
            key,
            run,
            "failed" if first_failure else "done",
            reason=first_failure,
            entries=entries,
            extra={"reload_pending": True} if reload else None,
        )
        await self._warn(session, warnings)
        return session, run

    async def _finish_save(
        self, session: Session, key: str, run: dict[str, Any], by_index: dict[Any, Any]
    ) -> tuple[Session, dict[str, Any]]:
        now = await self._now()
        entries = self._entries(session)
        warnings: list[dict[str, Any]] = []
        first_failure: str | None = None
        for item in run.get("plan") or []:
            result = by_index.get(item["index"]) or {}
            record = await self._record_one(session, item, result, now)
            entries[item["index"]]["save"] = record
            if key == SAVE_NOW and record["state"] in ("saved", "unchanged") and record["version"]:
                # the saver is not delivered its own version: it now stands on it
                entries[item["index"]]["version"] = record["version"]
                entries[item["index"]]["live_delivery"] = None
            if record["state"] in WARN_STATES:
                warnings.append(_warning(item["index"], item["name"], record["state"], "save"))
            if record["state"] == "failed" and first_failure is None:
                first_failure = str(record.get("reason"))[:REASON_MAX]
        session, run = await self._finish(
            session,
            key,
            run,
            "failed" if first_failure else "done",
            reason=first_failure,
            entries=entries,
        )
        await self._warn(session, warnings)
        return session, run

    async def _record_one(
        self, session: Session, item: dict[str, Any], result: dict[str, Any], now: datetime
    ) -> dict[str, Any]:
        """Turn one entry's save outcome into its `save` record, recording the version."""
        name, object_key = item["name"], item["key"]
        state = result.get("state")
        if state == "skipped":
            return _state("skipped", name, None, now, str(result.get("reason") or "skipped"))
        if state == "refused_secret":
            return _state("refused_secret", name, None, now)
        if state != "uploaded":
            reason = str(result.get("reason") or state or "no_outcome")[:REASON_MAX]
            return _state("failed", name, None, now, reason)
        try:
            manifest = result.get("manifest")
            base = item.get("base_version")
            async with self.pool.connection() as conn, conn.transaction():
                existing = await backups_repo.by_object_key(conn, object_key)
                if existing is not None:  # recorded before a crash: do not record it twice
                    outcome, version = (
                        ("orphan" if existing.state == "orphan" else "advanced"),
                        existing,
                    )
                else:
                    outcome, version = await backups_repo.record_save(
                        conn,
                        name=name,
                        object_key=object_key,
                        size=int(result["size"]),
                        digest=str(result["digest"]),
                        manifest={"files": manifest} if isinstance(manifest, list) else None,
                        base_version=uuid.UUID(base) if base else None,
                        origin="save",
                        session_slug=session.slug,
                        preset_id=session.preset_id,
                        project=session.project,
                        created_by=session.slug,
                        orphan=bool(item.get("orphan")),
                    )
                current = await backups_repo.current(conn, name) if outcome == "unchanged" else None
        except Exception as exc:  # noqa: BLE001 - one entry's record must not hold the discard
            log.exception("recording the backup %s of session %s failed", name, session.slug)
            return _state("failed", name, None, now, f"record_failed: {type(exc).__name__}")
        if outcome == "unchanged":
            await self._delete_object(object_key)
            return _state("unchanged", name, str(current.id) if current else None, now)
        assert version is not None
        return _state("orphan" if outcome == "orphan" else "saved", name, str(version.id), now)

    async def _delete_object(self, object_key: str) -> None:
        try:
            failures = await asyncio.to_thread(delete_keys, self.settings, [object_key])
        except Exception:  # noqa: BLE001 - the sweep removes an object nothing references
            log.exception("deleting the unchanged backup object failed")
            return
        if failures:
            log.warning("deleting the unchanged backup object failed: %s", failures[0].code)

    async def _finish(
        self,
        session: Session,
        key: str,
        run: dict[str, Any],
        state: str,
        *,
        nbytes: int | None = None,
        reason: str | None = None,
        entries: list[dict[str, Any]] | None = None,
        extra: dict[str, Any] | None = None,
    ) -> tuple[Session, dict[str, Any]]:
        now = await self._now()
        run = {
            **run,
            **(extra or {}),
            "state": state,
            "finished_at": now.isoformat(),
            "reason": reason,
        }
        if key in _TX_FIELD:
            run["bytes"] = nbytes
        warnings: list[dict[str, Any]] = []
        if entries is None and state == "failed" and run.get("plan"):
            entries = self._entries(session)
            field = "restore" if _is_restore(key) else "save"
            for item in run["plan"]:
                if key == LIVE:  # retried later, bounded (`live.Live._stale`)
                    entries[item["index"]]["live_delivery"] = _failed_delivery(
                        entries[item["index"]], item["name"], item["version"], now, reason
                    )
                    warnings.append(_failed_warning(item["index"], item["name"], reason))
                elif (entries[item["index"]].get(field) or {}).get("state") == "running":
                    entries[item["index"]][field] = _state(
                        "failed", item["name"], None, now, reason
                    )
        session = await self._write(session, key, run, entries)
        await self._warn(session, warnings)
        detail = f": {reason}" if reason else ""
        if state == "failed":
            log.warning("%s of session %s failed%s", _title(key), session.slug, detail)
        else:
            log.info("%s of session %s %s%s", _title(key), session.slug, state, detail)
        return session, run

    async def _delete_job(self, session: Session, key: str) -> None:
        """Delete the session's own cache Job (background propagation); never a foreign one."""
        ns, name = self.settings.namespace, job_name(session.slug, key)
        try:
            job = await self.kube.get_job(ns, name)
            if job is not None and is_owned(job, session.slug):
                await self.kube.delete("Job", ns, name)
        except KubeError as exc:  # the outcome is recorded; a later tick deletes it
            log.warning(
                "deleting the %s Job of session %s failed: %s", _title(key), session.slug, exc
            )

    # ------------------------------------------------------------------ helpers

    @staticmethod
    def _entries(session: Session) -> list[dict[str, Any]]:
        """Copies of the stored entry records, one per spec entry (new ones are empty)."""
        stored = save_rule.backup_state(session.status).get("entries")
        stored = stored if isinstance(stored, list) else []
        out = []
        for index in range(len(session.spec.backup or [])):
            record = (
                stored[index] if index < len(stored) and isinstance(stored[index], dict) else None
            )
            out.append(dict(record) if record else _entry_record(session.spec, index))
        return out

    async def _write(
        self,
        session: Session,
        key: str,
        run: dict[str, Any],
        entries: list[dict[str, Any]] | None = None,
    ) -> Session:
        """Write status.backup with this run (and entries), keeping the other runs' state."""
        backup = _with_run(save_rule.backup_state(session.status), key, run)
        if entries is not None:
            backup["entries"] = entries
        await self.write_status(session, {"backup": backup}, session.observed_generation)
        return replace(session, status={**session.status, "backup": backup})

    async def _warn(self, session: Session, warnings: list[dict[str, Any]]) -> None:
        """One `backup_warning` session event per warning (best effort: the session goes on)."""
        if not warnings:
            return
        base = time.time_ns() // 1000
        envelopes = [
            {"seq": base + i, "type": WARNING_EVENT, "data": w} for i, w in enumerate(warnings)
        ]
        try:
            async with self.pool.connection() as conn:
                await events_repo.insert_events(
                    conn, session.id, session.id, EVENT_SESSION, envelopes
                )
        except Exception:  # noqa: BLE001
            log.exception("backup warning events of session %s not written", session.slug)

    async def _now(self) -> datetime:
        async with self.pool.connection() as conn:
            return (await (await conn.execute("SELECT now()")).fetchone())[0]


def _new_run(now: datetime, plan: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "state": "running",
        "started_at": now.isoformat(),
        "finished_at": None,
        "reason": None,
        "job": None,
        "plan": plan,
    }


def _state(
    state: str, name: str | None, version: str | None, now: datetime, reason: str | None = None
) -> dict[str, Any]:
    record: dict[str, Any] = {
        "state": state,
        "name": name,
        "version": version,
        "at": now.isoformat(),
    }
    if reason:
        record["reason"] = reason[:REASON_MAX]
    return record


def _failed_delivery(
    entry: dict[str, Any], name: str, version: str, now: datetime, reason: str | None
) -> dict[str, Any]:
    """A failed live delivery record; `attempts` counts the failures of this same version."""
    last = entry.get("live_delivery") or {}
    same = last.get("version") == version
    attempts = int(last.get("attempts") or 0) if same else 0
    return {**_state("failed", name, version, now, reason), "attempts": attempts + 1}


def _failed_warning(index: int, name: str, reason: str | None) -> dict[str, Any]:
    warning = _warning(index, name, "failed", "restore")
    warning["message"] = f"live delivery of the new version failed: {reason or 'no_outcome'}"
    return warning


def _warning(index: int, name: str, state: str, direction: str) -> dict[str, Any]:
    return {"index": index, "name": name, "state": state, "direction": direction}
