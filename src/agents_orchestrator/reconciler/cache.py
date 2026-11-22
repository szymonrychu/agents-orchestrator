"""Volume restore and save Jobs and the Claude transcripts Jobs (S-77; S-68 D7).

The transcripts are the one per-session archive left (entry backups became volumes): a session
that mounts a `backup_restore` volume has its `/sessions` tree restored before the agent pods
start (the StatefulSet is held at zero replicas) and saved at the discard, each one Job of
`orch cache restore|save` in the configured cache image, never the session's `spec.image`. The
volume Jobs (`render_volume_job`) restore or save a volume's PVC (reconciler/volumes.py).

Status: `sessions.status.backup.transcripts.restore|save` are the crash-safe Job runs with the
shape of the init run: the state (and the plan) is written before the create and `job` only
after it, the outcome is recorded and then the Job deleted. The outcome is the last line of the
pod log, a JSON object (`{"entries": [...]}`); a line that cannot be read is a failure, never a
success.

The S3 credentials stay in the reconciler: the Job gets presigned, expiring URLs in
`CACHE_ENTRIES` and no Secret, `spec.env`, `envFrom` or service account token. The pod is
hardened (uid 1000, non-root, read-only root filesystem with an emptyDir `/tmp` as `HOME`, no
privilege escalation, all capabilities dropped, seccomp RuntimeDefault). A save archives into an
emptyDir `/scratch` sized to `cache_max_bytes`.
"""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any, Literal

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.cache import save_rule
from agents_orchestrator.cache.presign import presign_object, sessions_object_name
from agents_orchestrator.cache.save_rule import transcripts_skip
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.db.volumes import Volume
from agents_orchestrator.k8s.kube import KubeApi, KubeError, Obj, is_owned
from agents_orchestrator.k8s.labels import prefix, session_labels, volume_labels, workspace_pvc
from agents_orchestrator.k8s.render import SESSIONS_ROOT, WORKSPACE_VOLUME, agent_identity
from agents_orchestrator.reconciler.checks import (
    DEADLINE_GRACE_SECONDS,
    TAIL_LINES,
    container_exit_code,
    job_condition,
)
from agents_orchestrator.reconciler.init import REASON_MAX, StatusWriter
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SESSIONS_EXCLUDE, transcripts_enabled

log = logging.getLogger(__name__)

CACHE_LABEL = "agents.szymonrichert.pl/cache"
BACKOFF_LIMIT = 2
TMP_DIR = "/tmp"
BACKUP_PREFIX = backups_repo.RESERVED_PREFIX
VOLUME_ROOT = "/volume"  # where a volume Job pod sees the volume PVC
SCRATCH_DIR = "/scratch"
SESSIONS_SUBPATH = "sessions"  # the PVC subtree holding every agent pod's /sessions
# The outcome line carries a per-file manifest (100-200+ KB for a real workspace), far over the
# 16 KiB diagnostic tail of `log_tail`; this is its own cap. A longer line is treated as bad output.
OUTCOME_MAX_BYTES = 8 * 1024 * 1024

TX_RESTORE, TX_SAVE = "transcripts_restore", "transcripts_save"
_JOB_SUFFIX = {TX_RESTORE: "sessions-restore", TX_SAVE: "sessions-save"}
_TX_FIELD = {TX_RESTORE: "restore", TX_SAVE: "save"}


def job_name(slug: str, key: str) -> str:
    return f"{prefix(slug)}-{_JOB_SUFFIX[key]}"


def cache_labels(slug: str, key: str) -> dict[str, str]:
    return {**session_labels(slug), CACHE_LABEL: _JOB_SUFFIX[key]}


def _title(key: str) -> str:
    return _JOB_SUFFIX[key].replace("-", " ")


def get_run(status: dict[str, Any], key: str) -> dict[str, Any] | None:
    """The transcripts run record of `key` in `status.backup`, or None."""
    return save_rule.transcripts_run(status, _TX_FIELD[key])  # type: ignore[arg-type]


def _with_run(backup: dict[str, Any], key: str, run: dict[str, Any]) -> dict[str, Any]:
    return {**backup, "transcripts": {**(backup.get("transcripts") or {}), _TX_FIELD[key]: run}}


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
    if isinstance(value.get("entries"), list):
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


# The volume root is created by the CSI driver as uid 0 and fsGroup changes only its group, so git
# in the uid 1000 agent refuses a repository there ("dubious ownership"). The owner step hands the
# root (not recursive) to the agent user: uid 0 with CAP_CHOWN only. The root of `/volume` is the
# mount point itself, so no DAC traversal capability is needed.
OWNER_CONTAINER = "volume-owner"
OWNER_RESOURCES = {
    "requests": {"cpu": "10m", "memory": "16Mi"},
    "limits": {"cpu": "100m", "memory": "64Mi"},
}


def _owner_container(settings: Settings, mounts: list[dict[str, Any]]) -> dict[str, Any]:
    root = [m for m in mounts if m["mountPath"] == VOLUME_ROOT]
    return {
        "name": OWNER_CONTAINER,
        "image": settings.cache_image,
        "command": ["chown", "1000:1000", VOLUME_ROOT],
        "resources": OWNER_RESOURCES,
        "securityContext": {
            "runAsUser": 0,
            "runAsNonRoot": False,
            "readOnlyRootFilesystem": True,
            "allowPrivilegeEscalation": False,
            "capabilities": {"drop": ["ALL"], "add": ["CHOWN"]},
        },
        "volumeMounts": root,
    }


def _volume_pvc(volume: Volume, *, read_only: bool = False) -> Obj:
    ro = {"readOnly": True} if read_only else {}
    return {"name": "volume", "persistentVolumeClaim": {"claimName": volume.pvc_name, **ro}}


def _volume_job(
    volume: Volume,
    settings: Settings,
    action: str,
    name: str,
    pod_spec: Obj,
    annotations: dict[str, str] | None = None,
) -> Obj:
    labels = {**volume_labels(volume.id), CACHE_LABEL: f"volume-{action}"}
    metadata: dict[str, Any] = {
        "name": name,
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
            "template": {"metadata": {"labels": dict(labels)}, "spec": pod_spec},
        },
    }


# a finished owner Job the reconciler did not delete (the volume left provisioning, a crash)
OWNER_TTL_SECONDS = 3600


def volume_owner_job_name(volume: Volume) -> str:
    """Unique per owner attempt and derived only from the row, so a pass after a crash finds
    its own Job and never takes a previous attempt's Job for it."""
    return f"vol-{volume.id}-owner-{volume.owner_attempts}"


def render_volume_owner_job(volume: Volume, settings: Settings) -> Obj:
    """The owner Job of a new volume: the PVC at `/volume`, read-write, and only the owner step,
    run once before the volume turns ready (no pod mounts it before)."""
    mounts = [{"name": "volume", "mountPath": VOLUME_ROOT}]
    pod_spec = _hardened(settings, _owner_container(settings, mounts), [_volume_pvc(volume)])
    job = _volume_job(volume, settings, "owner", volume_owner_job_name(volume), pod_spec)
    job["spec"]["ttlSecondsAfterFinished"] = OWNER_TTL_SECONDS
    return job


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
    `exclude` globs as the entry's tar excludes; a restore ignores them. A restore runs the owner
    step first as an init container (the uid 1000 extract cannot take over a uid 0 root)."""
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
    volumes: list[Obj] = [_volume_pvc(volume, read_only=save), {"name": "tmp", "emptyDir": {}}]
    if save:
        mounts.append({"name": "scratch", "mountPath": SCRATCH_DIR})
        volumes.append(
            {"name": "scratch", "emptyDir": {"sizeLimit": str(settings.cache_max_bytes)}}
        )
    container = _cache_container(settings, action, _cache_env(settings, [entry]), mounts)
    pod_spec = _hardened(settings, container, volumes)
    if not save:
        pod_spec["initContainers"] = [_owner_container(settings, mounts)]
    name = volume_job_name(volume.name, action, n)
    return _volume_job(volume, settings, action, name, pod_spec, annotations)


def render_cache_job(
    session: Session, settings: Settings, key: str, entries: list[dict[str, Any]]
) -> Obj:
    """The transcripts restore or save Job of run `key`, given its entry (with its presigned
    `url`): the session PVC's `sessions` subPath at `/sessions`, read-only for a save."""
    save = key == TX_SAVE
    labels = cache_labels(session.slug, key)
    sessions_mount: dict[str, Any] = {
        "name": WORKSPACE_VOLUME,
        "mountPath": SESSIONS_ROOT,
        "subPath": SESSIONS_SUBPATH,
    }
    mounts: list[dict[str, Any]] = [{**sessions_mount, **({"readOnly": True} if save else {})}]
    mounts.append({"name": "tmp", "mountPath": TMP_DIR})
    volumes: list[Obj] = [_pvc_volume(session), {"name": "tmp", "emptyDir": {}}]
    if save:
        mounts.append({"name": "scratch", "mountPath": SCRATCH_DIR})
        volumes.append(
            {"name": "scratch", "emptyDir": {"sizeLimit": str(settings.cache_max_bytes)}}
        )
    container = _cache_container(
        settings, "save" if save else "restore", _cache_env(settings, entries), mounts
    )
    return _job(session, settings, key, _hardened(settings, container, volumes), labels)


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

    # ------------------------------------------------------------------ transcripts

    async def sessions_restore_step(
        self, session: Session, agents_exist: Callable[[], Awaitable[bool]]
    ) -> tuple[Session, CacheGate]:
        """Advance the one transcripts restore by at most one transition, before the pods start.

        Returns the session carrying the run and the gate: `hold` keeps the StatefulSet at zero
        replicas, `degraded` starts it after a failure. On the first converge, `agents_exist`
        decides: the session's StatefulSet or an agent pod already exists, so the pods wrote
        their own transcripts and no restore may replace them (`skipped`/`pods_exist`). An
        unconfigured cache records `skipped`/`not_configured`.
        """
        if not transcripts_enabled(session.spec):
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
        return transcripts_enabled(session.spec) and (run is None or run.get("state") == "running")

    async def transcripts_save_step(self, session: Session) -> tuple[Session, bool]:
        """Advance the transcripts save; True lets the discard on (a failure is recorded only)."""
        if not transcripts_enabled(session.spec):
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

    def _job_for(self, session: Session, key: str) -> Obj:
        settings = self.settings
        method: Literal["GET", "PUT"] = "GET" if key == TX_RESTORE else "PUT"
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
                await self.kube.create_job(self._job_for(session, key))
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
        results = [r for r in outcome.get("entries") or [] if isinstance(r, dict)]
        if not results and (failed or outcome.get("reason")):
            reason = str(outcome.get("reason") or "failed")[:REASON_MAX]
            return await self._finish(session, key, run, "failed", reason=reason)
        return await self._finish_tx(session, key, run, results[0] if results else {})

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

    async def _finish(
        self,
        session: Session,
        key: str,
        run: dict[str, Any],
        state: str,
        *,
        nbytes: int | None = None,
        reason: str | None = None,
    ) -> tuple[Session, dict[str, Any]]:
        now = await self._now()
        run = {
            **run,
            "state": state,
            "finished_at": now.isoformat(),
            "reason": reason,
            "bytes": nbytes,
        }
        session = await self._write(session, key, run)
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

    async def _write(self, session: Session, key: str, run: dict[str, Any]) -> Session:
        """Write status.backup with this run, keeping the other run's state."""
        backup = _with_run(save_rule.backup_state(session.status), key, run)
        await self.write_status(session, {"backup": backup}, session.observed_generation)
        return replace(session, status={**session.status, "backup": backup})

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


def _new_run(now: datetime, plan: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "state": "running",
        "started_at": now.isoformat(),
        "finished_at": None,
        "reason": None,
        "job": None,
        "plan": plan,
    }
