"""The volume reconciler: one pass per volume row, driven by the row's state (spec S-77 8.1).

Leader-only like the cache sweep: the reconciler singleton constructs it. A pass runs on NOTIFY
`volumes_changed` and every `reconcile_poll_seconds`. The PVC `vol-<name>` is applied by
server-side apply, grown by a patch of the storage request and deleted only here, after the
finalizer is removed. Restoring and saving drive one Job each (`render_volume_job`, named by
the id of the `restore_started`/`save_started` event, so a pass after a crash finds its own Job):
a restore first re-checks the pods (HARD RULE, `pods_using`) right before creating the Job. Orphan
claims (session row gone, no pod left) are released with actor system.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections import defaultdict
from typing import Any

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import rendering
from agents_orchestrator.cache.keys import object_key_for
from agents_orchestrator.cache.presign import delete_keys, presign_object
from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import volumes
from agents_orchestrator.db.pool import listen
from agents_orchestrator.k8s.kube import KubeApi, KubeError
from agents_orchestrator.k8s.labels import volume_labels
from agents_orchestrator.k8s.render import render_volume_pvc
from agents_orchestrator.reconciler.cache import (
    CACHE_LABEL,
    outcome_line,
    parse_outcome,
    render_volume_job,
    volume_job_name,
)
from agents_orchestrator.reconciler.checks import TAIL_LINES, container_exit_code, job_condition
from agents_orchestrator.reconciler.init import REASON_MAX
from agents_orchestrator.reconciler.volume_guard import pods_using
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import parse_quantity

log = logging.getLogger(__name__)

PVC = "PersistentVolumeClaim"

# fixed failure reasons (volumes.state_reason)
PVC_APPLY_FAILED = "pvc_apply_failed"
PVC_LOST = "pvc_lost"
PVC_MISSING = "pvc_missing"
PVC_RESIZE_FAILED = "pvc_resize_failed"

KEY_ANNOTATION = "agents.szymonrichert.pl/object-key"
BASE_ANNOTATION = "agents.szymonrichert.pl/base-version"


class VolumeReconciler:
    def __init__(self, pool: AsyncConnectionPool, kube: KubeApi, settings: Settings) -> None:
        self.pool, self.kube, self.settings = pool, kube, settings

    async def tick(self) -> None:
        """One pass over every live volume, then the orphan claims; one failure never stops it."""
        async with self.pool.connection() as conn:
            rows = await volumes.list(conn)
        for vol in rows:
            try:
                await self.reconcile(vol)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("volume %s pass failed", vol.name)
        try:
            await self.release_orphans()
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("volume orphan release failed")

    async def reconcile(self, vol: volumes.Volume) -> None:
        handler = getattr(self, f"_{vol.state}", None)
        if handler is not None:
            await handler(vol)

    # ------------------------------------------------------------------ states

    async def _provisioning(self, vol: volumes.Volume) -> None:
        try:
            await self.kube.apply(render_volume_pvc(vol, self.settings))
        except KubeError:
            log.warning("volume %s: PVC apply failed", vol.name, exc_info=True)
            await self._set(vol, "failed", PVC_APPLY_FAILED)
            return
        phase = ((await self._pvc(vol)) or {}).get("status", {}).get("phase")
        if phase == "Bound":
            await self._set(vol, "ready")
        elif phase == "Lost":
            await self._set(vol, "failed", PVC_LOST)

    async def _resizing(self, vol: volumes.Volume) -> None:
        pvc = await self._pvc(vol)
        if pvc is None:
            await self._set(vol, "failed", PVC_MISSING)
            return
        requested = pvc["spec"]["resources"]["requests"]["storage"]
        if parse_quantity(requested) != parse_quantity(vol.size):
            try:
                await self.kube.merge_patch(
                    PVC,
                    self.settings.namespace,
                    vol.pvc_name,
                    {"spec": {"resources": {"requests": {"storage": vol.size}}}},
                )
            except KubeError:
                log.warning("volume %s: PVC resize failed", vol.name, exc_info=True)
                await self._set(vol, "failed", PVC_RESIZE_FAILED)
                return
        capacity = (pvc.get("status") or {}).get("capacity", {}).get("storage")
        if capacity is not None and parse_quantity(capacity) >= parse_quantity(vol.size):
            await self._set(vol, "ready")

    async def _deleting(self, vol: volumes.Volume) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            if await volumes.unreleased_claims(conn, vol.id):
                await volumes.refuse_delete(conn, vol.id, reason="claims_present")
                return
        try:  # a pod the claims do not know about keeps the finalizer; retried next tick
            found = await pods_using(
                self.kube, self.settings.namespace, volume_id=vol.id, pvc_name=vol.pvc_name
            )
        except KubeError:
            log.warning("volume %s: pod lookup failed, delete retried", vol.name, exc_info=True)
            return
        if found:
            log.warning("volume %s: delete held, pods still use it: %s", vol.name, found)
            return
        pvc = await self._pvc(vol)
        if pvc is None:
            async with self.pool.connection() as conn, conn.transaction():
                await volumes.finish_delete(conn, vol.id)
            return
        if pvc["metadata"].get("finalizers"):
            await self.kube.merge_patch(
                PVC, self.settings.namespace, vol.pvc_name, {"metadata": {"finalizers": None}}
            )
        await self.kube.delete(PVC, self.settings.namespace, vol.pvc_name)

    async def _ready(self, vol: volumes.Volume) -> None:
        if await self._pvc(vol) is None:
            await self._set(vol, "failed", PVC_MISSING)

    # ------------------------------------------------------------------ restore and save Jobs

    async def _restoring(self, vol: volumes.Volume) -> None:
        started = await self._started(vol, "restore_started")
        raw = (started.detail.get("version") if started else None) or ""
        try:
            version = uuid.UUID(str(raw))
        except ValueError:
            await self._end_restore(vol, None, "no_request")
            return
        assert started is not None
        job = await self.kube.get_job(
            self.settings.namespace, volume_job_name(vol.name, "restore", started.id)
        )
        if job is None:
            await self._start_restore(vol, started.id, version)
            return
        state, _result, reason = await self._observe(vol, job, "restore", {"restored"})
        if state == "running":
            return
        await self._end_restore(vol, version if state == "ok" else None, reason)
        await self._delete_job(job)

    async def _start_restore(self, vol: volumes.Volume, n: int, version: uuid.UUID) -> None:
        # HARD RULE: no Job while any pod mounts the volume (checked right before the create)
        found = await pods_using(
            self.kube, self.settings.namespace, volume_id=vol.id, pvc_name=vol.pvc_name
        )
        if found:
            async with self.pool.connection() as conn, conn.transaction():
                await volumes.abort_restore(conn, vol.id, reason="pod_found")
            return
        if not self.settings.cache_available:
            await self._end_restore(vol, None, "not_configured")
            return
        async with self.pool.connection() as conn:
            row = await backups_repo.get_version(
                conn, f"{backups_repo.RESERVED_PREFIX}{vol.name}", version
            )
        if row is None:
            await self._end_restore(vol, None, "version_missing")
            return
        try:
            url = presign_object(self.settings, "GET", row.object_key)
            await self.kube.create_job(
                render_volume_job(
                    vol, self.settings, action="restore", version=version, n=n, url=url
                )
            )
        except Exception as exc:  # noqa: BLE001 - a restore never wedges the volume
            log.exception("volume %s: creating the restore Job failed", vol.name)
            await self._end_restore(vol, None, f"job_create_failed: {type(exc).__name__}")

    async def _end_restore(
        self, vol: volumes.Volume, version: uuid.UUID | None, reason: str | None
    ) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            await volumes.finish_restore(conn, vol.id, version=version, reason=reason)

    async def _saving(self, vol: volumes.Volume) -> None:
        started = await self._started(vol, "save_started")
        if started is None:
            await self._end_save(vol, "save_failed", None, "no_request")
            return
        job = await self.kube.get_job(
            self.settings.namespace, volume_job_name(vol.name, "save", started.id)
        )
        if job is None:
            await self._start_save(vol, started)
            return
        state, result, reason = await self._observe(vol, job, "save", {"uploaded"})
        if state == "running":
            return
        annotations = (job.get("metadata") or {}).get("annotations") or {}
        try:
            if state == "ok" and result is not None:
                await self._record_save(vol, started, result, annotations)
            else:
                await self._end_save(vol, "save_failed", None, reason)
        finally:
            await self._delete_job(job)

    async def _start_save(self, vol: volumes.Volume, started: volumes.Event) -> None:
        if not self.settings.cache_available:
            await self._end_save(vol, "save_failed", None, "not_configured")
            return
        name = f"{backups_repo.RESERVED_PREFIX}{vol.name}"
        slug = started.detail.get("session_slug")
        try:
            async with self.pool.connection() as conn:
                now = (await (await conn.execute("SELECT now()")).fetchone())[0]
                base = await backups_repo.current(conn, name)
            key = object_key_for(
                self.settings.cache_s3_prefix,
                name,
                rendering.format_now(now),
                rendering.slug(slug) if slug else "user",
            )
            url = presign_object(self.settings, "PUT", key)
            await self.kube.create_job(
                render_volume_job(
                    vol,
                    self.settings,
                    action="save",
                    version=None,
                    n=started.id,
                    url=url,
                    annotations={
                        KEY_ANNOTATION: key,
                        BASE_ANNOTATION: str(base.id) if base else "",
                    },
                )
            )
        except Exception as exc:  # noqa: BLE001 - a save never wedges the volume
            log.exception("volume %s: creating the save Job failed", vol.name)
            await self._end_save(
                vol, "save_failed", None, f"job_create_failed: {type(exc).__name__}"
            )

    async def _record_save(
        self,
        vol: volumes.Volume,
        started: volumes.Event,
        result: dict[str, Any],
        annotations: dict[str, str],
    ) -> None:
        name = f"{backups_repo.RESERVED_PREFIX}{vol.name}"
        key = annotations.get(KEY_ANNOTATION, "")
        base = annotations.get(BASE_ANNOTATION) or None
        slug = started.detail.get("session_slug")
        files = result.get("manifest")
        manifest: dict[str, Any] = {"files": files} if isinstance(files, list) else {}
        if started.detail.get("live"):
            manifest["live"] = True
        preset_id = await self._preset_of(slug)
        try:
            async with self.pool.connection() as conn, conn.transaction():
                existing = await backups_repo.by_object_key(conn, key)
                if existing is not None:  # recorded before a crash: do not record it twice
                    outcome, version = "advanced", existing
                else:
                    outcome, version = await backups_repo.record_save(
                        conn,
                        name=name,
                        object_key=key,
                        size=int(result["size"]),
                        digest=str(result["digest"]),
                        manifest=manifest or None,
                        base_version=uuid.UUID(base) if base else None,
                        origin="save",
                        session_slug=slug,
                        preset_id=preset_id,
                        project=vol.projects[0] if vol.projects else None,
                        created_by=slug or started.actor,
                        orphan=False,
                        volume_writer=True,
                    )
                if outcome == "unchanged":
                    version = await backups_repo.current(conn, name)
                assert version is not None
                kind = "save_unchanged" if outcome == "unchanged" else "saved"
                await volumes.finish_save(conn, vol.id, kind=kind, version=version.id)
        except Exception as exc:  # noqa: BLE001
            log.exception("volume %s: recording the save failed", vol.name)
            await self._end_save(vol, "save_failed", None, f"record_failed: {type(exc).__name__}")
            return
        if outcome == "unchanged":
            await self._delete_object(key)

    async def _preset_of(self, slug: str | None) -> uuid.UUID | None:
        if not slug:
            return None
        async with self.pool.connection() as conn:
            cur = await conn.execute("SELECT preset_id FROM sessions WHERE slug = %s", (slug,))
            row = await cur.fetchone()
        return row[0] if row else None

    async def _end_save(
        self, vol: volumes.Volume, kind: str, version: uuid.UUID | None, reason: str | None
    ) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            await volumes.finish_save(conn, vol.id, kind=kind, version=version, reason=reason)  # type: ignore[arg-type]

    async def _delete_object(self, key: str) -> None:
        try:
            failures = await asyncio.to_thread(delete_keys, self.settings, [key])
        except Exception:  # noqa: BLE001 - the sweep removes an object nothing references
            log.exception("deleting the unchanged volume object failed")
            return
        if failures:
            log.warning("deleting the unchanged volume object failed: %s", failures[0].code)

    async def _started(self, vol: volumes.Volume, kind: str) -> volumes.Event | None:
        async with self.pool.connection() as conn:
            page = await volumes.events(conn, vol.id, limit=50)
        return next((e for e in page.items if e.kind == kind), None)

    async def _observe(
        self, vol: volumes.Volume, job: dict[str, Any], action: str, ok_states: set[str]
    ) -> tuple[str, dict[str, Any] | None, str | None]:
        """(state, result, reason): `running`, `ok` (the CLI reported one of `ok_states`) or
        `failed` with a fixed reason (`timeout`, `failed`, `exit N`, `bad_output` or the CLI's)."""
        status = job.get("status") or {}
        failed = job_condition(job, "Failed")
        if failed is not None and failed.get("reason") == "DeadlineExceeded":
            return "failed", None, "timeout"
        done = job_condition(job, "Complete") is not None or bool(status.get("succeeded"))
        broke = failed is not None or bool(status.get("failed"))
        if not done and not broke:
            return "running", None, None
        ns = self.settings.namespace
        selector = {**volume_labels(vol.id), CACHE_LABEL: f"volume-{action}"}
        pods = await self.kube.list_pods(ns, selector)
        pod = pods[-1] if pods else None
        tail = None
        if pod is not None:
            try:
                tail = outcome_line(
                    await self.kube.read_pod_log(ns, pod["metadata"]["name"], TAIL_LINES)
                )
            except KubeError:
                log.warning("volume %s: reading the %s log failed", vol.name, action)
        outcome = parse_outcome(tail)
        results = [r for r in (outcome or {}).get("entries") or [] if isinstance(r, dict)]
        if outcome is None or not results:
            code = container_exit_code(pod) if pod is not None and broke else None
            return (
                "failed",
                None,
                f"exit {code}" if code is not None else "failed" if broke else "bad_output",
            )
        result = results[0]
        if broke or result.get("state") not in ok_states:
            reason = str(result.get("reason") or result.get("state") or "failed")[:REASON_MAX]
            return "failed", None, reason
        return "ok", result, None

    async def _delete_job(self, job: dict[str, Any]) -> None:
        try:
            await self.kube.delete("Job", self.settings.namespace, job["metadata"]["name"])
        except KubeError:  # the outcome is recorded; the pod is finished and harmless
            log.warning("volume Job %s not deleted", job["metadata"]["name"])

    # ------------------------------------------------------------------ orphans

    async def release_orphans(self) -> None:
        """Release the claims of a session row that is gone, once no pod mounts any of its
        volumes (volume label or claimName): a pod still running keeps the claim."""
        async with self.pool.connection() as conn:
            orphans = await volumes.orphan_claims(conn)
        by_session: dict[Any, list[volumes.Claim]] = defaultdict(list)
        for c in orphans:
            by_session[c.session_id].append(c)
        for session_id, claims in by_session.items():
            try:
                if await self._any_pod(claims):
                    continue
                async with self.pool.connection() as conn, conn.transaction():
                    await volumes.release_claims(
                        conn, session_id, actor_kind="system", actor="reconciler"
                    )
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("releasing the orphan claims of session %s failed", session_id)

    async def _any_pod(self, claims: list[volumes.Claim]) -> bool:
        for c in claims:
            async with self.pool.connection() as conn:
                vol = await volumes.get(conn, c.volume_id)
            if vol is None:
                continue
            if await pods_using(
                self.kube, self.settings.namespace, volume_id=vol.id, pvc_name=vol.pvc_name
            ):
                return True
        return False

    # ------------------------------------------------------------------ helpers

    async def _pvc(self, vol: volumes.Volume) -> dict[str, Any] | None:
        return await self.kube.get_object(PVC, self.settings.namespace, vol.pvc_name)

    async def _set(self, vol: volumes.Volume, state: str, reason: str | None = None) -> None:
        async with self.pool.connection() as conn, conn.transaction():
            changed = await volumes.set_state(conn, vol.id, state, reason=reason, expect=vol.state)
        if not changed:
            log.info("volume %s: state changed meanwhile, %s skipped", vol.name, state)

    # ------------------------------------------------------------------ loop

    async def run(self, stop: asyncio.Event) -> None:
        """`tick` every `reconcile_poll_seconds` and at once on NOTIFY volumes_changed, until
        `stop`. The listener also wakes the loop when its LISTEN is (re)established."""
        wake = asyncio.Event()

        async def listener() -> None:
            async for _channel, _payload in listen(
                self.pool.conninfo, (volumes.CHANGED_CHANNEL,), on_ready=wake.set
            ):
                wake.set()

        listener_task = asyncio.create_task(listener())
        stop_wait = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                wake.clear()
                await self.tick()
                wake_wait = asyncio.create_task(wake.wait())
                try:
                    await asyncio.wait(
                        {wake_wait, stop_wait},
                        timeout=self.settings.reconcile_poll_seconds,
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                finally:
                    wake_wait.cancel()
        finally:
            listener_task.cancel()
            stop_wait.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await listener_task
