"""The volume reconciler: one pass per volume row, driven by the row's state (spec S-77 8.1).

Leader-only like the cache sweep: the reconciler singleton constructs it. A pass runs on NOTIFY
`volumes_changed` and every `reconcile_poll_seconds`. The PVC `vol-<name>` is applied by
server-side apply, grown by a patch of the storage request and deleted only here, after the
finalizer is removed. Restoring and saving are one hook method each (`_restoring`, `_saving`):
the restore/save Jobs are driven there. Orphan claims (session row gone, no pod left) are
released with actor system.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import defaultdict
from typing import Any

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import volumes
from agents_orchestrator.db.pool import listen
from agents_orchestrator.k8s.kube import KubeApi, KubeError
from agents_orchestrator.k8s.render import render_volume_pvc
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

    async def _restoring(self, vol: volumes.Volume) -> None:
        """Hook for the restore Job (added with the Job driver)."""

    async def _saving(self, vol: volumes.Volume) -> None:
        """Hook for the save Job (added with the Job driver)."""

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
            if await self._any_pod(claims):
                continue
            async with self.pool.connection() as conn, conn.transaction():
                await volumes.release_claims(
                    conn, session_id, actor_kind="system", actor="reconciler"
                )

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
            await volumes.set_state(conn, vol.id, state, reason=reason)

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
