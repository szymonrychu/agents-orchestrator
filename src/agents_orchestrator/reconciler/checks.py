"""Checks as Jobs (D10): a queued check runs as a Job on the session's shared workspace.

The reconciler claims queued checks and creates Job `agent-<slug>-check-<id>`; each tick it reads
the running ones and records the outcome. The Job lives in the one shared namespace and carries
both session labels, so prune and discard find it by selector. It mounts the session PVC with the
same mount as the agent pods and runs in the directory the agents' sessions work in (D27).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import queue, sessions
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s.kube import KubeApi, Obj, is_owned
from agents_orchestrator.k8s.labels import prefix, session_labels, workspace_pvc
from agents_orchestrator.k8s.render import (
    WORKSPACE_VOLUME,
    agent_identity,
    workdir_mount,
    workdir_path,
)
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

CHECK_LABEL = "agents.szymonrichert.pl/check"
BACKOFF_LIMIT = 0
DEADLINE_SECONDS = 1800
TTL_SECONDS = 3600
TAIL_LINES = 200
TAIL_BYTES = 16 * 1024
# A Job whose own deadline has not fired this long after the deadline is treated as timed out.
DEADLINE_GRACE_SECONDS = 120


def job_name(slug: str, check_id: int) -> str:
    return f"{prefix(slug)}-check-{check_id}"


def render_job(session: Session, check: queue.Check, settings: Settings) -> Obj:
    labels = {**session_labels(session.slug), CHECK_LABEL: str(check.id)}
    pod_spec: dict[str, Any] = {
        "restartPolicy": "Never",
        **agent_identity(settings),
        "securityContext": {
            "runAsUser": 1000,
            "fsGroup": 1000,
            "fsGroupChangePolicy": "OnRootMismatch",
        },
        "containers": [
            {
                "name": "check",
                "image": settings.check_image,
                "command": ["sh", "-c", check.command],
                "workingDir": workdir_path(session.spec),
                "volumeMounts": [workdir_mount(session.spec)],
            }
        ],
        "volumes": [
            {
                "name": WORKSPACE_VOLUME,
                "persistentVolumeClaim": {"claimName": workspace_pvc(session.slug, session.status)},
            }
        ],
    }
    if settings.image_pull_secret:
        pod_spec = {"imagePullSecrets": [{"name": settings.image_pull_secret}], **pod_spec}
    return {
        "apiVersion": "batch/v1",
        "kind": "Job",
        "metadata": {
            "name": job_name(session.slug, check.id),
            "namespace": settings.namespace,
            "labels": dict(labels),
        },
        "spec": {
            "backoffLimit": BACKOFF_LIMIT,
            "activeDeadlineSeconds": DEADLINE_SECONDS,
            "ttlSecondsAfterFinished": TTL_SECONDS,
            "template": {"metadata": {"labels": dict(labels)}, "spec": pod_spec},
        },
    }


def job_condition(job: Obj, kind: str) -> dict[str, Any] | None:
    for cond in (job.get("status") or {}).get("conditions") or []:
        if cond.get("type") == kind and cond.get("status") == "True":
            return cond
    return None


def log_tail(log_text: str) -> str:
    lines = log_text.splitlines()[-TAIL_LINES:]
    data = "\n".join(lines).encode()
    if len(data) > TAIL_BYTES:
        data = data[-TAIL_BYTES:]
    return data.decode(errors="ignore")


class CheckRunner:
    def __init__(self, pool: AsyncConnectionPool, kube: KubeApi, settings: Settings) -> None:
        self.pool, self.kube, self.settings = pool, kube, settings

    async def run_once(self) -> None:
        await self._start_queued()
        await self._observe_running()

    async def _complete(
        self, check: queue.Check, status: str, exit_code: int | None, tail: str | None
    ) -> None:
        async with self.pool.connection() as conn:
            await queue.complete_check(conn, check.id, status, exit_code, tail)

    async def _start_queued(self) -> None:
        while True:
            async with self.pool.connection() as conn:
                check = await queue.claim_check(conn)
            if check is None:
                return
            try:
                await self._start(check)
            except Exception as exc:
                log.exception("starting check %s failed", check.id)
                await self._complete(check, "failed", None, f"job_create_failed: {exc}"[:2000])

    async def _start(self, check: queue.Check) -> None:
        async with self.pool.connection() as conn:
            session = await sessions.get(conn, check.session_id)
        if session is None or session.desired == "deleting":
            await self._complete(check, "cancelled", None, None)
            return
        name = job_name(session.slug, check.id)
        existing = await self.kube.get_job(self.settings.namespace, name)
        if existing is not None:
            if not is_owned(existing, session.slug):
                await self._complete(check, "failed", None, "foreign_object")
                return
        else:
            await self.kube.create_job(render_job(session, check, self.settings))
        async with self.pool.connection() as conn:
            await queue.set_check_job(conn, check.id, name)

    async def _observe_running(self) -> None:
        async with self.pool.connection() as conn:
            running = await queue.list_running_checks(conn)
        for check in running:
            try:
                await self._observe(check)
            except Exception:
                log.exception("observing check %s failed", check.id)

    async def _observe(self, check: queue.Check) -> None:
        async with self.pool.connection() as conn:
            session = await sessions.get(conn, check.session_id)
        if session is None:
            return
        ns, name = self.settings.namespace, job_name(session.slug, check.id)
        job = await self.kube.get_job(ns, name)
        if job is None:
            if check.job_name is None:  # crashed between the claim and the create
                await self._start(check)
            else:
                await self._complete(check, "failed", None, "job_missing")
            return
        if not is_owned(job, session.slug):
            await self._complete(check, "failed", None, "foreign_object")
            return
        failed = job_condition(job, "Failed")
        if failed is not None and failed.get("reason") == "DeadlineExceeded":
            await self._complete(check, "failed", None, "timeout")
        elif job_condition(job, "Complete") is not None or (job.get("status") or {}).get(
            "succeeded"
        ):
            await self._finish(check, session, name, "passed")
        elif failed is not None or (job.get("status") or {}).get("failed"):
            await self._finish(check, session, name, "failed")
        elif time.time() - check.started_at.timestamp() > DEADLINE_SECONDS + DEADLINE_GRACE_SECONDS:
            await self._complete(check, "failed", None, "timeout")

    async def _finish(self, check: queue.Check, session: Session, name: str, status: str) -> None:
        ns = self.settings.namespace
        selector = {**session_labels(session.slug), CHECK_LABEL: str(check.id)}
        pods = [p for p in await self.kube.list_pods(ns, selector) if is_owned(p, session.slug)]
        pod = pods[-1] if pods else None
        exit_code = 0 if status == "passed" else None
        tail: str | None = None
        if pod is not None:
            if status == "failed":
                exit_code = container_exit_code(pod)
            text = await self.kube.read_pod_log(ns, pod["metadata"]["name"], TAIL_LINES)
            tail = log_tail(text)
        await self._complete(check, status, exit_code, tail)


def container_exit_code(pod: Obj) -> int | None:
    for cs in (pod.get("status") or {}).get("containerStatuses") or []:
        terminated = (cs.get("state") or {}).get("terminated")
        if terminated is not None and "exitCode" in terminated:
            return int(terminated["exitCode"])
    return None
