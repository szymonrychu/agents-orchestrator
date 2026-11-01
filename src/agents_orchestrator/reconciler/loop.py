"""Level-triggered reconcile: the sessions table drives Kubernetes and the agents table.

Every tick reads every session row and converges it (D6), so a crash mid-way leaves nothing
that the next tick does not repair. All calls target `settings.namespace`, the one namespace
shared with the orchestrator's own objects (D26). The guards that keep the reconciler off those:
- every list carries the session selector, and only objects carrying both the managed-by and
  the session label are ever deleted
- before applying a StatefulSet, Service, ConfigMap or PVC it reads the object; one that exists
  without both labels is foreign, and then nothing of that session is applied
- a live StatefulSet whose updateStrategy is not OnDelete is moved there by a strategy-only
  merge patch before it is applied (`_apply`, D1)
- Secrets are never read or listed: the session Secret is applied with the rest and deleted by
  its deterministic name

A deleting session (D15) waits for `pre_discard`, then loses every labelled object including
its PVC, and once none is left its row is purged, which cascades its whole history.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime
from typing import Any
from uuid import UUID

import httpx
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import queue, sessions
from agents_orchestrator.db.export import export_session_history
from agents_orchestrator.db.pool import listen, notify
from agents_orchestrator.db.sessions import Session, SessionNotFound
from agents_orchestrator.k8s.kube import FIELD_MANAGER, KubeApi, Obj, is_owned, pod_ready
from agents_orchestrator.k8s.labels import object_names, session_labels
from agents_orchestrator.k8s.render import pod_url, render_session
from agents_orchestrator.reconciler.cache import CacheGate, CacheRunner
from agents_orchestrator.reconciler.checks import CheckRunner
from agents_orchestrator.reconciler.discard_hook import HookResult, resolve_hook, run_discard_hook
from agents_orchestrator.reconciler.init import InitGate, InitRunner
from agents_orchestrator.reconciler.rollout import BUSY, LEADS, Replacer, agent_pods, terminating
from agents_orchestrator.reconciler.sweeper import CacheSweeper
from agents_orchestrator.reconciler.wrapper_sessions import WrapperSessionBootstrapper
from agents_orchestrator.settings import Settings
from agents_orchestrator.tokens import tokens_for

log = logging.getLogger(__name__)

CHANNELS = ("session_changed",)
GUARDED_KINDS = frozenset({"StatefulSet", "Service", "ConfigMap", "PersistentVolumeClaim"})
# Deletion order: the StatefulSet first so its pods stop using the config and the volume.
PRUNE_ORDER = ("StatefulSet", "Service", "ConfigMap", "PersistentVolumeClaim")
# A discard also deletes the check, init and cache Jobs, ahead of the PVC they mount. A converge
# never prunes them: they are created per check and once per session, not rendered.
DISCARD_ORDER = ("Job", *PRUNE_ORDER)

# Moves a live StatefulSet to OnDelete. A RollingUpdate one carries a defaulted, unowned
# `rollingUpdate.partition` that a server-side apply would keep next to `type: OnDelete`, which
# the API server rejects; the patch changes no template, so the controller rolls no pod (D1).
TO_ON_DELETE: Obj = {"spec": {"updateStrategy": {"type": "OnDelete", "rollingUpdate": None}}}

_HOOK_FAILURE_KEYS = ("discard_hook_failed", "discard_hook_failures", "discard_hook_next_at")

WrapperFactory = Callable[[str, str], Any]
"""(base_url, auth_token) -> wrapper client; used by session bootstrap."""


# (ordinal, busy) of each draining agent; a leader counts as busy (D8), see `_draining`.
_DRAINING = (
    f"SELECT a.ordinal, {BUSY} OR {LEADS}"
    " FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id"
    " WHERE a.session_id = %s AND a.ordinal >= %s AND a.gone_at IS NULL"
    " ORDER BY a.ordinal"
)


class Reconciler:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        kube: KubeApi,
        settings: Settings,
        wrapper_factory: WrapperFactory | None = None,
    ) -> None:
        self.pool = pool
        self.kube = kube
        self.settings = settings
        if settings.cache_available:
            settings.require_cache()
        self.wrapper_factory = wrapper_factory
        self._warned_no_agent_url: set[UUID] = set()
        self.hook_transport: httpx.AsyncBaseTransport | None = None
        self.checks = CheckRunner(pool, kube, settings)
        self.init = InitRunner(pool, kube, settings, write_status=self._write_status)
        self.cache = CacheRunner(pool, kube, settings, write_status=self._write_status)
        self.sweeper = CacheSweeper(pool, settings)
        self.replacer = Replacer(pool, kube, settings)
        self.bootstrapper = (
            WrapperSessionBootstrapper(pool, wrapper_factory)
            if wrapper_factory is not None
            else None
        )

    # ------------------------------------------------------------------ lifecycle

    async def run(self, stop: asyncio.Event) -> None:
        """Reconcile every `reconcile_poll_seconds`, and at once on NOTIFY session_changed."""
        wake = asyncio.Event()

        async def listener() -> None:
            async for _channel, _payload in listen(self.pool.conninfo, CHANNELS):
                wake.set()

        listener_task = asyncio.create_task(listener())
        stop_wait = asyncio.create_task(stop.wait())
        sweep_task = asyncio.create_task(self.sweeper.run(stop))
        try:
            while not stop.is_set():
                wake.clear()
                try:
                    await self.reconcile_once()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("reconcile pass failed")
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
            sweep_task.cancel()  # a sweep cut short is idempotent: the next one replans
            with contextlib.suppress(asyncio.CancelledError):
                await sweep_task

    # ------------------------------------------------------------------ one pass

    async def reconcile_once(self) -> None:
        await self._run_restarts()
        await self._run_rebootstraps()
        await self.checks.run_once()
        async with self.pool.connection() as conn:
            rows = await sessions.list_sessions(conn)
        for session in rows:
            try:
                if session.desired == "deleting":
                    await self._discard(session)
                else:
                    await self._converge(session)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("reconcile of session %s failed", session.slug)
                await self._record_error(session, exc)

    async def pre_discard(self, session: Session) -> bool:
        """The gate before a discard deletes anything (D15): True lets the deletion proceed.

        With no hook, or after the export was delivered, it is True. Otherwise the history
        export goes to the hook: success records `discard_hook_delivered` (database clock), a
        failure records `discard_hook_failed`, `discard_hook_failures` and
        `discard_hook_next_at` and returns False, so nothing is deleted. The hook is retried
        once the database clock passes `discard_hook_next_at`.
        """
        hook = resolve_hook(session, self.settings)
        status = session.status
        if hook is None or status.get("discard_hook_delivered"):
            return True
        next_at = status.get("discard_hook_next_at")
        async with self.pool.connection() as conn:
            if (
                next_at
                and (
                    await (
                        await conn.execute("SELECT %s::timestamptz > now()", (next_at,))
                    ).fetchone()
                )[0]
            ):
                return False
            try:
                result = await run_discard_hook(
                    hook,
                    session,
                    export_session_history(conn, session.id),
                    self.settings.discard_hook_timeout_seconds,
                    transport=self.hook_transport,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("discard hook of session %s crashed", session.slug)
                result = HookResult(False, f"{type(exc).__name__}: {exc}")
        if result.ok:
            async with self.pool.connection() as conn:
                now = (await (await conn.execute("SELECT now()")).fetchone())[0]
            await self._write_status(
                session,
                {"discard_hook_delivered": now.isoformat()},
                session.observed_generation,
                drop=_HOOK_FAILURE_KEYS,
            )
            return True
        failures = int(status.get("discard_hook_failures", 0)) + 1
        delay = self.settings.retry_delay_seconds(failures)
        async with self.pool.connection() as conn:
            retry_at = (
                await (
                    await conn.execute("SELECT now() + make_interval(secs => %s)", (delay,))
                ).fetchone()
            )[0]
        log.warning("discard hook of session %s failed: %s", session.slug, result.reason)
        await self._write_status(
            session,
            {
                "discard_hook_failed": result.reason,
                "discard_hook_failures": failures,
                "discard_hook_next_at": retry_at.isoformat(),
            },
            session.observed_generation,
        )
        return False

    # ------------------------------------------------------------------ an active session

    async def _converge(self, session: Session) -> None:
        ns, slug = self.settings.namespace, session.slug
        if not self.settings.agent_api_url and session.id not in self._warned_no_agent_url:
            self._warned_no_agent_url.add(session.id)
            log.warning("ORCH_AGENT_API_URL unset: session %s agents get no ORCH_AGENT_URL", slug)
        replicas = session.spec.replicas
        applied, since = await self._drain_replicas(session)
        rendered = (
            session
            if applied == replicas
            else replace(session, spec=session.spec.model_copy(update={"replicas": applied}))
        )
        objs = render_session(rendered, self.settings, tokens_for(self.settings, session.id))

        foreign = await self._foreign(slug, objs)
        if foreign:
            await self._write_status(
                session,
                {"phase": "error", "message": "; ".join(foreign)},
                session.observed_generation,
            )
            return

        # The Claude transcripts are restored before any agent pod starts: the StatefulSet stays
        # at zero replicas until that restore has an outcome (D7). It comes first, so a crash
        # after the StatefulSet apply never reads as a session whose pods already existed.
        session, sessions_gate = await self.cache.sessions_restore_step(
            session, lambda: self._agents_exist(slug)
        )
        for obj in objs:
            if sessions_gate.hold and obj["kind"] == "StatefulSet":
                obj = {**obj, "spec": {**obj["spec"], "replicas": 0}}
            await self._apply(obj)
        desired = {(o["kind"], o["metadata"]["name"]) for o in objs}
        await self._prune(slug, desired)
        cache_gate = await self.cache.restore_step(session)
        # init waits for the restore, so a script never runs on a workspace about to be replaced
        gate = await self.init.step(session) if not cache_gate.hold else InitGate()

        ready = await self._observe(session)
        draining = await self._draining(session.id, replicas)
        if ready >= replicas:
            phase, message = "ready", ""
        else:
            phase = "pending" if ready == 0 else "degraded"
            message = f"{ready} of {replicas} pods ready"
        problems: list[str] = []
        gates = (sessions_gate, cache_gate, gate)
        if any(g.hold for g in gates):  # sessions wait for the restores and the init run
            phase = "pending"
        else:
            problems = await self._bootstrap_wrapper_sessions(session)
            if any(g.degraded for g in gates):  # a failed restore or init starts them anyway
                phase = "degraded"
        rollout = await self.replacer.step(session)  # outdated pods roll one idle pod a tick
        fields: dict[str, Any] = {"phase": phase, "ready": ready, "replicas": replicas}
        if rollout is not None:
            fields["rollout"] = rollout
        drain = ""
        if draining:
            fields["drain"] = {
                "target": replicas,
                "since": since or (await self._db_now()).isoformat(),
                "waiting": [ordinal for ordinal, busy in draining if busy],
            }
            drain = f"draining {len(draining)} agents"
        fields["message"] = "; ".join(
            filter(None, [message, drain, *(g.message for g in gates), *problems])
        )
        log.debug("session %s in %s: %s", slug, ns, phase)
        drop = (*(() if draining else ("drain",)), *(() if rollout else ("rollout",)))
        await self._write_status(session, fields, session.generation, drop=drop)

    async def _apply(self, obj: Obj) -> None:
        """Server-side apply; a live StatefulSet not on OnDelete is first moved there."""
        if obj["kind"] == "StatefulSet":
            ns, name = self.settings.namespace, obj["metadata"]["name"]
            live = await self.kube.get_object("StatefulSet", ns, name)
            strategy = ((live or {}).get("spec") or {}).get("updateStrategy") or {}
            if live is not None and strategy.get("type") != "OnDelete":
                log.info("statefulset %s: moving to updateStrategy OnDelete", name)
                await self.kube.merge_patch("StatefulSet", ns, name, TO_ON_DELETE)
        await self.kube.apply(obj, field_manager=FIELD_MANAGER, force=True)

    async def _draining(self, session_id: UUID, replicas: int) -> list[tuple[int, bool]]:
        """(ordinal, busy) of each agent at or above `replicas` that is not gone (D3).

        Busy: its `agent_status.state` is `busy`, a message dispatched to it is still
        `dispatching` or `verifying`, or it is the session's current leader (D8: its pod is
        held until the leader row names a staying agent; the drain deadline bounds the hold).
        """
        async with self.pool.connection() as conn:
            cur = await conn.execute(_DRAINING, (session_id, replicas))
            return [(ordinal, busy) for ordinal, busy in await cur.fetchall()]

    async def _drain_replicas(self, session: Session) -> tuple[int, str | None]:
        """The StatefulSet replicas of this tick and the drain's `since`, None when none.

        A scale-down keeps the pods up to the highest busy draining agent, so idle agents above
        it go at once (a StatefulSet removes from the top) and the rest as they become idle.
        Never below the target, never above the replicas already applied, so a pod that is gone
        is not recreated. `scale_drain_seconds` after the first tick that saw the drain (kept
        in `status.drain.since`, so it survives a failover) the target applies regardless.
        """
        target = session.spec.replicas
        draining = await self._draining(session.id, target)
        if not draining:
            return target, None
        now = await self._db_now()
        since = (session.status.get("drain") or {}).get("since") or now.isoformat()
        busy = [ordinal for ordinal, is_busy in draining if is_busy]
        elapsed = (now - datetime.fromisoformat(since)).total_seconds()
        if not busy or elapsed >= self.settings.scale_drain_seconds:
            return target, since
        name = object_names(session.slug)["statefulset"]
        sts = await self.kube.get_object("StatefulSet", self.settings.namespace, name)
        current = ((sts or {}).get("spec") or {}).get("replicas", target)
        return max(target, min(1 + max(busy), current)), since

    async def _bootstrap_wrapper_sessions(self, session: Session) -> list[str]:
        """Give every ready agent without a live session one (D6); messages of the failures."""
        if self.bootstrapper is None:
            return []
        async with self.pool.connection() as conn:
            agents = [
                a
                for a in await agents_repo.list_agents(conn, session.id)
                if a.ready and a.gone_at is None
            ]
        token = tokens_for(self.settings, session.id).auth
        return await self.bootstrapper.bootstrap_many(agents, session.spec, token)

    async def _agents_exist(self, slug: str) -> bool:
        """The session's StatefulSet or one of its agent pods exists."""
        ns = self.settings.namespace
        if await self.kube.get_object("StatefulSet", ns, object_names(slug)["statefulset"]):
            return True
        return bool(agent_pods(slug, await self.kube.list_pods(ns, session_labels(slug))))

    async def _foreign(self, slug: str, objs: list[Obj]) -> list[str]:
        """`foreign_object <kind>/<name>` for each rendered name held by an unlabelled object."""
        found = []
        for obj in objs:
            kind, name = obj["kind"], obj["metadata"]["name"]
            if kind not in GUARDED_KINDS:
                continue  # Secrets cannot be read; the reserved prefix protects them
            existing = await self.kube.get_object(kind, self.settings.namespace, name)
            if existing is not None and not is_owned(existing, slug):
                found.append(f"foreign_object {kind}/{name}")
        return found

    async def _prune(
        self, slug: str, desired: set[tuple[str, str]], order: tuple[str, ...] = PRUNE_ORDER
    ) -> None:
        ns, selector = self.settings.namespace, session_labels(slug)
        for kind in order:
            for obj in await self.kube.list_objects(kind, ns, selector):
                name = obj["metadata"]["name"]
                if (kind, name) not in desired and is_owned(obj, slug):
                    await self.kube.delete(kind, ns, name)

    async def _observe(self, session: Session) -> int:
        """Pods into agents rows; returns the number of ready pods within replicas.

        A terminating pod is never ready: its agent is not picked while its grace period runs.
        """
        slug, replicas = session.slug, session.spec.replicas
        pods = await self.kube.list_pods(self.settings.namespace, session_labels(slug))
        seen = agent_pods(slug, pods)  # check Job pods are not agents
        ready = 0
        async with self.pool.connection() as conn:
            for ordinal, pod in sorted(seen.items()):
                name = pod["metadata"]["name"]
                is_ready = pod_ready(pod) and not terminating(pod) and ordinal < replicas
                ready += is_ready
                await agents_repo.upsert_agent(
                    conn,
                    session.id,
                    ordinal,
                    name,
                    pod_url(slug, name, self.settings),
                    (pod.get("status") or {}).get("phase"),
                    is_ready,
                    (pod.get("status") or {}).get("podIP"),
                )
            for agent in await agents_repo.list_agents(conn, session.id):
                if agent.ordinal in seen or agent.gone_at is not None:
                    continue
                if agent.ordinal >= replicas:
                    await agents_repo.mark_gone(conn, agent.id)
                elif agent.ready or agent.pod_phase is not None:
                    # the pod is being recreated: keep the agent, but never pick it meanwhile
                    await agents_repo.upsert_agent(
                        conn, session.id, agent.ordinal, agent.pod_name, agent.url, None, False
                    )
        return ready

    # ------------------------------------------------------------------ a deleting session

    async def _discard(self, session: Session) -> None:
        async with self.pool.connection() as conn:
            await queue.cancel_checks(conn, session.id)
        if not await self._quiesce(session):
            return
        if not await self.cache.save_step(session):
            await self._write_status(
                session,
                {"phase": "deleting", "message": "saving the workspace cache"},
                session.observed_generation,
            )
            return
        session = await self._reload(session)  # the workdir save may have just written its run
        if not await self.cache.save_step(session, "sessions"):
            await self._write_status(
                session,
                {"phase": "deleting", "message": "saving the Claude transcripts"},
                session.observed_generation,
            )
            return
        if not await self.pre_discard(session):
            await self._write_status(
                session,
                {"phase": "deleting", "message": "waiting for the pre-discard hook"},
                session.observed_generation,
            )
            return
        ns, slug = self.settings.namespace, session.slug
        await self._prune(slug, set(), DISCARD_ORDER)
        await self.kube.delete("Secret", ns, object_names(slug)["secret"])  # absent is fine

        remaining = await self._remaining(slug)
        if remaining:
            await self._write_status(
                session,
                {"phase": "deleting", "message": "waiting for " + ", ".join(remaining)},
                session.generation,
            )
            return
        async with self.pool.connection() as conn, conn.transaction():
            await sessions.purge(conn, session.id)
            await notify(conn, "status", {"session_id": str(session.id)})
        log.info("session %s discarded", slug)

    async def _reload(self, session: Session) -> Session:
        async with self.pool.connection() as conn:
            current = await sessions.get(conn, session.id)
        if current is None:
            raise SessionNotFound(str(session.id))
        return current

    async def _db_now(self) -> datetime:
        async with self.pool.connection() as conn:
            return (await (await conn.execute("SELECT now()")).fetchone())[0]

    async def _quiesce(self, session: Session) -> bool:
        """Stop the agents and let the ingester drain before the history is exported or saved.

        Only when an export is still owed (a hook that has not delivered) or a workspace cache
        or transcripts save is (`spec.cache` set, no save outcome yet). The StatefulSet is scaled
        to zero; once no agent pod is left, the ingester gets `discard_drain_seconds` to store
        the last events of their streams. The wait is bounded by
        `discard_quiesce_timeout_seconds` from the first pass, after which the discard goes on
        and logs what it could not confirm. True means the export and the saves may run.
        """
        status = session.status
        hook_owed = resolve_hook(session, self.settings) is not None and not status.get(
            "discard_hook_delivered"
        )
        save_owed = self.cache.save_owed(session) or self.cache.save_owed(session, "sessions")
        if not hook_owed and not save_owed:
            return True
        ns, slug = self.settings.namespace, session.slug
        now = await self._db_now()
        started = datetime.fromisoformat(status.get("discard_quiesce_started") or now.isoformat())
        fields: dict[str, Any] = {"discard_quiesce_started": started.isoformat()}

        name = object_names(slug)["statefulset"]
        sts = await self.kube.get_object("StatefulSet", ns, name)
        if sts is not None and is_owned(sts, slug) and (sts.get("spec") or {}).get("replicas") != 0:
            for obj in render_session(
                session, self.settings, tokens_for(self.settings, session.id)
            ):
                if obj["kind"] == "StatefulSet":
                    obj["spec"]["replicas"] = 0
                    await self._apply(obj)

        running = sorted(
            p["metadata"]["name"]
            for p in agent_pods(slug, await self.kube.list_pods(ns, session_labels(slug))).values()
        )
        timed_out = (now - started).total_seconds() >= self.settings.discard_quiesce_timeout_seconds
        if running and not timed_out:
            fields["message"] = "waiting for agent pods to stop: " + ", ".join(running)
            await self._write_status(
                session,
                {"phase": "deleting", **fields},
                session.observed_generation,
                drop=("discard_quiesced_at",),
            )
            return False
        if running:
            log.warning(
                "session %s: agent pods %s still running after %gs; exporting anyway",
                slug,
                running,
                self.settings.discard_quiesce_timeout_seconds,
            )
        else:
            quiesced = datetime.fromisoformat(status.get("discard_quiesced_at") or now.isoformat())
            fields["discard_quiesced_at"] = quiesced.isoformat()
            drained = (now - quiesced).total_seconds() >= self.settings.discard_drain_seconds
            if not drained and not timed_out:
                fields["message"] = "waiting for the ingester to drain the agent streams"
                await self._write_status(
                    session, {"phase": "deleting", **fields}, session.observed_generation
                )
                return False
        await self._write_status(session, fields, session.observed_generation)
        return True

    async def _remaining(self, slug: str) -> list[str]:
        ns, selector = self.settings.namespace, session_labels(slug)
        left = []
        for kind in DISCARD_ORDER:
            for obj in await self.kube.list_objects(kind, ns, selector):
                if is_owned(obj, slug):
                    left.append(f"{kind}/{obj['metadata']['name']}")
        for pod in await self.kube.list_pods(ns, selector):
            if is_owned(pod, slug):
                left.append(f"Pod/{pod['metadata']['name']}")
        return left

    # ------------------------------------------------------------------ restart commands

    async def _run_restarts(self) -> None:
        while True:
            async with self.pool.connection() as conn:
                cmd = await queue.claim_command(conn, ["restart"])
            if cmd is None:
                return
            try:
                status, result = await self._restart(cmd)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("restart command %s failed", cmd.id)
                status, result = "failed", {"error": str(exc) or type(exc).__name__}
            async with self.pool.connection() as conn:
                await queue.complete_command(conn, cmd.id, status, result)

    async def _restart(self, cmd: queue.Command) -> tuple[str, dict[str, Any]]:
        async with self.pool.connection() as conn:
            agent = await agents_repo.get_agent(conn, cmd.agent_id)
            session = await sessions.get(conn, cmd.session_id)
        if agent is None or session is None:
            return "failed", {"error": "agent_not_found"}
        if session.desired == "deleting":
            return "failed", {"error": "session_deleting"}
        if agent.gone_at is not None:
            return "failed", {"error": "agent_gone"}
        ns, slug = self.settings.namespace, session.slug
        pods = await self.kube.list_pods(ns, session_labels(slug))
        if not any(p["metadata"]["name"] == agent.pod_name and is_owned(p, slug) for p in pods):
            return "failed", {"error": "pod_not_found"}
        await self.kube.delete_pod(ns, agent.pod_name)
        return "done", {"pod": agent.pod_name}

    # ------------------------------------------------------------------ rebootstrap commands

    async def _run_rebootstraps(self) -> None:
        """Clear the session of each agent a rebootstrap command names and bootstrap it again."""
        while True:
            async with self.pool.connection() as conn:
                cmd = await queue.claim_command(conn, ["rebootstrap"])
            if cmd is None:
                return
            try:
                status, result = await self._rebootstrap(cmd)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("rebootstrap command %s failed", cmd.id)
                status, result = "failed", {"error": str(exc) or type(exc).__name__}
            async with self.pool.connection() as conn:
                await queue.complete_command(conn, cmd.id, status, result)

    async def _rebootstrap(self, cmd: queue.Command) -> tuple[str, dict[str, Any]]:
        async with self.pool.connection() as conn:
            agent = await agents_repo.get_agent(conn, cmd.agent_id)
            session = await sessions.get(conn, cmd.session_id)
        if agent is None or session is None:
            return "failed", {"error": "agent_not_found"}
        if agent.gone_at is not None:
            return "failed", {"error": "agent_gone"}
        if session.desired == "deleting":
            return "failed", {"error": "session_deleting"}
        async with self.pool.connection() as conn:
            await agents_repo.clear_wrapper_session(conn, agent.id)
        agent = replace(agent, wrapper_session_id=None, wrapper_session_created_at=None)
        if self.bootstrapper is None or not agent.ready:
            return "done", {"cleared": True, "bootstrapped": False}
        cache_gate = CacheGate.of(session.status.get("cache"), session.spec.cache is not None)
        gate = InitGate.of(session.status.get("init"), session.spec.init_scripts)
        held = cache_gate if cache_gate.hold else gate
        if held.hold:  # the converge bootstraps it once the restore and the init run are over
            return "done", {"cleared": True, "bootstrapped": False, "message": held.message}
        token = tokens_for(self.settings, session.id).auth
        problem = await self.bootstrapper.bootstrap(agent, session.spec, token)
        if problem:  # the session stays cleared; the next reconcile tick bootstraps it again (D6)
            return "done", {"cleared": True, "bootstrapped": False, "message": problem}
        return "done", {"cleared": True, "bootstrapped": True}

    # ------------------------------------------------------------------ status

    async def _write_status(
        self,
        session: Session,
        fields: dict[str, Any],
        observed_generation: int,
        drop: tuple[str, ...] = (),
    ) -> None:
        """Merge `fields` into the stored status; a write only when something changed.

        The row is re-read, so keys another step of this tick wrote (the discard hook's) are
        kept (`drop` removes some). Skipping unchanged writes keeps the reconciler's own NOTIFY
        from waking it.
        """
        async with self.pool.connection() as conn, conn.transaction():
            current = await sessions.get(conn, session.id)
            if current is None:
                return
            status = {k: v for k, v in current.status.items() if k not in drop} | fields
            if status == current.status and observed_generation == current.observed_generation:
                return
            await sessions.set_status(conn, session.id, status, observed_generation)
            await notify(conn, "status", {"session_id": str(session.id)})

    async def _record_error(self, session: Session, exc: Exception) -> None:
        try:
            await self._write_status(
                session,
                {"phase": "error", "message": str(exc) or type(exc).__name__},
                session.observed_generation,
            )
        except SessionNotFound:
            pass
        except Exception:
            log.exception("could not record the error of session %s", session.slug)
