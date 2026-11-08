"""Level-triggered reconcile: the sessions table drives Kubernetes and the agents table.

Every tick reads every session row and converges it (D6), so a crash mid-way leaves nothing
that the next tick does not repair. Calls target three scopes (`Reconciler._scopes`): the
session's own objects live in `settings.namespace`, the one namespace shared with the
orchestrator's own objects (D26); the RoleBindings of granted namespace access in each namespace
of `ORCH_KUBE_NAMESPACE_ACCESS_NAMESPACES`; the cluster-admin ClusterRoleBinding at cluster scope,
reached only while `ORCH_KUBE_CLUSTER_ACCESS_ENABLED` is on. Every bookkeeping key is (kind,
namespace, name) (`obj_key`): the same RoleBinding name exists in each granted namespace. The
guards that keep the reconciler off objects that are not its own:
- every list carries the session selector, and only objects carrying both the managed-by and
  the session label are ever deleted
- before applying a StatefulSet, Service, ConfigMap, PVC, ServiceAccount or binding it reads the
  object; one that exists without both labels is foreign, and then nothing of that session is
  applied
- a live ServiceAccount or binding with the session labels but another session's id
  (`SESSION_ID_LABEL`, a slug reused) is deleted and recreated, never adopted
- a live StatefulSet whose updateStrategy is not OnDelete is moved there by a strategy-only
  merge patch before it is applied (`_apply`, D1)
- Secrets are never read or listed: the session Secret is applied with the rest and deleted by
  its deterministic name
- a rendered object is applied only when it changed: when its fingerprint differs from the last
  one this process applied, or the live object `drift`s from it (a Secret, never read, is
  instead re-asserted every `SECRET_REASSERT_SECONDS`). A no-op pass patches nothing, so it
  never touches the StatefulSet (mx-99 #1039)
- every pod or StatefulSet this process deletes or scales away is logged at INFO with the
  session slug, the object and the reason (outdated revision, restart command, scale-down,
  no longer rendered, discard)

A deleting session (D15) waits for `pre_discard`, then loses every labelled object including
its PVC, and once none is left its row is purged, which cascades its whole history. A hook given
up on (`ORCH_DISCARD_HOOK_GIVE_UP_SECONDS` after its first failure, or at once on a forced
discard) is recorded in `discard_hook_abandoned`, and the PVC is then kept, detached, as the
backup of what was not exported (`_retain_pvc`, T-26).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import time
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import httpx
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import queue, sessions
from agents_orchestrator.db.export import export_session_history
from agents_orchestrator.db.pool import listen, notify
from agents_orchestrator.db.sessions import Session, SessionNotFound
from agents_orchestrator.k8s.kube import (
    FIELD_MANAGER,
    KubeApi,
    Obj,
    drift,
    is_owned,
    obj_namespace,
    pod_ready,
)
from agents_orchestrator.k8s.labels import (
    MANAGED_BY_LABEL,
    SESSION_ID_LABEL,
    SESSION_LABEL,
    object_names,
    session_labels,
    suffixed_workspace_pvc,
    workspace_pvc,
)
from agents_orchestrator.k8s.render import pod_url, render_session
from agents_orchestrator.reconciler.cache import CacheGate, CacheRunner
from agents_orchestrator.reconciler.checks import CheckRunner
from agents_orchestrator.reconciler.discard_hook import HookResult, resolve_hooks, run_discard_hook
from agents_orchestrator.reconciler.init import InitGate, InitRunner
from agents_orchestrator.reconciler.rollout import BUSY, LEADS, Replacer, agent_pods, terminating
from agents_orchestrator.reconciler.sweeper import CacheSweeper
from agents_orchestrator.reconciler.wrapper_sessions import WrapperSessionBootstrapper
from agents_orchestrator.settings import DiscardHook, Settings
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.models import describe_exception

log = logging.getLogger(__name__)

CHANNELS = ("session_changed",)
# The per-session Kubernetes access objects; each carries the session id (`SESSION_ID_LABEL`).
KUBE_KINDS = frozenset({"ServiceAccount", "RoleBinding", "ClusterRoleBinding"})
GUARDED_KINDS = (
    frozenset({"StatefulSet", "Service", "ConfigMap", "PersistentVolumeClaim"}) | KUBE_KINDS
)
# Deletion order: the StatefulSet first so its pods stop using the config, the volume and the
# ServiceAccount; the bindings before the ServiceAccount they name.
PRUNE_ORDER = (
    "StatefulSet",
    "Service",
    "ConfigMap",
    "PersistentVolumeClaim",
    "RoleBinding",
    "ClusterRoleBinding",
    "ServiceAccount",
)
# A discard also deletes the check, init and cache Jobs, ahead of the PVC they mount. A converge
# never prunes them: they are created per check and once per session, not rendered.
DISCARD_ORDER = ("Job", *PRUNE_ORDER)

# Moves a live StatefulSet to OnDelete. A RollingUpdate one carries a defaulted, unowned
# `rollingUpdate.partition` that a server-side apply would keep next to `type: OnDelete`, which
# the API server rejects; the patch changes no template, so the controller rolls no pod (D1).
TO_ON_DELETE: Obj = {"spec": {"updateStrategy": {"type": "OnDelete", "rollingUpdate": None}}}

# A Secret is never read, so an unchanged one is applied again only this often, which restores
# one deleted out of band without a PATCH on every pass.
SECRET_REASSERT_SECONDS = 600.0

_HOOK_FAILURE_KEYS = (
    "discard_hook_failed",
    "discard_hook_failures",
    "discard_hook_next_at",
    "discard_hook_first_failed_at",
    "discard_hook_errors",
)

# A workspace PVC kept after an abandoned hook carries this label (the session slug) instead of
# the session and managed-by labels, so no selector of the session finds it again; its
# annotations `<label>-session-id` and `<label>-reason` say what it was and why it was kept.
RETAINED_LABEL = "agents.szymonrichert.pl/retained-from"


def _abandoned_note(abandoned: dict[str, Any] | None) -> str:
    """The phase message part naming the hooks a discard goes on without ("" for none)."""
    if not abandoned:
        return ""
    return "discarding without the pre-discard hook " + ", ".join(
        f"{name} ({entry.get('reason')})" for name, entry in sorted(abandoned.items())
    )


def _deleting(message: str, status: dict[str, Any]) -> dict[str, Any]:
    """Status fields of a discard step, its message followed by the abandoned hooks if any."""
    note = _abandoned_note(status.get("discard_hook_abandoned"))
    return {"phase": "deleting", "message": "; ".join(filter(None, [message, note]))}


def _never_bootstrapped(agent: agents_repo.Agent) -> str | None:
    """A restart's warning for an agent whose wrapper session failed to start: the new pod
    re-runs the same bootstrap, so the restart does not fix the cause."""
    failure = agent.start_failure
    if agent.wrapper_session_id is not None or not failure:
        return None
    message = str(failure.get("message") or "").strip().splitlines()
    reason = f"{failure.get('step') or 'unknown'}: {message[0][:200] if message else 'no detail'}"
    return (
        f"agent never bootstrapped (last failure {reason}); the new pod re-runs the same"
        " bootstrap and fails the same way until that cause is fixed"
    )


ObjKey = tuple[str, str, str]
"""(kind, namespace, name) of an object; the namespace is "" for a cluster-scoped one."""


def obj_key(obj: Obj) -> ObjKey:
    return obj["kind"], obj_namespace(obj), obj["metadata"]["name"]


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
        # (kind, namespace, name) -> (fingerprint, monotonic time) of the last object this
        # process applied; see `_apply`
        self._applied: dict[ObjKey, tuple[str, float]] = {}
        self.hook_transport: httpx.AsyncBaseTransport | None = None
        self._hook_spent = 0.0  # seconds of discard hooks run in this pass
        self._hook_attempted: dict[UUID, float] = {}  # session -> monotonic start of its last hook
        # session -> lock held by its converge or discard and by each restart or rebootstrap
        # command of it, so the command task never acts on a session the pass is converging
        self._session_locks: dict[UUID, asyncio.Lock] = {}
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
        """Reconcile every `reconcile_poll_seconds`, and at once on NOTIFY session_changed.

        Restart and rebootstrap commands also run in their own task (`_command_loop`), on the
        same clock, so a pass held for minutes by slow wrapper creates cannot hold them.
        """
        wake = asyncio.Event()
        command_wake = asyncio.Event()

        async def listener() -> None:
            async for _channel, _payload in listen(self.pool.conninfo, CHANNELS):
                wake.set()
                command_wake.set()

        listener_task = asyncio.create_task(listener())
        stop_wait = asyncio.create_task(stop.wait())
        sweep_task = asyncio.create_task(self.sweeper.run(stop))
        command_task = asyncio.create_task(self._command_loop(stop, command_wake))
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
            # a command cut short stays `running`; it is not claimed again
            command_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await command_task

    async def _command_loop(self, stop: asyncio.Event, wake: asyncio.Event) -> None:
        """Drain the restart and rebootstrap commands every `reconcile_poll_seconds` and on
        NOTIFY, independently of the reconcile pass."""
        stop_wait = asyncio.create_task(stop.wait())
        try:
            while not stop.is_set():
                wake.clear()
                try:
                    await self.run_commands()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    log.exception("command pass failed")
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
            stop_wait.cancel()

    def _session_lock(self, session_id: UUID) -> asyncio.Lock:
        """The in-process lock serialising a session's converge or discard with its restart and
        rebootstrap commands. In-process is enough: the reconciler is a singleton
        (`runtime.singleton`), and an advisory lock held across a converge would pin a pooled
        connection for minutes."""
        return self._session_locks.setdefault(session_id, asyncio.Lock())

    def _prune_session_locks(self, live: set[UUID]) -> None:
        self._session_locks = {
            k: v for k, v in self._session_locks.items() if k in live or v.locked()
        }

    # ------------------------------------------------------------------ one pass

    def _start_pass(self) -> None:
        self._hook_spent = 0.0

    def _hook_budget_spent(self) -> bool:
        return self._hook_spent >= self.settings.discard_hook_pass_budget_seconds

    def _hook_deadline(self) -> float:
        """Wall-clock seconds one hook call may take: the overall timeout, capped by what is
        left of the pass budget, but never under connect + read (one fair try). A server that
        trickles a byte inside the read timeout is cut here, command hooks included. With this
        floor a pass spends under budget + floor (95 s by default) on hooks."""
        s = self.settings
        remaining = s.discard_hook_pass_budget_seconds - self._hook_spent
        floor = s.discard_hook_connect_timeout_seconds + s.discard_hook_read_timeout_seconds
        return min(s.discard_hook_timeout_seconds, max(remaining, floor))

    def _hook_order(self, rows: list[Session]) -> list[Session]:
        """Sessions whose hook ran longest ago (or never) first, the rest in database order, so
        stalled sessions at the head of the list cannot spend every pass's budget alone."""
        live = {r.id for r in rows}
        self._hook_attempted = {k: v for k, v in self._hook_attempted.items() if k in live}
        return sorted(rows, key=lambda r: self._hook_attempted.get(r.id, 0.0))

    async def reconcile_once(self) -> None:
        self._start_pass()
        # the command task drains these too; a claim is status-guarded, so each runs once
        await self.run_commands()
        await self.checks.run_once()
        async with self.pool.connection() as conn:
            rows = await sessions.list_sessions(conn)
        self._prune_session_locks({r.id for r in rows})
        for session in self._hook_order(rows):
            try:
                async with self._session_lock(session.id):
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

        Every owed hook (`_owed_hooks`: the settings hook, then the session's own URL, minus
        those already delivered) gets the history export, each on its own stream, and each runs
        whatever the others did, so one hook's outage never holds another's delivery. A success
        adds the hook's name to `discard_hook_delivered` (database clock) at once, so it is
        never sent again. Any failure records `discard_hook_failed` (`<name>: <reason>` of the
        first failing hook), `discard_hook_failures` and `discard_hook_next_at` (one shared
        backoff) and returns False, so nothing is deleted. The owed hooks are retried once the
        database clock passes `discard_hook_next_at`; True only when nothing is owed. Once the
        hooks of this pass have used `discard_hook_pass_budget_seconds`, a hook not yet run is
        left owed (nothing run or recorded for it) and the result is False: the session waits
        for the next pass. Each call is cut at `_hook_deadline`. A failure once
        `discard_hook_give_up_seconds` have passed since that hook's own first failure
        (`discard_hook_first_failed_at`, {name: time}, dropped once it is delivered) abandons
        the hooks that failed (`_abandon`, reason `timeout`) instead of backing off.
        """
        delivered, owed = self._owed_hooks(session)
        if not owed:
            return True
        if self._hook_budget_spent():
            return False  # this pass spent its hook time (discard_hook_pass_budget_seconds)
        status = session.status
        next_at = status.get("discard_hook_next_at")
        if next_at:
            async with self.pool.connection() as conn:
                cur = await conn.execute("SELECT %s::timestamptz > now()", (next_at,))
                if (await cur.fetchone())[0]:
                    return False
        failed = ""
        skipped = False
        errors: dict[str, str] = {}
        for hook in owed:
            if self._hook_budget_spent() and hook is not owed[0]:
                skipped = True  # a hook already ran this pass: the rest stays owed, unrecorded
                continue
            result = await self._run_hook(hook, session)
            if result.ok:
                delivered[hook.name] = (await self._db_now()).isoformat()
                last = hook is owed[-1] and not failed and not skipped
                await self._write_status(
                    session,
                    {"discard_hook_delivered": dict(delivered)},
                    session.observed_generation,
                    drop=_HOOK_FAILURE_KEYS if last else (),
                )
            else:
                log.warning(
                    "discard hook %s of session %s failed: %s",
                    hook.name,
                    session.slug,
                    result.reason,
                )
                failed = failed or f"{hook.name}: {result.reason}"
                errors[hook.name] = result.reason
        if not failed:
            return not skipped  # a skipped hook is still owed: nothing may be deleted unexported
        now = await self._db_now()
        # one give-up clock per hook: a hook's first failure is kept only while it keeps failing
        # (or is skipped), so a hook that recovers never spends another hook's window
        kept = status.get("discard_hook_first_failed_at")
        kept = kept if isinstance(kept, dict) else {}
        firsts = {n: t for n, t in kept.items() if n not in delivered and n not in errors}
        firsts |= {n: kept.get(n) or now.isoformat() for n in errors}
        give_up = self.settings.discard_hook_give_up_seconds
        expired = {
            n: e
            for n, e in errors.items()
            if (now - datetime.fromisoformat(firsts[n])).total_seconds() >= give_up
        }
        if len(expired) == len(errors):
            await self._abandon(session, expired, "timeout")
            return not skipped
        retrying = {n: e for n, e in errors.items() if n not in expired}
        failures = int(status.get("discard_hook_failures", 0)) + 1
        delay = self.settings.retry_delay_seconds(failures)
        retry = {
            "discard_hook_failed": next(f"{n}: {e}" for n, e in retrying.items()),
            "discard_hook_failures": failures,
            "discard_hook_next_at": (now + timedelta(seconds=delay)).isoformat(),
            "discard_hook_first_failed_at": {n: t for n, t in firsts.items() if n not in expired},
            "discard_hook_errors": {
                n: e
                for n, e in (dict(status.get("discard_hook_errors") or {}) | errors).items()
                if n not in delivered and n not in expired
            },
        }
        if expired:
            await self._abandon(session, expired, "timeout", retry)
        else:
            await self._write_status(session, retry, session.observed_generation)
        return False

    async def _abandon(
        self,
        session: Session,
        errors: dict[str, str],
        reason: str,
        retry: dict[str, Any] | None = None,
    ) -> None:
        """Give up on the hooks named in `errors` ({name: last error or ""}): each is recorded in
        `discard_hook_abandoned` ({reason, error, at}), so it is owed no more, and the retry
        state is replaced by `retry` (the hooks still failing; none: dropped). The phase message
        names the hooks discarded without, and the status write NOTIFYs `status` as a hook
        failure does. Logged at WARNING; the discard then goes on and keeps the workspace PVC
        (`_retain_pvc`). `reason` is `timeout` or `forced`."""
        at = (await self._db_now()).isoformat()
        abandoned = dict(session.status.get("discard_hook_abandoned") or {})
        for name, error in errors.items():
            abandoned[name] = {"reason": reason, "error": error or None, "at": at}
            log.warning(
                "discard hook %s of session %s abandoned (%s), discarding without it and keeping"
                " the workspace PVC; last error: %s",
                name,
                session.slug,
                reason,
                error or "none",
            )
        await self._write_status(
            session,
            {
                "phase": "deleting",
                "message": _abandoned_note(abandoned),
                "discard_hook_abandoned": abandoned,
            }
            | (retry or {}),
            session.observed_generation,
            drop=_HOOK_FAILURE_KEYS,
        )

    async def _abandon_forced(self, session: Session) -> Session:
        """A forced discard abandons every owed hook at once, unrun, with reason `forced`."""
        owed = self._owed_hooks(session)[1]
        if session.discard_forced_at is None or not owed:
            return session
        last = session.status.get("discard_hook_errors") or {}
        await self._abandon(session, {h.name: str(last.get(h.name) or "") for h in owed}, "forced")
        return await self._reload(session)

    def _owed_hooks(self, session: Session) -> tuple[dict[str, str], list[DiscardHook]]:
        """(what was delivered, the hooks still owed: neither delivered nor abandoned). A
        non-object `discard_hook_delivered` (written before hooks were tracked by name) reads as
        nothing delivered."""
        delivered = session.status.get("discard_hook_delivered")
        if not isinstance(delivered, dict):
            delivered = {}
        abandoned = session.status.get("discard_hook_abandoned") or {}
        owed = [
            h
            for h in resolve_hooks(session, self.settings)
            if h.name not in delivered and h.name not in abandoned
        ]
        return dict(delivered), owed

    async def _run_hook(self, hook: DiscardHook, session: Session) -> HookResult:
        """One hook fed a fresh export stream; a crash is a failure, never an escape."""
        started = time.monotonic()
        self._hook_attempted[session.id] = started
        try:
            async with self.pool.connection() as conn:
                try:
                    return await run_discard_hook(
                        hook,
                        session,
                        export_session_history(conn, session.id),
                        self._hook_deadline(),
                        connect_timeout=self.settings.discard_hook_connect_timeout_seconds,
                        read_timeout=self.settings.discard_hook_read_timeout_seconds,
                        transport=self.hook_transport,
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("discard hook %s of session %s crashed", hook.name, session.slug)
                    return HookResult(False, f"{type(exc).__name__}: {exc}")
        finally:
            self._hook_spent += time.monotonic() - started

    # ------------------------------------------------------------------ an active session

    async def _converge(self, session: Session) -> None:
        ns, slug = self.settings.namespace, session.slug
        if not self.settings.agent_api_url and session.id not in self._warned_no_agent_url:
            self._warned_no_agent_url.add(session.id)
            log.warning("ORCH_AGENT_API_URL unset: session %s agents get no ORCH_AGENT_URL", slug)
        session = await self._pin_workspace_pvc(session)
        replicas = session.spec.replicas
        applied, since = await self._drain_replicas(session)
        rendered = (
            session
            if applied == replicas
            else replace(session, spec=session.spec.model_copy(update={"replicas": applied}))
        )
        objs = render_session(rendered, self.settings, tokens_for(self.settings, session.id))

        foreign, live = await self._foreign(session, objs)
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
            reason = "scale-down"
            if sessions_gate.hold and obj["kind"] == "StatefulSet":
                obj = {**obj, "spec": {**obj["spec"], "replicas": 0}}
                reason = "held for the transcripts restore"
            await self._apply(obj, live.get(obj_key(obj)), reason)
        desired = {obj_key(o) for o in objs}
        await self._prune(slug, desired, reason="no longer rendered")
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

    async def _pin_workspace_pvc(self, session: Session) -> Session:
        """Record the session's workspace PVC name in `status.workspace_pvc` once, before any
        object of it is rendered. The deterministic `agent-<slug>-workspace` unless a PVC kept
        from an earlier session of the same slug (`RETAINED_LABEL`, `_retain_pvc`) holds that
        name: then `suffixed_workspace_pvc`, so the new session neither mounts the old workspace
        nor stops on a foreign object, and the backup stays untouched. Decided once: the slug is
        unique, so a retained PVC of it exists before this session's row does, and an operator
        deleting it later never moves the workspace."""
        if session.status.get("workspace_pvc"):
            return session
        ns, slug = self.settings.namespace, session.slug
        name = object_names(slug)["pvc"]
        pvc = await self.kube.get_object("PersistentVolumeClaim", ns, name)
        labels = ((pvc or {}).get("metadata") or {}).get("labels") or {}
        if pvc is not None and not is_owned(pvc, slug) and RETAINED_LABEL in labels:
            name = suffixed_workspace_pvc(slug, session.id)
            log.warning(
                "session %s: PersistentVolumeClaim %s is kept from an earlier session of this"
                " slug; using %s as the workspace",
                slug,
                object_names(slug)["pvc"],
                name,
            )
        await self._write_status(session, {"workspace_pvc": name}, session.observed_generation)
        return await self._reload(session)

    async def _apply(self, obj: Obj, live: Obj | None, reason: str) -> None:
        """Server-side apply `obj` unless it is unchanged; `reason` names a scale-down.

        Skipped when this process last applied the very same object (its fingerprint) and
        `live` still holds it (no `drift`). The fingerprint catches a field the render dropped
        (still owned live, so `drift` alone would miss it); `drift` catches an out-of-band edit
        and a deleted object, and is logged. A fresh process applies each object once. A Secret
        is never read: an unchanged one is skipped for `SECRET_REASSERT_SECONDS`.

        A live StatefulSet not on OnDelete is first moved there.
        """
        key = obj_key(obj)
        kind, ns, name = key
        slug = obj["metadata"]["labels"][SESSION_LABEL]
        fingerprint = hashlib.sha256(json.dumps(obj, sort_keys=True).encode()).hexdigest()
        last, now = self._applied.get(key), time.monotonic()
        if last is not None and last[0] == fingerprint:
            if kind == "Secret":
                if now - last[1] < SECRET_REASSERT_SECONDS:
                    return
            else:
                where = "the object is gone" if live is None else drift(obj, live)
                if where is None:
                    return
                log.info(
                    "session %s: %s %s drifted at %s, applying it again", slug, kind, name, where
                )
        if kind == "StatefulSet" and live is not None:
            spec = live.get("spec") or {}
            if (spec.get("updateStrategy") or {}).get("type") != "OnDelete":
                log.info("statefulset %s: moving to updateStrategy OnDelete", name)
                await self.kube.merge_patch("StatefulSet", ns, name, TO_ON_DELETE)
            before, after = spec.get("replicas", 0), obj["spec"]["replicas"]
            if after < before:
                log.info(
                    "session %s: scaling StatefulSet %s from %d to %d replicas (%s), removing"
                    " pods %s",
                    slug,
                    name,
                    before,
                    after,
                    reason,
                    ", ".join(f"{name}-{i}" for i in range(after, before)),
                )
        self._applied.pop(key, None)
        await self.kube.apply(obj, field_manager=FIELD_MANAGER, force=True)
        self._applied[key] = (fingerprint, now)

    async def _draining(self, session_id: UUID, replicas: int) -> list[tuple[int, bool]]:
        """(ordinal, busy) of each agent at or above `replicas` that is not gone (D3).

        Busy: its `agent_status.state` is `busy`, a message dispatched to it is still
        `dispatching` or `verifying`, a background subagent of it is still running (`BUSY`),
        or it is the session's current leader (D8: its pod is
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

    def _scopes(self, kind: str) -> list[str]:
        """The namespaces the reconciler lists `kind` in: RoleBindings in every allowlisted
        namespace, ClusterRoleBindings at cluster scope ("") only while cluster access is
        enabled, ServiceAccounts in `settings.namespace` only while either access is configured
        (the chart's `serviceaccounts` rule has the same condition), everything else in
        `settings.namespace`. Its RBAC reaches nowhere else, so nothing is ever listed outside
        the Role (no 403 loop)."""
        s = self.settings
        if kind == "RoleBinding":
            return list(s.kube_namespace_access_namespaces)
        if kind == "ClusterRoleBinding":
            return [""] if s.kube_cluster_access_enabled else []
        if kind == "ServiceAccount":
            granted = s.kube_cluster_access_enabled or s.kube_namespace_access_namespaces
            return [s.namespace] if granted else []
        return [s.namespace]

    async def _foreign(
        self, session: Session, objs: list[Obj]
    ) -> tuple[list[str], dict[ObjKey, Obj]]:
        """`foreign_object <kind>/<name>` for each rendered name held by an unlabelled object,
        and the live objects read on the way by `obj_key`, for `_apply`. A live ServiceAccount
        or binding of another session id (the slug reused) is deleted, logged and left out of
        the live objects, so it is recreated, never adopted."""
        slug = session.slug
        found: list[str] = []
        live: dict[ObjKey, Obj] = {}
        for obj in objs:
            key = obj_key(obj)
            kind, ns, name = key
            if kind not in GUARDED_KINDS:
                continue  # Secrets cannot be read; the reserved prefix protects them
            existing = await self.kube.get_object(kind, ns, name)
            if existing is None:
                continue
            if not is_owned(existing, slug):
                found.append(f"foreign_object {kind}/{name}")
                continue
            owner = (existing["metadata"].get("labels") or {}).get(SESSION_ID_LABEL)
            if kind in KUBE_KINDS and owner != str(session.id):
                log.warning(
                    "session %s: %s %s/%s belongs to session %s, recreating",
                    slug,
                    kind,
                    ns,
                    name,
                    owner,
                )
                self._applied.pop(key, None)
                await self.kube.delete(kind, ns, name)
                continue
            live[key] = existing
        return found, live

    async def _prune(
        self,
        slug: str,
        desired: set[ObjKey],
        order: tuple[str, ...] = PRUNE_ORDER,
        *,
        reason: str,
    ) -> None:
        selector = session_labels(slug)
        for kind in order:
            for ns in self._scopes(kind):
                for obj in await self.kube.list_objects(kind, ns, selector):
                    key: ObjKey = (kind, ns, obj["metadata"]["name"])
                    if key not in desired and is_owned(obj, slug):
                        own = ns in ("", self.settings.namespace)
                        where = key[2] if own else f"{key[2]} in {ns}"
                        log.info("session %s: deleting %s %s (%s)", slug, kind, where, reason)
                        self._applied.pop(key, None)
                        await self.kube.delete(kind, ns, key[2])

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
        session = await self._abandon_forced(session)
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
            session = await self._reload(session)  # a hook may have been abandoned meanwhile
            await self._write_status(
                session,
                _deleting("waiting for the pre-discard hook", session.status),
                session.observed_generation,
            )
            return
        ns, slug = self.settings.namespace, session.slug
        session = await self._reload(session)  # pre_discard may have abandoned a hook
        if session.status.get("discard_hook_abandoned"):
            await self._retain_pvc(session)
        await self._prune(slug, set(), DISCARD_ORDER, reason="discard")
        secret = object_names(slug)["secret"]
        self._applied.pop(("Secret", ns, secret), None)
        await self.kube.delete("Secret", ns, secret)  # absent is fine

        remaining = await self._remaining(slug)
        if remaining:
            await self._write_status(
                session,
                _deleting("waiting for " + ", ".join(remaining), session.status),
                session.generation,
            )
            return
        async with self.pool.connection() as conn, conn.transaction():
            await sessions.purge(conn, session.id)
            await notify(conn, "status", {"session_id": str(session.id)})
        log.info("session %s discarded", slug)

    async def _retain_pvc(self, session: Session) -> None:
        """Keep the workspace PVC of a discard that abandoned a hook: the history that hook did
        not get is lost with the row, so the workspace (workdir and the agents' /sessions
        transcripts) stays as the backup. The PVC loses the session and managed-by labels, so
        `_prune` and `_remaining` no longer see it, and gains `RETAINED_LABEL` plus annotations.
        An operator deletes it once it is no longer needed. Idempotent: a PVC already detached
        is not found by its labels and left alone."""
        ns, slug = self.settings.namespace, session.slug
        name = workspace_pvc(slug, session.status)
        pvc = await self.kube.get_object("PersistentVolumeClaim", ns, name)
        if pvc is None or not is_owned(pvc, slug):
            return
        hooks = ", ".join(sorted(session.status["discard_hook_abandoned"]))
        reasons = sorted(
            {v.get("reason", "") for v in session.status["discard_hook_abandoned"].values()}
        )
        patch: Obj = {
            "metadata": {
                "labels": {SESSION_LABEL: None, MANAGED_BY_LABEL: None, RETAINED_LABEL: slug},
                "annotations": {
                    f"{RETAINED_LABEL}-session-id": str(session.id),
                    f"{RETAINED_LABEL}-reason": (
                        f"discard hook abandoned ({', '.join(reasons)}): {hooks}"
                    ),
                },
            }
        }
        await self.kube.merge_patch("PersistentVolumeClaim", ns, name, patch)
        self._applied.pop(("PersistentVolumeClaim", ns, name), None)
        log.warning(
            "session %s: kept PersistentVolumeClaim %s as the backup (discard hook %s abandoned)",
            slug,
            name,
            hooks,
        )

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
        hook_owed = bool(self._owed_hooks(session)[1])
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
                    await self._apply(obj, sts, "discard")

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
            for scope in self._scopes(kind):
                for obj in await self.kube.list_objects(kind, scope, selector):
                    if is_owned(obj, slug):
                        left.append(f"{kind}/{obj['metadata']['name']}")
        for pod in await self.kube.list_pods(ns, selector):
            if is_owned(pod, slug):
                left.append(f"Pod/{pod['metadata']['name']}")
        return left

    # ------------------------------------------------------------------ restart commands

    async def run_commands(self) -> None:
        """Every queued restart, then the self-restart notes, then every queued rebootstrap."""
        await self._run_restarts()
        await self._release_notes()
        await self._run_rebootstraps()

    async def _run_restarts(self) -> None:
        while True:
            async with self.pool.connection() as conn:
                cmd = await queue.claim_command(conn, ["restart"])
            if cmd is None:
                return
            try:
                async with self._session_lock(cmd.session_id):
                    status, result = await self._restart(cmd)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("restart command %s failed", cmd.id)
                status, result = "failed", {"error": describe_exception(exc)}
            async with self.pool.connection() as conn:
                await queue.complete_command(conn, cmd.id, status, result)

    async def _release_notes(self) -> None:
        async with self.pool.connection() as conn:
            await queue.release_ready_notes(conn)

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
        log.info(
            "session %s: deleting pod %s (restart command %d%s)",
            slug,
            agent.pod_name,
            cmd.id,
            f", issued by session {cmd.issued_by_session}" if cmd.issued_by_session else "",
        )
        await self.kube.delete_pod(ns, agent.pod_name)
        warning = _never_bootstrapped(agent)
        if warning:
            log.warning("session %s: restart of pod %s: %s", slug, agent.pod_name, warning)
            return "done", {"pod": agent.pod_name, "warning": warning}
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
                async with self._session_lock(cmd.session_id):
                    status, result = await self._rebootstrap(cmd)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("rebootstrap command %s failed", cmd.id)
                status, result = "failed", {"error": describe_exception(exc)}
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
                {"phase": "error", "message": describe_exception(exc)},
                session.observed_generation,
            )
        except SessionNotFound:
            pass
        except Exception:
            log.exception("could not record the error of session %s", session.slug)
