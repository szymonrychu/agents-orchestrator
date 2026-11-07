"""Idle-only replacement of outdated agent pods under `updateStrategy: OnDelete` (D1, D2).

The StatefulSet controller never replaces an agent pod by itself: a template change only moves
the StatefulSet's `status.updateRevision`. Every converge, `Replacer.step` compares each agent
pod's `controller-revision-hash` label with it and deletes at most one outdated pod of the
session, which the controller recreates on the new template:

- only while every agent pod of the session is Ready and none is terminating, so the previous
  replacement is Ready on the new revision before the next pod goes, and a terminating pod
  (often still Ready, still on its old revision) is never deleted twice
- never while its agent is in a turn (`TURN_BUSY`: state `busy`, a dispatch reservation, or a
  message dispatched to it is still `dispatching` or `verifying`) or leads (`LEADS`). There is
  no deadline for a turn: a pod running a deploy is never rolled by that deploy (D2)
- never while its agent runs background subagents (`BACKGROUND_BUSY`), until the rollout is
  `ORCH_ROLLOUT_MAX_DEFER_SECONDS` old (`since`): a background stop that never arrives does
  not hold a rollout forever. `BUSY` is both parts, the predicate the scale-down drain uses
- the leader last: once only the leader's pod is outdated (`leader_since`, the first such
  tick), the leader has `ORCH_ROLLOUT_HANDOVER_SECONDS` to hand over on its own (the
  dispatcher sends it a `rollout_due` note meanwhile); after a voluntary move its pod rolls
  as a member's. Once the window is over, the leader is not in a turn and an idle eligible
  member exists, a `handover` request with reason `rollout` is queued from the leader, the one
  request the reconciler inserts; the dispatcher's pass applies it only at the leader's true
  boundary (turn ended, background subagents reported, the latter only until the request is
  older than the cap), so leadership never moves mid-turn for a rollout; the old leader's pod
  is deleted only once `session_leaders` names another agent. A session of one agent rolls
  its pod when idle.

Before the delete the agent is marked unready, so the dispatcher stops picking it, and its idle
test is read again; a message claimed between that read and the delete is retried by the
verify path, never dropped. The step reports `status.rollout`
`{revision, pending, waiting, leader_pending, since, leader_since}`, None once nothing is
pending; `since` and `leader_since` (ISO, database clock) are carried from the stored value
while its `revision` matches, so they survive a reconciler failover.

Assumed, confirmed on a real Kubernetes cluster by orch-e2e-rollout-mid-turn: the controller
labels every pod it creates `controller-revision-hash: <the ControllerRevision name>`, the name
that `status.updateRevision` holds.
"""

from __future__ import annotations

import logging
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership
from agents_orchestrator.db.eligibility import BACKGROUND_BUSY, ELIGIBLE
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s.kube import KubeApi, Obj, is_owned, pod_ready
from agents_orchestrator.k8s.labels import object_names, prefix, session_labels
from agents_orchestrator.settings import Settings

log = logging.getLogger(__name__)

REVISION_LABEL = "controller-revision-hash"
ROLLOUT_REASON = "rollout"

# The in-turn predicate of an agent, shared by the scale-down drain and the replacer (reuse.md:
# one busy query). Aliases: `a` the agent, `s` its `agent_status` row (LEFT JOIN). It also holds
# an agent with a dispatch reservation: its last dispatch is not older than its free_since, the
# exact negation of the dispatcher's idle test (`db/queue.py` `_PICK`: `free_since >
# last_dispatched_at`), so a pod just handed a turn is never removed before the wrapper reports
# busy.
TURN_BUSY = (
    "COALESCE(s.state = 'busy', false)"
    " OR (a.last_dispatched_at IS NOT NULL"
    " AND NOT COALESCE(s.free_since > a.last_dispatched_at, false))"
    " OR EXISTS ("
    "SELECT 1 FROM messages m WHERE m.session_id = a.session_id"
    " AND m.agent_id = a.id AND m.status IN ('dispatching', 'verifying'))"
)
# Busy: in a turn or running background subagents (the drain; the replacer reads both apart).
BUSY = f"({TURN_BUSY}) OR {BACKGROUND_BUSY}"
LEADS = "EXISTS (SELECT 1 FROM session_leaders l WHERE l.agent_id = a.id)"

# Every live agent of a session: in a turn, running background work, leading, and able to take
# the lead from a leader whose pod rolls (eligible, idle and not in a turn; the pass ranks
# background-busy agents last itself).
_AGENTS = (
    "SELECT a.id, a.ordinal, a.pod_name, a.url, a.pod_phase, a.pod_ip,"
    f" ({TURN_BUSY}) AS turn_busy, {BACKGROUND_BUSY} AS background, ({LEADS}) AS leads,"
    f" ({ELIGIBLE}) AND COALESCE(s.state = 'idle', false) AND NOT ({TURN_BUSY}) AS can_lead"
    " FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id"
    " WHERE a.session_id = %s AND a.gone_at IS NULL ORDER BY a.ordinal"
)
_HELD = (
    f"SELECT ({TURN_BUSY}) AS turn_busy, {BACKGROUND_BUSY} AS background, ({LEADS}) AS leads"
    " FROM agents a LEFT JOIN agent_status s ON s.agent_id = a.id WHERE a.id = %s"
)
_QUEUED = (
    "SELECT 1 FROM leadership_requests WHERE session_id = %s AND status = 'queued'"
    " AND kind = 'handover' AND reason = %s"
)


@dataclass(frozen=True)
class Rollout:
    revision: str
    pending: list[int]  # ordinals whose pod is not on `revision`
    waiting: list[int]  # pending ordinals held because their agent is busy
    leader_pending: bool  # the leader's pod is pending and waits for the rollout handover
    since: str  # ISO time of the first tick `revision` was pending
    leader_since: str | None  # ISO time of the first tick only the leader's pod was pending

    def status(self) -> dict[str, Any]:
        return {
            "revision": self.revision,
            "pending": self.pending,
            "waiting": self.waiting,
            "leader_pending": self.leader_pending,
            "since": self.since,
            "leader_since": self.leader_since,
        }


def agent_pods(slug: str, pods: list[Obj]) -> dict[int, Obj]:
    """The session's own agent pods (`agent-<slug>-<ordinal>`) by ordinal, never a Job's."""
    base = prefix(slug) + "-"
    out: dict[int, Obj] = {}
    for pod in pods:
        name = pod["metadata"]["name"]
        suffix = name[len(base) :] if name.startswith(base) else ""
        if is_owned(pod, slug) and suffix.isdigit():
            out[int(suffix)] = pod
    return out


def terminating(pod: Mapping[str, Any]) -> bool:
    return bool((pod.get("metadata") or {}).get("deletionTimestamp"))


def _elapsed(now: datetime, since: str) -> float:
    return (now - datetime.fromisoformat(since)).total_seconds()


class Replacer:
    def __init__(self, pool: AsyncConnectionPool, kube: KubeApi, settings: Settings) -> None:
        self.pool = pool
        self.kube = kube
        self.settings = settings

    async def step(self, session: Session) -> dict[str, Any] | None:
        """Replace at most one outdated pod; the session's `status.rollout`, None when none.

        While the controller has not observed the StatefulSet's latest generation its
        `updateRevision` may be stale, so nothing is replaced and the stored value is kept.
        """
        ns, slug = self.settings.namespace, session.slug
        name = object_names(slug)["statefulset"]
        sts = await self.kube.get_object("StatefulSet", ns, name)
        if sts is None or not is_owned(sts, slug):
            return None
        if ((sts.get("spec") or {}).get("updateStrategy") or {}).get("type") != "OnDelete":
            return None
        status = sts.get("status") or {}
        revision = status.get("updateRevision")
        generation = (sts.get("metadata") or {}).get("generation")
        lagging = generation is not None and status.get("observedGeneration", 0) < generation
        if not revision or lagging:
            return session.status.get("rollout")

        target = session.spec.replicas
        listed = await self.kube.list_pods(ns, session_labels(slug))
        pods = {o: p for o, p in agent_pods(slug, listed).items() if o < target}
        pending = sorted(
            o
            for o, p in pods.items()
            if (p["metadata"].get("labels") or {}).get(REVISION_LABEL, revision) != revision
        )
        if not pending:
            return None
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(_AGENTS, (session.id,))
            agents = {r["ordinal"]: r for r in await cur.fetchall()}
            await cur.execute("SELECT now() AS now")
            clock = await cur.fetchone()
        assert clock is not None
        now: datetime = clock["now"]
        stored = session.status.get("rollout") or {}
        carried = stored if stored.get("revision") == revision else {}
        since = carried.get("since") or now.isoformat()
        overdue = _elapsed(now, since) >= self.settings.rollout_max_defer_seconds
        for agent in agents.values():
            agent["busy"] = agent["turn_busy"] or (agent["background"] and not overdue)
        single = target == 1
        leader = next((o for o, a in agents.items() if a["leads"]), None)
        leader_pending = not single and leader in pending
        alone = leader_pending and pending == [leader]
        rollout = Rollout(
            revision=revision,
            pending=pending,
            waiting=[o for o in pending if o in agents and agents[o]["busy"]],
            leader_pending=leader_pending,
            since=since,
            leader_since=(carried.get("leader_since") or now.isoformat()) if alone else None,
        )
        settled = len(pods) == target and all(
            pod_ready(p) and not terminating(p) for p in pods.values()
        )
        if settled:
            await self._replace_one(session, pods, agents, rollout, leader, single, now, overdue)
        return rollout.status()

    async def _replace_one(
        self,
        session: Session,
        pods: dict[int, Obj],
        agents: dict[int, dict[str, Any]],
        rollout: Rollout,
        leader: int | None,
        single: bool,
        now: datetime,
        overdue: bool,
    ) -> None:
        idle = [
            o
            for o in rollout.pending
            if o in agents and not agents[o]["busy"] and (single or not agents[o]["leads"])
        ]
        if idle:
            agent = agents[idle[0]]
            await self._delete(session, agent, pods[idle[0]], single, rollout.revision, overdue)
            return
        # the leader's own background work does not hold the request: the pass waits for it
        if rollout.leader_since is None or leader is None or agents[leader]["turn_busy"]:
            return
        if _elapsed(now, rollout.leader_since) < self.settings.rollout_handover_seconds:
            return  # the leader's window to hand over on its own
        if any(a["can_lead"] for o, a in agents.items() if o != leader):
            await self._queue_handover(session, agents[leader]["id"])

    async def _delete(
        self,
        session: Session,
        agent: dict[str, Any],
        pod: Obj,
        single: bool,
        revision: str,
        overdue: bool,
    ) -> None:
        """Mark the agent unready, read its idle test again, then delete its pod."""
        async with self.pool.connection() as conn:
            await agents_repo.upsert_agent(
                conn,
                session.id,
                agent["ordinal"],
                agent["pod_name"],
                agent["url"],
                agent["pod_phase"],
                False,
                agent["pod_ip"],
            )
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(_HELD, (agent["id"],))
                held = await cur.fetchone()
        if (
            held is None
            or held["turn_busy"]
            or (held["background"] and not overdue)
            or (held["leads"] and not single)
        ):
            return  # claimed meanwhile: the next converge marks it ready from its pod again
        name = pod["metadata"]["name"]
        log.info(
            "session %s: replacing idle outdated pod %s (revision %s, update revision %s)",
            session.slug,
            name,
            (pod["metadata"].get("labels") or {}).get(REVISION_LABEL),
            revision,
        )
        await self.kube.delete_pod(self.settings.namespace, name)

    async def _queue_handover(self, session: Session, agent_id: Any) -> None:
        """Queue one rollout handover from the current leader, unless one is queued already."""
        async with self.pool.connection() as conn:
            current = await leadership.current_leader(conn, session.id)
            if current is None or current.agent_id != agent_id:
                return
            if await (await conn.execute(_QUEUED, (session.id, ROLLOUT_REASON))).fetchone():
                return
            await leadership.insert_request(
                conn,
                session.id,
                kind="handover",
                from_agent_id=current.agent_id,
                to_agent_id=None,
                reason=ROLLOUT_REASON,
                epoch=current.epoch,
            )
        log.info("session %s: rollout handover queued from %s", session.slug, current.pod_name)
