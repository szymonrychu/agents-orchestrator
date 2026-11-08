"""Rollouts under `updateStrategy: OnDelete`: the reconciler's idle-only replacer (D1, D2).

The StatefulSet controller never replaces an agent pod by itself; the reconciler deletes one
outdated pod (its `controller-revision-hash` is not the StatefulSet's `status.updateRevision`)
at a time per session, only while its agent is idle, the leader last after a rollout handover.
"""

from __future__ import annotations

import asyncio
import copy
import inspect
import uuid
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from factories import insert_message, set_agent_status
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import leadership, sessions
from agents_orchestrator.db.eligibility import BACKGROUND_BUSY
from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.k8s.render import HASH_ANNOTATION, render_session
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler import rollout as replacer
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for

KEY = "k" * 32
IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"
NS = "agents"
REVISION = "controller-revision-hash"  # the label the StatefulSet controller puts on its pods
TO_ON_DELETE = {"spec": {"updateStrategy": {"type": "OnDelete", "rollingUpdate": None}}}


def make_settings(dsn: str = "postgresql://u:p@db/orch", **kw) -> Settings:
    return Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE, **kw)


class Env:
    def __init__(self, pool, dsn: str, **settings) -> None:
        self.pool = pool
        self.kube = FakeKube(namespace=NS, pods_ready=True)
        self.settings = make_settings(dsn, **settings)
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=None)
        self.slug = "alpha"

    async def session(self, slug: str = "alpha", **spec) -> Session:
        self.slug = slug
        async with self.pool.connection() as conn:
            return await sessions.create(conn, slug, SessionSpec(**spec))

    async def update(self, session_id, **spec) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE sessions SET spec = %s, generation = generation + 1 WHERE id = %s",
                (Jsonb(SessionSpec(**spec).model_dump(mode="json", exclude_none=True)), session_id),
            )

    async def get(self, session_id) -> Session:
        async with self.pool.connection() as conn:
            got = await sessions.get(conn, session_id)
        assert got is not None
        return got

    async def agents(self, session_id) -> dict[int, agents_repo.Agent]:
        async with self.pool.connection() as conn:
            return {a.ordinal: a for a in await agents_repo.list_agents(conn, session_id)}

    async def rows(self, query: str, params: tuple = ()) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(query, params)
            return await cur.fetchall()

    async def execute(self, query: str, params: tuple = ()) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(query, params)

    async def tick(self, times: int = 1) -> None:
        for _ in range(times):
            await self.rec.reconcile_once()

    def pod_name(self, ordinal: int) -> str:
        return f"agent-{self.slug}-{ordinal}"

    def pod(self, ordinal: int) -> dict:
        return self.kube.get("Pod", self.pod_name(ordinal))

    def pod_revision(self, ordinal: int) -> str:
        return self.pod(ordinal)["metadata"]["labels"][REVISION]

    def uid(self, ordinal: int) -> str:
        return self.pod(ordinal)["metadata"]["uid"]

    def ready(self, ordinal: int) -> None:
        self.kube.set_pod(self.pod_name(ordinal), ready=True, phase="Running")

    def revision(self) -> str:
        return self.kube.get("StatefulSet", f"agent-{self.slug}")["status"]["updateRevision"]

    def pod_deletes(self) -> list[str]:
        return self.kube.deleted("Pod")

    async def rollout(self, session_id) -> dict | None:
        return (await self.get(session_id)).status.get("rollout")

    async def handovers(self, session_id) -> list[dict]:
        return await self.rows(
            "SELECT kind, from_agent_id, to_agent_id, reason, epoch, status"
            " FROM leadership_requests WHERE session_id = %s ORDER BY id",
            (session_id,),
        )


@pytest_asyncio.fixture
async def env(pool, dsn) -> Env:
    return Env(pool, dsn)


@pytest_asyncio.fixture
async def eager(pool, dsn) -> Env:
    """No leader window: the rollout handover is queued on the first tick it may be."""
    return Env(pool, dsn, rollout_handover_seconds=0)


async def lead(env: Env, session_id, agent_id) -> None:
    """Write the leader row the way the dispatcher does (the reconciler only reads it)."""
    await env.execute(
        "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
        " VALUES (%s, %s, 1, now(), 'initial')",
        (session_id, agent_id),
    )


async def rolling(env: Env, replicas: int = 3, *, slug: str = "alpha", **spec):
    """A session whose `replicas` pods are Ready and idle, then a template change.

    Every agent has a wrapper session and reports `idle`, so each could lead. Replacement pods
    come up unready (`pods_ready` off) until the test marks them Ready. Returns the session, its
    agents by ordinal and the revision every pod is on before the change.
    """
    spec.setdefault("max_replicas", replicas)
    env.kube.pods_ready = True
    c = await env.session(slug, replicas=replicas, **spec)
    await env.tick()
    agents = await env.agents(c.id)
    for agent in agents.values():
        await env.execute(
            "UPDATE agents SET wrapper_session_id = %s WHERE id = %s",
            (f"ws-{agent.ordinal}", agent.id),
        )
        await set_agent_status(env.pool, agent.id, state="idle")
    old = env.revision()
    assert {env.pod_revision(i) for i in range(replicas)} == {old}
    env.kube.pods_ready = False
    env.kube.reset_records()
    await env.update(c.id, replicas=replicas, env={"ROLL": {"value": "2"}}, **spec)
    return c, agents, old


# ---------------------------------------------------------------- render and the fake


def test_statefulset_update_strategy_on_delete():
    session = Session(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        slug="alpha",
        spec=SessionSpec(replicas=2),
        generation=1,
        observed_generation=0,
        desired="active",
        status={},
        created_at=datetime(2026, 10, 28, tzinfo=UTC),
    )
    settings = make_settings()
    (sts,) = [
        o
        for o in render_session(session, settings, tokens_for(settings, session.id))
        if o["kind"] == "StatefulSet"
    ]
    # the strategy and the pod template are one object: one server-side apply carries both
    assert sts["spec"]["updateStrategy"] == {"type": "OnDelete"}
    assert sts["spec"]["template"]["spec"]["containers"]


def _sts(name: str, *, env: str, replicas: int = 2, strategy: str | None = "OnDelete") -> dict:
    spec: dict = {
        "replicas": replicas,
        "selector": {"matchLabels": {"app": name}},
        "template": {
            "metadata": {"labels": {"app": name}},
            "spec": {"containers": [{"name": "c", "env": [{"name": "V", "value": env}]}]},
        },
    }
    if strategy is not None:
        spec["updateStrategy"] = {"type": strategy}
    return {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": {"name": name, "namespace": NS, "labels": {"app": name}},
        "spec": spec,
    }


async def test_fake_kube_on_delete_revisions():
    kube = FakeKube(namespace=NS, pods_ready=True)

    def revision_of(pod: str) -> str:
        return kube.get("Pod", pod)["metadata"]["labels"][REVISION]

    await kube.apply(_sts("agent-x", env="1"))
    sts = kube.get("StatefulSet", "agent-x")
    rev1 = sts["status"]["updateRevision"]
    assert rev1.startswith("agent-x-")
    assert sts["metadata"]["generation"] == sts["status"]["observedGeneration"] == 1
    assert [revision_of(p) for p in ("agent-x-0", "agent-x-1")] == [rev1, rev1]
    uids = {p: kube.get("Pod", p)["metadata"]["uid"] for p in ("agent-x-0", "agent-x-1")}

    # an unchanged apply is no new revision and no new generation
    await kube.apply(_sts("agent-x", env="1"))
    assert kube.get("StatefulSet", "agent-x")["status"]["updateRevision"] == rev1
    assert kube.get("StatefulSet", "agent-x")["metadata"]["generation"] == 1

    # OnDelete: a template change leaves every existing pod on its old revision
    await kube.apply(_sts("agent-x", env="2"))
    sts = kube.get("StatefulSet", "agent-x")
    rev2 = sts["status"]["updateRevision"]
    assert rev2 != rev1
    assert sts["metadata"]["generation"] == sts["status"]["observedGeneration"] == 2
    assert [revision_of(p) for p in ("agent-x-0", "agent-x-1")] == [rev1, rev1]
    assert {p: kube.get("Pod", p)["metadata"]["uid"] for p in uids} == uids

    # a deleted pod returns at the update revision, the other stays where it was
    assert await kube.delete_pod(NS, "agent-x-0")
    assert revision_of("agent-x-0") == rev2
    assert kube.get("Pod", "agent-x-0")["metadata"]["uid"] != uids["agent-x-0"]
    assert revision_of("agent-x-1") == rev1

    # a graceful delete keeps the pod, terminating on its old revision, until it is gone
    kube.graceful_pod_deletes = True
    assert await kube.delete_pod(NS, "agent-x-1")
    terminating = kube.get("Pod", "agent-x-1")
    assert terminating["metadata"]["deletionTimestamp"]
    assert revision_of("agent-x-1") == rev1
    kube.finish_termination("agent-x-1")
    assert revision_of("agent-x-1") == rev2
    assert "deletionTimestamp" not in kube.get("Pod", "agent-x-1")["metadata"]

    # RollingUpdate, the API server's default with a defaulted partition: a template change
    # makes the controller replace every pod at the update revision, recorded in `rolled`
    await kube.apply(_sts("agent-y", env="1", strategy=None))
    assert kube.get("StatefulSet", "agent-y")["spec"]["updateStrategy"] == {
        "type": "RollingUpdate",
        "rollingUpdate": {"partition": 0},
    }
    before = {p: kube.get("Pod", p)["metadata"]["uid"] for p in ("agent-y-0", "agent-y-1")}
    await kube.apply(_sts("agent-y", env="2", strategy="RollingUpdate"))
    rev = kube.get("StatefulSet", "agent-y")["status"]["updateRevision"]
    assert kube.rolled == ["agent-y-0", "agent-y-1"]
    assert all(kube.get("Pod", p)["metadata"]["uid"] != uid for p, uid in before.items())
    assert [revision_of(p) for p in before] == [rev, rev]

    # a paused controller lags: neither the status nor the pods follow the new template
    kube.controller_paused = True
    await kube.apply(_sts("agent-x", env="3"))
    sts = kube.get("StatefulSet", "agent-x")
    assert sts["metadata"]["generation"] == 3
    assert sts["status"] == {"updateRevision": rev2, "observedGeneration": 2}


async def test_fake_kube_strategy_patch_rolls_nothing():
    kube = FakeKube(namespace=NS, pods_ready=True)

    def revision_of(pod: str) -> str:
        return kube.get("Pod", pod)["metadata"]["labels"][REVISION]

    await kube.apply(_sts("agent-z", env="1", strategy=None))
    rev1 = kube.get("StatefulSet", "agent-z")["status"]["updateRevision"]
    uids = {p: kube.get("Pod", p)["metadata"]["uid"] for p in ("agent-z-0", "agent-z-1")}

    # server-side apply keeps the defaulted, unowned partition next to OnDelete: rejected
    with pytest.raises(KubeError, match="only allowed for updateStrategy 'RollingUpdate'"):
        await kube.apply(_sts("agent-z", env="2"))
    assert kube.get("StatefulSet", "agent-z")["spec"]["updateStrategy"]["type"] == "RollingUpdate"

    # a strategy-only merge patch changes no template: the controller rolls nothing
    out = await kube.merge_patch("StatefulSet", NS, "agent-z", TO_ON_DELETE)
    sts = kube.get("StatefulSet", "agent-z")
    assert out == sts and sts["spec"]["updateStrategy"] == {"type": "OnDelete"}
    assert sts["status"]["updateRevision"] == rev1
    assert sts["metadata"]["generation"] == sts["status"]["observedGeneration"] == 2
    assert kube.patches == [("StatefulSet", "agent-z", TO_ON_DELETE)]

    # from then on a template change leaves every pod where it was
    await kube.apply(_sts("agent-z", env="2"))
    assert kube.get("StatefulSet", "agent-z")["status"]["updateRevision"] != rev1
    assert kube.rolled == []
    assert {p: kube.get("Pod", p)["metadata"]["uid"] for p in uids} == uids
    assert [revision_of(p) for p in uids] == [rev1, rev1]


# ---------------------------------------------------------------- the deploy of this epic


def previous_release(env: Env, session: Session) -> tuple[dict, dict]:
    """(previous, target): the session's StatefulSet as the release before OnDelete rendered it.

    No updateStrategy (so RollingUpdate with a defaulted partition) and another config hash,
    as the default `restart_continue_prompt` of this release changes every session's config.
    """
    (target,) = [
        o
        for o in render_session(session, env.settings, tokens_for(env.settings, session.id))
        if o["kind"] == "StatefulSet"
    ]
    previous = copy.deepcopy(target)
    del previous["spec"]["updateStrategy"]
    previous["spec"]["template"]["metadata"]["annotations"][HASH_ANNOTATION] = "previous"
    return previous, target


async def test_rolling_update_statefulset_moves_to_on_delete_without_a_roll(env):
    """The deploy that introduces OnDelete rolls no pod by the controller (D1, amended).

    Before the apply the reconciler moves the live RollingUpdate StatefulSet to OnDelete with a
    strategy-only merge patch, which changes no template; the apply then brings the new
    template, the pods stay on the old revision and only the replacer rolls them, one idle pod
    at a time.
    """
    c = await env.session("alpha", replicas=3, max_replicas=3)
    previous, target = previous_release(env, await env.get(c.id))
    await env.kube.apply(previous)
    assert env.kube.get("StatefulSet", "agent-alpha")["spec"]["updateStrategy"] == {
        "type": "RollingUpdate",
        "rollingUpdate": {"partition": 0},
    }
    old = env.revision()
    uids = {i: env.uid(i) for i in range(3)}
    env.kube.reset_records()
    env.kube.pods_ready = False

    await env.tick()
    assert env.kube.patches == [("StatefulSet", "agent-alpha", TO_ON_DELETE)]
    assert env.kube.get("StatefulSet", "agent-alpha")["spec"]["updateStrategy"] == {
        "type": "OnDelete"
    }
    assert [a.obj for a in env.kube.applied("StatefulSet")] == [target]
    new = env.revision()
    assert new != old
    # the controller replaced nothing; the replacer took exactly one idle pod
    assert env.kube.rolled == []
    assert env.pod_deletes() == ["agent-alpha-0"]
    assert [env.pod_revision(i) for i in (1, 2)] == [old, old]
    assert [env.uid(i) for i in (1, 2)] == [uids[1], uids[2]]
    assert env.pod_revision(0) == new

    for ordinal in range(3):
        env.ready(ordinal)
        await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1", "agent-alpha-2"]
    assert [env.pod_revision(i) for i in range(3)] == [new, new, new]
    assert await env.rollout(c.id) is None
    # an OnDelete StatefulSet is never patched again, and the controller still rolled nothing
    assert env.kube.patches == [("StatefulSet", "agent-alpha", TO_ON_DELETE)]
    assert env.kube.rolled == []


async def test_discard_moves_a_rolling_update_statefulset_before_scaling_down(pool, dsn):
    # a session discarded across the deploy: the quiesce scale-down moves it to OnDelete first
    env = Env(pool, dsn, discard_hook_command="true", discard_drain_seconds=0)
    c = await env.session("alpha", replicas=2)
    previous, _ = previous_release(env, await env.get(c.id))
    await env.kube.apply(previous)
    env.kube.reset_records()
    env.kube.controller_paused = True  # the pods linger, so the discard stays in its quiesce
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id)

    await env.tick()
    sts = env.kube.get("StatefulSet", "agent-alpha")
    assert env.kube.patches == [("StatefulSet", "agent-alpha", TO_ON_DELETE)]
    assert sts["spec"]["updateStrategy"] == {"type": "OnDelete"}
    assert sts["spec"]["replicas"] == 0
    assert env.kube.rolled == []


# ---------------------------------------------------------------- the replacer


async def test_idle_outdated_pods_replaced_one_at_a_time(env):
    c, _, old = await rolling(env, replicas=3)
    await env.tick()
    new = env.revision()
    assert new != old
    assert env.pod_deletes() == ["agent-alpha-0"]
    assert env.pod_revision(0) == new
    assert [env.pod_revision(i) for i in (1, 2)] == [old, old]

    await env.tick(3)  # the replacement is not Ready yet: nothing else goes
    assert env.pod_deletes() == ["agent-alpha-0"]

    env.ready(0)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1"]
    await env.tick(2)
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1"]

    env.ready(1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1", "agent-alpha-2"]
    env.ready(2)
    await env.tick(2)
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1", "agent-alpha-2"]
    assert [env.pod_revision(i) for i in range(3)] == [new, new, new]
    assert await env.rollout(c.id) is None


async def test_terminating_pod_is_never_deleted_twice(env):
    # a real delete leaves the pod terminating, still Ready and on its old revision, for the
    # grace period: it is never deleted again and no other pod goes meanwhile
    c, _, old = await rolling(env, replicas=2)
    env.kube.graceful_pod_deletes = True
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0"]
    assert env.pod(0)["metadata"]["deletionTimestamp"] and env.pod_revision(0) == old
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-0"]
    agents = await env.agents(c.id)
    assert agents[0].ready is False  # a terminating pod is never picked

    env.kube.finish_termination("agent-alpha-0")
    await env.tick(2)  # back on the new revision, not Ready yet
    assert env.pod_deletes() == ["agent-alpha-0"]
    env.ready(0)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1"]


async def test_busy_outdated_pod_not_replaced(env):
    c, agents, _ = await rolling(env, replicas=2)
    await set_agent_status(env.pool, agents[0].id, state="busy")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-1"]
    rollout = await env.rollout(c.id)
    assert rollout is not None and (rollout["pending"], rollout["waiting"]) == ([0], [0])

    await set_agent_status(env.pool, agents[0].id, state="idle")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]


async def test_outdated_pod_with_inflight_message_not_replaced(env):
    c, agents, _ = await rolling(env, replicas=2)
    dispatching = await insert_message(env.pool, c.id, status="dispatching", agent_id=agents[0].id)
    await insert_message(env.pool, c.id, status="verifying", agent_id=agents[1].id)
    await env.tick(3)
    assert env.pod_deletes() == []
    rollout = await env.rollout(c.id)
    assert rollout is not None and (rollout["pending"], rollout["waiting"]) == ([0, 1], [0, 1])

    await env.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (dispatching["id"],))
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0"]


async def _dispatched_at(env: Env, agent_id, offset: str) -> None:
    """Stamp the agent's last dispatch `offset` (an interval) after its free_since."""
    await env.execute(
        "UPDATE agents a SET last_dispatched_at = s.free_since + %s::interval"
        " FROM agent_status s WHERE s.agent_id = a.id AND a.id = %s",
        (offset, agent_id),
    )


async def test_replacer_skips_agent_with_dispatch_reservation(env):
    # the dispatcher already handed agent 0 a turn (its last dispatch is newer than free_since)
    # but the wrapper has not reported busy yet: the pod must not be deleted mid-handoff
    c, agents, _ = await rolling(env, replicas=2)
    await _dispatched_at(env, agents[0].id, "1 microsecond")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-1"]
    rollout = await env.rollout(c.id)
    assert rollout is not None and (rollout["pending"], rollout["waiting"]) == ([0], [0])

    # the turn ends: free_since moves past the dispatch and the reservation is gone
    await set_agent_status(env.pool, agents[0].id, state="idle")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]


async def test_replacer_replaces_idle_agent_without_reservation(env):
    # last dispatch older than free_since (and a never dispatched agent, agent 1): both idle
    c, agents, old = await rolling(env, replicas=2)
    await _dispatched_at(env, agents[0].id, "-1 second")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0"]
    env.ready(0)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0", "agent-alpha-1"]


async def test_busy_outdated_pod_kept_across_ticks(pool, dsn):
    # D2: the deployer's pod is busy for the whole apply and smoke turn, so the deploy never
    # rolls it; no deadline forces it, however long the drain deadline is exceeded by
    env = Env(pool, dsn, scale_drain_seconds=0.5)
    c, agents, old = await rolling(env, replicas=1)
    await set_agent_status(env.pool, agents[0].id, state="busy")
    uid = env.uid(0)
    await env.tick(20)
    await asyncio.sleep(0.6)
    await env.tick(20)
    assert env.pod_deletes() == []
    assert env.uid(0) == uid and env.pod_revision(0) == old
    rollout = await env.rollout(c.id)
    assert rollout is not None and (rollout["pending"], rollout["waiting"]) == ([0], [0])

    # the same in a hive: the busy peer stays while the idle one rolls
    env2 = Env(pool, dsn, scale_drain_seconds=0.5)
    c2, agents2, old2 = await rolling(env2, replicas=2, slug="bravo")
    await set_agent_status(env2.pool, agents2[1].id, state="busy")
    uid2 = env2.uid(1)
    await env2.tick()
    env2.ready(0)
    await env2.tick(10)
    await asyncio.sleep(0.6)
    await env2.tick(10)
    assert env2.pod_deletes() == ["agent-bravo-0"]
    assert env2.uid(1) == uid2 and env2.pod_revision(1) == old2


async def test_leader_rolled_last_after_handover(eager):
    env = eager
    c, agents, _ = await rolling(env, replicas=3)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]  # the leader's pod is skipped
    env.ready(1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-2"]
    assert await env.handovers(c.id) == []  # pod 2 was not current yet
    rollout = await env.rollout(c.id)
    assert rollout is not None and rollout["leader_pending"] is True

    env.ready(2)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-2"]
    expected = {
        "kind": "handover",
        "from_agent_id": agents[0].id,
        "to_agent_id": None,
        "reason": "rollout",
        "epoch": 1,
        "status": "queued",
    }
    assert await env.handovers(c.id) == [expected]
    await env.tick(2)  # queued once, and the leader's pod is held while it still leads
    assert await env.handovers(c.id) == [expected]
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-2"]

    async with env.pool.connection() as conn:  # the dispatcher applies it like any handover
        change = await leadership.leadership_pass(conn, c.id)
    assert change is not None and change.reason == "handover: rollout"
    assert change.agent_id in (agents[1].id, agents[2].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-2", "agent-alpha-0"]
    env.ready(0)
    await env.tick()
    assert await env.rollout(c.id) is None
    rows = await env.rows("SELECT agent_id FROM session_leaders WHERE session_id = %s", (c.id,))
    assert rows == [{"agent_id": change.agent_id}]


async def test_rollout_handover_waits_for_an_idle_leader(eager):
    env = eager
    # like the lease and compaction handovers: leadership never moves mid-turn for a rollout
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)

    await set_agent_status(env.pool, agents[0].id, state="busy")
    await env.tick(3)
    assert await env.handovers(c.id) == []
    rollout = await env.rollout(c.id)
    assert rollout is not None and rollout["leader_pending"] is True
    assert (rollout["pending"], rollout["waiting"]) == ([0], [0])

    # a message still dispatching to the leader holds it the same way
    await set_agent_status(env.pool, agents[0].id, state="idle")
    held = await insert_message(env.pool, c.id, status="dispatching", agent_id=agents[0].id)
    await env.tick(2)
    assert await env.handovers(c.id) == []

    await env.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (held["id"],))
    await env.tick()
    assert [r["reason"] for r in await env.handovers(c.id)] == ["rollout"]
    await env.tick(2)  # queued once
    assert [r["reason"] for r in await env.handovers(c.id)] == ["rollout"]
    assert env.pod_deletes() == ["agent-alpha-1"]


async def test_queued_rollout_handover_applied_at_the_leaders_turn_boundary(eager):
    env = eager
    # queued while the leader was idle, then the leader claims a message before the pass: the
    # request stays queued until the turn ends, so leadership never moves mid-turn
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    env.ready(1)
    await env.tick()
    assert [r["status"] for r in await env.handovers(c.id)] == ["queued"]

    async def lpass():
        async with env.pool.connection() as conn:
            return await leadership.leadership_pass(conn, c.id)

    async def leader_id():
        got = await env.rows("SELECT agent_id FROM session_leaders WHERE session_id = %s", (c.id,))
        return got[0]["agent_id"]

    claimed = await insert_message(env.pool, c.id, status="dispatching", agent_id=agents[0].id)
    for status in ("dispatching", "verifying"):
        await env.execute("UPDATE messages SET status = %s WHERE id = %s", (status, claimed["id"]))
        assert await lpass() is None
        assert await leader_id() == agents[0].id
        assert [r["status"] for r in await env.handovers(c.id)] == ["queued"]
    await env.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (claimed["id"],))
    await set_agent_status(env.pool, agents[0].id, state="busy")
    assert await lpass() is None
    assert await leader_id() == agents[0].id
    assert [r["status"] for r in await env.handovers(c.id)] == ["queued"]
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]  # the leader's pod is held meanwhile

    await set_agent_status(env.pool, agents[0].id, state="idle")
    change = await lpass()
    assert change is not None
    assert (change.agent_id, change.reason) == (agents[1].id, "handover: rollout")
    assert [r["status"] for r in await env.handovers(c.id)] == ["applied"]
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]


async def test_leader_held_without_idle_eligible_member(eager):
    env = eager
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)

    # a busy peer cannot take the lead (a dead one is revived: test_dead_current_peer_revived)
    await set_agent_status(env.pool, agents[1].id, state="busy")
    await env.tick(3)
    assert await env.handovers(c.id) == []
    assert env.pod_deletes() == ["agent-alpha-1"]
    rollout = await env.rollout(c.id)
    assert rollout is not None and rollout["leader_pending"] is True
    assert rollout["pending"] == [0]

    # an idle peer without a wrapper session is not eligible either
    await env.execute("UPDATE agents SET wrapper_session_id = NULL WHERE id = %s", (agents[1].id,))
    await set_agent_status(env.pool, agents[1].id, state="idle")
    await env.tick(2)
    assert await env.handovers(c.id) == []

    await env.execute("UPDATE agents SET wrapper_session_id = 'ws' WHERE id = %s", (agents[1].id,))
    await env.tick()
    assert [r["reason"] for r in await env.handovers(c.id)] == ["rollout"]
    assert env.pod_deletes() == ["agent-alpha-1"]


async def test_single_agent_session_rolls_when_idle(env):
    c, agents, old = await rolling(env, replicas=1)
    await set_agent_status(env.pool, agents[0].id, state="busy")
    await env.tick(3)
    assert env.pod_deletes() == []
    await set_agent_status(env.pool, agents[0].id, state="idle")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-0"]
    assert env.pod_revision(0) == env.revision() != old

    # a hive scaled to one agent that leads: its pod rolls when idle, with no handover
    c2, agents2, _ = await rolling(env, replicas=1, slug="bravo", max_replicas=3)
    await lead(env, c2.id, agents2[0].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-bravo-0"]
    assert await env.handovers(c2.id) == []


async def test_status_exposes_pending_roll(env):
    c, agents, _ = await rolling(env, replicas=3)
    await set_agent_status(env.pool, agents[2].id, state="busy")
    await env.tick()
    new = env.revision()
    first = await env.rollout(c.id)
    since = first.pop("since")
    assert first == {
        "revision": new,
        "pending": [0, 1, 2],
        "waiting": [2],
        "leader_pending": False,
        "leader_since": None,
    }
    env.ready(0)
    await env.tick()
    assert (await env.rollout(c.id))["pending"] == [1, 2]
    env.ready(1)
    await env.tick()
    assert await env.rollout(c.id) == {
        "revision": new,
        "pending": [2],
        "waiting": [2],
        "leader_pending": False,
        "since": since,
        "leader_since": None,
    }
    await set_agent_status(env.pool, agents[2].id, state="idle")
    await env.tick()
    env.ready(2)
    await env.tick()
    assert "rollout" not in (await env.get(c.id)).status


def test_drain_and_rollout_share_busy_predicate():
    from agents_orchestrator.reconciler import loop, rollout

    assert rollout.BUSY in loop._DRAINING
    assert f"({rollout.TURN_BUSY}) OR {BACKGROUND_BUSY}" == rollout.BUSY
    # the replacer reads the turn and the background part apart: background work stops holding
    # a member once the rollout is older than the cap, a turn never does
    for query in (rollout._AGENTS, rollout._HELD):
        assert rollout.TURN_BUSY in query and BACKGROUND_BUSY in query
    assert rollout.LEADS in loop._DRAINING
    assert rollout.LEADS in rollout._AGENTS
    # spelled once: neither module respells the in-flight message test
    inflight = "m.status IN ('dispatching', 'verifying')"
    assert inflight in rollout.BUSY
    sources = inspect.getsource(loop) + inspect.getsource(rollout)
    assert sources.count(inflight) == 1


async def test_permission_mode_edit_rolls_idle_pods_and_waits_for_busy(env):
    """A permission-mode edit changes the config hash: idle pods roll, a busy one waits."""
    env.kube.pods_ready = True
    c = await env.session("alpha", replicas=2, max_replicas=2)
    await env.tick()
    agents = await env.agents(c.id)
    for agent in agents.values():
        await env.execute(
            "UPDATE agents SET wrapper_session_id = %s WHERE id = %s",
            (f"ws-{agent.ordinal}", agent.id),
        )
        await set_agent_status(env.pool, agent.id, state="idle")
    old = env.revision()
    await set_agent_status(env.pool, agents[0].id, state="busy")
    env.kube.pods_ready = False
    env.kube.reset_records()
    async with env.pool.connection() as conn:
        await sessions.set_permission_mode(conn, c.id, "auto")
    await env.tick()
    assert env.revision() != old
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-1"]
    await set_agent_status(env.pool, agents[0].id, state="idle")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]


# ---------------------------------------------------------------- background work and the window


def _running() -> dict[str, str]:
    """A background subagent started just now (the ingester's `background_subagents`)."""
    return {"sub-1": datetime.now(UTC).isoformat()}


async def _age(env: Env, session_id, key: str, seconds: float) -> None:
    """Move `status.rollout.<key>` `seconds` into the past (the database clock)."""
    await env.execute(
        "UPDATE sessions SET status = jsonb_set(status, ARRAY['rollout', %s],"
        " to_jsonb((now() - make_interval(secs => %s))::text)) WHERE id = %s",
        (key, seconds, session_id),
    )


async def test_member_with_background_not_rolled(env):
    c, agents, _ = await rolling(env, replicas=2)
    await set_agent_status(env.pool, agents[0].id, state="idle", background_subagents=_running())
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-1"]
    rollout = await env.rollout(c.id)
    assert rollout is not None and (rollout["pending"], rollout["waiting"]) == ([0], [0])

    await set_agent_status(env.pool, agents[0].id, state="idle")  # the subagent reported
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]


async def test_member_background_ignored_after_cap(env):
    # a background stop that never arrives must not block a rollout forever
    c, agents, _ = await rolling(env, replicas=2)
    await set_agent_status(env.pool, agents[0].id, state="idle", background_subagents=_running())
    await env.tick()
    env.ready(1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    await _age(env, c.id, "since", env.settings.rollout_max_defer_seconds - 60)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]  # not yet

    await _age(env, c.id, "since", env.settings.rollout_max_defer_seconds + 1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]

    # a turn holds the member past the cap all the same
    c2, agents2, _ = await rolling(env, replicas=2, slug="bravo")
    await set_agent_status(env.pool, agents2[0].id, state="busy")
    await env.tick()
    env.ready(1)
    await _age(env, c2.id, "since", env.settings.rollout_max_defer_seconds + 1)
    await env.tick(2)
    assert env.pod_deletes() == ["agent-bravo-1"]


async def test_leader_window_before_forced_handover(env):
    assert env.settings.rollout_handover_seconds == 1800
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)
    await env.tick(3)  # only the leader is pending: its window opens, nothing is forced
    assert await env.handovers(c.id) == []
    assert (await env.rollout(c.id))["leader_since"] is not None

    await _age(env, c.id, "leader_since", 1800 - 60)
    await env.tick()
    assert await env.handovers(c.id) == []

    await _age(env, c.id, "leader_since", 1800 + 1)
    await env.tick(3)  # queued once
    assert [(r["reason"], r["status"]) for r in await env.handovers(c.id)] == [
        ("rollout", "queued")
    ]
    assert env.pod_deletes() == ["agent-alpha-1"]


async def test_status_carries_since_and_leader_since(env):
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    first = await env.rollout(c.id)
    assert first is not None and first["since"] and first["leader_since"] is None
    revision = first["revision"]
    env.ready(1)
    await env.tick()
    second = await env.rollout(c.id)
    assert second["since"] == first["since"]  # carried while the revision matches
    leader_since = second["leader_since"]
    assert leader_since is not None and leader_since >= first["since"]
    await env.tick(2)
    third = await env.rollout(c.id)
    assert (third["since"], third["leader_since"]) == (first["since"], leader_since)

    # a newer template: a new revision starts its own clocks
    await env.update(c.id, replicas=2, max_replicas=2, env={"ROLL": {"value": "3"}})
    await env.tick()
    fourth = await env.rollout(c.id)
    assert fourth["revision"] != revision
    assert fourth["since"] > first["since"] and fourth["leader_since"] is None


async def test_voluntary_move_in_window_rolls_old_leader_as_member(env):
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    env.ready(1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    assert (await env.rollout(c.id))["leader_pending"] is True

    # the leader hands over on its own inside the window (the dispatcher writes the row)
    await env.execute(
        "UPDATE session_leaders SET agent_id = %s, epoch = epoch + 1,"
        " reason = 'handover: voluntary' WHERE session_id = %s",
        (agents[1].id, c.id),
    )
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-0"]
    assert await env.handovers(c.id) == []  # no forced handover was needed


# ---------------------------------------------------------- a dead peer on the current revision


async def _age_revived(env: Env, session_id, ordinal: int, seconds: float) -> None:
    """Move `status.rollout.revived.<ordinal>` `seconds` into the past (the database clock)."""
    await env.execute(
        "UPDATE sessions SET status = jsonb_set(status, ARRAY['rollout', 'revived', %s],"
        " to_jsonb((now() - make_interval(secs => %s))::text)) WHERE id = %s",
        (str(ordinal), seconds, session_id),
    )


async def _leader_alone_with_dead_peer(env: Env):
    """Two agents, the leader 0 the last outdated pod, peer 1 current but dead."""
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1"]
    env.ready(1)
    await set_agent_status(env.pool, agents[1].id, state="dead")
    return c, agents


async def test_dead_current_peer_revived(eager):
    env = eager
    c, agents = await _leader_alone_with_dead_peer(env)
    await env.tick()
    # the dead peer can never lead: its pod is replaced so its agent resumes, the leader's kept
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]
    assert await env.handovers(c.id) == []
    rollout = await env.rollout(c.id)
    assert rollout["leader_pending"] is True and list(rollout["revived"]) == ["1"]

    # once per revision: still dead after its new pod is Ready, it is not replaced again
    env.ready(1)
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]
    assert list((await env.rollout(c.id))["revived"]) == ["1"]

    # it comes back eligible: the forced handover proceeds as usual
    await set_agent_status(env.pool, agents[1].id, state="idle")
    await env.tick()
    assert [r["reason"] for r in await env.handovers(c.id)] == ["rollout"]
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]


async def test_dead_peer_revived_only_after_the_leader_window(env):
    c, agents = await _leader_alone_with_dead_peer(env)
    await env.tick(2)
    assert env.pod_deletes() == ["agent-alpha-1"]
    await _age(env, c.id, "leader_since", env.settings.rollout_handover_seconds + 1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]


async def test_lone_leader_pod_swapped_at_an_idle_boundary(eager):
    env = eager
    c, agents = await _leader_alone_with_dead_peer(env)
    await env.tick()
    env.ready(1)
    await env.tick(2)  # revived, still dead, inside the revive grace: the leader is kept
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]
    await _age_revived(env, c.id, 1, replacer.REVIVE_GRACE_SECONDS + 1)

    # the leader is the only live member: its own pod rolls, but never in an open turn ...
    await set_agent_status(env.pool, agents[0].id, state="busy")
    await env.tick(2)
    held = await insert_message(env.pool, c.id, status="dispatching", agent_id=agents[0].id)
    await set_agent_status(env.pool, agents[0].id, state="idle")
    await env.tick(2)
    await env.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (held["id"],))
    # ... nor with a background subagent inside the BACKGROUND_BUSY bound, even past the cap
    await set_agent_status(env.pool, agents[0].id, state="idle", background_subagents=_running())
    await _age(env, c.id, "since", env.settings.rollout_max_defer_seconds + 1)
    await env.tick(2)
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]

    await set_agent_status(env.pool, agents[0].id, state="idle")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1", "agent-alpha-0"]
    assert await env.handovers(c.id) == []  # no successor exists: no request
    # the pass finds no successor and keeps the leader row for the recreated pod
    async with env.pool.connection() as conn:
        assert await leadership.leadership_pass(conn, c.id) is None
    rows = await env.rows("SELECT agent_id FROM session_leaders WHERE session_id = %s", (c.id,))
    assert rows == [{"agent_id": agents[0].id}]
    env.ready(0)
    await env.tick()
    assert await env.rollout(c.id) is None


async def test_peer_dead_mid_turn_revived_then_lone_leader_swapped(eager):
    # the peer died in a turn it had accepted (busy -> dead, no idle transition): its dispatch
    # reservation stays, yet a dead agent is in no turn, so it neither blocks its revive nor
    # the lone-leader swap
    env = eager
    c, agents = await _leader_alone_with_dead_peer(env)
    await _dispatched_at(env, agents[1].id, "1 microsecond")
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]
    assert list((await env.rollout(c.id))["revived"]) == ["1"]

    env.ready(1)
    await env.tick(2)  # still dead inside the revive grace: the leader is kept
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]
    await _age_revived(env, c.id, 1, replacer.REVIVE_GRACE_SECONDS + 1)
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1", "agent-alpha-0"]
    assert await env.handovers(c.id) == []


async def test_dead_peer_with_inflight_message_not_revived(eager):
    # a message still `dispatching` to the dead peer holds its revive until verify settles it
    env = eager
    c, agents = await _leader_alone_with_dead_peer(env)
    held = await insert_message(env.pool, c.id, status="dispatching", agent_id=agents[1].id)
    await env.tick(2)
    assert env.pod_deletes() == ["agent-alpha-1"]
    await env.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (held["id"],))
    await env.tick()
    assert env.pod_deletes() == ["agent-alpha-1", "agent-alpha-1"]


async def test_lone_leader_kept_while_a_live_peer_is_busy(eager):
    env = eager
    c, agents, _ = await rolling(env, replicas=2)
    await lead(env, c.id, agents[0].id)
    await env.tick()
    env.ready(1)
    await set_agent_status(env.pool, agents[1].id, state="busy")
    await env.tick(3)
    assert env.pod_deletes() == ["agent-alpha-1"]  # a live peer will take over: no swap
