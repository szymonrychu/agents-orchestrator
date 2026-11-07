"""A no-op reconcile pass patches nothing, and every pod or StatefulSet deletion says why.

Production (mx-99 #1039): the reconciler server-side applied every rendered object on every
pass, so the agent StatefulSet saw a PATCH each tick (a server no-op), and the restart, prune,
scale-down and discard paths deleted pods or StatefulSets without a log line naming the reason.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest_asyncio
from factories import insert_command, set_agent_status
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.kube import drift
from agents_orchestrator.k8s.labels import session_labels
from agents_orchestrator.reconciler import Reconciler, loop
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

KEY = "k" * 32
IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"
GUARDED = ("PersistentVolumeClaim", "ConfigMap", "Service", "StatefulSet")
LOGGERS = ("agents_orchestrator.reconciler.loop", "agents_orchestrator.reconciler.rollout")


class Env:
    def __init__(self, pool, dsn: str) -> None:
        self.pool = pool
        self.kube = FakeKube(namespace="agents", pods_ready=True)
        self.settings = Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE)
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=None)

    async def session(self, slug: str = "alpha", **spec) -> sessions.Session:
        async with self.pool.connection() as conn:
            return await sessions.create(conn, slug, SessionSpec(**spec))

    async def update(self, session_id, **spec) -> None:
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE sessions SET spec = %s, generation = generation + 1 WHERE id = %s",
                (Jsonb(SessionSpec(**spec).model_dump(mode="json", exclude_none=True)), session_id),
            )

    async def agents(self, session_id) -> list[agents_repo.Agent]:
        async with self.pool.connection() as conn:
            return await agents_repo.list_agents(conn, session_id)

    def sts_patches(self) -> list[str]:
        return [a.name for a in self.kube.applied("StatefulSet")]


@pytest_asyncio.fixture
async def env(pool, dsn) -> Env:
    return Env(pool, dsn)


def reasons(caplog) -> list[str]:
    return [r.getMessage() for r in caplog.records if r.name in LOGGERS and r.levelname == "INFO"]


# ---------------------------------------------------------------- deterministic render

_RENDER = """
import json, sys
sys.path[:0] = ["tests", "src"]
from test_render import FULL_WRAPPER, TOKENS, AGENT_URL, make_session, make_settings
from agents_orchestrator.k8s import render
session = make_session(
    wrapper=FULL_WRAPPER, replicas=2, max_replicas=3,
    env={"B": {"value": "2"}, "A": {"value": "$(B)", "expand": True}, "C": {"value": "c"}},
)
print(json.dumps(render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS)))
"""


def test_render_is_identical_across_processes_and_hash_seeds():
    root = Path(__file__).resolve().parent.parent
    outs = {
        subprocess.run(
            [sys.executable, "-c", _RENDER],
            cwd=root,
            env={**os.environ, "PYTHONHASHSEED": seed},
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        for seed in ("1", "2", "3")
    }
    assert len(outs) == 1
    assert json.loads(outs.pop())  # a real render, not an empty output


# ---------------------------------------------------------------- the semantic compare


def test_drift_ignores_server_defaults_and_omitted_zero_values():
    desired = {
        "spec": {
            "ports": [{"name": "http", "port": 8080}],
            "env": [{"name": "A", "value": ""}],
            "flag": False,
        }
    }
    live = {
        "metadata": {"uid": "u", "resourceVersion": "7"},
        "spec": {
            "ports": [{"name": "http", "port": 8080, "protocol": "TCP"}],
            "env": [{"name": "A"}],  # the API server omits an empty value
        },
        "status": {"replicas": 1},
    }
    assert drift(desired, live) is None


def test_drift_names_a_changed_value_or_list():
    desired = {"spec": {"replicas": 2, "env": [{"name": "A", "value": "1"}]}}
    one = [{"name": "A", "value": "1"}]
    assert drift(desired, {"spec": {"replicas": 1, "env": one}}) == ".spec.replicas"
    assert drift(desired, {"spec": {"replicas": 2, "env": [{"name": "A", "value": "2"}]}}) == (
        ".spec.env[0].value"
    )
    assert drift(desired, {"spec": {"replicas": 2, "env": [*one, {"name": "B"}]}}) == ".spec.env"
    assert drift(desired, {"spec": {"env": one}}) == ".spec.replicas"
    assert drift(desired, {"spec": {"replicas": "2", "env": one}}) == ".spec.replicas"


def test_drift_compares_resource_quantities_by_value():
    """The API server canonicalises quantities on write, so "0.5" is stored as "500m"."""
    desired = {
        "spec": {
            "containers": [{"resources": {"limits": {"cpu": "0.5", "memory": "1024Mi"}}}],
            "volumes": [{"emptyDir": {"sizeLimit": "0.5Ti"}}],
            "resources": {"requests": {"storage": "1e3"}},
            "env": [{"name": "A", "value": "1e3"}],
        }
    }
    live = {
        "spec": {
            "containers": [{"resources": {"limits": {"cpu": "500m", "memory": "1Gi"}}}],
            "volumes": [{"emptyDir": {"sizeLimit": "512Gi"}}],
            "resources": {"requests": {"storage": "1k"}},
            "env": [{"name": "A", "value": "1e3"}],
        }
    }
    assert drift(desired, live) is None


def test_drift_still_sees_a_changed_quantity_and_ignores_other_strings():
    desired = {"spec": {"containers": [{"resources": {"limits": {"cpu": "0.5"}}}]}}
    where = ".spec.containers[0].resources.limits.cpu"
    assert drift(desired, {"spec": {"containers": [{"resources": {"limits": {"cpu": "1"}}}]}}) == (
        where
    )
    bad = {"spec": {"containers": [{"resources": {"limits": {"cpu": "plenty"}}}]}}
    assert drift(desired, bad) == where
    # an env value that merely looks like a quantity is compared as text
    env = {"env": [{"name": "A", "value": "1e3"}]}
    assert drift(env, {"env": [{"name": "A", "value": "1000"}]}) == ".env[0].value"


# ---------------------------------------------------------------- no-op passes


async def test_noop_pass_patches_nothing(env):
    await env.session("alpha", replicas=2)
    await env.rec.reconcile_once()
    assert sorted(a.kind for a in env.kube.applied()) == sorted((*GUARDED, "Secret"))
    revision = env.kube.get("StatefulSet", "agent-alpha")["status"]["updateRevision"]

    env.kube.reset_records()
    await env.rec.reconcile_once()
    await env.rec.reconcile_once()
    assert env.kube.applied() == []
    assert env.kube.patches == [] and env.kube.deletes == []
    assert env.kube.get("StatefulSet", "agent-alpha")["status"]["updateRevision"] == revision


async def test_canonicalised_quantities_are_no_drift(env, caplog):
    """The API server rewrites "0.5" to "500m" and "1024Mi" to "1Gi"; that is not drift."""
    await env.session(
        "alpha",
        pvc_size="1024Mi",
        resources={"requests": {"cpu": "0.25"}, "limits": {"cpu": "0.5", "memory": "1024Mi"}},
    )
    await env.rec.reconcile_once()
    sts = env.kube.get("StatefulSet", "agent-alpha")
    container = sts["spec"]["template"]["spec"]["containers"][0]
    assert container["resources"]["limits"] == {"cpu": "500m", "memory": "1Gi"}
    assert container["resources"]["requests"]["cpu"] == "250m"
    pvc = env.kube.get("PersistentVolumeClaim", "agent-alpha-workspace")
    assert pvc["spec"]["resources"]["requests"]["storage"] == "1Gi"
    env.kube.reset_records()

    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
        await env.rec.reconcile_once()
    assert env.kube.applied() == []
    assert not [m for m in reasons(caplog) if "drifted" in m]


async def test_changed_quantity_is_still_applied(env):
    c = await env.session("alpha", resources={"limits": {"cpu": "0.5"}})
    await env.rec.reconcile_once()
    env.kube.reset_records()

    await env.update(c.id, resources={"limits": {"cpu": "1"}})
    await env.rec.reconcile_once()
    assert env.sts_patches() == ["agent-alpha"]
    sts = env.kube.get("StatefulSet", "agent-alpha")
    assert sts["spec"]["template"]["spec"]["containers"][0]["resources"]["limits"]["cpu"] == "1"


async def test_spec_change_patches_once(env):
    c = await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    env.kube.reset_records()

    await env.update(c.id, replicas=1, env={"ROLL": {"value": "2"}})
    await env.rec.reconcile_once()
    await env.rec.reconcile_once()
    assert env.sts_patches() == ["agent-alpha"]
    assert sorted(a.kind for a in env.kube.applied() if a.kind != "Secret") == [
        "ConfigMap",
        "StatefulSet",
    ]


async def test_removed_field_is_applied(env):
    """A field the render drops is still owned live, so only the fingerprint notices it."""
    c = await env.session("alpha", resources={"limits": {"memory": "2Gi"}})
    await env.rec.reconcile_once()
    env.kube.reset_records()

    await env.update(c.id)
    await env.rec.reconcile_once()
    assert env.sts_patches() == ["agent-alpha"]
    container = env.kube.get("StatefulSet", "agent-alpha")["spec"]["template"]["spec"][
        "containers"
    ][0]
    assert "resources" not in container


async def test_out_of_band_drift_is_reverted_and_logged(env, caplog):
    await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    env.kube.get("ConfigMap", "agent-alpha-config")["data"]["config.yaml"] = "edited"
    env.kube.reset_records()

    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
    assert [a.name for a in env.kube.applied()] == ["agent-alpha-config"]
    assert env.kube.get("ConfigMap", "agent-alpha-config")["data"]["config.yaml"] != "edited"
    line = next(m for m in reasons(caplog) if "drifted" in m)
    assert "session alpha" in line and "agent-alpha-config" in line
    assert ".data.config.yaml" in line


async def test_unchanged_secret_is_reasserted_only_after_the_interval(env, monkeypatch):
    """A Secret is never read, so a deleted one comes back on a timer, not on every pass."""
    await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    del env.kube.objects[("Secret", "agents", "agent-alpha-ccw")]
    env.kube.reset_records()

    await env.rec.reconcile_once()
    assert env.kube.applied() == []

    clock = loop.time.monotonic() + loop.SECRET_REASSERT_SECONDS
    monkeypatch.setattr(loop.time, "monotonic", lambda: clock)
    await env.rec.reconcile_once()
    assert [(a.kind, a.name) for a in env.kube.applied()] == [("Secret", "agent-alpha-ccw")]
    await env.rec.reconcile_once()
    assert len(env.kube.applied()) == 1


async def test_deleted_object_is_recreated(env):
    await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    del env.kube.objects[("Service", "agents", "agent-alpha")]
    env.kube.reset_records()

    await env.rec.reconcile_once()
    assert [a.name for a in env.kube.applied("Service")] == ["agent-alpha"]


async def test_new_reconciler_applies_once_then_settles(env):
    await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    env.rec = Reconciler(env.pool, env.kube, env.settings, wrapper_factory=None)
    env.kube.reset_records()

    await env.rec.reconcile_once()
    assert env.sts_patches() == ["agent-alpha"]  # a fresh process re-asserts its render once
    await env.rec.reconcile_once()
    assert env.sts_patches() == ["agent-alpha"]


# ---------------------------------------------------------------- every deletion says why


async def test_rollout_replacement_logs_both_revisions(env, caplog):
    c = await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    old = env.kube.get("Pod", "agent-alpha-0")["metadata"]["labels"]["controller-revision-hash"]
    agent = (await env.agents(c.id))[0]
    await set_agent_status(env.pool, agent.id, state="idle")
    await env.update(c.id, replicas=1, env={"ROLL": {"value": "2"}})

    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
    new = env.kube.get("StatefulSet", "agent-alpha")["status"]["updateRevision"]
    assert env.kube.deleted("Pod") == ["agent-alpha-0"]
    line = next(m for m in reasons(caplog) if "agent-alpha-0" in m)
    assert "session alpha" in line and "idle" in line and "outdated" in line
    assert old in line and new in line


async def test_restart_command_logs_its_reason(env, caplog):
    c = await env.session("alpha", replicas=2)
    await env.rec.reconcile_once()
    agent = next(a for a in await env.agents(c.id) if a.ordinal == 1)
    cmd = await insert_command(env.pool, agent.id, kind="restart")

    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
    assert env.kube.deleted("Pod") == ["agent-alpha-1"]
    line = next(m for m in reasons(caplog) if "agent-alpha-1" in m)
    assert "session alpha" in line and "restart" in line and f"command {cmd['id']}" in line


async def test_scale_down_logs_the_pods_it_removes(env, caplog):
    c = await env.session("alpha", replicas=3, max_replicas=3)
    await env.rec.reconcile_once()
    await env.update(c.id, replicas=1, max_replicas=3)

    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
    assert env.kube.get("StatefulSet", "agent-alpha")["spec"]["replicas"] == 1
    line = next(m for m in reasons(caplog) if "scale" in m)
    assert "session alpha" in line and "agent-alpha" in line
    assert "agent-alpha-1" in line and "agent-alpha-2" in line and "3 to 1" in line


async def test_prune_logs_each_deleted_object(env, caplog):
    await env.session("alpha")
    env.kube.seed(
        {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": "agent-alpha-old", "labels": session_labels("alpha")},
        }
    )
    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
    assert env.kube.deleted("StatefulSet") == ["agent-alpha-old"]
    line = next(m for m in reasons(caplog) if "agent-alpha-old" in m)
    assert "session alpha" in line and "StatefulSet" in line and "no longer rendered" in line


async def test_discard_logs_each_deletion(env, caplog):
    c = await env.session("alpha", replicas=2)
    await env.rec.reconcile_once()
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id)

    with caplog.at_level("INFO"):
        await env.rec.reconcile_once()
    assert "agent-alpha" in env.kube.deleted("StatefulSet")
    lines = reasons(caplog)
    sts = next(
        m for m in lines if "StatefulSet agent-alpha " in m or "StatefulSet/agent-alpha " in m
    )
    assert "session alpha" in sts and "discard" in sts


async def test_discard_forgets_the_secret_it_deleted(env):
    c = await env.session("alpha", replicas=1)
    await env.rec.reconcile_once()
    assert ("Secret", "agents", "agent-alpha-ccw") in env.rec._applied
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id)

    await env.rec.reconcile_once()
    assert "agent-alpha-ccw" in env.kube.deleted("Secret")
    assert ("Secret", "agents", "agent-alpha-ccw") not in env.rec._applied
