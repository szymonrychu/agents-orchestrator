"""Checks as Jobs (D10): a queued check becomes a Job on the session PVC."""

from __future__ import annotations

import pytest
import pytest_asyncio
from factories import insert_check
from fakes.fake_kube import FakeKube
from psycopg.rows import dict_row

from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.labels import SESSION_LABEL, session_labels
from agents_orchestrator.k8s.render import render_session, workdir_mount, workdir_path
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.checks import CHECK_LABEL, TAIL_BYTES, TAIL_LINES
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for

pytestmark = pytest.mark.asyncio

IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"


class Env:
    def __init__(self, pool, dsn: str, **settings) -> None:
        self.pool = pool
        self.kube = FakeKube(namespace="agents")
        self.settings = Settings(
            database_url=dsn,
            master_key="k" * 32,
            agent_image=IMAGE,
            image_pull_secret=settings.pop("image_pull_secret", ""),
            **settings,
        )
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=None)

    async def session(self, slug: str = "alpha", **spec) -> sessions.Session:
        async with self.pool.connection() as conn:
            return await sessions.create(conn, slug, SessionSpec(**spec))

    async def check(self, check_id: int) -> dict:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute("SELECT * FROM checks WHERE id = %s", (check_id,))
            return await cur.fetchone()

    async def run(self, session, command: str = "make test") -> tuple[int, str]:
        row = await insert_check(self.pool, session.id, command=command)
        await self.rec.reconcile_once()
        return row["id"], f"agent-{session.slug}-check-{row['id']}"


@pytest_asyncio.fixture
async def env(pool, dsn) -> Env:
    return Env(pool, dsn)


async def test_job_rendered_with_pvc_mount(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c, "pytest -q")

    obj = env.kube.get("Job", job)
    sts = next(
        o
        for o in render_session(c, env.settings, tokens_for(env.settings, c.id))
        if o["kind"] == "StatefulSet"
    )
    pod = obj["spec"]["template"]["spec"]
    container = pod["containers"][0]
    assert container["command"] == ["sh", "-c", "pytest -q"]
    assert container["image"] == IMAGE
    assert container["volumeMounts"] == [workdir_mount(c.spec)]
    assert (
        container["volumeMounts"]
        == sts["spec"]["template"]["spec"]["containers"][0]["volumeMounts"][:1]
    )
    assert {
        "name": "workspace",
        "persistentVolumeClaim": {"claimName": "agent-alpha-workspace"},
    } in (pod["volumes"])
    assert workdir_mount(c.spec)["mountPath"] == "/workdir"
    assert workdir_mount(c.spec)["subPath"] == "workdir"
    assert pod["securityContext"] == {
        "runAsUser": 1000,
        "fsGroup": 1000,
        "fsGroupChangePolicy": "OnRootMismatch",
    }
    assert pod["restartPolicy"] == "Never"
    assert "imagePullSecrets" not in pod
    assert obj["spec"]["backoffLimit"] == 0
    assert obj["spec"]["activeDeadlineSeconds"] == 1800
    assert obj["spec"]["ttlSecondsAfterFinished"] == 3600
    row = await env.check(check_id)
    assert (row["status"], row["job_name"]) == ("running", job)


async def test_job_image_and_pull_secret(pool, dsn):
    e = Env(pool, dsn, check_image="img/check:1", image_pull_secret="harbor-pull")
    c = await e.session("alpha")
    _, job = await e.run(c)
    pod = e.kube.get("Job", job)["spec"]["template"]["spec"]
    assert pod["containers"][0]["image"] == "img/check:1"
    assert pod["imagePullSecrets"] == [{"name": "harbor-pull"}]


async def test_success_records_exit_and_log(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c)
    long_log = "\n".join(f"line {i}" for i in range(500))
    env.kube.finish_job(job, exit_code=0, log=long_log)
    await env.rec.reconcile_once()

    row = await env.check(check_id)
    assert (row["status"], row["exit_code"]) == ("passed", 0)
    lines = row["log_tail"].splitlines()
    assert len(lines) == TAIL_LINES == 200
    assert lines[-1] == "line 499"
    assert row["finished_at"] is not None


async def test_log_tail_capped_to_16kib(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c)
    env.kube.finish_job(job, exit_code=0, log=("x" * 1000 + "\n") * 100 + "last")
    await env.rec.reconcile_once()
    tail = (await env.check(check_id))["log_tail"]
    assert len(tail.encode()) <= TAIL_BYTES == 16 * 1024
    assert tail.endswith("last")


async def test_failure_records_exit_code(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c)
    env.kube.finish_job(job, exit_code=3, log="boom")
    await env.rec.reconcile_once()
    row = await env.check(check_id)
    assert (row["status"], row["exit_code"], row["log_tail"]) == ("failed", 3, "boom")


async def test_timeout_marks_failed(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c)
    env.kube.finish_job(job, deadline_exceeded=True, log="partial")
    await env.rec.reconcile_once()
    row = await env.check(check_id)
    assert (row["status"], row["exit_code"], row["log_tail"]) == ("failed", None, "timeout")


async def test_running_job_stays_running_and_vanished_job_fails(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c)
    await env.rec.reconcile_once()
    assert (await env.check(check_id))["status"] == "running"
    assert len(env.kube.applied("Job")) == 0  # created, never server-side applied
    await env.kube.delete("Job", "agents", job)
    await env.rec.reconcile_once()
    row = await env.check(check_id)
    assert (row["status"], row["exit_code"], row["log_tail"]) == ("failed", None, "job_missing")


async def test_foreign_job_is_not_adopted(env):
    c = await env.session("alpha")
    row = await insert_check(env.pool, c.id)
    env.kube.seed(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": f"agent-alpha-check-{row['id']}"},
            "spec": {"template": {"metadata": {}}},
        }
    )
    await env.rec.reconcile_once()
    got = await env.check(row["id"])
    assert (got["status"], got["log_tail"]) == ("failed", "foreign_object")


async def test_job_workdir_matches_session_workdir(env):
    c = await env.session("alpha", workdir="repo")
    _, job = await env.run(c)
    obj = env.kube.get("Job", job)
    container = obj["spec"]["template"]["spec"]["containers"][0]
    assert container["workingDir"] == workdir_path(c.spec) == "/workdir/repo"
    sts = next(
        o
        for o in render_session(c, env.settings, tokens_for(env.settings, c.id))
        if o["kind"] == "StatefulSet"
    )
    sts_mounts = sts["spec"]["template"]["spec"]["containers"][0]["volumeMounts"]
    assert container["volumeMounts"][0] in sts_mounts
    assert container["volumeMounts"][0]["mountPath"] == "/workdir"


async def test_job_in_namespace_labelled(env):
    c = await env.session("alpha")
    check_id, job = await env.run(c)
    obj = env.kube.get("Job", job)
    assert obj["metadata"]["namespace"] == env.settings.namespace == "agents"
    expected = {**session_labels("alpha"), CHECK_LABEL: str(check_id)}
    assert CHECK_LABEL == "agents.szymonrichert.pl/check"
    assert obj["metadata"]["labels"] == expected
    assert obj["spec"]["template"]["metadata"]["labels"] == expected
    assert obj["metadata"]["labels"][SESSION_LABEL] == "alpha"
    assert env.kube.forbidden == []


async def test_deleting_session_cancels_checks(env):
    c = await env.session("alpha")
    _, job = await env.run(c)
    await insert_check(env.pool, c.id, command="later")
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id)
    await env.rec.reconcile_once()

    assert not env.kube.has("Job", job)
    assert env.kube.names("Job") == []
    assert env.kube.names("Pod") == []
    assert job in env.kube.deleted("Job")
    assert env.kube.forbidden == []
    async with env.pool.connection() as conn:
        assert await sessions.get(conn, c.id) is None  # purged
    # the queued check was cancelled before the row's cascade removed it, never claimed
    assert env.kube.names("Job") == []


async def test_queued_check_cancelled_when_discard_waits(env):
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    queued = await insert_check(env.pool, c.id, command="later")

    class Hold(Reconciler):
        async def pre_discard(self, session):
            return False

    hold = Hold(env.pool, env.kube, env.settings)
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id)
    await hold.reconcile_once()
    assert (await env.check(queued["id"]))["status"] == "cancelled"
    assert env.kube.names("Job") == []
