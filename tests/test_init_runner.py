"""Init scripts run once per session as one Job, recorded in sessions.status.init (D2, D3)."""

from __future__ import annotations

import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

import pytest
import pytest_asyncio
from factories import insert_command
from fakes.fake_kube import FakeKube
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.k8s.labels import session_labels
from agents_orchestrator.k8s.render import workdir_mount, workdir_path
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.checks import (
    DEADLINE_GRACE_SECONDS,
    DEADLINE_SECONDS,
    TAIL_LINES,
)
from agents_orchestrator.reconciler.init import (
    INIT_LABEL,
    WORKDIR_ENV,
    InitGate,
    init_job_name,
    render_init_job,
    render_init_sh,
)
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import INIT_DIR, INIT_SCRIPTS_MAX, SessionSpec, init_scripts_hash
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient

pytestmark = pytest.mark.asyncio

KEY = "k" * 32
IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"


class Env:
    def __init__(self, pool, dsn: str, fake: FakeWrapper | None = None, **settings) -> None:
        self.pool, self.fake = pool, fake
        self.kube = FakeKube(namespace="agents")
        self.settings = Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE, **settings)
        factory = self._factory if fake is not None else None
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=factory)

    def _factory(self, url: str, token: str):
        return WrapperClient(self.fake.base_url, token)

    async def session(self, slug: str = "alpha", **spec) -> sessions.Session:
        async with self.pool.connection() as conn:
            c = await sessions.create(conn, slug, SessionSpec(**spec))
        if self.fake is not None:
            self.fake.token = tokens_for(self.settings, c.id).auth
        return c

    async def tick(self, times: int = 1) -> None:
        for _ in range(times):
            await self.rec.reconcile_once()

    async def status(self, session_id) -> dict:
        async with self.pool.connection() as conn:
            return (await sessions.get(conn, session_id)).status

    async def init(self, session_id) -> dict | None:
        return (await self.status(session_id)).get("init")

    async def set_scripts(self, session_id, scripts: list[str]) -> None:
        """An edit of spec.init_scripts the way the init scripts API writes it."""
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE sessions SET spec = jsonb_set(spec, '{init_scripts}', %s) WHERE id = %s",
                (Jsonb(scripts), session_id),
            )

    async def patch_init(self, session_id, **fields) -> None:
        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
            status = dict(c.status)
            status["init"] = {**status.get("init", {}), **fields}
            await sessions.set_status(conn, session_id, status, c.observed_generation)

    async def drop_init(self, session_id) -> None:
        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
            status = {k: v for k, v in c.status.items() if k != "init"}
            await sessions.set_status(conn, session_id, status, c.observed_generation)

    async def agents(self, session_id) -> list[agents_repo.Agent]:
        async with self.pool.connection() as conn:
            return await agents_repo.list_agents(conn, session_id)

    def creates(self, name: str) -> int:
        return sum(
            1 for c in self.kube.calls if (c.verb, c.kind, c.name) == ("create", "Job", name)
        )


@pytest_asyncio.fixture
async def env(pool, dsn) -> Env:
    return Env(pool, dsn)


@pytest_asyncio.fixture
async def fake():
    async with FakeWrapper() as fw:
        yield fw


@pytest_asyncio.fixture
async def wenv(pool, dsn, fake) -> Env:
    e = Env(pool, dsn, fake)
    e.kube.pods_ready = True
    return e


async def test_runs_once_and_records_success(env):
    scripts = ["echo hello", "touch done"]
    c = await env.session("alpha", init_scripts=scripts)
    job = init_job_name("alpha")
    assert job == "agent-alpha-init"

    await env.tick()
    init = await env.init(c.id)
    assert init["state"] == "running"
    assert init["scripts_hash"] == init_scripts_hash(scripts)
    assert init["started_at"] and init["job"] == job
    assert init.get("finished_at") is None
    assert env.kube.has("Job", job)

    await env.tick(3)
    assert env.creates(job) == 1
    assert (await env.init(c.id))["state"] == "running"

    long_log = "\n".join(f"line {i}" for i in range(500))
    env.kube.finish_job(job, exit_code=0, log=long_log)
    await env.tick()
    init = await env.init(c.id)
    assert (init["state"], init["exit_code"]) == ("succeeded", 0)
    assert init["finished_at"] and init["started_at"] <= init["finished_at"]
    lines = init["log_tail"].splitlines()
    assert len(lines) == TAIL_LINES and lines[-1] == "line 499"
    assert init["reason"] is None and init["job"] == job
    assert init["scripts_hash"] == init_scripts_hash(scripts)

    await env.tick(2)
    assert env.creates(job) == 1
    assert (await env.init(c.id))["state"] == "succeeded"
    status = await env.status(c.id)
    assert status["phase"] in {"pending", "ready", "degraded"}
    assert "init" not in status["message"]


async def test_failure_records_and_releases_sessions(wenv, fake):
    c = await wenv.session("alpha", init_scripts=["exit 3"])
    job = init_job_name("alpha")
    await wenv.tick()
    assert not fake.sessions

    wenv.kube.finish_job(job, exit_code=3, log="installing\nboom")
    await wenv.tick()
    init = await wenv.init(c.id)
    assert (init["state"], init["exit_code"]) == ("failed", 3)
    assert init["log_tail"] == "installing\nboom"
    assert init["finished_at"]
    assert not wenv.kube.has("Job", job)

    (agent,) = await wenv.agents(c.id)
    assert agent.wrapper_session_id is not None
    assert len(fake.sessions) == 1
    status = await wenv.status(c.id)
    assert status["phase"] == "degraded"
    assert "init failed" in status["message"] and "3" in status["message"]

    await wenv.tick()
    status = await wenv.status(c.id)
    assert status["phase"] == "degraded" and "init failed" in status["message"]
    assert wenv.creates(job) == 1


async def test_skipped_never_runs_after_edit(env):
    c = await env.session("alpha")
    job = init_job_name("alpha")
    await env.tick()
    assert await env.init(c.id) == {"state": "skipped", "scripts_hash": init_scripts_hash([])}
    assert not env.kube.has("Job", job)

    await env.set_scripts(c.id, ["echo now"])
    await env.tick(3)
    assert env.creates(job) == 0
    assert await env.init(c.id) == {"state": "skipped", "scripts_hash": init_scripts_hash([])}
    status = await env.status(c.id)
    assert "init" not in status["message"]


async def test_edit_after_run_never_reruns(env):
    scripts = ["echo one"]
    c = await env.session("alpha", init_scripts=scripts)
    job = init_job_name("alpha")
    await env.tick()

    await env.set_scripts(c.id, ["echo edited while running"])
    await env.tick()
    assert env.creates(job) == 1
    assert (await env.init(c.id))["scripts_hash"] == init_scripts_hash(scripts)

    env.kube.finish_job(job, exit_code=0, log="one")
    await env.tick()
    assert (await env.init(c.id))["state"] == "succeeded"

    await env.set_scripts(c.id, ["echo two", "echo three"])
    await env.tick(3)
    assert env.creates(job) == 1
    assert not env.kube.has("Job", job)
    init = await env.init(c.id)
    assert init["state"] == "succeeded"
    assert init["scripts_hash"] == init_scripts_hash(scripts)


async def test_crash_and_failure_reasons(env):
    # crashed between the running write and the create: the next tick creates the Job
    a = await env.session("alpha", init_scripts=["echo a"])
    await env.tick()
    env.kube.objects.pop(("Job", "agents", "agent-alpha-init"))
    await env.patch_init(a.id, job=None)
    await env.tick()
    init = await env.init(a.id)
    assert (init["state"], init["job"]) == ("running", "agent-alpha-init")
    assert env.kube.has("Job", "agent-alpha-init")
    assert env.creates("agent-alpha-init") == 2

    # a created Job vanished: job_missing
    b = await env.session("beta", init_scripts=["echo b"])
    await env.tick()
    env.kube.objects.pop(("Job", "agents", "agent-beta-init"))
    await env.tick()
    init = await env.init(b.id)
    assert (init["state"], init["reason"]) == ("failed", "job_missing")
    assert init["finished_at"] and init["exit_code"] is None

    # an unlabelled Job under the name: foreign_object, never deleted
    env.kube.seed(
        {
            "apiVersion": "batch/v1",
            "kind": "Job",
            "metadata": {"name": "agent-gamma-init", "labels": {"app": "other"}},
        }
    )
    g = await env.session("gamma", init_scripts=["echo g"])
    await env.tick(2)
    init = await env.init(g.id)
    assert (init["state"], init["reason"]) == ("failed", "foreign_object")
    assert init["job"] is None
    assert env.kube.has("Job", "agent-gamma-init")
    assert "agent-gamma-init" not in env.kube.deleted("Job")

    # a Job that turns foreign while running: foreign_object as well
    f = await env.session("phi", init_scripts=["echo f"])
    await env.tick()
    env.kube.get("Job", "agent-phi-init")["metadata"]["labels"] = {"app": "other"}
    await env.tick()
    init = await env.init(f.id)
    assert (init["state"], init["reason"]) == ("failed", "foreign_object")
    assert env.kube.has("Job", "agent-phi-init")

    # the create fails: job_create_failed with the message cut to 2000 chars
    env.kube.fail("Job", "agent-delta-init", KubeError("quota " + "x" * 3000), verbs=["create"])
    d = await env.session("delta", init_scripts=["echo d"])
    await env.tick()
    init = await env.init(d.id)
    assert init["state"] == "failed"
    assert init["reason"].startswith("job_create_failed: quota xx")
    assert len(init["reason"]) == 2000
    assert init["job"] is None
    env.kube.heal("Job", "agent-delta-init")
    await env.tick()
    assert env.creates("agent-delta-init") == 1
    assert not env.kube.has("Job", "agent-delta-init")

    # overdue with no outcome: timeout, and the Job is deleted
    e = await env.session("eps", init_scripts=["sleep 9999"])
    await env.tick()
    started = datetime.fromisoformat((await env.init(e.id))["started_at"])
    await env.patch_init(
        e.id, started_at=(started - timedelta(seconds=DEADLINE_SECONDS + 60)).isoformat()
    )
    await env.tick()  # past the Job's own deadline but within the grace: still running
    assert (await env.init(e.id))["state"] == "running"
    overdue = started - timedelta(seconds=DEADLINE_SECONDS + DEADLINE_GRACE_SECONDS + 5)
    await env.patch_init(e.id, started_at=overdue.isoformat())
    await env.tick()
    init = await env.init(e.id)
    assert (init["state"], init["reason"]) == ("failed", "timeout")
    assert not env.kube.has("Job", "agent-eps-init")

    # the Job's own deadline fired: timeout
    z = await env.session("zeta", init_scripts=["sleep 9999"])
    await env.tick()
    env.kube.finish_job("agent-zeta-init", deadline_exceeded=True, log="slow")
    await env.tick()
    init = await env.init(z.id)
    assert (init["state"], init["reason"]) == ("failed", "timeout")
    assert init["log_tail"] == "slow"
    assert not env.kube.has("Job", "agent-zeta-init")


async def test_sessions_held_while_running(wenv, fake):
    c = await wenv.session("alpha", init_scripts=["echo a"])
    await wenv.tick()
    assert (await wenv.init(c.id))["state"] == "running"
    (agent,) = await wenv.agents(c.id)
    assert agent.ready and agent.wrapper_session_id is None
    assert not fake.sessions
    status = await wenv.status(c.id)
    assert status["phase"] == "pending"
    assert "init running" in status["message"]

    await wenv.tick(2)
    assert not fake.sessions
    assert (await wenv.status(c.id))["phase"] == "pending"

    # absent init with scripts: a rebootstrap command does not bootstrap either
    await wenv.drop_init(c.id)
    cmd = await insert_command(wenv.pool, agent.id, kind="rebootstrap")
    await wenv.rec._run_rebootstraps()
    assert not fake.sessions
    async with wenv.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT status, result FROM commands WHERE id = %s", (cmd["id"],))
        row = await cur.fetchone()
    assert row["status"] == "done" and row["result"]["bootstrapped"] is False
    assert "init running" in row["result"]["message"]

    await wenv.tick()
    assert not fake.sessions
    assert wenv.creates("agent-alpha-init") == 1  # the existing owned Job is adopted

    wenv.kube.finish_job("agent-alpha-init", exit_code=0, log="ok")
    await wenv.tick()
    assert len(fake.sessions) == 1
    status = await wenv.status(c.id)
    assert status["phase"] == "ready" and "init" not in status["message"]


async def test_init_gate_shapes():
    assert InitGate.of(None, []) == InitGate()
    assert InitGate.of(None, ["x"]).hold
    assert InitGate.of({"state": "running"}, []).hold
    assert InitGate.of({"state": "skipped"}, ["x"]) == InitGate()
    assert InitGate.of({"state": "succeeded"}, ["x"]) == InitGate()
    failed = InitGate.of({"state": "failed", "exit_code": 2, "reason": None}, ["x"])
    assert (failed.hold, failed.degraded, failed.message) == (False, True, "init failed (exit 2)")
    timeout = InitGate.of({"state": "failed", "exit_code": None, "reason": "timeout"}, ["x"])
    assert timeout.message == "init failed: timeout"


async def test_render_init_job(pool, dsn, tmp_path):
    env = Env(pool, dsn, image_pull_secret="harbor-pull")
    scripts = ["echo first > first.txt", 'echo "second $FOO"\necho err >&2']
    c = await env.session(
        "alpha",
        workdir="repo",
        env={"FOO": "bar", "A": "b"},
        init_scripts=scripts,
        resources={"requests": {"cpu": "500m"}, "limits": {"memory": "1Gi"}},
    )
    obj = render_init_job(c, env.settings)
    assert obj["apiVersion"] == "batch/v1" and obj["kind"] == "Job"
    meta = obj["metadata"]
    assert (meta["name"], meta["namespace"]) == ("agent-alpha-init", "agents")
    labels = {**session_labels("alpha"), INIT_LABEL: "true"}
    assert INIT_LABEL == "agents.szymonrichert.pl/init"
    assert meta["labels"] == labels
    spec = obj["spec"]
    assert spec["backoffLimit"] == 0
    assert spec["activeDeadlineSeconds"] == 1800 == DEADLINE_SECONDS
    assert "ttlSecondsAfterFinished" not in spec
    assert spec["template"]["metadata"]["labels"] == labels
    pod = spec["template"]["spec"]
    assert pod["restartPolicy"] == "Never"
    assert pod["securityContext"] == {"runAsUser": 1000, "fsGroup": 1000}
    assert pod["imagePullSecrets"] == [{"name": "harbor-pull"}]
    assert pod["volumes"] == [
        {"name": "workspace", "persistentVolumeClaim": {"claimName": "agent-alpha-workspace"}}
    ]
    (container,) = pod["containers"]
    assert container["image"] == IMAGE
    # no workingDir: the runtime would create a missing /workdir/repo as root, and the uid 1000
    # script could then not write in it; the command creates it as the pod user instead
    assert "workingDir" not in container
    assert workdir_path(c.spec) == "/workdir/repo"
    assert {"name": WORKDIR_ENV, "value": "/workdir/repo"} in container["env"]
    assert container["volumeMounts"] == [workdir_mount(c.spec)]
    assert container["env"][:2] == [{"name": "A", "value": "b"}, {"name": "FOO", "value": "bar"}]
    assert container["resources"] == {"requests": {"cpu": "500m"}, "limits": {"memory": "1Gi"}}
    cmd = container["command"]
    assert cmd[:2] == ["sh", "-c"] and cmd[3] == "agent-init" and len(cmd) == 5
    init_sh = cmd[4]
    assert "alpha" in init_sh.splitlines()[0]
    assert "run once at session start by agents-orchestrator" in init_sh
    assert "orchestrator_init_scripts_edit" in init_sh and "next session" in init_sh
    assert f"# --- script 1 ---\n{scripts[0]}\n" in init_sh
    assert f"# --- script 2 ---\n{scripts[1]}" in init_sh

    # spec.image wins over the settings image; no pull secret when unset
    c2 = await Env(pool, dsn).session("beta", image="img/agent:2", init_scripts=["true"])
    no_secret = Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE, image_pull_secret="")
    plain = render_init_job(c2, no_secret)
    assert plain["spec"]["template"]["spec"]["containers"][0]["image"] == "img/agent:2"
    assert "imagePullSecrets" not in plain["spec"]["template"]["spec"]

    # the command really writes the files and returns the script's exit code
    sh = shutil.which("sh")
    assert sh is not None
    # the workdir does not exist yet (as on a fresh PVC): the command creates it
    target = tmp_path / "repo"
    run = subprocess.run(
        cmd,
        cwd=tmp_path,
        env={"PATH": "/usr/bin:/bin", "FOO": "bar", WORKDIR_ENV: str(target)},
        capture_output=True,
    )
    assert run.returncode == 0, run.stderr
    d = target / INIT_DIR
    assert (d / ".gitignore").read_text() == "*\n"
    assert (d / "init.sh").read_text() == init_sh
    log = (d / "init.log").read_text()
    assert "second bar" in log and "err" in log
    assert run.stdout.decode() == log
    assert (target / "first.txt").read_text() == "first\n"

    failing = render_init_job(
        await env.session("gamma", init_scripts=["echo before", "exit 7", "echo never"]),
        env.settings,
    )["spec"]["template"]["spec"]["containers"][0]["command"]
    work = tmp_path / "w2"
    work.mkdir()
    run = subprocess.run(
        failing,
        cwd=work,
        env={"PATH": "/usr/bin:/bin", WORKDIR_ENV: str(work)},
        capture_output=True,
    )
    assert run.returncode == 7
    log = Path(work / INIT_DIR / "init.log").read_text()
    assert "before" in log and "never" not in log

    unset = render_init_job(
        await env.session("delta", init_scripts=['echo "$NOT_SET"', "echo never"]),
        env.settings,
    )["spec"]["template"]["spec"]["containers"][0]["command"]
    work = tmp_path / "w3"
    work.mkdir()
    run = subprocess.run(
        unset, cwd=work, env={"PATH": "/usr/bin:/bin", WORKDIR_ENV: str(work)}, capture_output=True
    )
    assert run.returncode != 0
    assert "never" not in (work / INIT_DIR / "init.log").read_text()

    # the largest allowed set passes in argv chunks below the 128 KiB per-argument limit
    big = ["# " + "\u00e9" * 16000 + f"\necho big {n}" for n in range(INIT_SCRIPTS_MAX)]
    big_cmd = render_init_job(await env.session("eta", init_scripts=big), env.settings)["spec"][
        "template"
    ]["spec"]["containers"][0]["command"]
    assert len(big_cmd) > 5
    assert all(len(arg.encode()) < 128 * 1024 for arg in big_cmd)
    assert "".join(big_cmd[4:]) == render_init_sh("eta", big)
    work = tmp_path / "w4"
    work.mkdir()
    run = subprocess.run(
        big_cmd,
        cwd=work,
        env={"PATH": "/usr/bin:/bin", WORKDIR_ENV: str(work)},
        capture_output=True,
    )
    assert run.returncode == 0, run.stderr
    assert (work / INIT_DIR / "init.sh").read_text() == render_init_sh("eta", big)
    assert f"big {INIT_SCRIPTS_MAX - 1}" in (work / INIT_DIR / "init.log").read_text()


async def test_job_lifecycle_and_not_an_agent(env):
    a = await env.session("alpha", replicas=1, init_scripts=["echo a"])
    await env.tick()
    pods = env.kube.names("Pod")
    assert "agent-alpha-init-x7k2p" in pods and "agent-alpha-0" in pods
    assert [ag.ordinal for ag in await env.agents(a.id)] == [0]
    assert all("init" not in ag.pod_name for ag in await env.agents(a.id))

    await env.tick(2)  # a converge never prunes the init Job
    assert env.kube.has("Job", "agent-alpha-init")
    assert env.kube.deleted("Job") == []
    assert [ag.ordinal for ag in await env.agents(a.id)] == [0]

    env.kube.finish_job("agent-alpha-init", exit_code=0, log="ok")
    await env.tick()
    assert env.kube.deleted("Job") == ["agent-alpha-init"]  # deleted after the outcome
    assert not env.kube.has("Pod", "agent-alpha-init-x7k2p")
    assert (await env.init(a.id))["state"] == "succeeded"

    # crashed after the outcome was recorded but before the delete: the next tick deletes it
    b = await env.session("beta", init_scripts=["echo b"])
    await env.tick()
    env.kube.finish_job("agent-beta-init", exit_code=0, log="ok")
    env.kube.fail("Job", "agent-beta-init", verbs=["delete"], times=1)
    await env.tick()
    assert (await env.init(b.id))["state"] == "succeeded"
    assert env.kube.has("Job", "agent-beta-init")
    await env.tick()
    assert not env.kube.has("Job", "agent-beta-init")
    assert (await env.init(b.id))["state"] == "succeeded"

    # a discard deletes a running init Job
    g = await env.session("gamma", init_scripts=["sleep 100"])
    await env.tick()
    assert env.kube.has("Job", "agent-gamma-init")
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, g.id)
    await env.tick(2)
    assert "agent-gamma-init" in env.kube.deleted("Job")
    assert not env.kube.has("Job", "agent-gamma-init")
    async with env.pool.connection() as conn:
        assert await sessions.get(conn, g.id) is None


async def test_scripts_are_rendered_into_the_job_and_hash(env):
    tpl = ["echo {{ session_slug }} {{ env.FOO }}", "echo {{ preset_name }}|"]
    c = await env.session("alpha", env={"FOO": "bar"}, init_scripts=tpl)
    await env.tick()
    job = env.kube.get("Job", "agent-alpha-init")
    command = job["spec"]["template"]["spec"]["containers"][0]["command"]
    init_sh = "".join(command[4:])
    assert "echo alpha bar\n" in init_sh and "echo |\n" in init_sh
    assert "{{" not in init_sh
    rendered = ["echo alpha bar", "echo |"]
    assert (await env.init(c.id))["scripts_hash"] == init_scripts_hash(tpl, rendered)


async def test_hash_changes_with_template_or_env(env, pool):
    a = await env.session("alpha", env={"FOO": "1"}, init_scripts=["echo {{ env.FOO }}"])
    b = await env.session("beta", env={"FOO": "2"}, init_scripts=["echo {{ env.FOO }}"])
    c = await env.session("gamma", env={"FOO": "1"}, init_scripts=["echo {{ env.FOO }} x"])
    await env.tick()
    hashes = [(await env.init(s.id))["scripts_hash"] for s in (a, b, c)]
    assert len(set(hashes)) == 3


async def test_legacy_braces_fail_init_with_a_clear_reason(env):
    c = await env.session("alpha", init_scripts=["echo ok"])
    await env.set_scripts(c.id, ["awk '{{print $1}}'"])  # stored before templating existed
    await env.tick(2)
    init = await env.init(c.id)
    assert init["state"] == "failed"
    assert init["reason"].startswith("template_error:")
    assert "{{ '{{' }}" in init["reason"]
    assert env.creates("agent-alpha-init") == 0
