"""Session bootstrap and adoption by the reconciler (D6), against the in-process fake wrapper."""

from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from factories import insert_command, set_agent_status
from fakes.fake_kube import FakeKube
from fakes.fake_wrapper import FakeWrapper
from psycopg.rows import dict_row

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import queue, sessions
from agents_orchestrator.k8s.render import workdir_path
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler import wrapper_sessions as sessions_mod
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import WrapperError

pytestmark = pytest.mark.critical

KEY = "k" * 32
IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"


class Env:
    def __init__(self, pool, dsn: str, fake: FakeWrapper) -> None:
        self.pool, self.fake = pool, fake
        self.kube = FakeKube(namespace="agents")
        self.kube.pods_ready = True
        self.settings = Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE)
        self.factory_calls: list[tuple[str, str]] = []
        self.client_override = None
        self.rec = Reconciler(pool, self.kube, self.settings, wrapper_factory=self._factory)

    def _factory(self, url: str, token: str):
        self.factory_calls.append((url, token))
        if self.client_override is not None:
            return self.client_override
        return WrapperClient(self.fake.base_url, token)

    async def session(self, slug: str = "alpha", **spec) -> sessions.Session:
        async with self.pool.connection() as conn:
            c = await sessions.create(conn, slug, SessionSpec(**spec))
        self.fake.token = tokens_for(self.settings, c.id).auth
        return c

    async def agents(self, session_id) -> list[agents_repo.Agent]:
        async with self.pool.connection() as conn:
            return await agents_repo.list_agents(conn, session_id)

    async def status(self, session_id) -> dict:
        async with self.pool.connection() as conn:
            c = await sessions.get(conn, session_id)
        return c.status

    async def seed_session(self, session_id, state: str, transcript: bool = False) -> str:
        """A session already on the wrapper, created with the session token."""
        client = WrapperClient(self.fake.base_url, tokens_for(self.settings, session_id).auth)
        snap = await client.create_session("seed", "seed")
        if state != "ready":
            self.fake.set_session_state(
                state, has_transcript=transcript, session_id=snap.wrapper_session_id
            )
        if transcript:
            self.fake.sessions[snap.wrapper_session_id].turns_completed = 1
        return snap.wrapper_session_id


@pytest_asyncio.fixture
async def fake():
    async with FakeWrapper() as fw:
        yield fw


@pytest_asyncio.fixture
async def env(pool, dsn, fake) -> Env:
    return Env(pool, dsn, fake)


async def test_creates_session_for_ready_pod(env, fake):
    c = await env.session("alpha", workdir="repo", model="opus", effort="high")
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert len(fake.sessions) == 1
    ses = next(iter(fake.sessions.values()))
    assert ses.name == "repo"
    assert ses.model == "opus"
    assert agent.wrapper_session_id == ses.session_id
    assert agent.wrapper_session_created_at is not None

    await env.rec.reconcile_once()  # steady state: no second session
    assert len(fake.sessions) == 1


async def test_adopts_existing_session(env, fake):
    c = await env.session("alpha")
    sid = await env.seed_session(c.id, "busy")
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == sid
    assert list(fake.sessions) == [sid]


async def test_unready_pod_skipped(env, fake):
    env.kube.pods_ready = False
    c = await env.session("alpha")
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id is None
    assert fake.sessions == {}
    assert env.factory_calls == []


class Stub503:
    async def list_sessions(self):
        raise WrapperError(503, "booting", "not ready")


async def test_wrapper_unavailable_retries_next_tick(env, fake):
    c = await env.session("alpha")
    env.client_override = Stub503()
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id is None
    assert "503" in (await env.status(c.id))["message"]
    assert "agent-alpha-0" in (await env.status(c.id))["message"]

    env.client_override = None
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id is not None
    assert (await env.status(c.id))["message"] == ""


async def test_network_error_leaves_agent_without_session(env, fake):
    c = await env.session("alpha")
    await fake.set_unreachable(True)
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id is None
    assert (await env.status(c.id))["message"]
    await fake.set_unreachable(False)


async def test_lost_session_rebootstrapped(env, fake):
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    old = agent.wrapper_session_id
    del fake.sessions[old]

    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (None, old)
    assert list(fake.sessions) == [agent.wrapper_session_id]


async def test_uses_session_auth_token(env, fake):
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    expected = tokens_for(env.settings, c.id).auth
    assert env.factory_calls
    assert {token for _url, token in env.factory_calls} == {expected}
    assert all("agent-alpha-0.agent-alpha.agents.svc" in url for url, _t in env.factory_calls)
    assert len(fake.sessions) == 1  # the fake only answers to that token


async def test_adopts_only_live_sessions(env, fake):
    c = await env.session("alpha")
    stopped = await env.seed_session(c.id, "stopped")
    failed = await env.seed_session(c.id, "failed")
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (stopped, failed, None)
    assert fake.sessions[agent.wrapper_session_id].state == "ready"
    assert fake.sessions[stopped].state == "stopped"
    assert fake.sessions[failed].state == "failed"

    for state in ("creating", "ready", "busy"):
        assert state in sessions_mod.ADOPTABLE
    assert "stopped" not in sessions_mod.ADOPTABLE and "failed" not in sessions_mod.ADOPTABLE


async def test_resumes_stopped_session_with_transcript(env, fake):
    c = await env.session("alpha")
    sid = await env.seed_session(c.id, "stopped", transcript=True)
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == sid
    assert fake.sessions[sid].state == "ready"
    assert list(fake.sessions) == [sid]


async def test_failed_session_without_transcript_gets_new_session(env, fake):
    c = await env.session("alpha")
    sid = await env.seed_session(c.id, "failed", transcript=False)
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (None, sid)
    assert fake.sessions[sid].state == "failed"
    assert fake.sessions[agent.wrapper_session_id].state == "ready"


async def test_rebootstrap_command_clears_and_bootstraps(env, fake):
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    old = agent.wrapper_session_id
    del fake.sessions[old]  # the wrapper answered 404 on submit
    cmd = await insert_command(env.pool, agent.id, kind="rebootstrap")

    # no converge in between: the command alone must bootstrap in the same tick
    await env.rec._run_rebootstraps()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (None, old)
    async with env.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT status FROM commands WHERE id = %s", (cmd["id"],))
        assert (await cur.fetchone())["status"] == "done"
    async with env.pool.connection() as conn:
        assert await queue.claim_command(conn, ["rebootstrap"]) is None


async def test_rebootstrap_runs_inside_reconcile_tick(env, fake):
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    old = agent.wrapper_session_id
    del fake.sessions[old]
    await insert_command(env.pool, agent.id, kind="rebootstrap")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (None, old)
    assert list(fake.sessions) == [agent.wrapper_session_id]


async def test_session_workdir_matches_rendered_workdir_path(env, fake):
    c = await env.session("alpha", workdir="my-repo")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert fake.sessions[agent.wrapper_session_id].workdir == workdir_path(c.spec)
    assert workdir_path(c.spec) == "/workdir/my-repo"


async def test_bootstraps_run_concurrently_bounded_at_eight(env, fake):
    assert sessions_mod.MAX_CONCURRENT_BOOTSTRAPS == 8
    running = peak = 0

    class Slow:
        async def list_sessions(self):
            nonlocal running, peak
            running += 1
            peak = max(peak, running)
            await asyncio.sleep(0.05)
            running -= 1
            raise WrapperError(503, "booting", "later")

    env.client_override = Slow()
    c = await env.session("alpha", replicas=12)
    await env.rec.reconcile_once()
    assert 2 <= peak <= 8
    assert len(await env.agents(c.id)) == 12


async def _bootstrapped(env, state: str, transcript: bool):
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    sid = agent.wrapper_session_id
    assert sid is not None
    if state != "ready":
        env.fake.set_session_state(state, has_transcript=transcript, session_id=sid)
    if transcript:
        env.fake.sessions[sid].turns_completed = 1
    return c, sid


async def test_stored_stopped_session_is_resumed(env, fake):
    c, sid = await _bootstrapped(env, "stopped", transcript=True)
    assert fake.sessions[sid].state == "stopped"
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == sid
    assert fake.sessions[sid].state == "ready"
    assert list(fake.sessions) == [sid]


async def test_stored_failed_session_without_transcript_replaced(env, fake):
    c, sid = await _bootstrapped(env, "failed", transcript=False)
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (None, sid)
    assert fake.sessions[sid].state == "failed"
    assert fake.sessions[agent.wrapper_session_id].state == "ready"


async def test_stored_live_session_kept(env, fake):
    c, sid = await _bootstrapped(env, "busy", transcript=False)
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == sid
    assert list(fake.sessions) == [sid]


class ResumeFails:
    """The real client, except that resume answers with a fixed wrapper error."""

    def __init__(self, inner, status: int) -> None:
        self.inner, self.status = inner, status

    async def resume_session(self, wrapper_session_id: str):
        raise WrapperError(self.status, "resume", "nope")

    def __getattr__(self, name):
        return getattr(self.inner, name)


async def test_stored_stopped_session_refused_resume_replaced(env, fake):
    c, sid = await _bootstrapped(env, "stopped", transcript=True)
    env.client_override = ResumeFails(WrapperClient(fake.base_url, fake.token), 409)
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id not in (None, sid)
    assert fake.sessions[sid].state == "stopped"
    assert fake.sessions[agent.wrapper_session_id].state == "ready"


async def test_stored_stopped_session_resume_5xx_deferred_and_kept(env, fake):
    c, sid = await _bootstrapped(env, "stopped", transcript=True)
    env.client_override = ResumeFails(WrapperClient(fake.base_url, fake.token), 503)
    await env.rec.reconcile_once()

    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == sid
    assert list(fake.sessions) == [sid]
    assert "agent-alpha-0" in (await env.status(c.id))["message"]


async def _db_now(env):
    async with env.pool.connection() as conn:
        cur = await conn.execute("SELECT clock_timestamp()")
        return (await cur.fetchone())[0]


async def test_successful_resume_records_resume_marker(env, fake):
    """Both resume paths record `wrapper_resumed_at` (database clock); adopt and create clear it.

    The marker is the ingester's re-attach trigger (deploy-safe-agent-sessions D3): a resume
    changes it, so the ingester restarts the stream of the same wrapper session.
    """
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    created = agent.wrapper_session_id
    assert agent.wrapper_resumed_at is None  # create

    # 1. the stored session is resumed
    fake.set_session_state("stopped", has_transcript=True, session_id=created)
    fake.sessions[created].turns_completed = 1
    before = await _db_now(env)
    await env.rec.reconcile_once()
    after = await _db_now(env)
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == created
    assert agent.wrapper_resumed_at is not None
    assert before <= agent.wrapper_resumed_at <= after
    first = agent.wrapper_resumed_at

    await env.rec.reconcile_once()  # steady state: the adopted stored session keeps its marker
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_resumed_at == first

    # 2. `_pick` resumes another listed session once the stored one is gone
    other = await env.seed_session(c.id, "stopped", transcript=True)
    del fake.sessions[created]
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == other
    assert agent.wrapper_resumed_at is not None and agent.wrapper_resumed_at > first

    # 3. a create after a resume clears the marker
    del fake.sessions[other]
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    fresh = agent.wrapper_session_id
    assert fresh not in (None, created, other)
    assert agent.wrapper_resumed_at is None

    # 4. so does an adopt after a resume
    fake.set_session_state("stopped", has_transcript=True, session_id=fresh)
    fake.sessions[fresh].turns_completed = 1
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == fresh and agent.wrapper_resumed_at is not None
    busy = await env.seed_session(c.id, "busy")
    del fake.sessions[fresh]
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == busy
    assert agent.wrapper_resumed_at is None


async def test_live_stored_session_of_a_dead_agent_records_resume_marker(env, fake):
    """The wrapper resumed the stored session itself after the ingester honoured its close.

    A pod deleted mid-turn: its wrapper's graceful shutdown stops the session and emits
    `session_closed`, the snapshot is stopped, so the ingester marks the agent dead and ends its
    stream (D3). The new pod's wrapper resumes the session on start (`resume_on_start`, D5), so
    the reconciler lists the stored session live. Adopting it as is left the agent dead for good
    (kind e2e AC6, CI run 40676464845); the adopt of a dead agent records the resume marker, so
    the ingester re-attaches. A live agent's adopt keeps the marker as it is.
    """
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    sid = agent.wrapper_session_id
    await env.rec.reconcile_once()  # no status row yet: nothing to lift
    assert (await env.agents(c.id))[0].wrapper_resumed_at is None

    await set_agent_status(env.pool, agent.id, state="busy")
    await env.rec.reconcile_once()
    assert (await env.agents(c.id))[0].wrapper_resumed_at is None  # live: no re-attach

    await set_agent_status(env.pool, agent.id, state="dead")
    fake.set_session_state("busy", session_id=sid)  # the wrapper's own resume continues the turn
    before = await _db_now(env)
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert agent.wrapper_session_id == sid
    assert agent.wrapper_resumed_at is not None and agent.wrapper_resumed_at >= before
    assert ("POST", f"/v1/sessions/{sid}/resume") not in fake.requests  # adopted, not resumed
    assert list(fake.sessions) == [sid]
    first = agent.wrapper_resumed_at

    await set_agent_status(env.pool, agent.id, state="busy")  # the ingester re-attached
    await env.rec.reconcile_once()
    (agent,) = await env.agents(c.id)
    assert (agent.wrapper_session_id, agent.wrapper_resumed_at) == (sid, first)


async def test_restored_stopped_session_resumed_not_created(env, fake):
    """A recreated session's new agents resume the transcripts restored onto their pods (D7).

    Each pod's wrapper lists the session restored into its own `/sessions/<pod>/` as stopped
    with a transcript; the new agent row adopts and resumes it, and no session is created.
    """
    async with FakeWrapper() as other:
        c = await env.session("alpha", replicas=2, workdir="repo")
        other.token = fake.token
        wrappers = {"agent-alpha-0": fake, "agent-alpha-1": other}
        restored = {
            pod: w.add_persisted_session(
                workdir_path(c.spec), name="repo", created_at="2026-10-27T10:00:00.000Z"
            )
            for pod, w in wrappers.items()
        }

        def route(url: str, token: str) -> WrapperClient:
            env.factory_calls.append((url, token))
            pod = url.split("//", 1)[1].split(".", 1)[0]
            return WrapperClient(wrappers[pod].base_url, token)

        assert env.rec.bootstrapper is not None
        env.rec.bootstrapper.wrapper_factory = route
        await env.rec.reconcile_once()

        agents = await env.agents(c.id)
        assert sorted(a.pod_name for a in agents) == sorted(wrappers)
        for agent in agents:
            wrapper, sid = wrappers[agent.pod_name], restored[agent.pod_name]
            assert agent.wrapper_session_id == sid
            assert agent.wrapper_resumed_at is not None  # resumed, so the ingester re-attaches
            assert list(wrapper.sessions) == [sid]
            assert wrapper.sessions[sid].state == "ready"
            assert ("POST", f"/v1/sessions/{sid}/resume") in wrapper.requests

        await env.rec.reconcile_once()  # steady state: the resumed sessions are kept
        assert {a.pod_name: a.wrapper_session_id for a in await env.agents(c.id)} == restored
        for wrapper in wrappers.values():
            assert ("POST", "/v1/sessions") not in wrapper.requests
            assert len(wrapper.sessions) == 1
