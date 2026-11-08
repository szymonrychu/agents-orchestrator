"""Restart and rebootstrap commands run in their own task, so a slow reconcile pass (a wrapper
create blocked for minutes) cannot hold them; per session they never overlap a converge (T-79)."""

from __future__ import annotations

import asyncio
import time

import pytest_asyncio
from factories import insert_command
from fakes.fake_wrapper import FakeWrapper
from test_wrapper_session_bootstrap import Env

from agents_orchestrator.settings import Settings
from agents_orchestrator.wrapper.client import WrapperClient

TOKEN = "fixed-token"


@pytest_asyncio.fixture
async def fake():
    async with FakeWrapper() as fw:
        yield fw


@pytest_asyncio.fixture
async def other():
    async with FakeWrapper() as fw:
        fw.token = TOKEN
        yield fw


@pytest_asyncio.fixture
async def env(pool, dsn, fake, other) -> Env:
    """Session alpha's pods reach `fake`, every other session's pods reach `other`."""
    e = Env(pool, dsn, fake)

    def factory(url: str, _token: str) -> WrapperClient:
        return WrapperClient((fake if "alpha" in url else other).base_url, TOKEN)

    e.rec.bootstrapper.wrapper_factory = factory
    return e


async def command(env: Env, cmd_id: int) -> dict:
    async with env.pool.connection() as conn:
        cur = await conn.execute("SELECT status, result FROM commands WHERE id = %s", (cmd_id,))
        status, result = await cur.fetchone()
    return {"status": status, "result": result}


async def wait_for(predicate, timeout: float) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if await predicate():
            return True
        await asyncio.sleep(0.05)
    return False


async def creating(fake: FakeWrapper) -> bool:
    return any(s.state == "creating" for s in fake.sessions.values())


async def status_of(env: Env, cmd_id: int) -> str:
    return (await command(env, cmd_id))["status"]


async def test_restart_completes_while_another_bootstrap_blocks(env, fake, other):
    beta = await env.session("beta")
    await env.rec.reconcile_once()  # beta is up with its wrapper session
    (beta_agent,) = await env.agents(beta.id)
    assert beta_agent.wrapper_session_id is not None

    await env.session("alpha")
    fake.token = TOKEN
    gate = fake.gate_create()  # alpha's create holds the next pass until the gate opens
    env.rec.settings = Settings(
        database_url=env.settings.database_url,
        master_key=env.settings.master_key,
        agent_image=env.settings.agent_image,
        reconcile_poll_seconds=0.2,
    )
    stop = asyncio.Event()
    task = asyncio.create_task(env.rec.run(stop))
    try:
        await asyncio.wait_for(fake.create_entered.wait(), 10)
        cmd = await insert_command(env.pool, beta_agent.id, kind="restart")

        async def done() -> bool:
            return await status_of(env, cmd["id"]) == "done"

        assert await wait_for(done, 10), await command(env, cmd["id"])
        assert not gate.is_set()  # the slow pass is still inside alpha's create
        assert await creating(fake)
        assert ("Pod", "agents", "agent-beta-0") in env.kube.deletes
    finally:
        gate.set()
        stop.set()
        await asyncio.wait_for(task, 10)


async def test_restart_waits_for_the_converge_of_its_own_session(env, fake):
    alpha = await env.session("alpha")
    fake.token = TOKEN
    gate = fake.gate_create()
    rec_pass = asyncio.create_task(env.rec.reconcile_once())
    await asyncio.wait_for(fake.create_entered.wait(), 10)
    (agent,) = await env.agents(alpha.id)
    cmd = await insert_command(env.pool, agent.id, kind="restart")

    commands = asyncio.create_task(env.rec.run_commands())

    async def claimed() -> bool:
        return await status_of(env, cmd["id"]) == "running"

    # the command is claimed and now waits on alpha's session lock, held by the open converge
    assert await wait_for(claimed, 10)
    assert not commands.done()
    assert ("Pod", "agents", "agent-alpha-0") not in env.kube.deletes

    gate.set()
    await asyncio.wait_for(rec_pass, 10)
    await asyncio.wait_for(commands, 10)
    await env.rec.run_commands()  # a second drain finds the command already finished
    row = await command(env, cmd["id"])
    assert row["status"] == "done" and row["result"]["pod"] == "agent-alpha-0"
    assert env.kube.deletes.count(("Pod", "agents", "agent-alpha-0")) == 1


async def test_run_fails_commands_cut_short_by_a_shutdown(env):
    alpha = await env.session("alpha")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(alpha.id)
    restart = await insert_command(env.pool, agent.id, kind="restart", status="running")
    rebootstrap = await insert_command(env.pool, agent.id, kind="rebootstrap", status="running")
    queued = await insert_command(env.pool, agent.id, kind="rebootstrap")

    stop = asyncio.Event()
    stop.set()  # one start-up, no passes
    await asyncio.wait_for(env.rec.run(stop), 10)

    for cmd in (restart, rebootstrap):
        row = await command(env, cmd["id"])
        assert row["status"] == "failed", row
        assert row["result"] == {"error": "interrupted_by_restart"}
    assert await status_of(env, queued["id"]) != "failed"


async def test_command_exception_does_not_kill_the_command_loop(env, monkeypatch):
    calls = 0
    second = asyncio.Event()

    async def flaky() -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError("boom")
        second.set()

    monkeypatch.setattr(env.rec, "run_commands", flaky)
    env.rec.settings = Settings(
        database_url=env.settings.database_url,
        master_key=env.settings.master_key,
        agent_image=env.settings.agent_image,
        reconcile_poll_seconds=0.05,
    )
    stop = asyncio.Event()
    wake = asyncio.Event()
    task = asyncio.create_task(env.rec._command_loop(stop, wake))
    try:
        await asyncio.wait_for(second.wait(), 10)  # a pass after the failed one still ran
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task, 10)


async def test_restart_of_never_bootstrapped_agent_says_so(env, fake):
    alpha = await env.session("alpha")
    fake.token = TOKEN
    fake.set_create_error(500, "bootstrap_failed")
    await env.rec.reconcile_once()
    (agent,) = await env.agents(alpha.id)
    assert agent.wrapper_session_id is None

    cmd = await insert_command(env.pool, agent.id, kind="restart")
    await env.rec.run_commands()
    row = await command(env, cmd["id"])
    assert row["status"] == "done" and row["result"]["pod"] == "agent-alpha-0"
    warning = row["result"]["warning"]
    assert "never bootstrapped" in warning
    assert "create" in warning  # the recorded failing step
    assert "same bootstrap" in warning
