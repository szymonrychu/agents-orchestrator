"""A stalled discard hook server cannot hold a reconcile pass (no database needed)."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import pytest

from agents_orchestrator.db import sessions
from agents_orchestrator.reconciler import Reconciler, loop
from agents_orchestrator.reconciler.discard_hook import run_discard_hook
from agents_orchestrator.settings import DiscardHook, Settings
from agents_orchestrator.spec import SessionSpec

IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"


def make_session(**spec) -> sessions.Session:
    return sessions.Session(
        id=uuid.uuid4(),
        slug="alpha",
        spec=SessionSpec(**spec),
        generation=1,
        observed_generation=1,
        desired="deleting",
        status={},
        created_at=datetime.now(UTC),
    )


async def lines():
    yield '{"kind": "session"}\n'


@asynccontextmanager
async def stalled_server():
    """Accepts connections and never reads or answers; yields its URL."""
    held: list[asyncio.StreamWriter] = []
    stop = asyncio.Event()

    async def handle(reader, writer):
        held.append(writer)
        await stop.wait()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/hook"
    finally:
        stop.set()
        for writer in held:
            writer.close()
        server.close()
        await server.wait_closed()


async def test_stalled_server_fails_within_read_timeout():
    async with stalled_server() as url:
        started = time.monotonic()
        result = await run_discard_hook(
            DiscardHook("url", url),
            make_session(),
            lines(),
            60,  # the overall budget alone would hold the call for a minute
            connect_timeout=1,
            read_timeout=0.5,
        )
    assert time.monotonic() - started < 5
    assert not result.ok and "Timeout" in result.reason


def settings(**extra) -> Settings:
    return Settings(
        database_url="postgresql://x/y", master_key="k" * 32, agent_image=IMAGE, **extra
    )


class FakePool:
    @asynccontextmanager
    async def connection(self):
        yield object()


@pytest.fixture
def stub_export(monkeypatch):
    monkeypatch.setattr(loop, "export_session_history", lambda conn, session_id: lines())


async def test_pass_budget_skips_hooks_once_spent(stub_export):
    async with stalled_server() as url:
        cfg = settings(
            discard_hook_url=url,
            discard_hook_token="t0k3n",
            discard_hook_timeout_seconds=60,
            discard_hook_read_timeout_seconds=0.5,
            discard_hook_pass_budget_seconds=0.3,
        )
        rec = Reconciler(FakePool(), None, cfg, wrapper_factory=None)
        session = make_session()
        started = time.monotonic()
        result = await rec._run_hook(rec._owed_hooks(session)[1][0], session)
        assert not result.ok and time.monotonic() - started < 5
        # the budget is spent: the next deleting session's hook is left for the next pass,
        # without a request and without the database
        assert await rec.pre_discard(make_session()) is False


async def test_pass_budget_resets_each_pass(stub_export):
    cfg = settings(discard_hook_url="http://hook.example/x", discard_hook_token="t0k3n")
    rec = Reconciler(FakePool(), None, cfg, wrapper_factory=None)
    rec._hook_spent = cfg.discard_hook_pass_budget_seconds
    assert rec._hook_budget_spent()
    rec._start_pass()
    assert not rec._hook_budget_spent()


def test_hook_timeout_settings_defaults_and_env(monkeypatch):
    s = Settings(database_url="postgresql://x/y", master_key="k" * 32, agent_image=IMAGE)
    assert s.discard_hook_connect_timeout_seconds == 5
    assert s.discard_hook_read_timeout_seconds == 30
    assert s.discard_hook_pass_budget_seconds == 60
    monkeypatch.setenv("ORCH_DISCARD_HOOK_READ_TIMEOUT_SECONDS", "7")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_CONNECT_TIMEOUT_SECONDS", "2")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_PASS_BUDGET_SECONDS", "90")
    monkeypatch.setenv("ORCH_DATABASE_URL", "postgresql://x/y")
    monkeypatch.setenv("ORCH_MASTER_KEY", "k" * 32)
    monkeypatch.setenv("ORCH_AGENT_IMAGE", IMAGE)
    s = Settings()
    assert (
        s.discard_hook_read_timeout_seconds,
        s.discard_hook_connect_timeout_seconds,
        s.discard_hook_pass_budget_seconds,
    ) == (7, 2, 90)
