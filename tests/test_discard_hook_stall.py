"""A stalled discard hook server cannot hold a reconcile pass (no database needed)."""

from __future__ import annotations

import asyncio
import time
import uuid
from contextlib import asynccontextmanager
from datetime import UTC, datetime

import httpx
import pytest

from agents_orchestrator.db import sessions
from agents_orchestrator.reconciler import Reconciler, loop
from agents_orchestrator.reconciler.discard_hook import run_discard_hook
from agents_orchestrator.settings import DiscardHook, Settings, SettingsError
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


@asynccontextmanager
async def trickling_server(interval: float):
    """Starts a response and sends one header byte every `interval`, forever."""
    tasks: list[asyncio.Task] = []
    writers: list[asyncio.StreamWriter] = []

    async def handle(reader, writer):
        writers.append(writer)

        async def drip():
            try:
                writer.write(b"HTTP/1.1 200 OK\r\nX-Slow: ")
                while True:
                    writer.write(b"x")
                    await writer.drain()
                    await asyncio.sleep(interval)
            except (ConnectionError, asyncio.CancelledError):
                pass

        tasks.append(asyncio.create_task(drip()))

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/hook"
    finally:
        for task in tasks:
            task.cancel()
        for writer in writers:
            writer.close()
        server.close()
        await server.wait_closed()


async def test_trickling_server_is_cut_at_the_pass_deadline(stub_export):
    # one byte per 0.1 s never trips the 0.5 s read timeout; the 60 s overall timeout alone
    # would hold the call for a minute. The pass budget (floor: connect + read) bounds it.
    async with trickling_server(0.1) as url:
        cfg = settings(
            discard_hook_url=url,
            discard_hook_token="t0k3n",
            discard_hook_timeout_seconds=60,
            discard_hook_connect_timeout_seconds=0.5,
            discard_hook_read_timeout_seconds=0.5,
            discard_hook_pass_budget_seconds=0.3,
        )
        rec = Reconciler(FakePool(), None, cfg, wrapper_factory=None)
        session = make_session()
        started = time.monotonic()
        result = await rec._run_hook(rec._owed_hooks(session)[1][0], session)
    assert time.monotonic() - started < 5
    assert not result.ok and "timed out" in result.reason


async def test_command_hook_is_cut_at_the_pass_deadline(stub_export):
    cfg = settings(
        discard_hook_command="sleep 30",
        discard_hook_timeout_seconds=60,
        discard_hook_pass_budget_seconds=0.3,
        discard_hook_connect_timeout_seconds=0.2,
        discard_hook_read_timeout_seconds=0.2,
    )
    rec = Reconciler(FakePool(), None, cfg, wrapper_factory=None)
    session = make_session()
    started = time.monotonic()
    result = await rec._run_hook(rec._owed_hooks(session)[1][0], session)
    assert time.monotonic() - started < 5
    assert not result.ok and "timed out" in result.reason


def test_hook_deadline_is_bounded_by_remaining_budget_with_a_floor():
    cfg = settings(
        discard_hook_timeout_seconds=100,
        discard_hook_connect_timeout_seconds=5,
        discard_hook_read_timeout_seconds=30,
        discard_hook_pass_budget_seconds=60,
    )
    rec = Reconciler(FakePool(), None, cfg, wrapper_factory=None)
    assert rec._hook_deadline() == 60  # a fresh pass: the whole budget
    rec._hook_spent = 50
    assert rec._hook_deadline() == 35  # remaining 10 is under the floor (connect + read)
    rec._hook_spent = 59.9
    assert rec._hook_deadline() == 35
    small = settings(
        discard_hook_timeout_seconds=20,
        discard_hook_read_timeout_seconds=10,
        discard_hook_pass_budget_seconds=60,
    )
    assert Reconciler(FakePool(), None, small, wrapper_factory=None)._hook_deadline() == 20


async def test_connect_timeout_is_passed_and_fails_the_hook():
    seen = {}

    def handler(request):
        seen.update(request.extensions["timeout"])
        raise httpx.ConnectTimeout("no route", request=request)

    result = await run_discard_hook(
        DiscardHook("url", "http://hook.example/x"),
        make_session(),
        lines(),
        60,
        connect_timeout=1.5,
        read_timeout=2.5,
        transport=httpx.MockTransport(handler),
    )
    assert seen["connect"] == 1.5 and seen["read"] == 2.5 and seen["write"] == 2.5
    assert not result.ok and "ConnectTimeout" in result.reason


class FakeCursor:
    async def fetchone(self):
        return (datetime.now(UTC),)


class FakeConn:
    async def execute(self, *args, **kwargs):
        return FakeCursor()


class FakeDbPool:
    @asynccontextmanager
    async def connection(self):
        yield FakeConn()


def stall_transport(calls: list[str], hold: float = 10.0):
    async def handler(request):
        calls.append(request.url.host)
        if request.url.host.startswith("stall"):
            await asyncio.sleep(hold)
        return httpx.Response(200)

    return httpx.MockTransport(handler)


def deleting(slug: str, host: str) -> sessions.Session:
    s = make_session(discard_hook_url=f"http://{host}/x")
    return sessions.Session(**{**s.__dict__, "slug": slug})


async def test_healthy_session_behind_stalled_ones_gets_its_hook(stub_export, monkeypatch):
    cfg = settings(
        discard_hook_timeout_seconds=60,
        discard_hook_connect_timeout_seconds=0.05,
        discard_hook_read_timeout_seconds=0.05,
        discard_hook_pass_budget_seconds=0.2,
    )
    rec = Reconciler(FakeDbPool(), None, cfg, wrapper_factory=None)
    calls: list[str] = []
    rec.hook_transport = stall_transport(calls)

    async def no_status(*args, **kwargs):
        return None

    monkeypatch.setattr(rec, "_write_status", no_status)
    rows = [deleting("a", "stall-a"), deleting("b", "stall-b"), deleting("c", "ok-c")]
    for _ in range(3):
        rec._start_pass()
        for session in rec._hook_order(rows):
            await rec.pre_discard(session)
        if "ok-c" in calls:
            break
    assert "ok-c" in calls  # within the passes it takes to rotate through the stalled ones
    assert calls.count("stall-a") <= 2 and calls.count("stall-b") <= 2


async def test_budget_skip_leaves_the_second_owed_hook_unrun_and_blocks_discard(
    stub_export, monkeypatch
):
    cfg = settings(
        discard_hook_url="http://slow-ok.example/s",
        discard_hook_token="t0k3n",
        discard_hook_pass_budget_seconds=0.1,
        discard_hook_connect_timeout_seconds=0.5,
        discard_hook_read_timeout_seconds=0.5,
    )
    rec = Reconciler(FakeDbPool(), None, cfg, wrapper_factory=None)
    calls: list[str] = []

    async def handler(request):
        calls.append(request.url.host)
        if request.url.host == "slow-ok.example":
            await asyncio.sleep(0.15)  # delivers, but spends the whole budget
        return httpx.Response(200)

    rec.hook_transport = httpx.MockTransport(handler)
    writes: list[dict] = []

    async def record(session, fields, generation, **kwargs):
        writes.append(fields)

    monkeypatch.setattr(rec, "_write_status", record)
    session = make_session(discard_hook_url="http://own.example/x")
    assert await rec.pre_discard(session) is False  # the session hook is owed, not skipped away
    assert calls == ["slow-ok.example"]
    assert list(writes[-1]["discard_hook_delivered"]) == ["settings"]
    assert not any("discard_hook_failed" in w for w in writes)


async def test_discard_skips_deletion_while_the_hook_budget_is_spent(stub_export, monkeypatch):
    cfg = settings(discard_hook_url="http://hook.example/x", discard_hook_token="t0k3n")

    class Kube:
        def __getattr__(self, name):
            raise AssertionError(f"kube.{name} called: the session must not be touched")

    rec = Reconciler(FakeDbPool(), Kube(), cfg, wrapper_factory=None)
    rec._hook_spent = cfg.discard_hook_pass_budget_seconds
    ran: list[str] = []

    async def cancel_checks(conn, session_id):
        return None

    async def quiesce(session):
        return True

    async def save_step(session, kind="workdir"):
        return True

    async def reload(session):
        return session

    async def write_status(session, fields, generation, **kwargs):
        ran.append(fields["message"])

    async def run_hook(hook, session):
        raise AssertionError("no hook may run once the budget is spent")

    async def purge(conn, session_id):
        raise AssertionError("the session must not be purged unexported")

    monkeypatch.setattr(loop.queue, "cancel_checks", cancel_checks)
    monkeypatch.setattr(loop.sessions, "purge", purge)
    monkeypatch.setattr(rec, "_quiesce", quiesce)
    monkeypatch.setattr(rec.cache, "save_step", save_step)
    monkeypatch.setattr(rec, "_reload", reload)
    monkeypatch.setattr(rec, "_write_status", write_status)
    monkeypatch.setattr(rec, "_run_hook", run_hook)
    await rec._discard(make_session())
    assert ran == ["waiting for the pre-discard hook"]


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("ORCH_DISCARD_HOOK_CONNECT_TIMEOUT_SECONDS", "61"),
        ("ORCH_DISCARD_HOOK_READ_TIMEOUT_SECONDS", "301"),
        ("ORCH_DISCARD_HOOK_PASS_BUDGET_SECONDS", "601"),
        ("ORCH_DISCARD_HOOK_CONNECT_TIMEOUT_SECONDS", "0"),
        ("ORCH_DISCARD_HOOK_READ_TIMEOUT_SECONDS", "0"),
        ("ORCH_DISCARD_HOOK_PASS_BUDGET_SECONDS", "-1"),
    ],
)
def test_hook_timeout_settings_are_bounded(monkeypatch, name, value):
    monkeypatch.setenv("ORCH_DATABASE_URL", "postgresql://x/y")
    monkeypatch.setenv("ORCH_MASTER_KEY", "k" * 32)
    monkeypatch.setenv("ORCH_AGENT_IMAGE", IMAGE)
    monkeypatch.setenv(name, value)
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert name in exc.value.problems


def test_read_timeout_may_not_exceed_the_overall_timeout():
    with pytest.raises(SettingsError) as exc:
        settings(discard_hook_timeout_seconds=20, discard_hook_read_timeout_seconds=30)
    assert "ORCH_DISCARD_HOOK_READ_TIMEOUT_SECONDS" in exc.value.problems
    settings(discard_hook_timeout_seconds=30, discard_hook_read_timeout_seconds=30)
