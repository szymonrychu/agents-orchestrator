"""Entrypoints: commands, advisory singletons, signal shutdown."""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import socket
import subprocess
import sys
import time

import httpx
import psycopg
import pytest
from psycopg.conninfo import conninfo_to_dict, make_conninfo

from agents_orchestrator import cli
from agents_orchestrator.runtime.run import run_service
from agents_orchestrator.runtime.singleton import advisory_singleton, lock_key
from agents_orchestrator.settings import Settings


def _env(dsn: str, **extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith("ORCH_")}
    env.update({"ORCH_DATABASE_URL": dsn, "ORCH_LOG_LEVEL": "INFO", **extra})
    return env


def _orch(*args: str, dsn: str, **extra: str) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "agents_orchestrator.cli", *args],
        env=_env(dsn, **extra),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )


def _lock_held(admin_dsn_: str, name: str) -> bool:
    key = lock_key(name)
    with psycopg.connect(admin_dsn_) as conn:
        row = conn.execute(
            "SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND granted"
            " AND ((classid::bigint << 32) | objid::bigint) = %s",
            (key,),
        ).fetchone()
    return row[0] > 0


def _wait_for(pred, timeout: float = 20.0):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        if pred():
            return
        time.sleep(0.1)
    raise AssertionError("condition not met in time")


def test_cli_registers_all_commands(capsys):
    help_text = cli.build_parser().format_help()
    for name in ("api", "dispatcher", "ingester", "reconciler", "migrate"):
        assert name in help_text


def test_migrate_command_applies_schema(empty_dsn):
    proc = _orch("migrate", dsn=empty_dsn)
    out, _ = proc.communicate(timeout=60)
    assert proc.returncode == 0, out
    with psycopg.connect(empty_dsn) as conn:
        assert conn.execute("SELECT count(*) FROM schema_migrations").fetchone()[0] > 0
        assert conn.execute("SELECT to_regclass('public.sessions')").fetchone()[0] is not None
    json.loads(out.strip().splitlines()[-1])  # logs are JSON lines


def test_api_uvicorn_config_ignores_proxy_headers(dsn):
    config = cli.build_uvicorn_config(
        Settings(database_url=dsn, http_host="127.0.0.1", http_port=1234)
    )
    assert config.proxy_headers is False
    assert config.host == "127.0.0.1"
    assert config.port == 1234


def test_api_command_serves_healthz(dsn):
    with socket.socket() as s1, socket.socket() as s2, socket.socket() as s3:
        for s in (s1, s2, s3):
            s.bind(("127.0.0.1", 0))
        port, agent_port, service_port = (s.getsockname()[1] for s in (s1, s2, s3))
    proc = _orch(
        "api",
        dsn=dsn,
        ORCH_HTTP_HOST="127.0.0.1",
        ORCH_HTTP_PORT=str(port),
        ORCH_AGENT_PORT=str(agent_port),
        ORCH_SERVICE_PORT=str(service_port),
        ORCH_ALLOWED_CIDRS="127.0.0.0/8",
    )
    try:
        url = f"http://127.0.0.1:{port}/healthz"

        def up() -> bool:
            assert proc.poll() is None, proc.stdout.read()
            try:
                return httpx.get(url, timeout=1).json() == {"status": "ok"}
            except httpx.HTTPError:
                return False

        _wait_for(up)
    finally:
        proc.send_signal(signal.SIGTERM)
        proc.communicate(timeout=30)
    assert proc.returncode == 0


def test_sigterm_stops_loop_cleanly(dsn, admin_dsn):
    proc = _orch("ingester", dsn=dsn)
    try:
        _wait_for(lambda: _lock_held(dsn, "ingester"))
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=30)
    finally:
        if proc.poll() is None:
            proc.kill()
    assert proc.returncode == 0, out
    assert not _lock_held(dsn, "ingester")


async def test_singleton_second_waits_until_first_releases(dsn):
    kw = {"heartbeat_seconds": 0.5, "tcp_user_timeout_seconds": 5}
    async with advisory_singleton(dsn, "waiter", **kw) as first:
        assert first.held
        acquired = asyncio.Event()

        async def second() -> None:
            async with advisory_singleton(dsn, "waiter", **kw) as sing:
                assert sing.held
                acquired.set()

        task = asyncio.create_task(second())
        await asyncio.sleep(1.5)
        assert not acquired.is_set()
    await asyncio.wait_for(task, 10)
    assert acquired.is_set()


async def test_singleton_lost_connection_stops_holder(dsn, admin_dsn):
    stop = asyncio.Event()

    async def service(_pool):
        class S:
            async def run(self, stop: asyncio.Event) -> None:
                await stop.wait()

        return S()

    settings = Settings(database_url=dsn, lock_heartbeat_seconds=0.5, shutdown_grace_seconds=5)
    task = asyncio.create_task(
        run_service(settings, service, singleton="victim", stop=stop, handle_signals=False)
    )
    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        end = time.monotonic() + 10
        while not _lock_held(admin_dsn, "victim"):
            assert time.monotonic() < end
            await asyncio.sleep(0.1)
        admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_locks WHERE locktype='advisory' AND granted"
            " AND ((classid::bigint << 32) | objid::bigint) = %s",
            (lock_key("victim"),),
        )
    assert await asyncio.wait_for(task, 15) == 3
    assert stop.is_set()


async def test_singleton_heartbeat_detects_lost_lock(dsn):
    async with advisory_singleton(
        dsn, "quiet", heartbeat_seconds=0.3, tcp_user_timeout_seconds=5
    ) as sing:
        assert sing.held
        # drop the lock on the live connection: no connection error, only a negative heartbeat
        await sing.conn.execute("SELECT pg_advisory_unlock_all()")
        await asyncio.wait_for(sing.stop.wait(), 5)
        assert sing.lost


class _BlackholeProxy:
    """TCP proxy that can silently stop forwarding while keeping sockets open."""

    def __init__(self, host: str, port: int) -> None:
        self.target = (host, port)
        self.blackhole = False
        self.server: asyncio.Server | None = None
        self.writers: list[asyncio.StreamWriter] = []

    async def start(self) -> int:
        self.server = await asyncio.start_server(self._handle, "127.0.0.1", 0)
        return self.server.sockets[0].getsockname()[1]

    async def _pipe(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        with contextlib.suppress(Exception):
            while data := await r.read(65536):
                if not self.blackhole:
                    w.write(data)
                    await w.drain()
        with contextlib.suppress(Exception):
            if not self.blackhole:
                w.close()

    async def _handle(self, r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        ur, uw = await asyncio.open_connection(*self.target)
        self.writers += [w, uw]
        await asyncio.gather(self._pipe(r, uw), self._pipe(ur, w))

    async def stop(self) -> None:
        for w in self.writers:
            w.close()
        assert self.server is not None
        self.server.close()


async def test_singleton_blackholed_connection_exits_3(dsn):
    info = conninfo_to_dict(dsn)
    proxy = _BlackholeProxy(info["host"], int(info["port"]))
    port = await proxy.start()
    proxied = make_conninfo(dsn, host="127.0.0.1", port=port)
    settings = Settings(
        database_url=proxied,
        lock_heartbeat_seconds=0.5,
        lock_tcp_user_timeout_seconds=2,
        shutdown_grace_seconds=5,
    )
    stop = asyncio.Event()

    async def service(_pool):
        class S:
            async def run(self, stop: asyncio.Event) -> None:
                await stop.wait()

        return S()

    # the service pool must not go through the blackhole: only the lock connection does
    async def run() -> int:
        return await run_service(
            settings.model_copy(update={"database_url": proxied}),
            service,
            singleton="hole",
            stop=stop,
            handle_signals=False,
        )

    try:
        task = asyncio.create_task(run())
        end = time.monotonic() + 10
        while not _lock_held(dsn, "hole"):
            assert time.monotonic() < end
            await asyncio.sleep(0.1)
        proxy.blackhole = True
        started = time.monotonic()
        code = await asyncio.wait_for(task, 20)
        elapsed = time.monotonic() - started
    finally:
        await proxy.stop()
    assert code == 3
    assert elapsed <= 2 + 0.5 + 3  # tcp_user_timeout + one heartbeat + slack


@pytest.mark.parametrize("name", ["ingester", "reconciler"])
def test_lock_keys_are_stable_and_distinct(name):
    assert lock_key(name) == lock_key(name)
    assert lock_key("ingester") != lock_key("reconciler")
    assert -(2**63) <= lock_key(name) < 2**63
