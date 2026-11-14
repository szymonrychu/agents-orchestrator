"""Run one orchestrator service: JSON logs, SIGTERM/SIGINT shutdown, optional singleton."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import signal
import sys
import time
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from psycopg_pool import AsyncConnectionPool

from agents_orchestrator.db.pool import open_pool
from agents_orchestrator.runtime.singleton import EXIT_LOCK_LOST, advisory_singleton
from agents_orchestrator.settings import Settings

log = logging.getLogger("agents_orchestrator")


class Service(Protocol):
    async def run(self, stop: asyncio.Event) -> None: ...


ServiceFactory = Callable[[AsyncConnectionPool], "Service | Awaitable[Service]"]


_RESERVED = frozenset({"ts", "level", "logger", "msg", "exc"})


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(record.created))
            + f".{int(record.msecs):03d}Z",
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        fields = getattr(record, "fields", None)
        if isinstance(fields, dict):
            out.update({k: v for k, v in fields.items() if k not in _RESERVED})
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level.upper())


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)


def remove_signal_handlers() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        with contextlib.suppress(Exception):
            loop.remove_signal_handler(sig)


async def _drain(task: asyncio.Task[None], grace: float) -> None:
    """Wait up to `grace` for the loop to finish its iteration, then cancel it."""
    try:
        async with asyncio.timeout(grace):
            await asyncio.shield(task)
    except TimeoutError:
        log.warning("service did not stop within %.1fs; cancelling", grace)
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError, Exception):
            await task
    except asyncio.CancelledError:
        raise
    except Exception:
        log.exception("service failed")
        raise


async def run_service(
    settings: Settings,
    factory: ServiceFactory,
    *,
    singleton: str | None = None,
    stop: asyncio.Event | None = None,
    handle_signals: bool = True,
) -> int:
    """Run the service until stopped; return the process exit code (0, 3 lock lost, 1 failure)."""
    stop = stop if stop is not None else asyncio.Event()
    if handle_signals:
        install_signal_handlers(stop)
    grace = settings.shutdown_grace_seconds
    try:
        async with contextlib.AsyncExitStack() as stack:
            held = None
            if singleton is not None:
                held = await stack.enter_async_context(
                    advisory_singleton(
                        settings.database_url,
                        singleton,
                        heartbeat_seconds=settings.lock_heartbeat_seconds,
                        tcp_user_timeout_seconds=settings.lock_tcp_user_timeout_seconds,
                        stop=stop,
                    )
                )
                if not held.held and not held.lost:
                    return 0  # stopped while a hot standby
            pool = await open_pool(settings.database_url)
            try:
                service = factory(pool)
                if asyncio.iscoroutine(service):
                    service = await service
                task = asyncio.create_task(service.run(stop), name="service")
                stop_wait = asyncio.create_task(stop.wait())
                await asyncio.wait({task, stop_wait}, return_when=asyncio.FIRST_COMPLETED)
                stop_wait.cancel()
                stop.set()
                await _drain(task, grace)
            finally:
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(grace):
                        await pool.close()
            return EXIT_LOCK_LOST if held is not None and held.lost else 0
    except Exception:
        log.exception("fatal")
        return 1
    finally:
        if handle_signals:
            remove_signal_handlers()
