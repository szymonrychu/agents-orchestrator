"""FastAPI app factory."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import __version__
from agents_orchestrator.api import (
    agent,
    archives,
    autoclose,
    chat,
    errors,
    messages,
    presets,
    sessions,
    streams,
    templates,
    workspace,
)
from agents_orchestrator.api.allowlist import PortGate, SourceAllowlist
from agents_orchestrator.api.errors import error_response
from agents_orchestrator.api.listener import Listener
from agents_orchestrator.api.origin import OriginAllowlist
from agents_orchestrator.api.service_auth import ServiceGate
from agents_orchestrator.db.pool import open_pool
from agents_orchestrator.settings import Settings


def create_app(
    settings: Settings,
    pool: AsyncConnectionPool | None = None,
    stream_poll_seconds: float = streams.DEFAULT_POLL_SECONDS,
) -> FastAPI:
    owns_pool = pool is None
    listener = Listener(settings.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if owns_pool:
            app.state.pool = await open_pool(settings.database_url)
        await listener.start()
        sweeper = asyncio.create_task(autoclose.sweep(app.state.pool), name="autoclose")
        app.state.autoclose_task = sweeper
        try:
            yield
        finally:
            sweeper.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await sweeper
            await listener.stop()
            await workspace.close_client(app)
            await chat.close_client(app)
            if owns_pool:
                await app.state.pool.close()

    app = FastAPI(
        title="agents-orchestrator",
        version=__version__,
        lifespan=lifespan,
        openapi_url="/api/v1/openapi.json",
        docs_url="/api/v1/docs",
        redoc_url=None,
    )
    app.state.settings = settings
    app.state.pool = pool
    app.state.listener = listener
    app.state.wrapper_http = None
    app.state.chat_http = None
    app.state.stream_poll_seconds = stream_poll_seconds
    errors.install(app)
    app.include_router(sessions.router)
    app.include_router(agent.router)
    app.include_router(chat.router)
    app.include_router(presets.router)
    app.include_router(templates.router)
    app.include_router(archives.router)
    app.include_router(messages.router)
    app.include_router(streams.router)
    app.include_router(workspace.router)

    @app.get("/healthz", include_in_schema=False)
    async def healthz() -> dict:
        return {"status": "ok"}

    @app.get("/readyz", include_in_schema=False)
    async def readyz():
        try:
            async with app.state.pool.connection(timeout=3) as conn:
                await conn.execute("SELECT 1")
        except Exception as exc:
            return error_response(503, "internal", f"database unavailable: {type(exc).__name__}")
        return JSONResponse({"status": "ready"})

    app.add_middleware(OriginAllowlist, origins=settings.allowed_origins)
    app.add_middleware(
        SourceAllowlist,
        networks=settings.allowed_networks(),
        skip_port=settings.agent_port,
        service_port=settings.service_port,
    )
    app.add_middleware(
        ServiceGate,
        service_port=settings.service_port,
        token_hashes=settings.service_token_hashes(),
    )
    app.add_middleware(PortGate, agent_port=settings.agent_port, service_port=settings.service_port)
    return app
