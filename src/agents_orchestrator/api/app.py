"""FastAPI app factory."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool

from agents_orchestrator import __version__
from agents_orchestrator.api import (
    admin,
    admin_relay,
    agent,
    archives,
    autoclose,
    chat,
    errors,
    forward,
    messages,
    presets,
    projects,
    proposal_routes,
    proposal_sweep,
    secret_store,
    session_labels,
    sessions,
    streams,
    tasks,
    templates,
    workspace,
)
from agents_orchestrator.api.allowlist import PortGate, SourceAllowlist
from agents_orchestrator.api.errors import error_response
from agents_orchestrator.api.listener import Listener
from agents_orchestrator.api.origin import OriginAllowlist
from agents_orchestrator.api.secret_store import KubeApi
from agents_orchestrator.api.service_auth import ServiceGate
from agents_orchestrator.api.usage_gate import UsageSource
from agents_orchestrator.db.pool import open_pool
from agents_orchestrator.settings import Settings


# Module level, not closures in `create_app`: FastAPI keeps a process-wide cache keyed by
# endpoint function (fastapi.dependencies.models), so an endpoint closing over its app pins
# that app, its routes and its state for the life of the process.
async def healthz() -> dict:
    return {"status": "ok"}


async def readyz(request: Request):
    try:
        async with request.app.state.pool.connection(timeout=3) as conn:
            await conn.execute("SELECT 1")
    except Exception as exc:
        return error_response(503, "internal", f"database unavailable: {type(exc).__name__}")
    return JSONResponse({"status": "ready"})


def create_app(
    settings: Settings,
    pool: AsyncConnectionPool | None = None,
    stream_poll_seconds: float = streams.DEFAULT_POLL_SECONDS,
    kube: KubeApi | None = None,
    usage_transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    owns_pool = pool is None
    listener = Listener(settings.database_url)
    usage = (
        UsageSource(
            settings.usage_url,
            settings.usage_token,
            settings.usage_cache_seconds,
            settings.usage_timeout_seconds,
            transport=usage_transport,
        )
        if settings.usage_available
        else None
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if owns_pool:
            app.state.pool = await open_pool(settings.database_url)
        await listener.start()
        sweeper = asyncio.create_task(autoclose.sweep(app.state.pool, settings), name="autoclose")
        app.state.autoclose_task = sweeper
        proposal_sweeper = asyncio.create_task(proposal_sweep.sweep(app), name="proposal_sweep")
        app.state.proposal_sweep_task = proposal_sweeper
        try:
            yield
        finally:
            for task in (sweeper, proposal_sweeper):
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
            await listener.stop()
            await workspace.close_client(app)
            await chat.close_client(app)
            await tasks.close_client(app)
            await forward.close_client(app, admin_relay.STATE_ATTR)
            await secret_store.close_kube(app)
            if usage is not None:
                await usage.aclose()
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
    app.state.taskboard_http = None
    app.state.admin_http = None
    app.state.kube = kube  # given (tests) or built on first use by api.secret_store
    app.state.kube_owned = False
    app.state.stream_poll_seconds = stream_poll_seconds
    app.state.usage = usage  # the usage gate's source; None when the gate is off
    errors.install(app)
    app.include_router(sessions.router)
    app.include_router(agent.router)
    app.include_router(admin.router)
    app.include_router(admin.session_router)
    app.include_router(proposal_routes.proposal_router)
    app.include_router(chat.router)
    app.include_router(tasks.router)
    app.include_router(session_labels.router)
    app.include_router(presets.router)
    app.include_router(proposal_routes.main_router)
    app.include_router(projects.router)
    app.include_router(templates.router)
    app.include_router(archives.router)
    app.include_router(messages.router)
    app.include_router(streams.router)
    app.include_router(workspace.router)

    app.add_api_route("/healthz", healthz, include_in_schema=False)
    app.add_api_route("/readyz", readyz, include_in_schema=False)
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
        scopes=settings.service_scopes,
    )
    app.add_middleware(PortGate, agent_port=settings.agent_port, service_port=settings.service_port)
    return app
