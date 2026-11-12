"""Project scope of agent sessions (projects plan 3.2, 3.3, 8): the `AGENT_SCOPE` table covers
every agent-port route, `/me` and `GET /agent/v1/projects` show the caller's scope, and the
taskboard relay forwards `X-Orch-Projects` computed by the orchestrator, never the agent's."""

from __future__ import annotations

import httpx
from factories import two_project_fixture
from test_api_agent import AGENT_PORT, headers, make_settings
from test_readme_api import _walk

from agents_orchestrator.api import admin, proposal_routes
from agents_orchestrator.api.allowlist import is_agent_path
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.scope import AGENT_SCOPE, PROJECTS_HEADER, scope_of
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

TB_URL = "http://taskboard.test:9002"


def agent_routes() -> set[tuple[str, str]]:
    app = create_app(Settings(database_url="postgresql://unused/db"), pool=None)
    return {
        (m, path)
        for path, methods in _walk(app)
        if is_agent_path(path)
        for m in methods - {"HEAD", "OPTIONS"}
    }


def test_every_agent_route_is_classified():
    routes = agent_routes()
    unclassified = sorted(routes - set(AGENT_SCOPE))
    assert not unclassified, f"agent routes missing from AGENT_SCOPE: {unclassified}"
    stale = sorted(set(AGENT_SCOPE) - routes)
    assert not stale, f"AGENT_SCOPE rows with no route: {stale}"
    assert set(AGENT_SCOPE.values()) == {"own", "peers", "project", "superleader"}
    # session-spec-patch and proposals are on the agent port and classified own
    spec_patch = {(m, r.path) for r in admin.session_router.routes for m in r.methods}
    proposals = {(m, r.path) for r in proposal_routes.proposal_router.routes for m in r.methods}
    assert spec_patch and proposals
    assert {AGENT_SCOPE[k] for k in spec_patch | proposals} == {"own"}
    assert AGENT_SCOPE[("POST", "/agent/v1/chat/messages")] == "own"
    assert AGENT_SCOPE[("GET", "/agent/v1/projects")] == "project"


def test_agent_scope_table_covers_mindmap_routes():
    rows = {k: v for k, v in AGENT_SCOPE.items() if k[1].startswith("/agent/v1/mindmap")}
    assert {m for m, _ in rows} == {"GET", "POST", "PATCH", "DELETE"}
    assert {p for _, p in rows} == {"/agent/v1/mindmap/{path:path}"}
    assert set(rows.values()) == {"project"}


async def test_scope_of_superleader_is_star(pool):
    seats = await two_project_fixture(pool)
    scopes = {}
    async with pool.connection() as conn:
        for name, seat in seats.items():
            session = await sessions_repo.get(conn, seat.id)
            assert session is not None
            scopes[name] = scope_of(session)
    assert scopes == {"A1": "a", "A2": "a", "B1": "b", "S": "*"}


def _client(app, ip: str) -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app, client=(ip, 4321))
    return httpx.AsyncClient(transport=transport, base_url=f"http://orch:{AGENT_PORT}")


async def test_me_shows_project_and_scope(pool, dsn):
    seats = await two_project_fixture(pool)
    app = create_app(make_settings(dsn), pool)
    seen = {}
    for name in ("A1", "B1", "S"):
        async with _client(app, seats[name].ip) as http:
            r = await http.get("/agent/v1/me", headers=headers(seats[name].id))
        assert r.status_code == 200, r.text
        seen[name] = (r.json()["project"], r.json()["projects_scope"])
    assert seen == {"A1": ("a", "a"), "B1": ("b", "b"), "S": ("a", "*")}


async def _projects(app, seat) -> list[str]:
    async with _client(app, seat.ip) as http:
        r = await http.get("/agent/v1/projects", headers=headers(seat.id))
    assert r.status_code == 200, r.text
    return [p["slug"] for p in r.json()]


async def test_agent_projects_own_only(pool, dsn):
    seats = await two_project_fixture(pool)
    app = create_app(make_settings(dsn), pool)
    assert await _projects(app, seats["A1"]) == ["a"]
    assert await _projects(app, seats["B1"]) == ["b"]
    # the caller's own project is answered even once archived
    async with pool.connection() as conn:
        await conn.execute("UPDATE projects SET archived_at = now() WHERE slug = 'b'")
    async with _client(app, seats["B1"].ip) as http:
        r = await http.get("/agent/v1/projects", headers=headers(seats["B1"].id))
    assert [(p["slug"], p["archived"]) for p in r.json()] == [("b", True)]


async def test_agent_projects_superleader_all(pool, dsn):
    seats = await two_project_fixture(pool)
    app = create_app(make_settings(dsn), pool)
    assert await _projects(app, seats["S"]) == ["a", "b", "default"]
    async with pool.connection() as conn:
        await conn.execute("UPDATE projects SET archived_at = now() WHERE slug = 'b'")
    # archived projects stay listed for a superleader (T-120 slice 2)
    assert await _projects(app, seats["S"]) == ["a", "b", "default"]


def _taskboard_app(pool, dsn):
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"items": [], "next_cursor": None})

    settings = make_settings(dsn).model_copy(
        update={"taskboard_url": TB_URL, "taskboard_token": "tb-token"}
    )
    app = create_app(settings, pool)
    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return app, seen


async def test_task_relay_sends_projects_header(pool, dsn):
    seats = await two_project_fixture(pool)
    app, seen = _taskboard_app(pool, dsn)
    for name in ("A1", "B1", "S"):
        async with _client(app, seats[name].ip) as http:
            r = await http.get("/agent/v1/tasks", headers=headers(seats[name].id))
            assert r.status_code == 200, r.text
            r = await http.post(
                "/agent/v1/tasks/T-1/comments", headers=headers(seats[name].id), json={"x": 1}
            )
            assert r.status_code == 200, r.text
    assert [s.headers[PROJECTS_HEADER] for s in seen] == ["a", "a", "b", "b", "*", "*"]


async def test_agent_supplied_projects_header_is_dropped(pool, dsn):
    seats = await two_project_fixture(pool)
    app, seen = _taskboard_app(pool, dsn)
    async with _client(app, seats["B1"].ip) as http:
        r = await http.get(
            "/agent/v1/tasks", headers={**headers(seats["B1"].id), PROJECTS_HEADER: "*"}
        )
    assert r.status_code == 200, r.text
    [sent] = seen
    assert sent.headers.get_list(PROJECTS_HEADER) == ["b"]
