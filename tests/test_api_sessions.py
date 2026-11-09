"""Sessions and agents routes, error shape, source allowlist and the layering rule."""

from __future__ import annotations

import ast
import asyncio
import gc
import uuid
import weakref
from datetime import datetime
from pathlib import Path

import httpx
import pytest
import pytest_asyncio
from factories import insert_agent, insert_message, insert_session, set_agent_status
from psycopg.types.json import Jsonb

import agents_orchestrator.api as api_pkg
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.settings import Settings

PEER = ("10.1.2.3", 5555)


def make_settings(dsn: str = "postgresql://unused/unused", **kw) -> Settings:
    kw.setdefault("allowed_cidrs", "10.1.2.0/24")
    kw.setdefault("agent_image", "img:1")
    return Settings(database_url=dsn, **kw)


def client_for(app, peer=PEER, **kw) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=peer),
        base_url="http://orch",
        **kw,
    )


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        yield c


def assert_error(resp, status, code):
    assert resp.status_code == status, resp.text
    body = resp.json()
    assert set(body) == {"error"}
    assert body["error"]["code"] == code
    assert isinstance(body["error"]["message"], str)


async def test_create_and_get_session(http):
    r = await http.post("/api/v1/sessions", json={"slug": "alpha", "spec": {"replicas": 2}})
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["generation"] == 1
    assert created["slug"] == "alpha"
    got = (await http.get(f"/api/v1/sessions/{created['id']}")).json()
    assert got["generation"] == 1
    assert got["observed_generation"] == 0
    assert got["desired"] == "active"
    assert got["spec"]["replicas"] == 2
    assert got["summary"]["replicas"] == 2
    assert got["summary"]["state"] == "booting"
    listed = (await http.get("/api/v1/sessions")).json()
    assert [c["slug"] for c in listed] == ["alpha"]


@pytest.mark.parametrize(
    "body",
    [
        {"slug": "Bad_Slug", "spec": {}},
        {"slug": "orchestrator-x", "spec": {}},
        {"slug": "ok", "spec": {"replicas": 0}},
        {"slug": "ok", "spec": {"nope": 1}},
        {"spec": {}},
    ],
)
async def test_create_rejects_bad_slug_422(http, body):
    assert_error(await http.post("/api/v1/sessions", json=body), 422, "invalid_request")


async def test_duplicate_slug_409(http):
    body = {"slug": "dup", "spec": {}}
    assert (await http.post("/api/v1/sessions", json=body)).status_code == 201
    assert_error(await http.post("/api/v1/sessions", json=body), 409, "conflict")


def _walk_routes(router, prefix: str = ""):
    """(path, methods) of every route; included routers are lazy, so descend into them."""
    for route in router.routes:
        if hasattr(route, "original_router"):
            yield from _walk_routes(route.original_router, prefix + route.include_context.prefix)
        elif hasattr(route, "methods"):
            yield prefix + route.path, route.methods


async def test_patch_session_retired_405(http):
    cid = (await http.post("/api/v1/sessions", json={"slug": "p", "spec": {}})).json()["id"]
    r = await http.patch(f"/api/v1/sessions/{cid}", json={"spec": {"replicas": 3}})
    assert r.status_code == 405, r.text
    assert (await http.get(f"/api/v1/sessions/{cid}")).json()["generation"] == 1
    assert not [
        path
        for path, methods in _walk_routes(http._transport.app)  # type: ignore[attr-defined]
        if path.startswith("/api/v1")
        and "PATCH" in methods
        and not path.endswith(("/labels", "/project"))
        and not path.startswith("/api/v1/projects/")
    ]
    spec = (await http.get("/api/v1/openapi.json")).json()
    assert not [
        p
        for p, ops in spec["paths"].items()
        if p.startswith("/api/v1")
        and "patch" in ops
        and not p.endswith(("/labels", "/project"))
        and not p.startswith("/api/v1/projects/")
    ]


async def forced_at(pool, session_id: str):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT desired, discard_forced_at FROM sessions WHERE id = %s", (session_id,)
        )
        return await cur.fetchone()


async def test_delete_force_marks_a_forced_discard_also_when_already_deleting(http, pool):
    """T-26: `?force=true` abandons the owed pre-discard hooks; it also works on a session
    already stuck in deleting, where a plain repeat delete changes nothing."""
    cid = (await http.post("/api/v1/sessions", json={"slug": "f", "spec": {}})).json()["id"]
    r = await http.delete(f"/api/v1/sessions/{cid}?force=false")
    assert r.status_code == 202, r.text
    assert await forced_at(pool, cid) == ("deleting", None)
    r = await http.delete(f"/api/v1/sessions/{cid}?force=true")
    assert r.status_code == 202, r.text
    assert r.json()["desired"] == "deleting"
    desired, forced = await forced_at(pool, cid)
    assert desired == "deleting" and forced is not None
    # a repeat force keeps the first stamp
    assert (await http.delete(f"/api/v1/sessions/{cid}?force=true")).status_code == 202
    assert (await forced_at(pool, cid))[1] == forced
    for bad in ("force=yes", "force=true&force=true", "force=true&purge=true"):
        assert_error(await http.delete(f"/api/v1/sessions/{cid}?{bad}"), 422, "invalid_request")

    other = (await http.post("/api/v1/sessions", json={"slug": "g", "spec": {}})).json()["id"]
    r = await http.delete(f"/api/v1/sessions/{other}?force=true")
    assert r.status_code == 202, r.text
    desired, forced = await forced_at(pool, other)
    assert desired == "deleting" and forced is not None


async def test_delete_is_discard_without_purge(http, pool):
    cid = (await http.post("/api/v1/sessions", json={"slug": "d", "spec": {}})).json()["id"]
    r = await http.delete(f"/api/v1/sessions/{cid}")
    assert r.status_code == 202
    assert r.json()["desired"] == "deleting"
    assert r.json()["summary"]["state"] == "deleting"
    again = await http.delete(f"/api/v1/sessions/{cid}")
    assert again.status_code == 202
    assert again.json()["desired"] == "deleting"
    assert_error(await http.delete(f"/api/v1/sessions/{cid}?purge=true"), 422, "invalid_request")
    async with pool.connection() as conn:
        await repo.purge(conn, uuid.UUID(cid))
    assert_error(await http.get(f"/api/v1/sessions/{cid}"), 404, "not_found")
    assert_error(await http.delete(f"/api/v1/sessions/{cid}"), 404, "not_found")


async def test_agents_view_joins_status(http, pool):
    c = await insert_session(pool, spec={"replicas": 2})
    a0 = await insert_agent(pool, c["id"], ready=True, pod_name="web-0")
    await insert_agent(pool, c["id"], ready=False, pod_name="web-1")
    await insert_agent(pool, c["id"], ready=False, pod_name="web-2", gone_at=None)
    await set_agent_status(
        pool,
        a0["id"],
        state="busy",
        current_turn_id="t1",
        context_used_tokens=1000,
        context_window_tokens=4000,
        context_pct=25.0,
        peers=[{"name": "x"}, {"name": "y"}],
    )
    r = await http.get(f"/api/v1/sessions/{c['id']}/agents")
    assert r.status_code == 200
    agents = r.json()
    assert [a["ordinal"] for a in agents] == [0, 1, 2]
    first = agents[0]
    assert first["pod_name"] == "web-0"
    assert first["ready"] is True
    assert first["state"] == "busy"
    assert first["current_turn_id"] == "t1"
    assert first["context"] == {"used_tokens": 1000, "window_tokens": 4000, "used_percentage": 25.0}
    assert first["peers"] == 2
    assert first["gone"] is False
    assert first["free_since"] is not None
    assert agents[1]["state"] == "unknown"
    assert agents[1]["context"] is None
    assert agents[1]["peers"] == 0
    assert_error(await http.get(f"/api/v1/sessions/{uuid.uuid4()}/agents"), 404, "not_found")


async def test_session_summary_state_and_gauge_max(http, pool):
    c = await insert_session(pool, spec={"replicas": 3, "auto_compact_pct": 80})
    a0 = await insert_agent(pool, c["id"], ready=True)
    a1 = await insert_agent(pool, c["id"], ready=True)
    await insert_agent(pool, c["id"], ready=False)
    await set_agent_status(pool, a0["id"], state="idle", context_pct=30.0)
    await set_agent_status(pool, a1["id"], state="idle", context_pct=62.5)
    first = await insert_message(pool, c["id"], status="queued")
    await insert_message(pool, c["id"], status="queued")
    await insert_message(pool, c["id"], status="failed")
    summary = (await http.get(f"/api/v1/sessions/{c['id']}")).json()["summary"]
    assert datetime.fromisoformat(summary.pop("oldest_queued_at")) == first["created_at"]
    assert summary == {
        "state": "waiting",
        "start_state": "ready",
        "bootstrap_failures": [],
        "ready": 2,
        "replicas": 3,
        "context_pct_max": 62.5,
        "auto_compact_pct": 80,
        "queued": 2,
        "failed": 1,
        "attention_agents": 0,
        "bootstrap_failed_agents": 0,
        "permission_unavailable_agents": 0,
    }
    await set_agent_status(pool, a1["id"], state="busy", context_pct=62.5)
    summary = (await http.get("/api/v1/sessions")).json()[0]["summary"]
    assert summary["state"] == "working"

    empty = await insert_session(pool, spec={})
    got = (await http.get(f"/api/v1/sessions/{empty['id']}")).json()["summary"]
    assert got["context_pct_max"] is None
    assert got["state"] == "booting"
    assert got["auto_compact_pct"] == 95

    bad = await insert_session(pool, spec={}, status={"error": "boom"})
    assert (await http.get(f"/api/v1/sessions/{bad['id']}")).json()["summary"]["state"] == "error"


async def test_session_view_exposes_discard_hook_state(http, pool):
    c = await insert_session(pool, desired="deleting", status={"discard_hook_failed": "http 500"})
    view = (await http.get(f"/api/v1/sessions/{c['id']}")).json()
    assert view["status"]["discard_hook_failed"] == "http 500"
    assert view["summary"]["state"] == "deleting"


async def test_errors_use_wrapper_shape(http):
    assert_error(await http.get(f"/api/v1/sessions/{uuid.uuid4()}"), 404, "not_found")
    assert_error(await http.get("/api/v1/sessions/not-a-uuid"), 422, "invalid_request")
    assert_error(await http.get("/api/v1/nothing-here"), 404, "not_found")
    assert_error(await http.post("/api/v1/sessions", content=b"{"), 422, "invalid_request")


async def test_unexpected_error_is_internal(pool, dsn):
    app = create_app(make_settings(dsn), pool)

    @app.get("/api/v1/boom")
    async def boom():
        raise RuntimeError("x")

    transport = httpx.ASGITransport(app=app, client=PEER, raise_app_exceptions=False)
    async with httpx.AsyncClient(transport=transport, base_url="http://orch") as c:
        assert_error(await c.get("/api/v1/boom"), 500, "internal")


def _imports(path: Path) -> set[str]:
    out: set[str] = set()
    for node in ast.walk(ast.parse(path.read_text())):
        if isinstance(node, ast.Import):
            out.update(a.name for a in node.names)
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if node.level and mod in ("k8s", "wrapper", "reconciler", "tokens"):
                mod = "agents_orchestrator." + mod
            out.add(mod)
            out.update(f"{mod}.{a.name}" for a in node.names)
    return out


def test_api_never_imports_k8s_or_wrapper():
    """Only api/workspace.py (the proxy seam, N1) may call wrappers; it, api/agent.py and
    api/sessions.py (agent-token verify for services) may derive tokens. Nothing in api imports
    k8s or the reconciler, except api/secret_store.py, the seam that stores webhook endpoint
    credentials (superleader-crud D9)."""
    always = ("agents_orchestrator.k8s", "agents_orchestrator.reconciler")
    k8s_seam = "secret_store.py"
    wrapper = "agents_orchestrator.wrapper"
    tokens = "agents_orchestrator.tokens"
    seam = "workspace.py"
    token_users = {seam, "agent.py", "sessions.py"}
    files = list(Path(api_pkg.__file__).parent.glob("*.py"))
    assert len(files) >= 6
    assert seam in {f.name for f in files}
    for f in files:
        for n in _imports(f):
            if f.name != k8s_seam:
                assert not n.startswith(always), f"{f.name} imports {n}"
            if f.name != seam:
                assert not n.startswith(wrapper), f"{f.name} imports {n}"
            if f.name not in token_users:
                assert not n.startswith(tokens), f"{f.name} imports {n}"
    assert "agent.py" in {f.name for f in files}
    assert any(n.startswith(tokens) for n in _imports(files[0].with_name("agent.py")))
    assert any(
        n.startswith("agents_orchestrator.wrapper") for n in _imports(files[0].with_name(seam))
    )


async def test_meta_defaults(pool, dsn):
    settings = make_settings(
        dsn,
        default_model="m1",
        default_effort="high",
        models="m1,m2",
        agent_images="img:2",
        max_replicas=4,
        max_pvc_size="50Gi",
        max_sessions=7,
        discard_hook_url="https://secret.example/hook",
    )
    async with client_for(create_app(settings, pool)) as c:
        meta = (await c.get("/api/v1/meta")).json()
    assert "secret.example" not in str(meta)
    assert meta["defaults"] == {
        "model": "m1",
        "effort": "high",
        "workdir": "workdir",
        "resources": {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "4", "memory": "8Gi"},
        },
        "any_mcp_config": {},
        "discard_hook": "url",
        "replicas": 1,
        "auto_compact_pct": 95,
        "pvc_size": "20Gi",
        "storage_class": "rook-ceph-rwx",
        "max_attempts": 5,
    }
    assert meta["agent_image"] == "img:1"
    assert meta["agent_images"] == ["img:1", "img:2"]
    assert meta["models"] == ["m1", "m2"]
    assert meta["caps"] == {"max_replicas": 4, "max_pvc_size": "50Gi", "max_sessions": 7}
    assert meta["discard_hook"] == "url"

    async with client_for(create_app(make_settings(dsn), pool)) as c:
        assert (await c.get("/api/v1/meta")).json()["discard_hook"] == "none"
    cmd = make_settings(dsn, discard_hook_command="/bin/export")
    async with client_for(create_app(cmd, pool)) as c:
        meta = (await c.get("/api/v1/meta")).json()
    assert meta["discard_hook"] == "command"
    assert "/bin/export" not in str(meta)


async def test_foreign_peer_forbidden_except_health(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app, peer=("10.9.9.9", 1)) as c:
        for method, path in [
            ("GET", "/api/v1/sessions"),
            ("POST", "/api/v1/sessions"),
            ("GET", "/api/v1/meta"),
            ("GET", "/api/v1/openapi.json"),
            ("GET", "/api/v1/docs"),
            ("GET", "/anything"),
        ]:
            assert_error(await c.request(method, path), 403, "forbidden")
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.get("/readyz")).status_code == 200
    async with client_for(app, peer=("10.1.2.9", 1)) as c:
        assert (await c.get("/api/v1/sessions")).status_code == 200
        assert (await c.get("/api/v1/openapi.json")).status_code == 200
    async with client_for(app, peer=("::ffff:10.1.2.9", 1)) as c:
        assert (await c.get("/api/v1/sessions")).status_code == 200
    async with client_for(app, peer=("not-an-ip", 1)) as c:
        assert_error(await c.get("/api/v1/sessions"), 403, "forbidden")


async def test_forwarded_headers_ignored_by_allowlist(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    spoof = {
        "X-Forwarded-For": "10.1.2.9",
        "X-Real-IP": "10.1.2.9",
        "Forwarded": "for=10.1.2.9",
    }
    async with client_for(app, peer=("172.16.0.5", 1), headers=spoof) as c:
        assert_error(await c.get("/api/v1/sessions"), 403, "forbidden")
    async with client_for(app, peer=("10.1.2.9", 1), headers={"X-Forwarded-For": "8.8.8.8"}) as c:
        assert (await c.get("/api/v1/sessions")).status_code == 200


async def test_health_and_readiness(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.get("/readyz")).status_code == 200
    await pool.close()
    async with client_for(app) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert_error(await c.get("/readyz"), 503, "internal")


async def test_dropped_app_is_freed(pool, dsn):
    # FastAPI caches endpoint functions process-wide; a route closing over its app kept every
    # app ever built alive, and the suite's heap grew until gen2 GC pauses broke time-bound tests
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        assert (await c.get("/healthz")).status_code == 200
        assert (await c.get("/readyz")).status_code == 200
    ref = weakref.ref(app)
    del app, c
    gc.collect()
    assert ref() is None


async def test_lifespan_opens_and_closes_own_pool(dsn):
    app = create_app(make_settings(dsn))
    async with app.router.lifespan_context(app):
        async with client_for(app) as c:
            assert (await c.get("/readyz")).status_code == 200
            assert (await c.get("/api/v1/sessions")).json() == []
        pool = app.state.pool
    assert pool.closed


async def test_openapi_and_docs_paths(http):
    assert (await http.get("/api/v1/openapi.json")).json()["info"]["title"]
    assert (await http.get("/api/v1/docs")).status_code == 200


@pytest.mark.parametrize(
    "spec",
    [
        {"image": "evil:latest"},
        {"env": {"CCW_TOKEN": {"value": "x"}}},
        {"env": {"HTTPS_PROXY": {"value": "x"}}},
        {"replicas": 5},
        {"pvc_size": "200Gi"},
        {"any_mcp_config": {"plugins": "nope"}},
    ],
)
async def test_spec_hardening_enforced_on_create(pool, dsn, spec):
    app = create_app(make_settings(dsn, max_replicas=4, max_pvc_size="100Gi"), pool)
    async with client_for(app) as c:
        assert_error(
            await c.post("/api/v1/sessions", json={"slug": "h", "spec": spec}),
            422,
            "invalid_request",
        )
        ok = await c.post("/api/v1/sessions", json={"slug": "fine", "spec": {"image": "img:1"}})
        assert ok.status_code == 201


async def test_session_count_cap_409(pool, dsn):
    app = create_app(make_settings(dsn, max_sessions=2), pool)
    async with client_for(app) as c:
        for slug in ("one", "two"):
            assert (
                await c.post("/api/v1/sessions", json={"slug": slug, "spec": {}})
            ).status_code == 201
        assert_error(
            await c.post("/api/v1/sessions", json={"slug": "three", "spec": {}}),
            409,
            "session_limit",
        )


async def test_meta_returns_all_form_defaults(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as c:
        meta = (await c.get("/api/v1/meta")).json()
    assert meta["models"][0] == "claude-opus-5-5"
    assert meta["efforts"] == ["low", "medium", "high", "xhigh", "max"]
    d = meta["defaults"]
    assert d["model"] == "claude-opus-5-5"
    assert d["effort"] == ""
    assert d["workdir"] == "workdir"
    assert d["resources"] == {
        "requests": {"cpu": "500m", "memory": "1Gi"},
        "limits": {"cpu": "4", "memory": "8Gi"},
    }
    assert d["any_mcp_config"] == {}
    assert d["discard_hook"] == "none"
    for key in ("replicas", "pvc_size", "storage_class", "max_attempts", "auto_compact_pct"):
        assert d[key] not in (None, ""), key


async def test_spec_rejects_unknown_model_and_effort(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        for spec in ({"model": "gpt-9"}, {"effort": "turbo"}):
            r = await c.post("/api/v1/sessions", json={"slug": "bad", "spec": spec})
            assert_error(r, 422, "invalid_request")
            assert next(iter(spec.values())) in r.json()["error"]["message"]
        ok = {"model": "claude-haiku-5-5", "effort": "max"}
        assert (
            await c.post("/api/v1/sessions", json={"slug": "ok", "spec": ok})
        ).status_code == 201
        empty = {"effort": ""}
        assert (
            await c.post("/api/v1/sessions", json={"slug": "ok2", "spec": empty})
        ).status_code == 201


async def test_spec_rejects_explicit_request_above_limit(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        res = {"requests": {"cpu": "8"}, "limits": {"cpu": "2"}}
        r = await c.post("/api/v1/sessions", json={"slug": "bad", "spec": {"resources": res}})
        assert_error(r, 422, "invalid_request")
        ok = {"requests": {"cpu": "2"}, "limits": {"memory": "2Gi"}}
        r = await c.post("/api/v1/sessions", json={"slug": "ok", "spec": {"resources": ok}})
        assert r.status_code == 201


async def test_write_fills_default_resources_per_key(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        r = await c.post("/api/v1/sessions", json={"slug": "a", "spec": {}})
        assert r.json()["spec"]["resources"] == {
            "requests": {"cpu": "500m", "memory": "1Gi"},
            "limits": {"cpu": "4", "memory": "8Gi"},
        }
        spec = {"resources": {"requests": {"cpu": "2"}, "limits": {"memory": "2Gi"}}}
        r = await c.post("/api/v1/sessions", json={"slug": "b", "spec": spec})
        assert r.json()["spec"]["resources"] == {
            "requests": {"cpu": "2", "memory": "1Gi"},
            "limits": {"cpu": "4", "memory": "2Gi"},
        }
        # guard: default limit 4 < explicit request 8 stays empty; default request 1Gi > limit
        spec = {"resources": {"requests": {"cpu": "8"}, "limits": {"memory": "512Mi"}}}
        r = await c.post("/api/v1/sessions", json={"slug": "g", "spec": spec})
        assert r.json()["spec"]["resources"] == {
            "requests": {"cpu": "8"},
            "limits": {"memory": "512Mi"},
        }


async def test_create_high_request_without_limit(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        spec = {"resources": {"requests": {"cpu": "8"}}, "replicas": 2}
        r = await c.post("/api/v1/sessions", json={"slug": "hi", "spec": spec})
        assert r.status_code == 201, r.text
        assert "cpu" not in r.json()["spec"]["resources"]["limits"]


async def test_old_cluster_routes_gone(http):
    created = await http.post("/api/v1/sessions", json={"slug": "alpha", "spec": {}})
    assert created.status_code == 201, created.text
    sid = created.json()["id"]
    assert (await http.get("/api/v1/sessions")).status_code == 200
    assert (await http.get(f"/api/v1/sessions/{sid}")).status_code == 200
    retired = "/api/v1/clusters"
    for method, path in (
        ("GET", retired),
        ("POST", retired),
        ("GET", f"{retired}/{sid}"),
        ("GET", f"{retired}/{sid}/messages"),
        ("DELETE", f"{retired}/{sid}"),
    ):
        assert_error(await http.request(method, path, json={}), 404, "not_found")


_STEP = {
    "step": "install_plugins",
    "status": "failed",
    "message": "plugin x not found",
    "started_at": "2026-11-01T10:00:00+00:00",
    "finished_at": "2026-11-01T10:00:05+00:00",
}


async def test_agent_attention_and_bootstrap_fields(http, pool):
    c = await insert_session(pool, spec={"replicas": 2})
    a0 = await insert_agent(pool, c["id"], ready=True)
    a1 = await insert_agent(pool, c["id"], ready=False)
    await set_agent_status(pool, a0["id"], state="idle")
    await set_agent_status(pool, a1["id"], state="booting")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET waiting_reason='question',"
            " waiting_since='2026-11-01T10:01:00+00:00' WHERE agent_id=%s",
            (a0["id"],),
        )
        await conn.execute(
            "UPDATE agent_status SET bootstrap_step=%s, bootstrap_failure=%s WHERE agent_id=%s",
            (Jsonb(_STEP), Jsonb(_STEP), a1["id"]),
        )
    agents = (await http.get(f"/api/v1/sessions/{c['id']}/agents")).json()
    assert agents[0]["state"] == "idle"
    assert agents[0]["attention"]["reason"] == "question"
    assert datetime.fromisoformat(agents[0]["attention"]["since"]) == datetime.fromisoformat(
        "2026-11-01T10:01:00+00:00"
    )
    assert agents[0]["bootstrap"] is None and agents[0]["bootstrap_failure"] is None
    assert agents[1]["attention"] is None
    assert agents[1]["bootstrap"]["step"] == "install_plugins"
    assert agents[1]["bootstrap_failure"]["message"] == "plugin x not found"
    summary = (await http.get(f"/api/v1/sessions/{c['id']}")).json()["summary"]
    assert summary["state"] == "waiting"  # all idle: the name clash is left alone
    assert summary["attention_agents"] == 1
    assert summary["bootstrap_failed_agents"] == 1


async def test_agent_view_and_summary_surface_permission_mode_unavailable(pool, dsn):
    c = await insert_session(pool, slug="pmu")
    a0 = await insert_agent(pool, c["id"], ready=True)
    a1 = await insert_agent(pool, c["id"], ready=True)
    await set_agent_status(pool, a0["id"], state="idle")
    await set_agent_status(pool, a1["id"], state="idle")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agent_status SET permission_mode_state='unavailable',"
            " permission_mode_reason='unavailable for this model' WHERE agent_id=%s",
            (a0["id"],),
        )
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        agents = (await http.get(f"/api/v1/sessions/{c['id']}/agents")).json()
        assert agents[0]["permission_mode_state"] == "unavailable"
        assert agents[0]["permission_mode_reason"] == "unavailable for this model"
        assert agents[1]["permission_mode_state"] is None
        assert agents[1]["permission_mode_reason"] is None
        summary = (await http.get(f"/api/v1/sessions/{c['id']}")).json()["summary"]
        assert summary["permission_unavailable_agents"] == 1


# ---------------------------------------------------------------- session project (projects P4)

PROJECTS = "/api/v1/projects"
S = "/api/v1/sessions"


async def make_project(http, slug, archived=False):
    r = await http.post(PROJECTS, json={"slug": slug, "name": slug.title()})
    assert r.status_code == 201, r.text
    if archived:
        r = await http.patch(f"{PROJECTS}/{slug}", json={"archived": True})
        assert r.status_code == 200, r.text


async def test_create_takes_body_project(http):
    await make_project(http, "alpha")
    r = await http.post(S, json={"slug": "in-alpha", "spec": {}, "project": "alpha"})
    assert r.status_code == 201, r.text
    assert r.json()["project"] == "alpha"
    assert (await http.get(f"{S}/{r.json()['id']}")).json()["project"] == "alpha"
    assert (await http.get(f"{PROJECTS}/alpha")).json()["sessions"] == 1


async def test_spec_only_session_defaults_to_default(http):
    r = await http.post(S, json={"slug": "plain", "spec": {}})
    assert r.status_code == 201, r.text
    assert r.json()["project"] == "default"


async def test_create_archived_project_422(http):
    await make_project(http, "gone", archived=True)
    r = await http.post(S, json={"slug": "late", "spec": {}, "project": "gone"})
    assert_error(r, 422, "project_archived")
    r = await http.post(S, json={"slug": "late", "spec": {}, "project": "nope"})
    assert_error(r, 422, "unknown_project")
    assert (await http.get(S)).json() == []


async def test_create_waits_on_project_lock_and_sees_archive(http, pool):
    """The create holds the project row FOR SHARE: an archive committed while the create waits
    turns the create into 422 `project_archived`, never a session in an archived project."""
    await make_project(http, "alpha")
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("UPDATE projects SET archived_at = now() WHERE slug = 'alpha'")
        create = asyncio.create_task(
            http.post(S, json={"slug": "racer", "spec": {}, "project": "alpha"})
        )
        done, _ = await asyncio.wait({create}, timeout=0.5)
        assert not done
    assert_error(await create, 422, "project_archived")


async def _projects_of(http, **params) -> list[str]:
    r = await http.get(S, params=params)
    assert r.status_code == 200, r.text
    return sorted(s["slug"] for s in r.json())


async def test_list_filter_project(http):
    await make_project(http, "alpha")
    await make_project(http, "beta")
    for slug, project in (("a1", "alpha"), ("a2", "alpha"), ("b1", "beta")):
        r = await http.post(S, json={"slug": slug, "spec": {}, "project": project})
        assert r.status_code == 201, r.text
    await http.post(S, json={"slug": "d1", "spec": {}})
    assert await _projects_of(http, project="alpha") == ["a1", "a2"]
    assert await _projects_of(http, project="beta") == ["b1"]
    assert await _projects_of(http, project="default") == ["d1"]
    assert await _projects_of(http) == ["a1", "a2", "b1", "d1"]
    assert_error(await http.get(S, params={"project": "nope"}), 422, "unknown_project")
    # an archived project still lists its sessions
    await http.patch(f"{PROJECTS}/beta", json={"archived": True})
    assert await _projects_of(http, project="beta") == ["b1"]


async def test_list_superleaders_include(http):
    await make_project(http, "alpha")
    await http.post(S, json={"slug": "a1", "spec": {}, "project": "alpha"})
    await http.post(S, json={"slug": "boss", "spec": {"session_messaging": True}})
    await http.post(S, json={"slug": "other", "spec": {"session_messaging": False}})
    assert await _projects_of(http, project="alpha") == ["a1"]
    assert await _projects_of(http, project="alpha", superleaders="include") == ["a1", "boss"]
    assert await _projects_of(http, superleaders="include") == ["a1", "boss", "other"]
    assert_error(await http.get(S, params={"superleaders": "only"}), 422, "invalid_request")


async def test_list_slug_exact(http):
    await http.post(S, json={"slug": "exact", "spec": {}})
    await http.post(S, json={"slug": "exact-more", "spec": {}})
    assert await _projects_of(http, slug="exact") == ["exact"]
    assert await _projects_of(http, slug="exac") == []
    assert await _projects_of(http, slug="exact", project="default") == ["exact"]


async def test_patch_project_moves(http):
    await make_project(http, "alpha")
    sid = (await http.post(S, json={"slug": "mover", "spec": {}})).json()["id"]
    r = await http.patch(f"{S}/{sid}/project", json={"project": "alpha"})
    assert r.status_code == 200, r.text
    assert r.json()["project"] == "alpha"
    assert r.json()["generation"] == 1  # not a spec change
    assert (await http.get(f"{S}/{sid}")).json()["project"] == "alpha"
    assert await _projects_of(http, project="alpha") == ["mover"]
    assert_error(
        await http.patch(f"{S}/{uuid.uuid4()}/project", json={"project": "alpha"}),
        404,
        "not_found",
    )
    assert_error(
        await http.patch(f"{S}/{sid}/project", json={"project": "nope"}), 422, "unknown_project"
    )
    assert_error(
        await http.patch(f"{S}/{sid}/project", json={"project": "alpha", "x": 1}),
        422,
        "invalid_request",
    )


async def test_patch_project_deleting_409(http, pool):
    await make_project(http, "alpha")
    c = await insert_session(pool, slug="dying", desired="deleting")
    r = await http.patch(f"{S}/{c['id']}/project", json={"project": "alpha"})
    assert_error(r, 409, "session_deleting")


async def test_patch_project_archived_422(http):
    """Moving a session into an archived project is a new placement: 422 `project_archived`."""
    await make_project(http, "gone", archived=True)
    sid = (await http.post(S, json={"slug": "stay", "spec": {}})).json()["id"]
    r = await http.patch(f"{S}/{sid}/project", json={"project": "gone"})
    assert_error(r, 422, "project_archived")
    assert (await http.get(f"{S}/{sid}")).json()["project"] == "default"


async def test_patch_project_waits_on_project_lock_and_sees_archive(http, pool):
    await make_project(http, "alpha")
    sid = (await http.post(S, json={"slug": "racer", "spec": {}})).json()["id"]
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("UPDATE projects SET archived_at = now() WHERE slug = 'alpha'")
        move = asyncio.create_task(http.patch(f"{S}/{sid}/project", json={"project": "alpha"}))
        done, _ = await asyncio.wait({move}, timeout=0.5)
        assert not done
    assert_error(await move, 422, "project_archived")


async def test_existing_session_keeps_working_after_project_archived(http):
    await make_project(http, "alpha")
    sid = (await http.post(S, json={"slug": "old", "spec": {}, "project": "alpha"})).json()["id"]
    await http.patch(f"{PROJECTS}/alpha", json={"archived": True})
    r = await http.post(f"{S}/{sid}/messages", json={"text": "still here"})
    assert r.status_code == 202, r.text
    assert (await http.get(f"{S}/{sid}")).json()["project"] == "alpha"
    r = await http.patch(f"{S}/{sid}/labels", json={"labels": ["kept"]})
    assert r.status_code == 200, r.text
