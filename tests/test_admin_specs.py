"""DELETE /agent/v1/admin/specs/{key} and POST .../comments/{comment_id}/dismiss: the superleader
deletes a spec and dismisses a suggestion through the taskboard relay, as every project (`*`)."""

from __future__ import annotations

import httpx
import pytest
from fakes.fake_kube import FakeKube
from test_admin_audit_detail import admin_rows
from test_agent_session_messaging import alone
from test_api_agent import AGENT_PORT, KEY, client_for, headers
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings

TB_URL = "http://taskboard.test:9002"
TB_TOKEN = "tb-admin-specs-token"
S = f"{ADMIN_PREFIX}/specs"
DISMISSED = {"id": 7, "state": "dismissed"}


def make_app(dsn, pool, seen, *, configured=True, reply=None):
    kw = {"taskboard_url": TB_URL, "taskboard_token": TB_TOKEN} if configured else {}
    settings = Settings(
        database_url=dsn,
        allowed_cidrs="10.1.2.0/24",
        agent_image="img:1",
        master_key=KEY,
        agent_port=AGENT_PORT,
        **kw,
    )
    app = create_app(settings, pool, kube=FakeKube(namespace="agents"))

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return reply(request) if reply else httpx.Response(204)

    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return app


@pytest.fixture
def seen():
    return []


@pytest.fixture
async def boss(pool):
    return await alone(pool, "sp-boss", superleader=True)


async def call(app, who, method, path, **kw):
    async with client_for(app, who.leader_ip) as http:
        return await http.request(method, path, headers=headers(who.id), **kw)


async def test_admin_spec_delete_forwarded(pool, dsn, boss, seen):
    app = make_app(dsn, pool, seen)
    r = await call(app, boss, "DELETE", f"{S}/S-1")
    assert r.status_code == 204, r.text
    (up,) = seen
    assert up.method == "DELETE"
    assert up.url.raw_path == b"/taskboard/api/v1/specs/S-1"
    assert up.headers["x-orch-projects"] == "*"
    assert up.headers["authorization"] == f"Bearer {TB_TOKEN}"
    assert up.headers["x-orch-session"] == str(boss.id)


async def test_admin_suggestion_dismiss_forwarded(pool, dsn, boss, seen):
    app = make_app(dsn, pool, seen, reply=lambda r: httpx.Response(200, json=DISMISSED))
    r = await call(app, boss, "POST", f"{S}/S-1/comments/7/dismiss", json={})
    assert r.status_code == 200, r.text
    assert r.json() == DISMISSED
    (up,) = seen
    assert up.method == "POST"
    assert up.url.raw_path == b"/taskboard/api/v1/specs/S-1/comments/7/dismiss"
    assert up.headers["x-orch-projects"] == "*"


async def test_admin_spec_upstream_error_passes_through(pool, dsn, boss, seen):
    err = {"error": {"code": "not_found", "message": "no such spec"}}
    app = make_app(dsn, pool, seen, reply=lambda r: httpx.Response(404, json=err))
    r = await call(app, boss, "DELETE", f"{S}/S-9")
    assert r.status_code == 404 and r.json() == err


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("DELETE", f"{S}/S-x"),
        ("DELETE", f"{S}/S-1x"),
        ("POST", f"{S}/S-1/comments/x/dismiss"),
        ("POST", f"{S}/S-1/comments/7/accept"),
        ("POST", f"{S}/S-1/comments/7/dismiss/extra"),
    ],
)
async def test_admin_spec_paths_outside_allowlist_404(pool, dsn, boss, seen, method, path):
    app = make_app(dsn, pool, seen)
    r = await call(app, boss, method, path, json={})
    assert r.status_code in (404, 405), r.text
    assert seen == []


async def test_allowlist_positive_control(pool, dsn, boss, seen):
    """The refusals above are the allowlist's, not a missing route: a valid path is forwarded."""
    app = make_app(dsn, pool, seen)
    assert (await call(app, boss, "DELETE", f"{S}/S-12")).status_code == 204
    assert len(seen) == 1


@pytest.mark.parametrize(
    ("method", "path"), [("DELETE", f"{S}/S-1"), ("POST", f"{S}/S-1/comments/7/dismiss")]
)
async def test_admin_spec_routes_regular_leader_403(pool, dsn, seen, method, path):
    app = make_app(dsn, pool, seen)
    plain = await alone(pool, "sp-plain")
    r = await call(app, plain, method, path)
    assert_error(r, 403, "not_superleader")
    assert seen == []


async def test_admin_spec_routes_audited(pool, dsn, boss, seen):
    app = make_app(dsn, pool, seen, reply=lambda r: httpx.Response(200, json=DISMISSED))
    await call(app, boss, "DELETE", f"{S}/S-1")
    await call(app, boss, "POST", f"{S}/S-1/comments/7/dismiss")
    rows = await admin_rows(pool)
    assert [(r["method"], r["status"]) for r in rows] == [("DELETE", 200), ("POST", 200)]
    assert [r["path"] for r in rows] == [f"{S}/S-1", f"{S}/S-1/comments/7/dismiss"]


@pytest.mark.parametrize(
    ("method", "path"), [("DELETE", f"{S}/S-1"), ("POST", f"{S}/S-1/comments/7/dismiss")]
)
async def test_admin_spec_routes_taskboard_unconfigured_503(pool, dsn, boss, seen, method, path):
    app = make_app(dsn, pool, seen, configured=False)
    assert_error(await call(app, boss, method, path), 503, "taskboard_unconfigured")
    assert seen == []


def test_spec_delete_declares_204_only():
    import json
    import pathlib

    doc = json.loads((pathlib.Path(__file__).parent.parent / "openapi.json").read_text())
    codes = doc["paths"][f"{S}/{{key}}"]["delete"]["responses"]
    assert "204" in codes and "200" not in codes
