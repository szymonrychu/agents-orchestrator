"""Mindmap relay agent routes: `/agent/v1/mindmap/*` forwarded to the agents-mindmap service.

Same rig as the taskboard relay tests (in-memory identity stand-in, `httpx.MockTransport`
upstream), so it needs no Docker.
"""

from __future__ import annotations

import httpx
import pytest
from test_agent_tasks import TB_TOKEN, TB_URL, kwargs, make_rig, secrets
from test_api_sessions import assert_error

from agents_orchestrator.api import mindmap
from agents_orchestrator.settings import Settings

MM_URL = "http://mindmap.test:8082"
MM_TOKEN = "mm-" + secrets.token_hex(16)
PREFIX = "/mindmap/api/v1"

# every service-listener route of agents-mindmap openapi-service.json
_CASES = (
    ("GET", "/maps"), ("POST", "/maps"), ("GET", "/maps/M-1"), ("PATCH", "/maps/M-1"),
    ("GET", "/maps/M-1/graph"), ("POST", "/maps/M-1/nodes"), ("POST", "/maps/M-1/directions"),
    ("POST", "/maps/M-1/links"), ("GET", "/nodes/N-2"), ("PATCH", "/nodes/N-2"),
    ("GET", "/nodes/N-2/traverse"), ("GET", "/nodes/N-2/versions"),
    ("GET", "/nodes/N-2/versions/3"), ("POST", "/nodes/N-2/growth-areas"),
    ("POST", "/nodes/N-2/links"), ("PATCH", "/growth-areas/G-4"), ("POST", "/edges"),
    ("DELETE", "/edges/E-5"), ("POST", "/knowledge/notes"), ("GET", "/links"),
    ("DELETE", "/links/7"), ("GET", "/directions"), ("PATCH", "/directions/D-6"),
    ("GET", "/projects"), ("GET", "/search"),
)  # fmt: skip
ROUTES = tuple((method, "/agent/v1/mindmap" + tail, PREFIX + tail) for method, tail in _CASES)


def rig_for(monkeypatch, **kw):
    kw.setdefault("mindmap_url", MM_URL)
    kw.setdefault("mindmap_token", MM_TOKEN)
    rig = make_rig(monkeypatch, **kw)
    rig.app.state.mindmap_http = httpx.AsyncClient(
        transport=httpx.MockTransport(rig.upstream.handler)
    )
    return rig


async def test_relay_each_allowlisted_route(monkeypatch):
    rig = rig_for(monkeypatch)
    for method, path, upstream_path in ROUTES:
        rig.upstream.seen.clear()
        r = await rig.call(method, path + "?cursor=c1", **kwargs(method))
        assert r.status_code == 200, (method, path, r.text)
        (sent,) = rig.upstream.seen
        assert sent.method == method
        assert sent.url.raw_path == (upstream_path + "?cursor=c1").encode()
        assert sent.url.host == "mindmap.test"
        assert sent.headers["authorization"] == f"Bearer {MM_TOKEN}"
        assert sent.headers["x-orch-session"] == str(rig.session.id)
        assert sent.headers["x-orch-slug"] == "tb-caller"
        assert sent.headers["x-orch-projects"] == "default"
        assert set(sent.headers) <= {
            "host", "authorization", "x-orch-session", "x-orch-slug", "x-orch-projects",
            "accept", "accept-encoding", "connection", "user-agent", "content-type",
            "content-length",
        }, (method, path, sorted(sent.headers))  # fmt: skip


@pytest.mark.parametrize(
    "tail",
    [
        "/maps/M-1/../x",
        "/maps/%2e%2e/graph",
        "/maps/M-1%2f..%2fnodes",
        "/maps/M-1/%2e/graph",
        "/maps//graph",
        "/nodes/N-1/versions/%2e%2e",
        "/maps/M-1/",
    ],
)
async def test_relay_refuses_segments(monkeypatch, tail):
    rig = rig_for(monkeypatch)
    for method in ("GET", "POST", "PATCH", "DELETE"):
        assert_error(
            await rig.call(method, "/agent/v1/mindmap" + tail, **kwargs(method)), 404, "not_found"
        )
    assert rig.upstream.seen == []


@pytest.mark.parametrize(
    "tail",
    [
        "/maps/M-x", "/maps/N-1", "/nodes/M-1", "/nodes/N-1/history", "/edges/E-1/x",
        "/growth-areas/G-1/x", "/links/x", "/stream", "/maps/M-1/graph/x", "/admin",
        "/knowledge/notes/1", "/nodes/N-1/versions/x",
    ],
)  # fmt: skip
async def test_unlisted_paths_are_404(monkeypatch, tail):
    rig = rig_for(monkeypatch)
    assert_error(await rig.call("GET", "/agent/v1/mindmap" + tail), 404, "not_found")
    assert rig.upstream.seen == []


@pytest.mark.parametrize(
    "method,tail",
    [
        ("DELETE", "/maps/M-1"), ("DELETE", "/nodes/N-1"), ("DELETE", "/growth-areas/G-1"),
        ("PUT", "/maps/M-1"), ("POST", "/maps/M-1/graph"), ("GET", "/edges"),
        ("PATCH", "/links/7"),
    ],
)  # fmt: skip
async def test_method_not_in_the_service_surface_is_404(monkeypatch, method, tail):
    rig = rig_for(monkeypatch)
    r = await rig.call(method, "/agent/v1/mindmap" + tail, **kwargs(method))
    assert r.status_code in (404, 405)
    assert rig.upstream.seen == []


async def test_unconfigured_503_from_mindmap_settings(monkeypatch):
    rig = make_rig(monkeypatch)  # taskboard configured, mindmap not
    assert_error(await rig.call("GET", "/agent/v1/mindmap/maps"), 503, "mindmap_unconfigured")
    assert (await rig.call("GET", "/agent/v1/tasks")).status_code == 200
    assert len(rig.upstream.seen) == 1  # the taskboard call only


async def test_taskboard_unconfigured_does_not_block_mindmap(monkeypatch):
    rig = rig_for(monkeypatch, taskboard_url="", taskboard_token="")
    assert (await rig.call("GET", "/agent/v1/mindmap/maps")).status_code == 200
    assert rig.upstream.seen[0].headers["authorization"] == f"Bearer {MM_TOKEN}"


def test_target_from_mindmap_settings():
    s = Settings(
        database_url="postgresql://unused/db", mindmap_url=MM_URL + "/", mindmap_token=MM_TOKEN
    )
    t = mindmap.mindmap_target(s)
    assert t.base == MM_URL + PREFIX
    assert t.token == MM_TOKEN
    assert t.unavailable == "mindmap_unavailable"
    assert t.state_attr == "mindmap_http"
    assert TB_URL not in t.base and t.token != TB_TOKEN


async def test_mindmap_relay_mine_false(monkeypatch):
    rig = rig_for(monkeypatch, labels=["a", "b"])
    await rig.call("GET", "/agent/v1/mindmap/maps?labels=mine")
    (sent,) = rig.upstream.seen
    assert sent.url.query == b"labels=mine"


async def test_upstream_status_and_error_body_pass_through(monkeypatch):
    rig = rig_for(monkeypatch)
    rig.upstream.reply = httpx.Response(
        409, json={"error": {"code": "version_conflict", "message": "m", "version": 4}}
    )
    r = await rig.call("PATCH", "/agent/v1/mindmap/nodes/N-2", json={"title": "x"})
    assert r.status_code == 409
    assert r.json() == {"error": {"code": "version_conflict", "message": "m", "version": 4}}


async def test_upstream_204_passes_through_as_204(monkeypatch):
    rig = rig_for(monkeypatch)
    rig.upstream.reply = httpx.Response(204)
    r = await rig.call("DELETE", "/agent/v1/mindmap/edges/E-5")
    assert r.status_code == 204 and r.content == b""


async def test_unreachable_upstream_is_503_mindmap_unavailable(monkeypatch):
    rig = rig_for(monkeypatch)

    async def boom(request):
        raise httpx.ConnectError("down")

    rig.upstream.reply = boom
    assert_error(await rig.call("GET", "/agent/v1/mindmap/maps"), 503, "mindmap_unavailable")


async def test_writes_touch_the_session_reads_do_not(monkeypatch):
    rig = rig_for(monkeypatch)
    await rig.call("GET", "/agent/v1/mindmap/maps")
    assert rig.db_touched() == []
    await rig.call("POST", "/agent/v1/mindmap/maps", json={"title": "x"})
    assert len(rig.db_touched()) == 1


async def test_body_cap_413(monkeypatch):
    rig = rig_for(monkeypatch)
    cap = mindmap.MAX_BODY_BYTES
    path = "/agent/v1/mindmap/knowledge/notes"
    assert_error(await rig.call("POST", path, content=b"a" * (cap + 1)), 413, "payload_too_large")
    assert rig.upstream.seen == []


async def test_token_never_logged(monkeypatch, caplog):
    rig = rig_for(monkeypatch)
    caplog.set_level("DEBUG")
    await rig.call("GET", "/agent/v1/mindmap/maps")
    assert MM_TOKEN not in caplog.text
