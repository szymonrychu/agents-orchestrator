"""Spec relay agent routes: `/agent/v1/specs/*` forwarded to the taskboard upstream.

Reuses the task relay rig (in-memory identity stand-in and an `httpx.MockTransport` upstream), so
it needs no Docker.
"""

from __future__ import annotations

import json

import httpx
import pytest
from test_agent_tasks import TB_TOKEN, kwargs, make_rig
from test_api_sessions import assert_error

from agents_orchestrator.api import specs, tasks

P = "research/a.md"
_CASES = (
    ("GET", ""), ("POST", ""), ("GET", "/S-1"), ("PATCH", "/S-1"), ("GET", "/S-1/docs"),
    ("GET", f"/S-1/docs/{P}"), ("PUT", f"/S-1/docs/{P}"), ("GET", f"/S-1/docs/{P}/versions"),
    ("GET", "/S-1/docs/plan.md"), ("GET", "/S-1/comments"), ("POST", "/S-1/comments"),
    ("PATCH", "/S-1/comments/7"), ("POST", "/S-1/comments/7/resolve"),
)  # fmt: skip
ROUTES = tuple(
    (method, "/agent/v1/specs" + tail, "/taskboard/api/v1/specs" + tail) for method, tail in _CASES
)


async def test_forwards_verified_session_slug_and_projects(monkeypatch):
    rig = make_rig(monkeypatch)
    for method, path, upstream_path in ROUTES:
        rig.upstream.seen.clear()
        r = await rig.call(method, path + "?cursor=c1", **kwargs(method))
        assert r.status_code == 200, (path, r.text)
        (sent,) = rig.upstream.seen
        assert sent.method == method
        assert sent.url.raw_path == (upstream_path + "?cursor=c1").encode()
        assert sent.headers["authorization"] == f"Bearer {TB_TOKEN}"
        assert sent.headers["x-orch-session"] == str(rig.session.id)
        assert sent.headers["x-orch-slug"] == "tb-caller"
        assert sent.headers["x-orch-projects"] == "default"


async def test_labels_mine_is_not_expanded(monkeypatch):
    rig = make_rig(monkeypatch, labels=["a"])
    await rig.call("GET", "/agent/v1/specs?labels=mine")
    (sent,) = rig.upstream.seen
    assert sent.url.query == b"labels=mine"


@pytest.mark.parametrize(
    "path",
    [
        "/agent/v1/specs/S-1/docs/%2e%2e/x.md",
        "/agent/v1/specs/S-1/docs/a/%2e%2e/x.md",
        "/agent/v1/specs/S-1/docs/a%2f..%2fx.md",
        "/agent/v1/specs/S-1/comments/1/accept",
        "/agent/v1/specs/S-1/comments/1/dismiss",
        "/agent/v1/specs/S-1/plan",
        "/agent/v1/specs/S-1/",
        "/agent/v1/specs/S-1/docs/x.txt",
        "/agent/v1/specs/S-1/docs/Upper.md",
        "/agent/v1/specs/S-x",
        "/agent/v1/specs/stream",
        "/agent/v1/specs/S-1/approve",
    ],
)
async def test_unlisted_paths_are_404(monkeypatch, path):
    rig = make_rig(monkeypatch)
    for method in ("GET", "POST", "PUT", "PATCH"):
        assert_error(await rig.call(method, path, **kwargs(method)), 404, "not_found")
    assert rig.upstream.seen == []


async def test_dotdot_inside_the_path_is_refused(monkeypatch):
    rig = make_rig(monkeypatch)
    r = await rig.call("GET", "/agent/v1/specs/S-1/docs/a..b.md")
    assert_error(r, 404, "not_found")
    assert rig.upstream.seen == []


async def test_delete_is_404_without_a_route(monkeypatch):
    rig = make_rig(monkeypatch)
    for path in ("/agent/v1/specs/S-1", "/agent/v1/specs"):
        r = await rig.call("DELETE", path)
        assert r.status_code in (404, 405), path
    assert rig.upstream.seen == []
    assert "DELETE" not in {m for route in specs.router.routes for m in route.methods}


async def test_body_cap_413(monkeypatch):
    rig = make_rig(monkeypatch)
    cap = tasks.MAX_BODY_BYTES
    path = f"/agent/v1/specs/S-1/docs/{P}"
    assert_error(await rig.call("PUT", path, content=b"a" * (cap + 1)), 413, "payload_too_large")
    assert rig.upstream.seen == []
    assert (await rig.call("PUT", path, content=b"a" * cap)).status_code == 200


def _typical_markdown(size: int) -> str:
    block = (
        '## Design\n\n- item one, with `code` and a "quoted" word\n- item two\n\n'
        '```python\ndef f(x):\n    return {"k": x}\n```\n\n'
        "Plain paragraph text that explains the approach in some detail.\n\n"
    )
    return (block * (size // len(block) + 1))[:size]


async def test_typical_192k_document_fits(monkeypatch):
    rig = make_rig(monkeypatch)
    text = _typical_markdown(192 * 1024)
    assert len(text.encode()) == 192 * 1024
    body = json.dumps({"body": text, "base_version": 1}, ensure_ascii=False).encode()
    assert len(body) < tasks.MAX_BODY_BYTES
    r = await rig.call(
        "PUT",
        f"/agent/v1/specs/S-1/docs/{P}",
        content=body,
        headers={"content-type": "application/json"},
    )
    assert r.status_code == 200, r.text
    (sent,) = rig.upstream.seen
    assert json.loads(sent.content)["body"] == text


async def test_unconfigured_503(monkeypatch):
    rig = make_rig(monkeypatch, taskboard_url="", taskboard_token="")
    assert_error(await rig.call("GET", "/agent/v1/specs"), 503, "taskboard_unconfigured")


async def test_upstream_down_503(monkeypatch):
    rig = make_rig(monkeypatch)

    async def refused(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    rig.upstream.reply = refused
    assert_error(await rig.call("GET", "/agent/v1/specs"), 503, "taskboard_unavailable")


async def test_upstream_status_and_body_pass_through(monkeypatch):
    rig = make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(412, json={"error": {"code": "stale_version"}})
    r = await rig.call("PUT", f"/agent/v1/specs/S-1/docs/{P}", json={"body": "x"})
    assert r.status_code == 412 and r.json() == {"error": {"code": "stale_version"}}


async def test_spec_write_touches_session(monkeypatch):
    rig = make_rig(monkeypatch)
    for method, path, _ in ROUTES:
        for status in (200, 201, 400, 404, 412, 503):
            rig.db_touched().clear()
            rig.upstream.reply = httpx.Response(status, json={})
            await rig.call(method, path, **kwargs(method))
            write = method != "GET" and status < 300
            assert rig.db_touched() == ([rig.session.id] if write else []), (method, path, status)


def test_every_relayed_write_method_is_a_stamped_one():
    methods = {m for route in specs.router.routes for m in route.methods}
    assert methods - {"GET"} == tasks._WRITES
