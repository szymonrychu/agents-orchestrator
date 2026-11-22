"""Spec relay agent routes: `/agent/v1/specs/*` forwarded to the taskboard upstream.

Reuses the task relay rig (in-memory identity stand-in and an `httpx.MockTransport` upstream), so
it needs no Docker.
"""

from __future__ import annotations

import dataclasses
import json

import httpx
import pytest
from test_agent_session_messaging import hive
from test_agent_tasks import TB_TOKEN, kwargs, make_rig
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api import specs, tasks
from agents_orchestrator.api.app import create_app

P = "research/a.md"
_CASES = (
    ("GET", ""), ("POST", ""), ("GET", "/S-1"), ("PATCH", "/S-1"), ("GET", "/S-1/docs"),
    ("GET", f"/S-1/docs/{P}"), ("PUT", f"/S-1/docs/{P}"), ("GET", f"/S-1/docs/{P}/versions"),
    ("GET", "/S-1/docs/plan.md"), ("GET", "/S-1/comments"), ("POST", "/S-1/comments"),
    ("PATCH", "/S-1/comments/7"), ("POST", "/S-1/comments/7/resolve"),
    ("GET", "/S-1/plan"), ("PUT", "/S-1/plan"), ("GET", "/S-1/steps"), ("PUT", "/S-1/steps"),
    ("POST", "/S-1/steps"), ("PUT", "/S-1/steps/order"), ("POST", "/S-1/steps/next"),
    ("GET", "/S-1/gate"), ("GET", f"/S-1/docs/{P}/section"),
    ("GET", "/queue"),
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
        "/agent/v1/specs/S-1/docs/a/%2e/b.md",
        "/agent/v1/specs/S-1/docs/%2e/b.md",
        "/agent/v1/specs/S-1/docs/a%2f%2fb.md",
        "/agent/v1/specs/S-1/comments/1/accept",
        "/agent/v1/specs/S-1/comments/1/dismiss",
        "/agent/v1/specs/S-1/steps/x",
        "/agent/v1/specs/S-1/steps/order/x",
        "/agent/v1/specs/S-1/gate/x",
        "/agent/v1/specs/S-1/plan/x",
        "/agent/v1/specs/S-1/docs/a.md/section/x",
        "/agent/v1/specs/S-1/",
        "/agent/v1/specs/S-1/docs/x.txt",
        "/agent/v1/specs/S-1/docs/Upper.md",
        "/agent/v1/specs/S-x",
        "/agent/v1/specs/stream",
        "/agent/v1/specs/S-1/approve",
        "/agent/v1/specs/queue/x/move",
        "/agent/v1/specs/queue/12",
        "/agent/v1/specs/queue/12/remove",
        "/agent/v1/specs/queue/12/move/x",
        "/agent/v1/specs/queue/",
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


async def test_steps_next_204_passes_through_without_body(monkeypatch):
    rig = make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(204)
    r = await rig.call("POST", "/agent/v1/specs/S-1/steps/next", json={})
    assert r.status_code == 204
    assert r.content == b""


async def test_empty_non_204_answer_is_still_503(monkeypatch):
    rig = make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(200, content=b"")
    r = await rig.call("POST", "/agent/v1/specs/S-1/steps/next", json={})
    assert_error(r, 503, "taskboard_unavailable")


async def test_queue_list_is_relayed(monkeypatch):
    rig = make_rig(monkeypatch)
    r = await rig.call("GET", "/agent/v1/specs/queue?project=a")
    assert r.status_code == 200
    (sent,) = rig.upstream.seen
    assert sent.method == "GET"
    assert sent.url.raw_path == b"/taskboard/api/v1/specs/queue?project=a"
    assert sent.headers["x-orch-projects"] == "default"


async def test_queue_move_is_relayed(monkeypatch):
    rig = make_rig(monkeypatch)
    rig.db.sessions[rig.session.id] = dataclasses.replace(
        rig.session, spec=rig.session.spec.model_copy(update={"session_messaging": True})
    )
    body = {"position": 1}
    r = await rig.call("POST", "/agent/v1/specs/queue/12/move", json=body)
    assert r.status_code == 200
    (sent,) = rig.upstream.seen
    assert sent.method == "POST"
    assert sent.url.raw_path == b"/taskboard/api/v1/specs/queue/12/move"
    assert json.loads(sent.content) == body


async def test_queue_delete_stays_unrelayed(monkeypatch):
    rig = make_rig(monkeypatch)
    r = await rig.call("DELETE", "/agent/v1/specs/queue/12")
    assert r.status_code in (404, 405)
    assert rig.upstream.seen == []


@pytest.mark.parametrize(
    ("caller", "status"),
    [("member", 403), ("leader", 403), ("superleader_member", 403), ("superleader", 200)],
)
async def test_spec_queue_move_relay_superleader_only(pool, dsn, caller, status):
    """Decision 922: the queue move reaches the taskboard only for a superleader (the leader of
    a superleader session); anyone else is 403 superleader_required and nothing is forwarded.
    The queue read stays open to every role."""
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={"items": [], "next_cursor": None})

    settings = make_settings(dsn).model_copy(
        update={"taskboard_url": "http://taskboard.test:9002", "taskboard_token": TB_TOKEN}
    )
    app = create_app(settings, pool)
    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    if caller.startswith("superleader"):
        s = await hive(pool, "q-boss", superleader=True)
    else:
        s = await hive(pool, "q-hive")
    ip = s.member_ip if caller.endswith("member") else s.leader_ip
    async with client_for(app, ip) as http:
        move = await http.post(
            "/agent/v1/specs/queue/12/move", json={"position": 1}, headers=headers(s.id)
        )
        listed = await http.get("/agent/v1/specs/queue", headers=headers(s.id))
    assert listed.status_code == 200, listed.text
    if status == 403:
        assert_error(move, 403, "superleader_required")
        assert [r.method for r in seen] == ["GET"]
    else:
        assert move.status_code == 200, move.text
        assert [r.method for r in seen] == ["POST", "GET"]


@pytest.mark.parametrize(
    ("path", "upstream", "patch"),
    [
        ("/agent/v1/specs/S-1", "/taskboard/api/v1/specs/S-1", {"status": "archived"}),
        ("/agent/v1/tasks/T-1", "/taskboard/api/v1/tasks/T-1", {"status": "dropped"}),
    ],
)
async def test_leader_archives_spec_of_p(pool, dsn, path, upstream, patch):
    """Decision 942: a leader archives specs and closes tasks of its projects through the plain
    relays (no gate change): the write is forwarded with its home and granted projects, and the
    taskboard filters on them."""
    seen: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        return httpx.Response(200, json={})

    settings = make_settings(dsn).model_copy(
        update={"taskboard_url": "http://taskboard.test:9002", "taskboard_token": TB_TOKEN}
    )
    app = create_app(settings, pool)
    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    s = await hive(pool, "arch-hive")
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET extra_projects = '{b}' WHERE id = %s", (s.id,))
    async with client_for(app, s.leader_ip) as http:
        r = await http.patch(path, json=patch, headers=headers(s.id))
    assert r.status_code == 200, r.text
    (sent,) = seen
    assert (sent.method, sent.url.path) == ("PATCH", upstream)
    assert json.loads(sent.content) == patch
    assert sent.headers["x-orch-projects"] == "default,b"
