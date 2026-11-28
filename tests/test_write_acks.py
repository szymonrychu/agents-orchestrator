"""S-83 step 10 (decision 926): agent-port writes answer a short `WriteAck`, the web routes keep
the full views. Relays are checked on the taskboard/mindmap rig (no Docker), the rest over HTTP."""

from __future__ import annotations

import dataclasses

import httpx
import pytest
from fakes.fake_kube import FakeKube
from test_agent_session_messaging import alone
from test_agent_tasks import make_rig
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import client_for as main_client
from test_mindmap_relay import rig_for

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

pytestmark = pytest.mark.critical

A = ADMIN_PREFIX
RELAY_CASES = [
    ("specs", "PATCH", "/agent/v1/specs/S-2", 200, {"key": "S-2", "version": 4, "status": "draft"}),
    ("specs", "POST", "/agent/v1/specs/S-2/comments", 201, {"id": 7, "status": "open"}),
    ("specs", "PUT", "/agent/v1/specs/S-2/plan", 200, {"goal": "g", "version": 5}),
    ("mindmap", "POST", "/agent/v1/mindmap/maps", 201, {"key": "M-1", "title": "t", "version": 1}),
    ("mindmap", "PATCH", "/agent/v1/mindmap/nodes/N-2", 200, {"key": "N-2", "status": "open"}),
]  # fmt: skip
RELAY_EXPECT = {
    "PATCH /agent/v1/specs/S-2": {"id": "S-2", "version": 4, "status": "draft"},
    "POST /agent/v1/specs/S-2/comments": {"id": 7, "version": None, "status": "open"},
    "PUT /agent/v1/specs/S-2/plan": {"id": "/specs/S-2/plan", "version": 5, "status": None},
    "POST /agent/v1/mindmap/maps": {"id": "M-1", "version": 1, "status": None},
    "PATCH /agent/v1/mindmap/nodes/N-2": {"id": "N-2", "version": None, "status": "open"},
}


@pytest.mark.parametrize(("relay", "method", "path", "status", "upstream"), RELAY_CASES)
async def test_agent_write_routes_return_ack(monkeypatch, relay, method, path, status, upstream):
    rig = rig_for(monkeypatch) if relay == "mindmap" else make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(status, json=upstream)
    r = await rig.call(method, path, json={"title": "x"})
    assert r.status_code == status, r.text
    assert r.json() == RELAY_EXPECT[f"{method} {path}"]


TASK = {"key": "T-1", "version": 3, "url": "u", "status": "open", "comments": [{"id": 9}]}
DECISION = {"id": 12, "task_key": "T-1", "status": "open", "url": "u"}


@pytest.mark.parametrize(
    ("method", "path", "upstream"),
    [
        ("POST", "/agent/v1/tasks", TASK),
        ("PATCH", "/agent/v1/tasks/T-1", TASK),
        ("POST", "/agent/v1/tasks/T-1/comments", TASK),
        ("PUT", "/agent/v1/tasks/T-1/todo", {**TASK, "todo": [{"id": "a1", "text": "t"}]}),
        ("PATCH", "/agent/v1/tasks/T-1/todo/a1", TASK),
        ("PUT", "/agent/v1/tasks/T-1/deps", TASK),
        ("POST", "/agent/v1/tasks/T-1/decisions", DECISION),
        ("POST", "/agent/v1/tasks/T-1/decisions/12/withdraw", {**DECISION, "status": "withdrawn"}),
        ("POST", "/agent/v1/tasks/T-1/approvals", {**DECISION, "expires_at": "t"}),
        ("POST", "/agent/v1/specs", {"key": "S-3", "name": "n", "version": 1, "status": "draft"}),
        ("PUT", "/agent/v1/specs/S-2/docs/plan.md", {"path": "plan.md", "version": 3, "body": "b"}),
        ("PUT", "/agent/v1/specs/S-2/steps", {"keys": ["S-2.1", "S-2.2"], "steps_version": 2}),
        ("POST", "/agent/v1/specs/S-2/steps/next", {"key": "S-2.1", "title": "do", "status": "a"}),
    ],
)  # fmt: skip
async def test_plugin_shaped_relay_writes_return_full_body(monkeypatch, method, path, upstream):
    """The plugin shapes these answers itself (views.py ACKS, specs_local, plan_local), so the
    relay keeps the upstream body whole, numeric ids included."""
    rig = make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(200, json=upstream)
    r = await rig.call(method, path, json={})
    assert r.json() == upstream


async def test_relay_writes_off_the_ack_list_keep_full_body(monkeypatch):
    """Only the listed single-object writes are acked: the queue move answers the whole queue
    (a `project` field, a list of items), which reaches the caller unchanged."""
    rig = make_rig(monkeypatch)
    rig.db.sessions[rig.session.id] = dataclasses.replace(
        rig.session, spec=rig.session.spec.model_copy(update={"session_messaging": True})
    )
    queue = {"project": "a", "state": {"status": "on"}, "items": [{"id": 12, "position": 1}]}
    rig.upstream.reply = httpx.Response(200, json=queue)
    r = await rig.call("POST", "/agent/v1/specs/queue/12/move", json={"position": 1})
    assert r.json() == queue


async def _admin(pool, dsn, who, method, path, **kw):
    app = create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))
    async with client_for(app, who.leader_ip) as http:
        return await http.request(method, path, headers=headers(who.id), **kw)


PRESET = {"name": "ackp", "spec": {"env": {"PLAIN": {"value": "v"}}}}
ADMIN_CASES = [
    ("POST", f"{A}/projects", {"slug": "ackproj", "name": "Ack"}, 201, {"id": "ackproj"}),
    ("POST", f"{A}/presets", PRESET, 201, {}),
    ("PUT", f"{A}/sessions/ack-boss/labels", {"labels": ["a"]}, 200, {"id": "ack-boss"}),
]  # fmt: skip


@pytest.mark.parametrize(("method", "path", "body", "status", "want"), ADMIN_CASES)
async def test_admin_write_routes_return_ack(pool, dsn, method, path, body, status, want):
    boss = await alone(pool, "ack-boss", superleader=True)
    r = await _admin(pool, dsn, boss, method, path, json=body)
    assert r.status_code == status, r.text
    got = r.json()
    assert set(got) == {"id", "version", "status"}
    assert {k: got[k] for k in want} == want
    assert got["id"]


async def test_admin_preset_and_session_writes_return_ack(pool, dsn):
    boss = await alone(pool, "ack-boss2", superleader=True)
    made = await _admin(pool, dsn, boss, "POST", f"{A}/presets", json=PRESET)
    pid = made.json()["id"]
    patched = await _admin(
        pool,
        dsn,
        boss,
        "PATCH",
        f"{A}/presets/{pid}",
        json={"spec": {"env": {"PLAIN": {"value": "w"}}}},
    )
    assert patched.json() == {"id": pid, "version": None, "status": None}
    session = await _admin(
        pool,
        dsn,
        boss,
        "PATCH",
        f"{A}/sessions/{boss.slug}",
        json={"spec": {"env": {"X": {"value": "1"}}}},
    )
    assert session.status_code == 200, session.text
    assert set(session.json()) == {"id", "version", "status"}
    assert session.json()["id"] == boss.slug and session.json()["status"] == "active"
    proposal = await _admin(
        pool,
        dsn,
        boss,
        "POST",
        f"{A}/proposals",
        json={
            "kind": "preset",
            "target": "ackp",
            "patch": {"env": {"PLAIN": {"value": "z"}}},
            "reason": "r",
        },
    )
    assert proposal.status_code == 201, proposal.text
    assert set(proposal.json()) == {"id", "version", "status"}
    assert proposal.json()["status"] == "open" and proposal.json()["version"] == 1


@pytest.mark.parametrize(
    ("method", "path", "body", "keys"),
    [
        ("POST", "/api/v1/projects", {"slug": "webproj", "name": "W"}, {"slug", "archived"}),
        ("POST", "/api/v1/presets", PRESET, {"spec", "superleader_manageable"}),
    ],
)
async def test_user_write_routes_keep_full_view(pool, dsn, method, path, body, keys):
    app = create_app(make_settings(dsn), pool)
    async with main_client(app) as http:
        r = await http.request(method, path, json=body)
    assert r.status_code == 201, r.text
    assert keys <= set(r.json())
