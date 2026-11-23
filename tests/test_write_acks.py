"""S-83 step 10 (decision 926): agent-port writes answer a short `WriteAck`, the web routes keep
the full views. Relays are checked on the taskboard/mindmap rig (no Docker), the rest over HTTP."""

from __future__ import annotations

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

A = ADMIN_PREFIX
RELAY_CASES = [
    ("tasks", "POST", "/agent/v1/tasks", 201, {"key": "T-1", "title": "x", "status": "open"}),
    ("tasks", "PATCH", "/agent/v1/tasks/T-1", 200, {"key": "T-1", "status": "done", "x": 1}),
    ("tasks", "POST", "/agent/v1/tasks/T-1/comments", 201, {"id": 9, "body": "hi"}),
    ("specs", "PATCH", "/agent/v1/specs/S-2", 200, {"key": "S-2", "version": 4, "status": "draft"}),
    ("specs", "PUT", "/agent/v1/specs/S-2/docs/plan.md", 201, {"path": "plan.md", "version": 3}),
    ("specs", "PUT", "/agent/v1/specs/S-2/plan", 200, {"goal": "g", "version": 5}),
    ("mindmap", "POST", "/agent/v1/mindmap/maps", 201, {"key": "M-1", "title": "t", "version": 1}),
    ("mindmap", "PATCH", "/agent/v1/mindmap/nodes/N-2", 200, {"key": "N-2", "status": "open"}),
]  # fmt: skip
RELAY_EXPECT = {
    "POST /agent/v1/tasks": {"id": "T-1", "version": None, "status": "open"},
    "PATCH /agent/v1/tasks/T-1": {"id": "T-1", "version": None, "status": "done"},
    "POST /agent/v1/tasks/T-1/comments": {"id": "9", "version": None, "status": None},
    "PATCH /agent/v1/specs/S-2": {"id": "S-2", "version": 4, "status": "draft"},
    "PUT /agent/v1/specs/S-2/docs/plan.md": {"id": "plan.md", "version": 3, "status": None},
    "PUT /agent/v1/specs/S-2/plan": {
        "id": "/specs/S-2/plan",
        "version": 5,
        "status": None,
    },
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


@pytest.mark.parametrize(
    ("method", "path", "upstream"),
    [
        ("POST", "/agent/v1/specs/S-2/steps/next", {"key": "S-2.1", "title": "do", "status": "a"}),
        ("PUT", "/agent/v1/specs/S-2/steps", {"keys": ["S-2.1", "S-2.2"]}),
        ("GET", "/agent/v1/tasks/T-1", {"key": "T-1", "title": "x", "status": "open"}),
    ],
)  # fmt: skip
async def test_relay_answers_that_carry_the_work_stay_whole(monkeypatch, method, path, upstream):
    rig = make_rig(monkeypatch)
    rig.upstream.reply = httpx.Response(200, json=upstream)
    r = await rig.call(method, path, json={})
    assert r.json() == upstream


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
