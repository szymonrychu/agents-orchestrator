"""`PUT /agent/v1/session/labels`: an agent replaces the labels of its own session only.

Runs the real `require_agent` against the in-memory identity stand-in of the chat relay tests;
the labels write is stood in by an in-memory `sessions_repo.set_labels`, so it needs no Docker.
"""

from __future__ import annotations

import dataclasses
import uuid
from typing import Any

import httpx
import pytest
from test_agent_chat import AGENT_PORT, KEY, LEADER_IP, FakeDb, agent_token
from test_api_sessions import assert_error

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.settings import Settings

URL = "/agent/v1/session/labels"


class Rig:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, **session_kw: Any) -> None:
        self.db = FakeDb()
        self.db.install(monkeypatch)
        self.session = self.db.add_session("lab-caller", hive=False, **session_kw)
        self.other = self.db.add_session("lab-other", hive=False, leader_ip="10.1.2.99")
        self.writes: list[uuid.UUID] = []

        async def set_labels(
            _conn: Any, session_id: uuid.UUID, labels: list[str]
        ) -> sessions_repo.Session:
            self.writes.append(session_id)
            updated = dataclasses.replace(self.db.sessions[session_id], labels=labels)
            self.db.sessions[session_id] = updated
            return updated

        monkeypatch.setattr(sessions_repo, "set_labels", set_labels)
        settings = Settings(
            database_url="postgresql://unused/db",
            allowed_cidrs="10.1.2.0/24",
            master_key=KEY,
            http_port=8000,
            agent_port=AGENT_PORT,
            service_port=8002,
        )
        self.app = create_app(settings, pool=None)
        self.app.state.pool = self.db

    async def put(self, body: Any, *, token: str | None = None, **headers: str) -> httpx.Response:
        sid = self.session.id
        hdrs = {"X-Orch-Session": str(sid), "Authorization": f"Bearer {token or agent_token(sid)}"}
        hdrs.update(headers)
        transport = httpx.ASGITransport(app=self.app, client=(LEADER_IP, 4321))
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://orch:{AGENT_PORT}"
        ) as c:
            return await c.put(URL, headers=hdrs, json=body)

    def labels(self, session: sessions_repo.Session) -> list[str]:
        return self.db.sessions[session.id].labels


async def test_sets_own_session_labels(monkeypatch):
    rig = Rig(monkeypatch)
    r = await rig.put({"labels": [" Team-A ", "bot", "team-a"]})
    assert r.status_code == 200, r.text
    assert r.json() == {"session_id": str(rig.session.id), "labels": ["team-a", "bot"]}
    assert rig.labels(rig.session) == ["team-a", "bot"]


async def test_body_session_id_is_rejected_422(monkeypatch):
    rig = Rig(monkeypatch)
    r = await rig.put({"labels": ["a"], "session_id": str(rig.other.id)})
    assert r.status_code == 422, r.text
    assert rig.writes == []


async def test_cannot_target_other_session(monkeypatch):
    rig = Rig(monkeypatch)
    r = await rig.put({"labels": ["mine"]})
    assert r.status_code == 200, r.text
    assert rig.writes == [rig.session.id]
    assert rig.labels(rig.other) == []
    # a path id is not a route: the other session cannot be addressed at all
    sid = rig.session.id
    transport = httpx.ASGITransport(app=rig.app, client=(LEADER_IP, 4321))
    async with httpx.AsyncClient(transport=transport, base_url=f"http://orch:{AGENT_PORT}") as c:
        r = await c.put(
            f"/agent/v1/session/{rig.other.id}/labels",
            headers={"X-Orch-Session": str(sid), "Authorization": f"Bearer {agent_token(sid)}"},
            json={"labels": ["x"]},
        )
    assert r.status_code in (404, 405)
    assert rig.labels(rig.other) == []


@pytest.mark.parametrize(
    "labels",
    [
        ["bad label"],
        ["project:other"],
        ["ok", "Project:x"],
        ["UPPER!"],
        [""],
        ["a" * 41],
        [f"l{i}" for i in range(21)],
        "x",
    ],
)
async def test_invalid_label_422(monkeypatch, labels):
    rig = Rig(monkeypatch)
    r = await rig.put({"labels": labels})
    assert r.status_code == 422, r.text
    assert rig.writes == []


async def test_trigger_session_allowed(monkeypatch):
    rig = Rig(monkeypatch, created_by_service="agents-webhook")
    r = await rig.put({"labels": ["from-trigger"]})
    assert r.status_code == 200, r.text
    assert rig.labels(rig.session) == ["from-trigger"]


async def test_requires_agent_token_401(monkeypatch):
    rig = Rig(monkeypatch)
    r = await rig.put({"labels": ["a"]}, token="wrong")
    assert_error(r, 401, "unauthorized")
    assert rig.writes == []


async def test_idempotent_put(monkeypatch):
    rig = Rig(monkeypatch)
    first = await rig.put({"labels": ["a", "b"]})
    second = await rig.put({"labels": ["a", "b"]})
    assert first.status_code == second.status_code == 200
    assert first.json() == second.json()
    assert rig.labels(rig.session) == ["a", "b"]
    cleared = await rig.put({"labels": []})
    assert cleared.json()["labels"] == []
