"""`GET /agent/v1/activity`: the caller session's recent digest lines on demand (hive D3).

The `test_activity_*` cases run against Postgres (real SQL, real identity). The `test_offline_*`
cases fake the repository reads, as `test_agent_chat.py` does, so the route logic runs without
Docker.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from factories import insert_event, insert_session
from test_agent_chat import AGENT_PORT, LEADER_IP, MEMBER_IP, FakeDb, agent_token, settings
from test_api_agent import client_for, headers, make_settings
from test_api_leadership import hive

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import events as events_repo
from agents_orchestrator.dispatcher.feed import PENDING_BASH_TIMEOUT_SECONDS
from agents_orchestrator.spec import WORKDIR_ROOT

IP0, IP1 = "10.9.0.10", "10.9.0.11"
URL = "/agent/v1/activity"
ROOT = f"{WORKDIR_ROOT}/workdir"


def call_data(uid: str, tool: str, tool_input: dict[str, Any]) -> dict[str, Any]:
    return {"tool_use_id": uid, "tool_name": tool, "tool_input": tool_input}


async def get(dsn, pool, c, ip=IP0, query: str = "", **kw):
    async with client_for(create_app(make_settings(dsn), pool), ip) as http:
        return await http.get(URL + query, headers=kw.pop("headers", headers(c["id"])), **kw)


async def edit(pool, c, agent, path, ts=None):
    return await insert_event(
        pool,
        c["id"],
        agent["id"],
        type="tool_call",
        data=call_data(f"w-{uuid.uuid4().hex[:6]}", "Write", {"file_path": path}),
        ts=ts,
    )


async def test_activity_lists_digest_lines(pool, dsn):
    c, a0, a1 = await hive(pool)
    await edit(pool, c, a0, f"{ROOT}/src/x.py")
    await insert_event(
        pool,
        c["id"],
        a1["id"],
        type="tool_call",
        data=call_data("b1", "Bash", {"command": "pytest -q"}),
    )
    await insert_event(
        pool,
        c["id"],
        a1["id"],
        type="tool_result",
        data={"tool_use_id": "b1", "tool_name": "Bash", "exit_code": 1, "is_error": True},
    )
    await insert_event(
        pool,
        c["id"],
        a0["id"],
        type="turn_completed",
        data={"record": {"final_text": "done\nsecond line"}},
    )
    await insert_event(pool, c["id"], a0["id"], type="message", data={"text": "ignored"})
    other = await insert_session(pool, spec={"replicas": 1})
    await edit(pool, other, a0, f"{ROOT}/other-session.py")
    r = await get(dsn, pool, c)
    assert r.status_code == 200, r.text
    lines = r.json()["lines"]
    assert [(x["agent"], x["text"]) for x in lines] == [
        (a0["pod_name"], f"{a0['pod_name']} wrote src/x.py"),
        (a1["pod_name"], f"{a1['pod_name']} ran: pytest -q (exit 1)"),
        (a0["pod_name"], f"{a0['pod_name']} finished a turn: done"),
    ]
    assert [x["id"] for x in lines] == sorted(x["id"] for x in lines)
    assert all(isinstance(x["id"], int) and x["ts"] for x in lines)
    assert set(lines[0]) == {"id", "agent", "text", "ts"}


async def test_activity_agent_filter(pool, dsn):
    c, a0, a1 = await hive(pool)
    await edit(pool, c, a0, f"{ROOT}/a.py")
    await edit(pool, c, a1, f"{ROOT}/b.py")
    r = await get(dsn, pool, c, query=f"?agent={a1['pod_name']}")
    assert [x["text"] for x in r.json()["lines"]] == [f"{a1['pod_name']} wrote b.py"]
    r = await get(dsn, pool, c, query=f"?agent={a0['pod_name']}")
    assert [x["agent"] for x in r.json()["lines"]] == [a0["pod_name"]]
    r = await get(dsn, pool, c, query="?agent=nobody-9")
    assert (r.status_code, r.json()) == (200, {"lines": []})
    # a gone agent's lines stay history
    async with pool.connection() as conn:
        await conn.execute("UPDATE agents SET gone_at = now() WHERE id = %s", (a0["id"],))
    r = await get(dsn, pool, c, IP1, query=f"?agent={a0['pod_name']}")
    assert [x["text"] for x in r.json()["lines"]] == [f"{a0['pod_name']} wrote a.py"]


async def test_activity_limit_bounds(pool, dsn):
    c, a0, _ = await hive(pool)
    for i in range(60):
        await edit(pool, c, a0, f"{ROOT}/f{i:02d}.py")
    default = (await get(dsn, pool, c)).json()["lines"]
    assert len(default) == 50
    assert default[0]["text"].endswith("wrote f10.py")
    assert default[-1]["text"].endswith("wrote f59.py")
    three = (await get(dsn, pool, c, query="?limit=3")).json()["lines"]
    assert [x["text"].rsplit(" ", 1)[1] for x in three] == ["f57.py", "f58.py", "f59.py"]
    assert len((await get(dsn, pool, c, query="?limit=200")).json()["lines"]) == 60
    assert len((await get(dsn, pool, c, query="?limit=1")).json()["lines"]) == 1
    for bad in ("0", "201", "-1", "abc"):
        r = await get(dsn, pool, c, query=f"?limit={bad}")
        assert (r.status_code, r.json()["error"]["code"]) == (422, "invalid_request"), bad


async def test_activity_requires_agent_identity(pool, dsn):
    c, a0, a1 = await hive(pool)
    other = await insert_session(pool, spec={"replicas": 1})
    await edit(pool, other, a0, f"{ROOT}/secret.py")
    r = await get(dsn, pool, c, headers={})
    assert (r.status_code, r.json()["error"]["code"]) == (401, "unauthorized")
    r = await get(dsn, pool, c, headers=headers(c["id"], "wrong"))
    assert r.status_code == 401
    r = await get(dsn, pool, c, ip="10.9.9.9")
    assert (r.status_code, r.json()["error"]["code"]) == (403, "forbidden")
    # the answer is the caller's own session only
    r = await get(dsn, pool, c)
    assert r.json() == {"lines": []}


async def test_activity_omits_pending_bash(pool, dsn):
    c, a0, _ = await hive(pool)
    old = datetime.now(UTC) - timedelta(seconds=PENDING_BASH_TIMEOUT_SECONDS + 30)
    await insert_event(
        pool,
        c["id"],
        a0["id"],
        type="tool_call",
        data=call_data("young", "Bash", {"command": "make build"}),
    )
    await insert_event(
        pool,
        c["id"],
        a0["id"],
        type="tool_call",
        data=call_data("stale", "Bash", {"command": "sleep 999"}),
        ts=old,
    )
    texts = [x["text"] for x in (await get(dsn, pool, c)).json()["lines"]]
    assert texts == [f"{a0['pod_name']} ran: sleep 999"]
    await insert_event(
        pool,
        c["id"],
        a0["id"],
        type="tool_result",
        data={"tool_use_id": "young", "tool_name": "Bash", "exit_code": 0},
    )
    texts = [x["text"] for x in (await get(dsn, pool, c)).json()["lines"]]
    assert texts == [
        f"{a0['pod_name']} ran: make build (ok)",
        f"{a0['pod_name']} ran: sleep 999",
    ]


# ------------------------------------------------------------------ offline (no Docker)

T0 = datetime(2026, 10, 29, 12, 0, 0, tzinfo=UTC)


def row(i: int, agent: str, kind: str, data: dict[str, Any], ts: datetime = T0) -> dict[str, Any]:
    return {"id": i, "type": kind, "data": data, "ts": ts, "agent": agent}


def write_row(i: int, agent: str, path: str, ts: datetime = T0) -> dict[str, Any]:
    return row(i, agent, "tool_call", call_data(f"w{i}", "Write", {"file_path": path}), ts)


class Rig:
    def __init__(self, monkeypatch: pytest.MonkeyPatch, rows: list[dict[str, Any]]):
        self.db = FakeDb()
        self.db.install(monkeypatch)
        self.session = self.db.add_session("s-1", hive=True)
        self.asked: list[tuple[uuid.UUID, int]] = []

        async def recent(_conn: Any, session_id: uuid.UUID, limit: int = 1000):
            self.asked.append((session_id, limit))
            return rows

        monkeypatch.setattr(events_repo, "recent_activity_rows", recent)
        self.app = create_app(settings(), pool=self.db)

    async def get(self, query: str = "", ip: str = LEADER_IP, **kw: Any):
        import httpx

        transport = httpx.ASGITransport(app=self.app, client=(ip, 4321))
        sid = self.session.id
        hdrs = kw.pop(
            "headers",
            {"X-Orch-Session": str(sid), "Authorization": f"Bearer {agent_token(sid)}"},
        )
        async with httpx.AsyncClient(
            transport=transport, base_url=f"http://orch:{AGENT_PORT}"
        ) as http:
            return await http.get(URL + query, headers=hdrs, **kw)


async def test_offline_lines_oldest_first_with_ts_and_relative_paths(monkeypatch):
    rows = [
        write_row(5, "pod-1", f"{ROOT}/src/a.py", T0 - timedelta(minutes=3)),
        write_row(9, "pod-0", f"{ROOT}/b.py", T0),
        write_row(11, "pod-0", "/etc/outside"),
    ]
    rig = Rig(monkeypatch, rows)
    r = await rig.get()
    assert r.status_code == 200, r.text
    assert r.json() == {
        "lines": [
            {
                "id": 5,
                "agent": "pod-1",
                "text": "pod-1 wrote src/a.py",
                "ts": (T0 - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
            },
            {"id": 9, "agent": "pod-0", "text": "pod-0 wrote b.py", "ts": "2026-10-29T12:00:00Z"},
            {
                "id": 11,
                "agent": "pod-0",
                "text": "pod-0 wrote /etc/outside",
                "ts": "2026-10-29T12:00:00Z",
            },
        ]
    }
    assert rig.asked == [(rig.session.id, events_repo.ACTIVITY_WINDOW)]


async def test_offline_filter_limit_and_empty(monkeypatch):
    rows = [write_row(i, f"pod-{i % 2}", f"{ROOT}/f{i}.py") for i in range(1, 8)]
    rig = Rig(monkeypatch, rows)
    ids = lambda r: [x["id"] for x in r.json()["lines"]]  # noqa: E731
    assert ids(await rig.get()) == [1, 2, 3, 4, 5, 6, 7]
    assert ids(await rig.get("?limit=3")) == [5, 6, 7]
    assert ids(await rig.get("?agent=pod-1")) == [1, 3, 5, 7]
    assert ids(await rig.get("?agent=pod-1&limit=2")) == [5, 7]
    assert ids(await rig.get("?agent=ghost")) == []
    for bad in ("0", "201", "x"):
        r = await rig.get(f"?limit={bad}")
        assert (r.status_code, r.json()["error"]["code"]) == (422, "invalid_request"), bad
    assert [x["id"] for x in (await rig.get("?limit=200")).json()["lines"]] == [1, 2, 3, 4, 5, 6, 7]


async def test_offline_pending_bash_omitted_until_result(monkeypatch):
    fresh = datetime.now(UTC)
    call = row(1, "pod-0", "tool_call", call_data("u1", "Bash", {"command": "make"}), fresh)
    rig = Rig(monkeypatch, [call])
    assert (await rig.get()).json() == {"lines": []}
    result = row(2, "pod-0", "tool_result", {"tool_use_id": "u1", "exit_code": 2})
    rig = Rig(monkeypatch, [call, result])
    assert [x["text"] for x in (await rig.get()).json()["lines"]] == ["pod-0 ran: make (exit 2)"]


async def test_offline_identity(monkeypatch):
    rig = Rig(monkeypatch, [write_row(1, "pod-0", f"{ROOT}/a.py")])
    r = await rig.get(headers={})
    assert (r.status_code, r.json()["error"]["code"]) == (401, "unauthorized")
    r = await rig.get(ip="10.9.9.9")
    assert (r.status_code, r.json()["error"]["code"]) == (403, "forbidden")
    assert rig.asked == []
    assert (await rig.get(ip=MEMBER_IP)).status_code == 200
