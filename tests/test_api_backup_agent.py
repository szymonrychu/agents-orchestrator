"""On-demand backup save and status routes of the agent API (S-68, T-519; decision 760)."""

from __future__ import annotations

import json

import pytest
from factories import insert_agent, insert_session
from test_api_agent import IP0, IP1, client_for, headers, make_settings, set_ip

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.reconciler.cache import SAVE_NOW, job_name

pytestmark = pytest.mark.asyncio

SKILLS = {"root": "claude", "path": "skills", "mode": "both", "name": "skills/{{ project }}"}
SRC = {"root": "workspace", "path": "src", "mode": "restore", "name": "src/{{ project }}"}


async def session_with(pool, entries):
    c = await insert_session(pool, spec={"replicas": 2, "max_replicas": 4, "backup": entries})
    a0 = await insert_agent(pool, c["id"], ordinal=0, ready=True)
    a1 = await insert_agent(pool, c["id"], ordinal=1, ready=True)
    await set_ip(pool, a0["id"], IP0)
    await set_ip(pool, a1["id"], IP1)
    return c, a0, a1


async def post(app, c, body, ip=IP0):
    async with client_for(app, ip) as http:
        return await http.post("/agent/v1/backup/save", json=body, headers=headers(c["id"]))


async def test_save_route_member_and_leader(pool, dsn):
    c, a0, a1 = await session_with(pool, [SRC, SKILLS])
    app = create_app(make_settings(dsn), pool)
    for ip in (IP0, IP1):  # any agent of the session, leader or member
        async with pool.connection() as conn:
            await conn.execute(
                "UPDATE sessions SET status = status - 'backup_save' WHERE id = %s", (c["id"],)
            )
        r = await post(app, c, {"root": "claude", "path": "skills"}, ip)
        assert r.status_code == 202, r.text
        body = r.json()
        assert body["entry_index"] == 1
        assert body["job"] == job_name(c["slug"], SAVE_NOW)
    async with pool.connection() as conn:
        status = (await sessions_repo.get(conn, c["id"])).status
    assert status["backup_save"]["entry_index"] == 1


async def test_save_route_no_entry_404(pool, dsn):
    c, _, _ = await session_with(pool, [SRC, SKILLS])
    app = create_app(make_settings(dsn), pool)
    for body in (
        {"root": "claude", "path": "rules"},  # no such entry
        {"root": "workspace", "path": "skills"},  # wrong root
        {"root": "workspace", "path": "src"},  # restore-only: nothing is saved
    ):
        r = await post(app, c, body)
        assert r.status_code == 404, (body, r.text)
        assert r.json()["error"]["code"] == "no_entry"
    nothing = await insert_session(pool, spec={"replicas": 1})
    await insert_agent(pool, nothing["id"], ordinal=0, ready=True)
    r = await post(app, nothing, {"root": "claude", "path": "skills"})
    assert r.status_code in (403, 404)


async def test_save_while_one_is_open_is_409(pool, dsn):
    c, _, _ = await session_with(pool, [SKILLS])
    app = create_app(make_settings(dsn), pool)
    assert (await post(app, c, {"root": "claude", "path": "skills"})).status_code == 202
    r = await post(app, c, {"root": "claude", "path": "skills"}, IP1)
    assert r.status_code == 409 and r.json()["error"]["code"] == "save_running"


async def test_save_while_the_run_is_running_is_409(pool, dsn):
    run = {"state": "running", "plan": [], "job": "x"}
    c, _, _ = await session_with(pool, [SKILLS])
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET status = %s::jsonb WHERE id = %s",
            (json.dumps({"backup": {"runs": {"save_now": run}}}), c["id"]),
        )
    r = await post(create_app(make_settings(dsn), pool), c, {"root": "claude", "path": "skills"})
    assert r.status_code == 409 and r.json()["error"]["code"] == "save_running"


async def test_backup_status_lists_entries(pool, dsn):
    c, _, _ = await session_with(pool, [SRC, SKILLS])
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as http:
        r = await http.get("/agent/v1/backup", headers=headers(c["id"]))
    assert r.status_code == 200, r.text
    entries = r.json()["entries"]
    assert [(e["index"], e["root"], e["path"], e["mode"]) for e in entries] == [
        (0, "workspace", "src", "restore"),
        (1, "claude", "skills", "both"),
    ]
    assert entries[1]["name"] == "skills/{{ project }}" and entries[1]["live"] is False
    assert entries[1]["restore"] is None and entries[1]["save"] is None
