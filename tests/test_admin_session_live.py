"""Admin routes for the live work of any session (T-120): activity, events, message queue, file
locks and commands answer another session's data to a superleader, unmanageable and raw-spec
targets included, and a regular leader gets 403."""

from __future__ import annotations

import pytest
from factories import attach_preset, insert_command, insert_event, insert_message
from test_agent_session_messaging import alone, hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue
from agents_orchestrator.spec import WORKDIR_ROOT

S = f"{ADMIN_PREFIX}/sessions"
LIVE = ["activity", "events", "messages", "locks", "commands"]


@pytest.fixture
async def boss(pool):
    return await alone(pool, "lv-boss", superleader=True)


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


@pytest.fixture
def call(app, boss):
    async def go(path, **params):
        async with client_for(app, boss.leader_ip) as http:
            return await http.get(path, headers=headers(boss.id), params=params)

    return go


@pytest.fixture(params=["unmanageable", "raw_spec"])
async def target(request, pool):
    other = await hive(pool, f"lv-{request.param}".replace("_", "-"))
    if request.param == "unmanageable":
        await attach_preset(pool, other.id, manageable=False)
    return other


async def seed(pool, other):
    await insert_event(
        pool,
        other.id,
        other.leader_id,
        type="tool_call",
        data={
            "tool_use_id": "t1",
            "tool_name": "Write",
            "tool_input": {"file_path": f"{WORKDIR_ROOT}/workdir/a.py"},
        },
    )
    await insert_event(pool, other.id, other.leader_id, type="message", data={"text": "hi"})
    await insert_message(pool, other.id, text="queued one")
    await insert_message(pool, other.id, text="failed one", status="failed")
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO file_locks (session_id, path, agent_id) VALUES (%s, 'src/a.py', %s)",
            (other.id, other.leader_id),
        )
        return await queue.enqueue_command(conn, other.leader_id, "restart")


async def test_every_live_route_answers_another_sessions_data(call, pool, target):
    command_id = await seed(pool, target)
    base = f"{S}/{target.slug}"
    act = await call(f"{base}/activity")
    assert act.status_code == 200, act.text
    assert [x["text"] for x in act.json()["lines"]] and "a.py" in act.json()["lines"][0]["text"]
    ev = await call(f"{base}/events")
    assert ev.status_code == 200, ev.text
    assert {e["type"] for e in ev.json()["events"]} == {"tool_call", "message"}
    only = await call(f"{base}/events", types="message")
    assert [e["type"] for e in only.json()["events"]] == ["message"]
    msgs = await call(f"{base}/messages")
    assert sorted(m["text"] for m in msgs.json()) == ["failed one", "queued one"]
    failed = await call(f"{base}/messages", status="failed")
    assert [m["text"] for m in failed.json()] == ["failed one"]
    locks = await call(f"{base}/locks")
    assert [x["path"] for x in locks.json()["locks"]] == ["src/a.py"]
    cmds = await call(f"{base}/commands")
    assert [c["id"] for c in cmds.json()] == [command_id]
    assert cmds.json()[0]["kind"] == "restart"


async def test_commands_limit_and_order(call, pool, target):
    first = await insert_command(pool, target.leader_id)
    second = await insert_command(pool, target.leader_id)
    r = await call(f"{S}/{target.slug}/commands", limit=1)
    assert [c["id"] for c in r.json()] == [second["id"]]
    assert first["id"] != second["id"]
    assert (await call(f"{S}/{target.slug}/commands", limit=201)).status_code == 422


@pytest.mark.parametrize("route", LIVE)
async def test_unknown_slug_404(call, route):
    assert_error(await call(f"{S}/no-such-session/{route}"), 404, "not_found")


@pytest.mark.parametrize("route", LIVE)
async def test_regular_leader_403(app, pool, route):
    leader = await hive(pool, "lv-leader")
    victim = await hive(pool, "lv-victim")
    async with client_for(app, leader.leader_ip) as http:
        for slug in (victim.slug, leader.slug):
            r = await http.get(f"{S}/{slug}/{route}", headers=headers(leader.id))
            assert r.status_code == 403, r.text


async def test_events_both_cursors_400_and_bad_status_422(call, target):
    base = f"{S}/{target.slug}"
    assert_error(await call(f"{base}/events", before_id=5, after_id=1), 400, "bad_request")
    assert_error(await call(f"{base}/messages", status="bogus"), 422, "invalid_request")
    assert_error(await call(f"{base}/messages", status=","), 422, "invalid_request")


async def test_command_get_other_issuer_200(call, app, pool, boss, target):
    rival = await alone(pool, "lv-rival", superleader=True)
    async with pool.connection() as conn:
        main_cmd = await queue.enqueue_command(conn, target.leader_id, "restart")
    r = await call(f"{ADMIN_PREFIX}/commands/{main_cmd}")
    assert r.status_code == 200, r.text
    assert r.json()["id"] == main_cmd
    async with client_for(app, rival.leader_ip) as http:
        seen = await http.get(f"{ADMIN_PREFIX}/commands/{main_cmd}", headers=headers(rival.id))
    assert seen.status_code == 200
    assert_error(await call(f"{ADMIN_PREFIX}/commands/999999"), 404, "not_found")


async def test_agent_activity_unchanged(app, pool):
    mine = await hive(pool, "lv-mine")
    other = await hive(pool, "lv-theirs")
    for sess, name in ((mine, "mine.py"), (other, "theirs.py")):
        await insert_event(
            pool,
            sess.id,
            sess.leader_id,
            type="tool_call",
            data={
                "tool_use_id": name,
                "tool_name": "Write",
                "tool_input": {"file_path": f"{WORKDIR_ROOT}/workdir/{name}"},
            },
        )
    async with client_for(app, mine.leader_ip) as http:
        r = await http.get("/agent/v1/activity", headers=headers(mine.id))
    assert r.status_code == 200, r.text
    texts = " ".join(x["text"] for x in r.json()["lines"])
    assert "mine.py" in texts and "theirs.py" not in texts
