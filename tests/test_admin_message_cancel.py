"""DELETE /agent/v1/admin/sessions/{slug}/messages/{message_id}: a superleader cancels a queued
message of any manageable session (its own included); the session guard sits in the UPDATE."""

from __future__ import annotations

import pytest
from factories import attach_preset, insert_message
from test_agent_session_messaging import hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue

S = f"{ADMIN_PREFIX}/sessions"


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


async def cancel(app, who, slug, message_id, ip=None):
    async with client_for(app, ip or who.leader_ip) as http:
        return await http.delete(f"{S}/{slug}/messages/{message_id}", headers=headers(who.id))


async def status_of(pool, message_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT status FROM messages WHERE id = %s", (message_id,))
        return (await cur.fetchone())[0]


@pytest.fixture
async def world(pool):
    boss = await hive(pool, "mc-boss", superleader=True)
    other = await hive(pool, "mc-other")
    third = await hive(pool, "mc-third")
    await attach_preset(pool, other.id, manageable=True)
    await attach_preset(pool, third.id, manageable=True)
    return boss, other, third


async def test_cancel_queued_message_of_manageable_session_204(app, pool, world):
    boss, other, _ = world
    msg = await insert_message(pool, other.id, text="q", status="queued")
    r = await cancel(app, boss, other.slug, msg["id"])
    assert r.status_code == 204, r.text
    assert r.content == b""
    assert await status_of(pool, msg["id"]) == "cancelled"


async def test_cancel_message_of_another_session_through_slug_404(app, pool, world):
    boss, other, third = world
    msg = await insert_message(pool, third.id, text="q", status="queued")
    r = await cancel(app, boss, other.slug, msg["id"])
    assert_error(r, 404, "not_found")
    assert r.json()["error"]["message"] == "message not found"
    assert await status_of(pool, msg["id"]) == "queued"
    ok = await cancel(app, boss, third.slug, msg["id"])  # control: the right slug cancels it
    assert ok.status_code == 204, ok.text


async def test_cancel_not_queued_409(app, pool, world):
    boss, other, _ = world
    msg = await insert_message(pool, other.id, text="q", status="failed")
    r = await cancel(app, boss, other.slug, msg["id"])
    assert_error(r, 409, "not_queued")
    assert await status_of(pool, msg["id"]) == "failed"


async def test_cancel_unknown_message_404(app, pool, world):
    boss, other, _ = world
    r = await cancel(app, boss, other.slug, 987654321)
    assert_error(r, 404, "not_found")
    assert r.json()["error"]["message"] == "message not found"


async def test_cancel_own_session_message_allowed(app, pool, world):
    boss, _, _ = world
    msg = await insert_message(pool, boss.id, text="q", status="queued")
    r = await cancel(app, boss, boss.slug, msg["id"])
    assert r.status_code == 204, r.text
    assert await status_of(pool, msg["id"]) == "cancelled"


async def test_cancel_on_unmanageable_session_403(app, pool):
    boss = await hive(pool, "mc-boss-u", superleader=True)
    other = await hive(pool, "mc-other-u")
    await attach_preset(pool, other.id, manageable=False)
    msg = await insert_message(pool, other.id, text="q", status="queued")
    r = await cancel(app, boss, other.slug, msg["id"])
    assert_error(r, 403, "not_manageable")
    assert await status_of(pool, msg["id"]) == "queued"


async def test_cancel_regular_leader_403(app, pool, world):
    _, other, _ = world
    reg = await hive(pool, "mc-reg")
    msg = await insert_message(pool, other.id, text="q", status="queued")
    r = await cancel(app, reg, other.slug, msg["id"])
    assert_error(r, 403, "not_superleader")
    assert await status_of(pool, msg["id"]) == "queued"


async def test_repo_cancel_guarded_by_session(pool, world):
    _, other, third = world
    msg = await insert_message(pool, third.id, text="q", status="queued")
    async with pool.connection() as conn:
        assert await queue.cancel_message(conn, msg["id"], session_id=other.id) is False
    assert await status_of(pool, msg["id"]) == "queued"
    async with pool.connection() as conn:
        assert await queue.cancel_message(conn, msg["id"], session_id=third.id) is True
    assert await status_of(pool, msg["id"]) == "cancelled"
