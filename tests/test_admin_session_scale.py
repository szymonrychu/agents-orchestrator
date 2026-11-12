"""POST /agent/v1/admin/sessions/{slug}/scale: a superleader sets replicas of any manageable
session; its own session is refused (422 `own_session`)."""

from __future__ import annotations

import pytest
from factories import attach_preset, insert_agent, insert_session
from test_admin_audit_detail import admin_rows
from test_agent_scale import replicas_of
from test_agent_session_messaging import hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions"


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


async def scale(app, who, slug, replicas, ip=None):
    async with client_for(app, ip or who.leader_ip) as http:
        return await http.post(
            f"{S}/{slug}/scale", headers=headers(who.id), json={"replicas": replicas}
        )


async def pair(pool, tag):
    boss = await hive(pool, f"sc-boss-{tag}", superleader=True)
    other = await hive(pool, f"sc-other-{tag}")
    await attach_preset(pool, other.id, manageable=True)
    return boss, other


async def test_superleader_scales_manageable_session_down(app, pool):
    boss, other = await pair(pool, "a")
    r = await scale(app, boss, other.slug, 1)
    assert r.status_code == 202, r.text
    assert r.json() == {
        "session_id": str(other.id),
        "replicas": 1,
        "previous_replicas": 2,
        "max_replicas": 2,
    }
    assert await replicas_of(pool, other.id) == 1


async def test_admin_scale_out_of_range_422(app, pool):
    boss, other = await pair(pool, "b")
    r = await scale(app, boss, other.slug, 3)
    assert_error(r, 422, "out_of_range")


async def test_admin_scale_static_409(app, pool):
    boss = await hive(pool, "sc-boss-c", superleader=True)
    row = await insert_session(
        pool, slug="sc-static", spec={"replicas": 2, "max_replicas": 2, "static": True}
    )
    await insert_agent(pool, row["id"], ordinal=0, ready=True)
    await attach_preset(pool, row["id"], manageable=True)
    r = await scale(app, boss, "sc-static", 1)
    assert_error(r, 409, "static_session")


async def test_admin_scale_deleting_409(app, pool):
    boss = await hive(pool, "sc-boss-d", superleader=True)
    other = await hive(pool, "sc-other-d", desired="deleting")
    await attach_preset(pool, other.id, manageable=True)
    r = await scale(app, boss, other.slug, 1)
    assert_error(r, 409, "session_deleting")


async def test_admin_scale_unmanageable_403(app, pool):
    boss = await hive(pool, "sc-boss-e", superleader=True)
    other = await hive(pool, "sc-other-e")
    await attach_preset(pool, other.id, manageable=False)
    r = await scale(app, boss, other.slug, 1)
    assert_error(r, 403, "not_manageable")
    assert await replicas_of(pool, other.id) == 2


async def test_admin_scale_own_session_422(app, pool):
    boss = await hive(pool, "sc-boss-f", superleader=True)
    r = await scale(app, boss, boss.slug, 1)
    assert_error(r, 422, "own_session")
    r = await scale(app, boss, boss.slug, 1, ip=boss.member_ip)
    assert_error(r, 422, "own_session")
    assert await replicas_of(pool, boss.id) == 2


async def test_admin_scale_audit_detail(app, pool):
    boss, other = await pair(pool, "g")
    r = await scale(app, boss, other.slug, 1)
    assert r.status_code == 202, r.text
    (row,) = await admin_rows(pool)
    assert row["fields"] == ["replicas"]
    assert row["detail"] == {"replicas_before": 2, "replicas_after": 1}


async def test_admin_scale_regular_leader_403(app, pool):
    leader = await hive(pool, "sc-leader")
    other = await hive(pool, "sc-other-h")
    await attach_preset(pool, other.id, manageable=True)
    r = await scale(app, leader, other.slug, 1)
    assert_error(r, 403, "not_superleader")


async def test_admin_scale_unknown_404(app, pool):
    boss = await hive(pool, "sc-boss-i", superleader=True)
    r = await scale(app, boss, "no-such", 1)
    assert_error(r, 404, "not_found")
