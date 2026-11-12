"""PUT /agent/v1/admin/sessions/{slug}/labels: a superleader replaces the labels of any manageable
session, or of its own."""

from __future__ import annotations

import pytest
from factories import attach_preset
from test_admin_audit_detail import admin_rows
from test_agent_session_messaging import hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.labels import normalize_labels

S = f"{ADMIN_PREFIX}/sessions"


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


async def put(app, who, slug, body, ip=None):
    async with client_for(app, ip or who.leader_ip) as http:
        return await http.put(f"{S}/{slug}/labels", headers=headers(who.id), json=body)


async def stored(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT labels FROM sessions WHERE id = %s", (session_id,))
        return (await cur.fetchone())[0]


async def test_superleader_sets_labels_of_manageable_session(app, pool):
    boss = await hive(pool, "lb-boss", superleader=True)
    other = await hive(pool, "lb-other")
    await attach_preset(pool, other.id, manageable=True)
    r = await put(app, boss, other.slug, {"labels": ["Team-A", " x "]})
    assert r.status_code == 200, r.text
    want = normalize_labels(["Team-A", " x "])
    assert r.json() == {"session_id": str(other.id), "labels": want}
    assert await stored(pool, other.id) == want


async def test_labels_of_unmanageable_session_403(app, pool):
    boss = await hive(pool, "lb-boss-u", superleader=True)
    other = await hive(pool, "lb-other-u")
    await attach_preset(pool, other.id, manageable=False)
    before = await stored(pool, other.id)
    r = await put(app, boss, other.slug, {"labels": ["a"]})
    assert_error(r, 403, "not_manageable")
    assert await stored(pool, other.id) == before


async def test_labels_of_raw_spec_session_403(app, pool):
    boss = await hive(pool, "lb-boss-r", superleader=True)
    other = await hive(pool, "lb-other-r")
    r = await put(app, boss, other.slug, {"labels": ["a"]})
    assert_error(r, 403, "not_manageable")


async def test_labels_of_own_session_allowed(app, pool):
    boss = await hive(pool, "lb-boss-o", superleader=True)
    r = await put(app, boss, boss.slug, {"labels": ["mine"]})
    assert r.status_code == 200, r.text
    r = await put(app, boss, boss.slug, {"labels": ["member"]}, ip=boss.member_ip)
    assert r.status_code == 200, r.text
    assert await stored(pool, boss.id) == ["member"]


async def test_labels_unknown_slug_404(app, pool):
    boss = await hive(pool, "lb-boss-n", superleader=True)
    r = await put(app, boss, "no-such", {"labels": []})
    assert_error(r, 404, "not_found")


async def test_labels_regular_leader_403(app, pool):
    leader = await hive(pool, "lb-leader")
    other = await hive(pool, "lb-other-l")
    await attach_preset(pool, other.id, manageable=True)
    r = await put(app, leader, other.slug, {"labels": ["a"]})
    assert_error(r, 403, "not_superleader")


async def test_labels_extra_field_422(app, pool):
    boss = await hive(pool, "lb-boss-x", superleader=True)
    other = await hive(pool, "lb-other-x")
    await attach_preset(pool, other.id, manageable=True)
    r = await put(app, boss, other.slug, {"labels": [], "x": 1})
    assert r.status_code == 422


async def test_labels_audit_fields(app, pool):
    boss = await hive(pool, "lb-boss-a", superleader=True)
    other = await hive(pool, "lb-other-a")
    await attach_preset(pool, other.id, manageable=True)
    r = await put(app, boss, other.slug, {"labels": ["a"]})
    assert r.status_code == 200, r.text
    (row,) = await admin_rows(pool)
    assert row["fields"] == ["labels"]
    assert row["status"] == 200
    assert "detail" not in row
