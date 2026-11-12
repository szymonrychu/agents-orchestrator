"""Session provenance in the session view (which service or superleader created a session) and
its inactivity fields (quiet clock, reminder and close times, a pending inactivity close)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta

import httpx
import pytest
from factories import insert_message, insert_session
from fakes.fake_kube import FakeKube
from test_admin_sessions import P, S, boss  # noqa: F401  (boss is a fixture)
from test_api_agent import KEY, headers
from test_api_agent import client_for as agent_client
from test_api_sessions import client_for as main_client
from test_api_sessions import make_settings

from agents_orchestrator.api.app import create_app

TOKEN = "s3cret-webhook-token-value"
DEFAULT_ID = "00000000-0000-0000-0000-000000000001"


@pytest.fixture
def app(pool, dsn):
    digest = hashlib.sha256(TOKEN.encode()).hexdigest()
    settings = make_settings(dsn, service_tokens=f"agents-webhook:{digest}", master_key=KEY)
    return create_app(settings, pool, kube=FakeKube(namespace="agents"))


def service_client(app) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=("203.0.113.9", 4444)),
        base_url="http://orch:8002",
        headers={"Authorization": f"Bearer {TOKEN}"},
    )


async def views(http, session_id: str) -> tuple[dict, dict]:
    """The session as the list and as the detail route serve it."""
    listed = {s["id"]: s for s in (await http.get("/api/v1/sessions")).json()}
    detail = (await http.get(f"/api/v1/sessions/{session_id}")).json()
    return listed[session_id], detail


async def test_service_created_session_names_its_service_and_no_session(app):
    async with service_client(app) as http:
        r = await http.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "wh-1"})
        assert r.status_code == 201, r.text
        created = r.json()
        listed, detail = await views(http, created["id"])
    for view in (created, listed, detail):
        assert view["created_by_service"] == "agents-webhook"
        assert view["created_by_session"] is None


async def test_ui_created_session_has_no_provenance(app):
    async with main_client(app) as http:
        r = await http.post("/api/v1/sessions", json={"preset_id": DEFAULT_ID, "slug": "ui-1"})
        assert r.status_code == 201, r.text
        listed, detail = await views(http, r.json()["id"])
    for view in (r.json(), listed, detail):
        assert view["created_by_service"] is None
        assert view["created_by_session"] is None


async def test_superleader_created_session_names_the_creating_session(app, boss):  # noqa: F811
    async with agent_client(app, boss.leader_ip) as http:
        preset = (
            await http.post(P, headers=headers(boss.id), json={"name": "worker", "spec": {}})
        ).json()
        r = await http.post(
            S, headers=headers(boss.id), json={"preset_id": preset["id"], "slug": "child-1"}
        )
        assert r.status_code == 201, r.text
    async with main_client(app) as http:
        listed, detail = await views(http, r.json()["id"])
    for view in (r.json(), listed, detail):
        assert view["created_by_service"] is None
        assert view["created_by_session"] == str(boss.id)


# ---------------------------------------------------------------- inactivity fields

T = datetime(2026, 11, 2, 10, 0, tzinfo=UTC)


@pytest.fixture
def limited_app(pool, dsn):
    settings = make_settings(dsn, inactivity_reminder_seconds=3600, inactivity_close_seconds=14400)
    return create_app(settings, pool, kube=FakeKube(namespace="agents"))


async def _quiet_since(pool, spec: dict, at: datetime = T) -> str:
    """A session created an hour before `at` whose api-written update stamp is `at`."""
    row = await insert_session(pool, spec=spec)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET created_at = %s, last_update_at = %s WHERE id = %s",
            (at - timedelta(hours=1), at, row["id"]),
        )
    return str(row["id"])


def _at(value: str | None) -> datetime | None:
    return None if value is None else datetime.fromisoformat(value)


async def test_view_inactivity_fields(limited_app, pool):
    plain = await _quiet_since(pool, {})
    superleader = await _quiet_since(pool, {"session_messaging": True})
    no_reminder = await _quiet_since(pool, {"inactivity": {"reminder_seconds": 0}})
    closing = await _quiet_since(pool, {})
    reminded = T + timedelta(hours=1, minutes=1)
    deadline = T + timedelta(hours=4, minutes=5)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET inactivity_reminded_at = %s WHERE id = %s", (reminded, plain)
        )
        await conn.execute(
            "UPDATE sessions SET close_requested_at = %s, close_kind = 'inactivity',"
            " close_reason = 'No update for 4 h (inactivity limit 14400 s)',"
            " close_deadline_at = %s WHERE id = %s",
            (deadline - timedelta(minutes=5), deadline, closing),
        )
    async with main_client(limited_app) as http:
        for view in await views(http, plain):
            assert _at(view["last_update_at"]) == T
            assert _at(view["reminder_at"]) == T + timedelta(hours=1)
            assert _at(view["inactivity_closes_at"]) == T + timedelta(hours=4)
            assert _at(view["inactivity_reminded_at"]) == reminded
            assert view["close_kind"] is None and view["close_deadline_at"] is None
        for view in await views(http, superleader):
            assert _at(view["last_update_at"]) == T
            assert view["reminder_at"] is None and view["inactivity_closes_at"] is None
        for view in await views(http, no_reminder):
            assert view["reminder_at"] is None
            assert _at(view["inactivity_closes_at"]) == T + timedelta(hours=4)
        for view in await views(http, closing):
            assert view["close_kind"] == "inactivity"
            assert _at(view["close_deadline_at"]) == deadline
            assert view["reminder_at"] is None
            assert _at(view["inactivity_closes_at"]) == T + timedelta(hours=4)


async def test_view_inactivity_fields_off_by_default(app, pool):
    session = await _quiet_since(pool, {})
    async with main_client(app) as http:
        for view in await views(http, session):
            assert _at(view["last_update_at"]) == T
            assert view["reminder_at"] is None and view["inactivity_closes_at"] is None


async def test_view_last_update_is_quiet_clock(limited_app, pool):
    session = await _quiet_since(pool, {})
    inbound = await insert_message(pool, session, text="any news?")
    await insert_message(pool, session, text="notice", notice="inactivity_reminder")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE messages SET created_at = %s WHERE session_id = %s AND notice IS NOT NULL",
            (T + timedelta(days=1), session),
        )
    assert inbound["created_at"] > T
    async with main_client(limited_app) as http:
        for view in await views(http, session):
            assert _at(view["last_update_at"]) == inbound["created_at"]
            assert _at(view["reminder_at"]) == inbound["created_at"] + timedelta(hours=1)


async def test_view_last_user_message_at_null_then_set(app, pool):
    from agents_orchestrator.db import sessions as repo

    cl = await insert_session(pool)
    async with main_client(app) as http:
        listed, detail = await views(http, str(cl["id"]))
        assert listed["last_user_message_at"] is None
        assert detail["last_user_message_at"] is None
        async with pool.connection() as conn:
            await repo.mark_user_message(conn, cl["id"])
            cur = await conn.execute(
                "SELECT last_user_message_at FROM sessions WHERE id = %s", (cl["id"],)
            )
            row = (await cur.fetchone())[0]
        listed, detail = await views(http, str(cl["id"]))
    assert datetime.fromisoformat(detail["last_user_message_at"]) == row
    assert datetime.fromisoformat(listed["last_user_message_at"]) == row
