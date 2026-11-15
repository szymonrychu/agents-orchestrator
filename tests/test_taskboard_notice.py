"""The runaway-guard owner notice task: the taskboard client and the notify_parked step."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime

import httpx
from factories import insert_session
from test_park_sweep import WINDOW, guarded_agent, rows, session_row, sweep_park

from agents_orchestrator import runaway, taskboard_client
from agents_orchestrator.api import autoclose
from agents_orchestrator.api.scope import PROJECTS_HEADER
from agents_orchestrator.settings import Settings

TOKEN = "CANARY-notice-token"
SID = uuid.uuid4()


def settings(**kw) -> Settings:
    values = {
        "database_url": "postgresql://unused/db",
        "agent_image": "i",
        "taskboard_url": "http://taskboard",
        "taskboard_token": TOKEN,
    }
    return Settings(**{**values, **kw})


async def post(s, handler, **kw) -> bool:
    return await taskboard_client.create_notice_task(
        s,
        SID,
        "x",
        "p1,p2",
        title="T",
        body="B",
        client_key=kw.pop("client_key", "runaway:k:1"),
        transport=httpx.MockTransport(handler),
    )


async def test_create_notice_task_posts_headers_and_body():
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return httpx.Response(201, json={})

    key = runaway.client_key(SID, datetime.fromtimestamp(1794614400, UTC))
    assert key == f"runaway:{SID}:1794614400"
    assert await post(settings(), handler, client_key=key) is True
    (req,) = seen
    assert req.method == "POST" and str(req.url) == "http://taskboard/taskboard/api/v1/tasks"
    assert req.headers["X-Orch-Session"] == str(SID)
    assert req.headers["X-Orch-Slug"] == "x"
    assert req.headers[PROJECTS_HEADER] == "p1,p2"
    assert req.headers["Authorization"] == "Bearer " + TOKEN
    assert json.loads(req.content) == {
        "title": "T",
        "body": "B",
        "labels": ["runaway-guard"],
        "client_key": key,
    }


async def test_existing_task_200_is_success():
    assert await post(settings(), lambda r: httpx.Response(200, json={})) is True


async def test_no_taskboard_is_false_without_call():
    def handler(request):
        raise AssertionError("no call expected")

    assert await post(settings(taskboard_url="", taskboard_token=""), handler) is False


async def test_failure_logged_without_token(caplog):
    caplog.set_level(logging.DEBUG)
    assert await post(settings(), lambda r: httpx.Response(503, text=TOKEN)) is False
    assert "status 503" in caplog.text
    assert TOKEN not in caplog.text


async def test_transport_error_is_false(caplog):
    def handler(request):
        raise httpx.ConnectError("boom " + TOKEN)

    assert await post(settings(), handler) is False
    assert "ConnectError" in caplog.text and TOKEN not in caplog.text


async def _parked(pool):
    s = await insert_session(pool, slug="x-1")
    await guarded_agent(pool, s["id"], turns=301)
    assert await sweep_park(pool) == [s["id"]]
    return s


async def _notify(pool, cfg) -> None:
    async with pool.connection() as conn:
        await autoclose._notify_parked(conn, cfg)


async def test_notify_parked_marks_once_and_retries(pool, monkeypatch):
    s = await _parked(pool)
    statuses = [503, 201]
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(statuses.pop(0) if statuses else 500, json={})

    cfg = settings()
    real = httpx.AsyncClient

    class Client(real):
        def __init__(self, **kw):
            super().__init__(**{**kw, "transport": httpx.MockTransport(handler)})

    monkeypatch.setattr(taskboard_client.httpx, "AsyncClient", Client)
    await _notify(pool, cfg)
    assert (await session_row(pool, s["id"]))["park_notified_at"] is None
    await _notify(pool, cfg)
    row = await session_row(pool, s["id"])
    assert row["park_notified_at"] is not None
    n = len(requests)
    await _notify(pool, cfg)
    assert len(requests) == n == 2
    body = requests[1].content.decode()
    assert "runaway-guard" in body and str(s["id"]) in body
    assert requests[1].headers[PROJECTS_HEADER] == "default"
    assert WINDOW.isoformat() in body


async def test_no_taskboard_no_call(pool):
    s = await _parked(pool)
    await _notify(pool, settings(taskboard_url="", taskboard_token=""))
    assert (await session_row(pool, s["id"]))["park_notified_at"] is None
    assert await rows(pool, "SELECT 1 FROM sessions WHERE id = %s", (s["id"],))


async def test_unreadable_detail_gets_generic_notice_once(pool, monkeypatch):
    s = await _parked(pool)
    async with pool.connection() as conn:
        await conn.execute(
            'UPDATE sessions SET parked_detail = \'{"junk": "RAW-INPUT"}\'::jsonb WHERE id = %s',
            (s["id"],),
        )
    requests: list[httpx.Request] = []

    def handler(request):
        requests.append(request)
        return httpx.Response(201, json={})

    real = httpx.AsyncClient

    class Client(real):
        def __init__(self, **kw):
            super().__init__(**{**kw, "transport": httpx.MockTransport(handler)})

    monkeypatch.setattr(taskboard_client.httpx, "AsyncClient", Client)
    cfg = settings()
    await _notify(pool, cfg)
    await _notify(pool, cfg)
    assert len(requests) == 1
    assert "RAW-INPUT" not in requests[0].content.decode()
    assert (await session_row(pool, s["id"]))["park_notified_at"] is not None
