"""Event paging, the per-session event stream and the status stream (D11, D14)."""

from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator

import httpx
import psycopg
import pytest
import pytest_asyncio
import uvicorn
from factories import insert_agent, insert_message, insert_session, set_agent_status
from test_api_sessions import assert_error, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.listener import Listener
from agents_orchestrator.db import events as ev
from agents_orchestrator.db.pool import notify


class Served:
    def __init__(self, app, base_url: str):
        self.app = app
        self.base_url = base_url

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base_url, timeout=15)


@contextlib.asynccontextmanager
async def serve(app) -> AsyncIterator[Served]:
    """Real uvicorn on an ephemeral port: ASGITransport cannot stream incrementally."""
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    )
    task = asyncio.create_task(server.serve())
    while not server.started:
        await asyncio.sleep(0.01)
    port = server.servers[0].sockets[0].getsockname()[1]
    try:
        yield Served(app, f"http://127.0.0.1:{port}")
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 10)


@pytest_asyncio.fixture
async def srv(pool, dsn):
    app = create_app(make_settings(dsn, allowed_cidrs="127.0.0.0/8"), pool, stream_poll_seconds=0.3)
    async with serve(app) as s:
        yield s


class SSE:
    """Incremental SSE reader over an httpx streaming response."""

    def __init__(self, resp: httpx.Response):
        self.resp = resp
        self._lines = resp.aiter_lines()

    async def next(self, timeout: float = 10) -> dict:
        async def _read() -> dict:
            frame: dict = {}
            async for line in self._lines:
                if line == "":
                    if frame:
                        return frame
                    continue
                if line.startswith(":"):
                    continue
                key, _, value = line.partition(":")
                value = value.removeprefix(" ")
                if key == "data":
                    frame["data"] = json.loads(value)
                else:
                    frame[key] = value
            raise AssertionError("stream ended")

        return await asyncio.wait_for(_read(), timeout)


@contextlib.asynccontextmanager
async def open_sse(srv: Served, path: str, **kw) -> AsyncIterator[SSE]:
    async with srv.client() as c, c.stream("GET", path, **kw) as resp:
        assert resp.status_code == 200, await resp.aread()
        yield SSE(resp)


async def _seed(pool, n: int, **kw):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"])
    async with pool.connection() as conn:
        for i in range(1, n + 1):
            await ev.insert_events(
                conn,
                cl["id"],
                ag["id"],
                "s1",
                [{"seq": i, "ts": None, "type": "assistant_text", "data": {"text": f"m{i}"}}],
            )
    return cl, ag


def _ids(events) -> list[int]:
    return [e["id"] for e in events]


async def test_events_page_backwards(srv, pool):
    cl, _ = await _seed(pool, 7)
    async with srv.client() as c:
        url = f"/api/v1/sessions/{cl['id']}/events"
        newest = (await c.get(url, params={"limit": 3})).json()
        assert len(newest["events"]) == 3
        assert newest["has_more_before"] is True
        assert newest["next_after_id"] == newest["events"][-1]["id"]
        assert newest["events"][0]["data"] == {"text": "m5"}
        older = (
            await c.get(url, params={"limit": 3, "before_id": newest["events"][0]["id"]})
        ).json()
        assert [e["data"]["text"] for e in older["events"]] == ["m2", "m3", "m4"]
        assert older["has_more_before"] is True
        oldest = (
            await c.get(url, params={"limit": 3, "before_id": older["events"][0]["id"]})
        ).json()
        assert [e["data"]["text"] for e in oldest["events"]] == ["m1"]
        assert oldest["has_more_before"] is False
        fwd = (await c.get(url, params={"after_id": newest["events"][0]["id"]})).json()
        assert [e["data"]["text"] for e in fwd["events"]] == ["m6", "m7"]
        flt = (await c.get(url, params={"types": "nothing,assistant_text", "limit": 2})).json()
        assert len(flt["events"]) == 2
        none = (await c.get(url, params={"types": "nothing"})).json()
        assert none["events"] == []
        assert_error(await c.get(f"/api/v1/sessions/{cl['id']}x/events"), 422, "invalid_request")


async def test_event_view_wrapper_session_id(srv, pool):
    cl, _ = await _seed(pool, 1)
    async with srv.client() as c:
        page = (await c.get(f"/api/v1/sessions/{cl['id']}/events")).json()
    [event] = page["events"]
    assert event["wrapper_session_id"] == "s1"
    assert event["session_id"] == str(cl["id"])


async def test_events_page_rejects_both_cursors(srv, pool):
    cl, _ = await _seed(pool, 2)
    async with srv.client() as c:
        r = await c.get(
            f"/api/v1/sessions/{cl['id']}/events", params={"before_id": 5, "after_id": 1}
        )
        assert_error(r, 400, "bad_request")
        r = await c.get(
            f"/api/v1/sessions/{cl['id']}/events/stream", headers={"Last-Event-ID": "x"}
        )
        assert_error(r, 400, "bad_request")
        r = await c.get("/api/v1/sessions/00000000-0000-0000-0000-000000000000/events/stream")
        assert_error(r, 404, "not_found")


async def test_events_stream_live_append(srv, pool):
    cl, ag = await _seed(pool, 3)
    async with open_sse(srv, f"/api/v1/sessions/{cl['id']}/events/stream") as sse:
        # starts at the newest id: the 3 stored rows are not replayed
        await asyncio.sleep(0.2)
        async with pool.connection() as conn:
            new = await ev.insert_events(
                conn,
                cl["id"],
                ag["id"],
                "s1",
                [
                    {"seq": 4, "ts": None, "type": "tool_call", "data": {"tool_name": "Bash"}},
                    {"seq": 5, "ts": None, "type": "assistant_text", "data": {"text": "x"}},
                ],
            )
        a = await sse.next()
        b = await sse.next()
        assert (a["event"], b["event"]) == ("tool_call", "assistant_text")
        assert int(b["id"]) == new
        assert int(a["id"]) == new - 1
        assert a["data"]["id"] == int(a["id"])
        assert a["data"]["data"] == {"tool_name": "Bash"}
        assert a["data"]["agent_id"] == str(ag["id"])


async def test_events_stream_resumes_from_last_event_id(srv, pool):
    cl, ag = await _seed(pool, 5)
    async with srv.client() as c:
        stored = (await c.get(f"/api/v1/sessions/{cl['id']}/events")).json()["events"]
    ids = _ids(stored)
    async with open_sse(
        srv, f"/api/v1/sessions/{cl['id']}/events/stream", headers={"Last-Event-ID": str(ids[1])}
    ) as sse:
        got = [int((await sse.next())["id"]) for _ in range(3)]
        assert got == ids[2:]
        async with pool.connection() as conn:
            await ev.insert_events(
                conn, cl["id"], ag["id"], "s1", [{"seq": 6, "type": "idle", "data": {}}]
            )
        nxt = await sse.next()
        assert int(nxt["id"]) == ids[-1] + 1
    async with open_sse(
        srv, f"/api/v1/sessions/{cl['id']}/events/stream", params={"after_id": ids[3]}
    ) as sse:
        assert [int((await sse.next())["id"]) for _ in range(2)] == [ids[4], ids[4] + 1]


async def test_status_stream_snapshot_and_delta(srv, pool):
    cl = await insert_session(pool)
    other = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"], ready=True)
    await set_agent_status(pool, ag["id"], state="idle")
    async with open_sse(srv, "/api/v1/stream") as sse:
        snap = await sse.next()
        assert snap["event"] == "snapshot"
        by_id = {x["session"]["id"]: x for x in snap["data"]["sessions"]}
        assert set(by_id) == {str(cl["id"]), str(other["id"])}
        assert [a["id"] for a in by_id[str(cl["id"])]["agents"]] == [str(ag["id"])]
        assert by_id[str(cl["id"])]["agents"][0]["state"] == "idle"

        await set_agent_status(pool, ag["id"], state="busy", current_turn_id="t1")
        await insert_message(pool, cl["id"])
        async with pool.connection() as conn:
            await notify(conn, "status", {"session_id": str(cl["id"])})
        seen = {}
        for _ in range(3):
            f = await sse.next()
            seen[f["event"]] = f["data"]
        assert set(seen) == {"session_status", "agent_status", "message_status"}
        assert seen["session_status"]["session"]["id"] == str(cl["id"])
        assert seen["session_status"]["session"]["summary"]["state"] == "working"
        assert seen["agent_status"]["session_id"] == str(cl["id"])
        assert seen["agent_status"]["agent"]["state"] == "busy"
        ms = seen["message_status"]
        assert ms["queued"] == 1 and ms["failed"] == 0
        assert [m["status"] for m in ms["messages"]] == ["queued"]


async def test_status_stream_carries_attention(srv, pool):
    cl = await insert_session(pool)
    ag = await insert_agent(pool, cl["id"], ready=True)
    await set_agent_status(pool, ag["id"], state="idle")
    async with open_sse(srv, "/api/v1/stream") as sse:
        snap = await sse.next()
        agent = snap["data"]["sessions"][0]["agents"][0]
        assert agent["attention"] is None and agent["bootstrap"] is None
        async with pool.connection() as conn:
            await conn.execute(
                "UPDATE agent_status SET waiting_reason='permission',"
                " waiting_since='2026-11-01T10:01:00+00:00' WHERE agent_id=%s",
                (ag["id"],),
            )
            await notify(conn, "status", {"session_id": str(cl["id"])})
        seen = {}
        for _ in range(2):
            f = await sse.next()
            seen[f["event"]] = f["data"]
        assert seen["agent_status"]["agent"]["attention"]["reason"] == "permission"
        assert seen["session_status"]["session"]["summary"]["attention_agents"] == 1


async def test_stream_recovers_from_lost_notify(srv, pool, dsn):
    cl, ag = await _seed(pool, 1)
    ag2 = await insert_agent(pool, cl["id"], ready=True)
    await set_agent_status(pool, ag2["id"], state="idle")
    async with open_sse(srv, "/api/v1/stream") as sse:
        assert (await sse.next())["event"] == "snapshot"
        # a change committed without any NOTIFY
        await set_agent_status(pool, ag2["id"], state="busy", current_turn_id="t9")
        got = {}
        for _ in range(2):
            f = await sse.next()
            got[f["event"]] = f["data"]
        assert got["agent_status"]["agent"]["state"] == "busy"
    async with open_sse(srv, f"/api/v1/sessions/{cl['id']}/events/stream") as sse:
        await asyncio.sleep(0.2)
        # a row inserted without NOTIFY (plain INSERT, no pg_notify)
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await conn.execute(
                "INSERT INTO events (session_id, agent_id, wrapper_session_id, seq, type, data)"
                " VALUES (%s,%s,'s1',99,'idle','{}')",
                (cl["id"], ag["id"]),
            )
        f = await sse.next()
        assert f["event"] == "idle"


async def test_shared_listener_single_connection(dsn, admin_dsn):
    listener = Listener(dsn)
    await listener.start()
    try:
        subs = [listener.subscribe(["events"]) for _ in range(25)]
        async with await psycopg.AsyncConnection.connect(admin_dsn, autocommit=True) as conn:
            cur = await conn.execute(
                "SELECT count(*) FROM pg_stat_activity"
                " WHERE datname = %s AND query LIKE 'LISTEN%%'",
                (dsn.rsplit("/", 1)[1],),
            )
            assert (await cur.fetchone())[0] == 1
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            await notify(conn, "events", {"session_id": "c", "max_id": 1})
        for s in subs:
            assert await s.wait(5) is True
    finally:
        await listener.stop()


async def test_slow_subscriber_is_dropped(dsn):
    listener = Listener(dsn, queue_size=2)
    await listener.start()
    try:
        slow = listener.subscribe(["events"])
        fast = listener.subscribe(["events"])
        async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as conn:
            for i in range(6):
                await notify(conn, "events", {"session_id": "c", "max_id": i})
                await asyncio.sleep(0.05)
                assert await fast.wait(1)
                fast.drain()
        with pytest.raises(Exception, match="dropped"):
            for _ in range(10):
                await slow.wait(0.2)
        assert fast.channels == {"events"}
        assert listener.subscriber_count() == 1
    finally:
        await listener.stop()


async def test_sse_headers(srv, pool):
    cl, _ = await _seed(pool, 1)
    for path in (f"/api/v1/sessions/{cl['id']}/events/stream", "/api/v1/stream"):
        async with srv.client() as c, c.stream("GET", path) as resp:
            assert resp.status_code == 200
            assert resp.headers["x-accel-buffering"] == "no"
            assert resp.headers["cache-control"] == "no-cache"
            assert resp.headers["content-type"].startswith("text/event-stream")


async def test_disconnect_unsubscribes(srv, pool):
    cl, _ = await _seed(pool, 1)
    listener = srv.app.state.listener
    async with open_sse(srv, "/api/v1/stream") as sse:
        await sse.next()
        assert listener.subscriber_count() == 1
    for _ in range(50):
        if listener.subscriber_count() == 0:
            break
        await asyncio.sleep(0.1)
    assert listener.subscriber_count() == 0


async def test_events_stream_ignores_other_sessions_notify(pool, dsn, monkeypatch):
    from agents_orchestrator.api import streams

    calls = 0
    real_page = ev.page

    async def counting(*a, **kw):
        nonlocal calls
        calls += 1
        return await real_page(*a, **kw)

    monkeypatch.setattr(streams.ev, "page", counting)
    cl, ag = await _seed(pool, 1)
    app = create_app(make_settings(dsn, allowed_cidrs="127.0.0.0/8"), pool, stream_poll_seconds=30)
    async with serve(app) as s, open_sse(s, f"/api/v1/sessions/{cl['id']}/events/stream"):
        await asyncio.sleep(0.5)
        base = calls
        async with pool.connection() as conn:
            await notify(conn, "events", {"session_id": "not-this-one", "max_id": 1})
        await asyncio.sleep(0.5)
        assert calls == base
        async with pool.connection() as conn:
            await notify(conn, "events", {"session_id": str(cl["id"]), "max_id": 1})
        await asyncio.sleep(0.5)
        assert calls > base
