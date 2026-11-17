"""The envelope relay worker: queued chat envelopes to the chat upstream (S-70)."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import httpx
from factories import insert_agent, insert_session, insert_turn, set_agent_status

from agents_orchestrator.db import envelope_outbox as outbox
from agents_orchestrator.db.envelope_outbox import Row
from agents_orchestrator.envelope_outbox.worker import MAX_ATTEMPTS, NOTICE, EnvelopeRelay
from agents_orchestrator.settings import Settings

MASTER_KEY = "k" * 40


class Upstream:
    """A scripted chat upstream: `answers` are consumed per call, then `default`."""

    def __init__(self, *answers: Any, default: Any = None) -> None:
        self.answers = list(answers)
        self.default = default or httpx.Response(202, json={"id": "send-1"})
        self.calls: list[httpx.Request] = []

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        answer = self.answers.pop(0) if self.answers else self.default
        if isinstance(answer, Exception):
            raise answer
        return answer

    def bodies(self) -> list[dict[str, Any]]:
        return [json.loads(c.content) for c in self.calls]


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _settings(dsn: str, **kw: Any) -> Settings:
    return Settings(
        database_url=dsn,
        master_key=MASTER_KEY,
        chat_url="https://chat.test",
        chat_token="chat-bearer",
        **kw,
    )


def _worker(pool, dsn, upstream, clock=None, **kw):
    http = httpx.AsyncClient(transport=httpx.MockTransport(upstream.handler))
    return EnvelopeRelay(pool, _settings(dsn), http, clock=clock or Clock(), **kw)


async def _seed(pool):
    s = await insert_session(pool)
    a = await insert_agent(pool, s["id"])
    return s, a


def _row(s, a, idx=1, kind="send", seq=None, **kw):
    kw.setdefault("body", f"msg {idx}")
    return Row(
        session_id=s["id"],
        agent_id=a["id"],
        wrapper_session_id="w",
        event_seq=seq if seq is not None else idx,
        envelope_index=idx,
        kind=kind,
        **kw,
    )


async def _insert(pool, *rows):
    async with pool.connection() as conn:
        await outbox.insert_rows(conn, list(rows))


async def _rows(pool):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT envelope_index, status, error_code, send_id, attempts FROM envelope_outbox"
            " ORDER BY id"
        )
        return await cur.fetchall()


async def test_order_and_accepted_with_relay_accounting(pool, dsn):
    s, a = await _seed(pool)
    await _insert(
        pool,
        _row(s, a, 1, reply_to="m1", room="!r:x", files=("a.png",)),
        _row(s, a, 2, kind="react", body=None, reply_to="m1", react_key="ok"),
    )
    up = Upstream(httpx.Response(202, json={"id": "s-1"}), httpx.Response(202, json={"id": "s-2"}))
    w = _worker(pool, dsn, up)
    assert await w.drain()
    assert [str(c.url) for c in up.calls] == [
        "https://chat.test/chat/v1/messages",
        "https://chat.test/chat/v1/reactions",
    ]
    assert up.bodies() == [
        {"text": "msg 1", "reply_to": "m1", "files": ["a.png"], "room": "!r:x"},
        {"to": "m1", "key": "ok"},
    ]
    first = up.calls[0].headers
    assert first["authorization"] == "Bearer chat-bearer"
    assert first["x-orch-session"] == str(s["id"])
    assert first["x-orch-slug"] == s["slug"]
    assert first["x-orch-agent"] == str(a["id"])
    assert await _rows(pool) == [(1, "accepted", None, "s-1", 1), (2, "accepted", None, "s-2", 1)]
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM chat_activity WHERE session_id = %s AND direction = 'out'",
            (s["id"],),
        )
        assert (await cur.fetchone())[0] == 2
        cur = await conn.execute("SELECT last_update_at FROM sessions WHERE id = %s", (s["id"],))
        assert (await cur.fetchone())[0] is not None


async def test_notice_posts_fixed_text(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1, kind="notice", body=None, room="!r:x"))
    up = Upstream()
    await _worker(pool, dsn, up).drain()
    assert up.bodies() == [{"text": NOTICE, "room": "!r:x"}]


async def test_4xx_is_refused_with_code_and_the_next_row_goes_on(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1), _row(s, a, 2))
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    up = Upstream(httpx.Response(403, json=err))
    await _worker(pool, dsn, up).drain()
    rows = await _rows(pool)
    assert rows[0][:3] == (1, "refused", "room_not_allowed")
    assert rows[1][:2] == (2, "accepted")


async def test_429_requeues_with_retry_after_and_holds_the_session(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1), _row(s, a, 2))
    clock = Clock()
    up = Upstream(httpx.Response(429, headers={"Retry-After": "30"}, json={"error": {}}))
    w = _worker(pool, dsn, up, clock)
    await w.drain()
    assert [r[:2] for r in await _rows(pool)] == [(1, "queued"), (2, "queued")]
    assert len(up.calls) == 1
    clock.now += 29
    assert not await w.drain()  # still backing off: the second row does not overtake
    assert len(up.calls) == 1
    clock.now += 2
    await w.drain()
    assert [r[:2] for r in await _rows(pool)] == [(1, "accepted"), (2, "accepted")]
    assert up.bodies()[1]["text"] == "msg 1"


async def test_5xx_and_network_errors_retry_then_fail_after_five_attempts(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1), _row(s, a, 2))
    clock = Clock()
    up = Upstream(
        httpx.ConnectError("down"),
        httpx.Response(503, json={}),
        httpx.Response(500, json={}),
        httpx.Response(502, json={}),
        httpx.Response(504, json={}),
    )
    w = _worker(pool, dsn, up, clock)
    for _ in range(MAX_ATTEMPTS):
        await w.drain()
        clock.now += 1000
    rows = await _rows(pool)
    assert rows[0][1] == "failed" and rows[0][4] == MAX_ATTEMPTS
    assert len(up.calls) == MAX_ATTEMPTS + 1  # the failed row's five tries, then row 2
    assert rows[1][:2] == (2, "accepted")


async def test_unexpected_error_after_claim_is_unknown_outcome(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1))

    def boom(_request):
        raise RuntimeError("bug")

    w = EnvelopeRelay(pool, _settings(dsn), httpx.AsyncClient(transport=httpx.MockTransport(boom)))
    await w.drain()
    assert (await _rows(pool))[0][1:3] == ("failed", "unknown_outcome")


async def test_run_sweeps_stale_sending_wakes_on_notify_and_stops(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1))
    async with pool.connection() as conn:
        await outbox.claim_next(conn, s["id"])
        await conn.execute("UPDATE envelope_outbox SET updated_at = now() - interval '5 minutes'")
    up = Upstream()
    w = _worker(pool, dsn, up, poll_seconds=60)
    stop = asyncio.Event()
    task = asyncio.create_task(w.run(stop))
    await asyncio.sleep(0.5)
    assert (await _rows(pool))[0][1:3] == ("failed", "unknown_outcome")
    assert up.calls == []  # at most once: the stale row is never sent again
    await _insert(pool, _row(s, a, 2))  # NOTIFY wakes the worker; the poll is 60 s away
    for _ in range(100):
        if up.calls:
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert up.bodies() == [{"text": "msg 2"}]
    assert (await _rows(pool))[1][:2] == (2, "accepted")


async def test_room_bound_is_relayed_through_the_occupier(pool, dsn):
    s, a = await _seed(pool)
    occ = await insert_session(pool)
    await insert_agent(pool, occ["id"])
    await _insert(pool, _row(s, a, 1, room="!r:x"))
    conflict = {"error": {"code": "room_bound", "message": "x", "occupier": str(occ["id"])}}
    up = Upstream(httpx.Response(409, json=conflict), httpx.Response(202, json={"id": "via-1"}))
    await _worker(pool, dsn, up).drain()
    assert up.calls[1].headers["x-orch-session"] == str(occ["id"])
    assert up.bodies()[1] == {"text": f"[{s['slug']}] msg 1", "room": "!r:x"}
    assert (await _rows(pool))[0][:4] == (1, "accepted", None, "via-1")
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM messages WHERE session_id = %s AND chat_relay_send = 'via-1'",
            (occ["id"],),
        )
        assert (await cur.fetchone())[0] == 1


async def test_relay_refusal_files_not_relayable_is_refused(pool, dsn):
    s, a = await _seed(pool)
    occ = await insert_session(pool)
    await insert_agent(pool, occ["id"])
    await _insert(pool, _row(s, a, 1, room="!r:x", files=("a.png",)))
    conflict = {"error": {"code": "room_bound", "message": "x", "occupier": str(occ["id"])}}
    up = Upstream(httpx.Response(409, json=conflict))
    await _worker(pool, dsn, up).drain()
    assert (await _rows(pool))[0][1:3] == ("refused", "files_not_relayable")


async def _refused_messages(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT text, target_agent_id, sender_session_id FROM messages WHERE session_id = %s",
            (session_id,),
        )
        return await cur.fetchall()


async def test_refusal_after_turn_ended_tells_the_agent_session(pool, dsn):
    s, a = await _seed(pool)
    await insert_turn(pool, a["id"], turn_id="t1", status="completed")
    async with pool.connection() as conn:
        await conn.execute("UPDATE turns SET completed_at = now() WHERE turn_id = 't1'")
    await _insert(pool, _row(s, a, 3, turn_id="t1"))
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    await _worker(pool, dsn, Upstream(httpx.Response(403, json=err))).drain()
    msgs = await _refused_messages(pool, s["id"])
    assert len(msgs) == 1
    text, target, sender = msgs[0]
    assert "envelope 3" in text and "room_not_allowed" in text
    assert target == a["id"] and sender is None


async def test_refusal_during_the_running_turn_is_left_to_the_stop_hook(pool, dsn):
    s, a = await _seed(pool)
    await insert_turn(pool, a["id"], turn_id="t1")
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    await _insert(pool, _row(s, a, 1, turn_id="t1"))
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    await _worker(pool, dsn, Upstream(httpx.Response(403, json=err))).drain()
    assert await _refused_messages(pool, s["id"]) == []
    assert (await _rows(pool))[0][1] == "refused"


async def test_refusal_without_turn_id_sends_no_message(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1))
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    await _worker(pool, dsn, Upstream(httpx.Response(403, json=err))).drain()
    assert await _refused_messages(pool, s["id"]) == []


async def test_ingester_runs_the_relay_when_chat_is_configured(pool, dsn, monkeypatch):
    from agents_orchestrator.envelope_outbox import worker as worker_mod
    from agents_orchestrator.ingester import Ingester

    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1))
    up = Upstream()
    real = httpx.AsyncClient
    monkeypatch.setattr(
        worker_mod.forward,
        "new_client",
        lambda: real(transport=httpx.MockTransport(up.handler)),
    )
    ing = Ingester(pool, _settings(dsn, ingest_refresh_seconds=0.05))
    stop = asyncio.Event()
    task = asyncio.create_task(ing.run(stop))
    for _ in range(100):
        if up.calls:
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 5)
    assert up.bodies() == [{"text": "msg 1"}]
