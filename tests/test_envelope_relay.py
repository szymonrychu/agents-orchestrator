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


async def _ids(pool):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT id FROM envelope_outbox ORDER BY id")
        return [r[0] for r in await cur.fetchall()]


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
    rid = (await _ids(pool))[0]
    assert await _refused_messages(pool, s["id"]) == [
        (
            f'<message-in channel="notice" from="{s["slug"]}" for="a{rid}" status="refused"'
            ' reason="room_not_allowed"/>\n',
            a["id"],
            None,
        )
    ]


async def test_refusal_during_the_running_turn_is_left_to_the_stop_hook(pool, dsn):
    s, a = await _seed(pool)
    await insert_turn(pool, a["id"], turn_id="t1")
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    await _insert(pool, _row(s, a, 1, turn_id="t1"))
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    await _worker(pool, dsn, Upstream(httpx.Response(403, json=err))).drain()
    assert await _refused_messages(pool, s["id"]) == []
    assert (await _rows(pool))[0][1] == "refused"


async def test_refusal_after_allowed_stop_before_turn_completed_tells_the_session(pool, dsn):
    s, a = await _seed(pool)
    await insert_turn(pool, a["id"], turn_id="t1")
    await set_agent_status(pool, a["id"], state="busy", current_turn_id="t1")
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO envelope_stop_checks (agent_id, turn_id, allowed_at)"
            " VALUES (%s, 't1', now())",
            (a["id"],),
        )
    await _insert(pool, _row(s, a, 2, turn_id="t1"))
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    await _worker(pool, dsn, Upstream(httpx.Response(403, json=err))).drain()
    msgs = await _refused_messages(pool, s["id"])
    rid = (await _ids(pool))[0]
    assert len(msgs) == 1 and f'for="a{rid}" status="refused"' in msgs[0][0]


async def test_refusal_and_session_message_are_one_transaction(pool, dsn, monkeypatch):
    from agents_orchestrator.db import queue

    s, a = await _seed(pool)
    await insert_turn(pool, a["id"], turn_id="t1", status="completed")
    async with pool.connection() as conn:
        await conn.execute("UPDATE turns SET completed_at = now() WHERE turn_id = 't1'")
    await _insert(pool, _row(s, a, 1, turn_id="t1"))

    async def boom(*_a, **_k):
        raise RuntimeError("db down")

    monkeypatch.setattr(queue, "enqueue_message", boom)
    err = {"error": {"code": "room_not_allowed", "message": "no"}}
    await _worker(pool, dsn, Upstream(httpx.Response(403, json=err))).drain()
    # the refused mark rolled back with the failed enqueue; the row is settled once, as failed
    assert (await _rows(pool))[0][1:3] == ("failed", "unknown_outcome")
    assert await _refused_messages(pool, s["id"]) == []


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


async def test_drain_sweeps_stale_sending_on_every_pass(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1), _row(s, a, 2))
    async with pool.connection() as conn:
        sent = await outbox.claim_next(conn, s["id"])
        await conn.execute(
            "UPDATE envelope_outbox SET updated_at = now() - interval '5 seconds' WHERE id = %s",
            (sent["id"],),
        )
    up = Upstream()
    w = _worker(pool, dsn, up)
    assert not await w.drain()  # 5 s old: still sending, blocks the session, not swept
    assert [r[1] for r in await _rows(pool)] == ["sending", "queued"]
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE envelope_outbox SET updated_at = now() - interval '2 minutes' WHERE id = %s",
            (sent["id"],),
        )
    assert await w.drain()  # aged past 60 s: swept, the next row goes out
    assert [r[1] for r in await _rows(pool)] == ["failed", "accepted"]
    assert len(up.calls) == 1


# ------------------------------------------------------------------ hive kinds (S-71)


def _plain_settings(dsn: str, **kw: Any) -> Settings:
    """No chat upstream: hive kinds settle anyway."""
    return Settings(database_url=dsn, master_key=MASTER_KEY, **kw)


async def _hive_worker(pool, dsn):
    return EnvelopeRelay(pool, _plain_settings(dsn), httpx.AsyncClient(), clock=Clock())


async def _messages(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT id, text, sender_session_id, hop, lane, envelope_ref, project_post_id"
            " FROM messages WHERE session_id = %s ORDER BY id",
            (session_id,),
        )
        return await cur.fetchall()


async def test_ingester_relay_settles_session_envelope_without_chat(pool, dsn):
    from agents_orchestrator.ingester import Ingester

    s, a = await _seed(pool)
    target = await insert_session(pool)
    await _insert(
        pool, _row(s, a, 1, kind="session", target=target["slug"], body="hi", lane="idle")
    )
    ing = Ingester(pool, _plain_settings(dsn, ingest_refresh_seconds=0.05))
    stop = asyncio.Event()
    task = asyncio.create_task(ing.run(stop))
    for _ in range(100):
        if (await _rows(pool))[0][1] == "accepted":
            break
        await asyncio.sleep(0.05)
    stop.set()
    await asyncio.wait_for(task, 5)
    rid = (await _ids(pool))[0]
    [msg] = await _messages(pool, target["id"])
    assert msg[1:] == ("hi", s["id"], 1, "idle", f"a{rid}", None)
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT status, message_id FROM envelope_outbox")
        assert await cur.fetchall() == [("accepted", msg[0])]


async def test_project_post_becomes_a_room_post(pool, dsn):
    async with pool.connection() as conn:
        await conn.execute("INSERT INTO projects (slug, name) VALUES ('a', 'A')")
    s = await insert_session(pool, project="a")
    a = await insert_agent(pool, s["id"])
    peer = await insert_session(pool, project="a")
    await _insert(pool, _row(s, a, 1, kind="project_post", body="news", target=peer["slug"]))
    assert await (await _hive_worker(pool, dsn)).drain()
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT id, project, text, sender_session_id FROM project_posts")
        [(post_id, project, text, sender)] = await cur.fetchall()
    assert (project, text, sender) == ("a", "news", s["id"])
    [msg] = await _messages(pool, peer["id"])
    assert msg[6] == post_id
    assert (await _rows(pool))[0][:4] == (1, "accepted", None, f"p{post_id}")


async def test_refused_core_writes_no_message(pool, dsn):
    s, a = await _seed(pool)
    await _insert(pool, _row(s, a, 1, kind="session", target="nobody"))
    assert await (await _hive_worker(pool, dsn)).drain()
    assert (await _rows(pool))[0][1:3] == ("refused", "not_found")
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM messages")
        assert (await cur.fetchone())[0] == 0


async def test_late_hop_limit_refusal_sends_a_notice_frame(pool, dsn):
    s, a = await _seed(pool)
    origin = await insert_session(pool)
    target = await insert_session(pool)
    await insert_turn(pool, a["id"], turn_id="t1", status="completed")
    async with pool.connection() as conn:
        await conn.execute("UPDATE turns SET completed_at = now() WHERE turn_id = 't1'")
        await conn.execute(
            "INSERT INTO messages (session_id, text, status, turn_id, sender_session_id, hop)"
            " VALUES (%s, 'in', 'sent', 't1', %s, %s)",
            (s["id"], origin["id"], _plain_settings("x").session_message_max_hops),
        )
    await _insert(pool, _row(s, a, 1, kind="session", target=target["slug"], turn_id="t1"))
    assert await (await _hive_worker(pool, dsn)).drain()
    rid = (await _ids(pool))[0]
    assert (await _rows(pool))[0][1:3] == ("refused", "hop_limit")
    assert await _messages(pool, target["id"]) == []
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT text, target_agent_id, lane FROM messages WHERE sender_session_id IS NULL"
        )
        assert await cur.fetchall() == [
            (
                f'<message-in channel="notice" from="{s["slug"]}" for="a{rid}"'
                ' status="refused" reason="hop_limit"/>\n',
                a["id"],
                "regular",
            )
        ]


async def test_stale_row_of_an_ended_turn_sends_a_failed_notice(pool, dsn):
    s, a = await _seed(pool)
    await insert_turn(pool, a["id"], turn_id="t1", status="completed")
    async with pool.connection() as conn:
        await conn.execute("UPDATE turns SET completed_at = now() WHERE turn_id = 't1'")
    await _insert(pool, _row(s, a, 1, turn_id="t1"))
    async with pool.connection() as conn:
        await outbox.claim_next(conn, s["id"])
        await conn.execute("UPDATE envelope_outbox SET updated_at = now() - interval '5 minutes'")
    await (await _hive_worker(pool, dsn)).drain()
    rid = (await _ids(pool))[0]
    assert [m[0] for m in await _refused_messages(pool, s["id"])] == [
        f'<message-in channel="notice" from="{s["slug"]}" for="a{rid}" status="failed"'
        ' reason="unknown_outcome"/>\n'
    ]
