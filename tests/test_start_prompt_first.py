"""The preset start prompt is the session's first turn (T-63).

Session create queues it as its own message (`carries_start_prompt`); the dispatcher claims
nothing else of the session until that message is sent, failed or cancelled.
"""

from __future__ import annotations

import asyncio
import uuid

import psycopg
from factories import insert_message, insert_session
from psycopg.rows import dict_row
from test_api_sessions import assert_error, client_for, make_settings
from test_api_usage_gate import Upstream, gated, windows
from test_dispatch_queue import _call, _claim, _ready_agent, _set_free
from test_service_port import TOKENS
from test_service_port import client as service_client

from agents_orchestrator import rendering
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import queue as q

C = "/api/v1/sessions"
P = "/api/v1/presets"


async def _rows(pool, session_id) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT * FROM messages WHERE session_id = %s ORDER BY id", (session_id,))
        return list(await cur.fetchall())


async def _preset(http, name: str, spec: dict) -> str:
    r = await http.post(P, json={"name": name, "spec": spec})
    assert r.status_code == 201, r.text
    return r.json()["id"]


async def _create(http, spec: dict, slug: str = "sp") -> dict:
    pid = await _preset(http, f"p-{slug}", spec)
    r = await http.post(C, json={"preset_id": pid, "slug": slug})
    assert r.status_code == 201, r.text
    return r.json()


# ---------------------------------------------------------------- create queues the prompt


async def test_create_queues_the_rendered_start_prompt_as_its_own_message(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        session = await _create(http, {"start_prompt": "You are on {{ session_slug }}."})
    [row] = await _rows(pool, session["id"])
    assert row["text"] == "You are on sp."
    assert row["carries_start_prompt"] is True
    assert row["status"] == "queued"
    assert row["lane"] == "regular"
    assert row["target_agent_id"] is None
    assert row["sender_session_id"] is None and row["notice"] is None


async def test_create_from_a_raw_spec_queues_the_start_prompt(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.post(C, json={"slug": "raw", "spec": {"start_prompt": "go"}})
        assert r.status_code == 201, r.text
    assert [m["text"] for m in await _rows(pool, r.json()["id"])] == ["go"]


async def test_create_without_start_prompt_or_with_an_empty_render_queues_nothing(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        plain = await _create(http, {}, slug="plain")
        empty = await _create(http, {"start_prompt": "{% if false %}x{% endif %}"}, slug="empty")
    assert await _rows(pool, plain["id"]) == []
    assert await _rows(pool, empty["id"]) == []


async def test_start_prompt_user_is_the_creating_service_or_empty(pool, dsn):
    app = create_app(make_settings(dsn, service_tokens=TOKENS), pool)
    async with client_for(app) as http:
        pid = await _preset(http, "who", {"start_prompt": "[{{ user }}]"})
        ui = await http.post(C, json={"preset_id": pid, "slug": "ui"})
    async with service_client(app, 8002) as svc:
        wh = await svc.post(C, json={"preset_id": pid, "slug": "wh"})
    assert [m["text"] for m in await _rows(pool, ui.json()["id"])] == ["[]"]
    assert [m["text"] for m in await _rows(pool, wh.json()["id"])] == ["[agents-webhook]"]


async def test_start_prompt_render_failure_refuses_the_create(pool, dsn, monkeypatch):
    from agents_common.templating import TemplateError

    def boom(template, context):
        raise TemplateError("bad template")

    async with client_for(create_app(make_settings(dsn), pool)) as http:
        pid = await _preset(http, "bad", {"start_prompt": "x"})
        # Spec validation passes (its trial render is stubbed), so the 422 comes from the
        # render in _queue_start_prompt inside the create transaction.
        monkeypatch.setattr(rendering, "problem", lambda template, surface, env: (None, None))
        monkeypatch.setattr(rendering, "render_text", boom)
        r = await http.post(C, json={"preset_id": pid, "slug": "bad"})
    assert_error(r, 422, "invalid_request")
    assert not r.json()["error"]["message"].startswith("preset ")
    assert "start_prompt" in r.json()["error"]["message"]
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM sessions WHERE slug = 'bad'")
        assert (await cur.fetchone())[0] == 0
        cur = await conn.execute("SELECT count(*) FROM messages")
        assert (await cur.fetchone())[0] == 0


async def test_usage_gate_refusal_leaves_no_start_prompt(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=85.0)))
    async with client_for(app) as http:
        pid = await _preset(
            http, "capped", {"usage_quota": {"seven_day_pct": 80}, "start_prompt": "hi"}
        )
        r = await http.post(C, json={"preset_id": pid, "slug": "over"})
    assert_error(r, 409, "usage_quota")
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM messages")
        assert (await cur.fetchone())[0] == 0


# ---------------------------------------------------------------- posts are never prefixed


async def test_service_first_post_is_the_second_message_and_the_prompt_is_sent_once(pool, dsn):
    # agents-webhook, agents-schedule and the Matrix connector create, then post their first
    # event through the service port: the event is turn 2 and the start prompt appears once.
    app = create_app(make_settings(dsn, service_tokens=TOKENS), pool)
    async with client_for(app) as http:
        pid = await _preset(http, "evt", {"start_prompt": "preset start"})
    preamble = "You are chatting through Matrix."
    async with service_client(app, 8002) as svc:
        session = (await svc.post(C, json={"preset_id": pid, "slug": "evt"})).json()
        r = await svc.post(f"{C}/{session['id']}/messages", json={"text": f"{preamble}\n\nhi"})
        assert r.status_code == 202, r.text
        await svc.post(f"{C}/{session['id']}/messages", json={"text": "again"})
    rows = await _rows(pool, session["id"])
    assert [m["text"] for m in rows] == ["preset start", f"{preamble}\n\nhi", "again"]
    assert [m["carries_start_prompt"] for m in rows] == [True, False, False]
    assert sum(m["text"].count("preset start") for m in rows) == 1


async def test_post_keeps_its_lane_and_attachments_unprefixed(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        session = await _create(http, {"start_prompt": "ctx"})
        r = await http.post(
            f"{C}/{session['id']}/messages",
            json={
                "text": "go",
                "lane": "priority",
                "attachments": [{"name": "a.txt", "mime": "text/plain", "data_b64": "aGk="}],
            },
        )
        assert r.status_code == 202, r.text
        got = (await http.get(f"/api/v1/messages/{r.json()['message_id']}")).json()
    assert got["text"] == "go"
    assert got["lane"] == "priority"
    assert [a["name"] for a in got["attachments"]] == ["a.txt"]


# ---------------------------------------------------------------- the dispatch gate


async def _carrier(pool, session_id, **kw) -> int:
    kw.setdefault("lane", "regular")
    return (await insert_message(pool, session_id, text="start", carries_start_prompt=True, **kw))[
        "id"
    ]


async def _others(pool, session_id, other_agent_id) -> list[int]:
    """One message of every kind that could overtake the start prompt."""
    async with pool.connection() as conn:
        peer = await q.enqueue_message(
            conn, session_id, "peer", sender_session_id=uuid.uuid4(), sender_slug="boss"
        )
        idle = await q.enqueue_message(conn, session_id, "idle", lane="idle")
        prio = await q.enqueue_message(conn, session_id, "prio", lane="priority")
        targeted = await q.enqueue_message(conn, session_id, "to-a1", other_agent_id)
        notice = await q.enqueue_message(
            conn, session_id, "handover", other_agent_id, notice="handover"
        )
        reminder = await q.enqueue_message(
            conn, session_id, "remind", lane="regular", notice="inactivity_reminder"
        )
    return [peer, idle, prio, targeted, notice, reminder]


async def _drain(pool) -> list[int]:
    """Claim and send until nothing is claimable; the claimed ids in order."""
    order = []
    while (claim := await _claim(pool)) is not None:
        order.append(claim.message_id)
        await _call(pool, q.mark_sent, claim.message_id, f"t{claim.message_id}")
        await _set_free(pool, claim.agent_id)
    return order


async def test_carrier_is_claimed_before_every_other_message_of_any_lane(pool):
    sid = (await insert_session(pool))["id"]
    a0 = await _ready_agent(pool, sid, free_ago=5)
    a1 = await _ready_agent(pool, sid, free_ago=4)
    await _ready_agent(pool, sid, free_ago=3)
    others = await _others(pool, sid, a1["id"])
    carrier = await _carrier(pool, sid)  # queued after the others: still first
    first = await _claim(pool)
    assert first is not None and first.message_id == carrier
    assert first.agent_id == a0["id"]
    for _ in range(3):  # free agents everywhere, the gate holds them all while dispatching
        assert await _claim(pool) is None
    assert await _call(pool, q.mark_sent, carrier, "t0")
    await _set_free(pool, a0["id"])
    assert sorted(await _drain(pool)) == sorted(others)


async def test_carrier_routes_to_the_leader_and_holds_a_free_non_leader(pool):
    sid = (await insert_session(pool))["id"]
    leader = await _ready_agent(pool, sid, free_ago=1)
    other = await _ready_agent(pool, sid, free_ago=9)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 1, now(), 'start')",
            (sid, leader["id"]),
        )
        await conn.execute(
            "UPDATE agents SET wrapper_session_id = NULL, ready = false WHERE id = %s",
            (leader["id"],),
        )  # the leader is still booting
        targeted = await q.enqueue_message(conn, sid, "to other", other["id"])
    carrier = await _carrier(pool, sid)
    assert await _claim(pool) is None  # targeted at a free non-leader, still held (D23 d off)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_session_id = 'w', ready = true WHERE id = %s",
            (leader["id"],),
        )
    claim = await _claim(pool)
    assert claim is not None and (claim.message_id, claim.agent_id) == (carrier, leader["id"])
    assert await _call(pool, q.mark_sent, carrier, "t0")
    claim = await _claim(pool)
    assert claim is not None and claim.message_id == targeted


async def test_verifying_carrier_holds_the_session(pool):
    sid = (await insert_session(pool))["id"]
    await _ready_agent(pool, sid)
    carrier = await _carrier(pool, sid, status="verifying")
    await insert_message(pool, sid, text="later", lane="priority")
    assert await _claim(pool) is None
    async with pool.connection() as conn:
        await conn.execute("UPDATE messages SET status = 'sent' WHERE id = %s", (carrier,))
    claim = await _claim(pool)
    assert claim is not None and claim.message_id != carrier


async def test_carrier_in_backoff_holds_the_session(pool):
    sid = (await insert_session(pool))["id"]
    await _ready_agent(pool, sid)
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT now() + interval '1 hour'")
        later = (await cur.fetchone())[0]
    await _carrier(pool, sid, not_before=later)
    await insert_message(pool, sid, text="later", lane="priority")
    assert await _claim(pool) is None


async def test_failed_or_cancelled_carrier_releases_and_a_retry_holds_again(pool):
    sid = (await insert_session(pool))["id"]
    await _ready_agent(pool, sid, free_ago=2)
    await _ready_agent(pool, sid, free_ago=1)
    carrier = await _carrier(pool, sid, status="failed", error="rejected")
    later = (await insert_message(pool, sid, text="later", lane="regular"))["id"]
    late2 = (await insert_message(pool, sid, text="late2", lane="regular"))["id"]
    claim = await _claim(pool)
    assert claim is not None and claim.message_id == later
    assert await _call(pool, q.mark_sent, later, "t1")
    assert await _call(pool, q.retry_message, carrier)
    claim = await _claim(pool)  # the retried carrier closes the gate again and goes first
    assert claim is not None and claim.message_id == carrier
    assert await _claim(pool) is None
    assert await _call(pool, q.mark_sent, carrier, "t0")
    claim = await _claim(pool)
    assert claim is not None and claim.message_id == late2

    sid2 = (await insert_session(pool))["id"]
    await _ready_agent(pool, sid2)
    cancelled = await _carrier(pool, sid2)
    other = (await insert_message(pool, sid2, text="other", lane="priority"))["id"]
    assert await _call(pool, q.cancel_message, cancelled)
    claim = await _claim(pool)
    assert claim is not None and claim.message_id == other


async def test_gate_is_per_session(pool):
    gated_sid = (await insert_session(pool))["id"]
    free_sid = (await insert_session(pool))["id"]
    await _ready_agent(pool, free_sid)
    await _carrier(pool, gated_sid)  # no agent yet: the carrier waits, the session is held
    await insert_message(pool, gated_sid, text="held", lane="priority")
    plain = (await insert_message(pool, free_sid, text="plain", lane="regular"))["id"]
    claim = await _claim(pool)
    assert claim is not None and claim.message_id == plain
    assert await _claim(pool) is None


async def test_concurrent_dispatchers_claim_only_the_carrier_first(pool, dsn):
    sid = (await insert_session(pool))["id"]
    agents = [await _ready_agent(pool, sid, free_ago=i) for i in range(6)]
    others = await _others(pool, sid, agents[1]["id"])
    carrier = await _carrier(pool, sid)
    conns = [await psycopg.AsyncConnection.connect(dsn, autocommit=True) for _ in range(6)]
    try:
        claims = await asyncio.gather(*(q.claim_and_pick(c) for c in conns))
        got = [c for c in claims if c is not None]
        assert [c.message_id for c in got] == [carrier]
        claims = await asyncio.gather(*(q.claim_and_pick(c) for c in conns))
        assert claims == [None] * len(conns)  # dispatching: still held on every dispatcher
        assert await q.mark_sent(conns[0], carrier, "t0")
    finally:
        for c in conns:
            await c.close()
    assert sorted(await _drain(pool)) == sorted(others)


async def test_peer_message_right_after_create_waits_for_the_start_prompt(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        session = await _create(http, {"start_prompt": "start prompt marker"})
    sid = uuid.UUID(session["id"])
    async with pool.connection() as conn:
        peer = await q.enqueue_message(
            conn, sid, "peer first", sender_session_id=uuid.uuid4(), sender_slug="boss"
        )
    await _ready_agent(pool, sid)
    order = await _drain(pool)
    texts = {m["id"]: m["text"] for m in await _rows(pool, sid)}
    assert [texts[i] for i in order] == ["start prompt marker", "peer first"]
    assert order[1] == peer
