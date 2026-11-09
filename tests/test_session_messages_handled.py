"""The `handled` list on session_send and leadership handover: the leader marks incoming session
messages it answered or that need no answer; whatever is not marked carries over."""

from __future__ import annotations

from factories import insert_message
from test_agent_session_messaging import _app, _exec, _rows, _send, alone, hive
from test_api_agent import client_for, headers


async def incoming(pool, session, sender, *, notice: str | None = None) -> int:
    """One session message from `sender` to `session`, as the sender's leader sent it."""
    async with pool.connection() as conn:
        cur = await conn.execute(
            "INSERT INTO messages (session_id, text, status, lane, sender_session_id,"
            " sender_slug, notice) VALUES (%s, 'ping', 'sent', 'regular', %s, %s, %s) RETURNING id",
            (session.id, sender.id, sender.slug, notice),
        )
        return (await cur.fetchone())[0]


async def handled_ids(pool) -> set[int]:
    return {r["id"] for r in await _rows(pool, "handled_at IS NOT NULL")}


async def post_handover(app, caller, **body):
    body.setdefault("reason", "context is full")
    body.setdefault("epoch", 1)
    async with client_for(app, caller.leader_ip) as http:
        return await http.post(
            "/agent/v1/leadership/handover", json=body, headers=headers(caller.id)
        )


async def test_send_marks_own_incoming(pool, dsn):
    mine = await hive(pool, "hd-mine")
    peer = await alone(pool, "hd-peer")
    m1 = await incoming(pool, mine, peer)
    r = await _send(_app(pool, dsn), mine, peer.slug, handled=[m1])
    assert r.status_code == 202, r.text
    assert r.json()["handled"] == [m1]
    assert await handled_ids(pool) == {m1}


async def test_send_ignores_foreign_notice_and_repeat_ids(pool, dsn):
    mine = await hive(pool, "hd-mine2")
    peer = await alone(pool, "hd-peer2")
    other = await alone(pool, "hd-other2")
    foreign = await incoming(pool, other, peer)
    notice = await incoming(pool, mine, peer, notice="handover")
    plain = await incoming(pool, mine, peer)
    done = await incoming(pool, mine, peer)
    await _exec(pool, "UPDATE messages SET handled_at = now() WHERE id = %s", (done,))
    own = await insert_message(pool, mine.id)  # not a session message: no sender
    ids = [foreign, notice, done, own["id"], 999999]
    r = await _send(_app(pool, dsn), mine, peer.slug, handled=ids)
    assert r.status_code == 202, r.text
    assert r.json()["handled"] == []
    assert await handled_ids(pool) == {done}
    assert plain not in await handled_ids(pool)


async def test_refused_send_marks_nothing(pool, dsn):
    mine = await hive(pool, "hd-mine3")
    peer = await alone(pool, "hd-peer3")
    m1 = await incoming(pool, mine, peer)
    r = await _send(_app(pool, dsn), mine, peer.slug, handled=[m1], ip=mine.member_ip)
    assert (r.status_code, r.json()["error"]["code"]) == (403, "not_leader")
    r = await _send(_app(pool, dsn), mine, "no-such-session", handled=[m1])
    assert r.status_code == 404
    assert await handled_ids(pool) == set()


async def test_handover_marks_handled(pool, dsn):
    mine = await hive(pool, "hd-mine4")
    peer = await alone(pool, "hd-peer4")
    m1 = await incoming(pool, mine, peer)
    app = _app(pool, dsn)
    r = await post_handover(app, mine, epoch=7, handled=[m1])
    assert (r.status_code, r.json()["error"]["code"]) == (409, "stale_epoch")
    assert await handled_ids(pool) == set()
    r = await post_handover(app, mine, handled=[m1])
    assert r.status_code == 202, r.text
    assert r.json()["handled"] == [m1]
    assert await handled_ids(pool) == {m1}


async def test_non_leader_handover_marks_nothing(pool, dsn):
    mine = await hive(pool, "hd-mine6")
    peer = await alone(pool, "hd-peer6")
    m1 = await incoming(pool, mine, peer)
    async with client_for(_app(pool, dsn), mine.member_ip) as http:
        r = await http.post(
            "/agent/v1/leadership/handover",
            json={"reason": "context is full", "epoch": 1, "handled": [m1]},
            headers=headers(mine.id),
        )
    assert (r.status_code, r.json()["error"]["code"]) == (409, "not_leader")
    assert await handled_ids(pool) == set()


async def test_handled_max_50(pool, dsn):
    mine = await hive(pool, "hd-mine5")
    peer = await alone(pool, "hd-peer5")
    app = _app(pool, dsn)
    r = await _send(app, mine, peer.slug, handled=list(range(1, 52)))
    assert r.status_code == 422
    r = await post_handover(app, mine, handled=list(range(1, 52)))
    assert r.status_code == 422
