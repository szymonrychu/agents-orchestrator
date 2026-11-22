import asyncio

from factories import insert_agent, insert_session

from agents_orchestrator.db import envelope_outbox as outbox
from agents_orchestrator.db.envelope_outbox import Row
from agents_orchestrator.db.pool import listen


async def _seed(pool):
    s = await insert_session(pool)
    a = await insert_agent(pool, s["id"])
    return s["id"], a["id"]


def _row(sid, aid, idx=1, seq=1, **kw):
    return Row(
        session_id=sid,
        agent_id=aid,
        wrapper_session_id="w",
        event_seq=seq,
        envelope_index=idx,
        kind="send",
        body="hi",
        **kw,
    )


async def test_insert_is_idempotent(pool):
    sid, aid = await _seed(pool)
    async with pool.connection() as conn:
        assert await outbox.insert_rows(conn, [_row(sid, aid, 1), _row(sid, aid, 2)]) == 2
        assert await outbox.insert_rows(conn, [_row(sid, aid, 1), _row(sid, aid, 3)]) == 1
        cur = await conn.execute("SELECT count(*) FROM envelope_outbox")
        assert (await cur.fetchone())[0] == 3


async def test_insert_notifies_only_for_queued(pool, dsn):
    sid, aid = await _seed(pool)
    got: list = []

    async def collect():
        async for channel, payload in listen(dsn, [outbox.CHANNEL]):
            got.append((channel, payload))

    task = asyncio.create_task(collect())
    await asyncio.sleep(0.5)
    async with pool.connection() as conn:
        await outbox.insert_rows(conn, [_row(sid, aid, 1, status="invalid", error_code="x")])
        await outbox.insert_rows(conn, [_row(sid, aid, 2)])
    for _ in range(100):
        if got:
            break
        await asyncio.sleep(0.05)
    task.cancel()
    assert got == [("envelope_outbox", {"session_id": str(sid)})]


async def test_claim_next_oldest_first_one_at_a_time(pool):
    sid, aid = await _seed(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(conn, [_row(sid, aid, 1), _row(sid, aid, 2)])
        first = await outbox.claim_next(conn, sid)
        assert first["envelope_index"] == 1
        assert first["status"] == "sending" and first["attempts"] == 1
        assert await outbox.claim_next(conn, sid) is None  # one in flight
        assert await outbox.mark(conn, first["id"], "sending", "accepted", send_id="s1")
        second = await outbox.claim_next(conn, sid)
        assert second["envelope_index"] == 2
        assert await outbox.queued_sessions(conn) == []


async def test_mark_is_status_guarded(pool):
    sid, aid = await _seed(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(conn, [_row(sid, aid)])
        row = await outbox.claim_next(conn, sid)
        assert not await outbox.mark(conn, row["id"], "queued", "accepted")
        assert await outbox.mark(conn, row["id"], "sending", "refused", error_code="no")
        assert not await outbox.mark(conn, row["id"], "sending", "accepted")
        cur = await conn.execute("SELECT status, error_code FROM envelope_outbox")
        assert await cur.fetchone() == ("refused", "no")


async def test_mark_rejects_unknown_field(pool):
    import pytest

    async with pool.connection() as conn:
        with pytest.raises(ValueError):
            await outbox.mark(conn, 1, "queued", "failed", status="x")


async def test_rows_for_turn(pool):
    sid, aid = await _seed(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(
            conn, [_row(sid, aid, 1, turn_id="t1"), _row(sid, aid, 2, turn_id="t2")]
        )
        rows = await outbox.rows_for_turn(conn, aid, "t1")
        assert [r["envelope_index"] for r in rows] == [1]


async def test_sweep_stale_sending(pool):
    sid, aid = await _seed(pool)
    async with pool.connection() as conn:
        await outbox.insert_rows(conn, [_row(sid, aid, 1), _row(sid, aid, 2)])
        stale = await outbox.claim_next(conn, sid)
        await conn.execute(
            "UPDATE envelope_outbox SET updated_at = now() - interval '2 minutes' WHERE id = %s",
            (stale["id"],),
        )
        assert len(await outbox.sweep_stale_sending(conn)) == 1
        cur = await conn.execute(
            "SELECT status, error_code FROM envelope_outbox WHERE id = %s", (stale["id"],)
        )
        assert await cur.fetchone() == ("failed", "unknown_outcome")
        fresh = await outbox.claim_next(conn, sid)
        assert await outbox.sweep_stale_sending(conn) == []
        assert fresh["status"] == "sending"
        assert await outbox.queued_sessions(conn) == []


async def test_row_round_trips_hive_columns(pool):
    sid, aid = await _seed(pool)
    row = Row(
        session_id=sid,
        agent_id=aid,
        kind="agent",
        wrapper_session_id="w",
        event_seq=1,
        envelope_index=1,
        target="pod-1",
        lane="idle",
        notify="idle",
        handled=(3, 4),
        project="p",
    )
    async with pool.connection() as conn:
        await outbox.insert_rows(conn, [row])
        claimed = await outbox.claim_next(conn, sid)
        assert (claimed["target"], claimed["lane"], claimed["notify"]) == ("pod-1", "idle", "idle")
        assert (claimed["handled"], claimed["project"]) == ([3, 4], "p")
        assert claimed["message_id"] is None
        cur = await conn.execute(
            "INSERT INTO messages (session_id, text) VALUES (%s, 'x') RETURNING id", (sid,)
        )
        mid = (await cur.fetchone())[0]
        assert await outbox.mark(conn, claimed["id"], "sending", "accepted", message_id=mid)
        cur = await conn.execute("SELECT message_id FROM envelope_outbox")
        assert (await cur.fetchone())[0] == mid
