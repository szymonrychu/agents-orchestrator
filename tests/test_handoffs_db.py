"""db.handoffs: the agent_handoffs rows and the agent_limits start-context record (Postgres)."""

from datetime import UTC, datetime, timedelta

import psycopg.errors
import pytest
from factories import insert_agent, insert_session

from agents_orchestrator.db import handoffs


async def _agent(pool):
    session = await insert_session(pool)
    agent = await insert_agent(pool, session["id"])
    return session["id"], agent["id"]


async def test_one_open_row_per_agent(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        first = await handoffs.insert(conn, session_id, agent_id, "strong", "notified")
    async with pool.connection() as conn:
        assert (await handoffs.open_row(conn, agent_id)) == first
        with pytest.raises(psycopg.errors.UniqueViolation):
            await handoffs.insert(conn, session_id, agent_id, "voluntary", "requested")
    async with pool.connection() as conn:
        assert await handoffs.close(conn, first.id, "done", "x")
        assert await handoffs.open_row(conn, agent_id) is None
        second = await handoffs.insert(conn, session_id, agent_id, "voluntary", "requested")
    assert second.id != first.id
    assert second.phase == "requested"


async def test_advance_is_status_guarded(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, agent_id, "strong", "notified")
        assert not await handoffs.advance(conn, row.id, ["requested"], "handed", handoff_text="t")
        after = await handoffs.open_row(conn, agent_id)
        assert after == row
        assert await handoffs.advance(
            conn, row.id, ["notified", "extended"], "requested", request_message_id=7
        )
        after = await handoffs.open_row(conn, agent_id)
    assert after.phase == "requested"
    assert after.request_message_id == 7


async def test_extend_once(pool):
    session_id, agent_id = await _agent(pool)
    deadline = datetime.now(UTC) + timedelta(minutes=10)
    async with pool.connection() as conn:
        await handoffs.insert(
            conn, session_id, agent_id, "strong", "notified", deadline_at=deadline
        )
        row = await handoffs.extend(conn, agent_id, 300)
        assert row is not None
        assert row.phase == "extended"
        assert row.extended_at is not None
        assert row.deadline_at - deadline == timedelta(seconds=300)
        assert await handoffs.extend(conn, agent_id, 300) is None
        again = await handoffs.open_row(conn, agent_id)
    assert again.deadline_at == row.deadline_at


async def test_close_sets_reason_and_time(pool):
    session_id, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        row = await handoffs.insert(conn, session_id, agent_id, "shutdown", "notified")
        assert await handoffs.close(conn, row.id, "cancelled", "agent_gone")
        assert not await handoffs.close(conn, row.id, "done", "again")
        cur = await conn.execute(
            "SELECT phase, close_reason, closed_at FROM agent_handoffs WHERE id = %s", (row.id,)
        )
        phase, reason, closed_at = await cur.fetchone()
    assert (phase, reason) == ("cancelled", "agent_gone")
    assert closed_at is not None


async def test_limits_start_upsert(pool):
    _, agent_id = await _agent(pool)
    async with pool.connection() as conn:
        assert await handoffs.limits_start(conn, agent_id) is None
        await handoffs.set_limits_start(conn, agent_id, 1234)
        assert await handoffs.limits_start(conn, agent_id) == 1234
        await handoffs.set_limits_start(conn, agent_id, 99)
        assert await handoffs.limits_start(conn, agent_id) == 99
        await handoffs.set_limits_start(conn, agent_id, None)
        assert await handoffs.limits_start(conn, agent_id) is None
