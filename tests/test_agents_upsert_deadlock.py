"""upsert_agent must not deadlock with an ingester transaction holding an agent_status row."""

from __future__ import annotations

import asyncio

import psycopg
from factories import insert_agent, insert_session, set_agent_status

from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import status


async def _lock_waiters(pool) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT count(*) FROM pg_stat_activity"
            " WHERE datname = current_database() AND wait_event_type = 'Lock'"
        )
        return (await cur.fetchone())[0]


async def test_upsert_agent_does_not_deadlock_with_ingester_seq_update(pool, dsn):
    session = await insert_session(pool)
    agent = await insert_agent(pool, session["id"], ordinal=0)
    await set_agent_status(pool, agent["id"], state="idle")

    ingester = await psycopg.AsyncConnection.connect(dsn)
    try:
        # The ingester transaction: first UPDATE of the agent_status row, not yet committed.
        await ingester.execute(
            "UPDATE agent_status SET state='busy', updated_at=now() WHERE agent_id=%s",
            (agent["id"],),
        )

        async def reconcile():
            async with pool.connection() as conn:
                await agents_repo.upsert_agent(conn, session["id"], 0, "p", None, "Running", True)

        task = asyncio.create_task(reconcile())
        for _ in range(500):
            if await _lock_waiters(pool):
                break
            await asyncio.sleep(0.02)
        else:
            raise AssertionError("upsert_agent never blocked on the ingester transaction")

        # Second write of the same row: its FK check takes FOR KEY SHARE on the agents row.
        await status.set_last_seq(ingester, agent["id"], 5)
        await ingester.commit()
        await asyncio.wait_for(task, 20)
    finally:
        await ingester.close()

    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT last_seq FROM agent_status WHERE agent_id=%s", (agent["id"],)
        )
        assert (await cur.fetchone())[0] == 5
