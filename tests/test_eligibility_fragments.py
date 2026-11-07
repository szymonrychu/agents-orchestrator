"""The eligibility and handover fragments are spelled once, in `db/eligibility.py`."""

from __future__ import annotations

import inspect
import uuid

from factories import insert_agent, insert_session, set_agent_status

from agents_orchestrator.db import eligibility, leadership, queue, sessions
from agents_orchestrator.dispatcher import hive


def test_no_limit_is_the_only_spelling_of_the_literal() -> None:
    assert str(eligibility.NO_LIMIT) in eligibility.REPLICAS
    expected = f"coalesce((c.spec->>'replicas')::int, {eligibility.NO_LIMIT})"
    assert expected == eligibility.REPLICAS
    assert eligibility.STAYS.endswith(f"{eligibility.NO_LIMIT})")


def test_fragments_compose_from_each_other() -> None:
    assert eligibility.STAYS in eligibility.READY_STAYS
    assert eligibility.READY_STAYS in eligibility.ELIGIBLE
    assert eligibility.LEAVING in eligibility.HANDOVER_TRIGGER
    assert not hasattr(eligibility, "COMPACTED")


def test_call_sites_embed_the_shared_fragments() -> None:
    assert eligibility.ELIGIBLE in leadership._CANDIDATES
    assert eligibility.HANDOVER_TRIGGER in leadership._CANDIDATES
    assert eligibility.ELIGIBLE in leadership._FREE_LONGEST
    assert eligibility.ELIGIBLE in hive._CANDIDATES
    assert eligibility.HANDOVER_TRIGGER in queue._HANDOVER_PENDING
    assert eligibility.ELIGIBLE in queue._HANDOVER_PENDING


def test_eligible_excludes_dead_agents() -> None:
    # D4: one fragment, embedded by both the pick predicate and the manual make-leader target
    assert "'dead'" in eligibility.DEAD
    assert f"NOT {eligibility.DEAD}" in eligibility.READY_STAYS
    assert eligibility.READY_STAYS in eligibility.ELIGIBLE
    assert eligibility.DEAD in leadership._CANDIDATES
    # spelled once: no other module respells the dead-state test
    for module in (leadership, queue, sessions):
        assert "'dead'" not in inspect.getsource(module)


async def _busy(pool, agent_id: uuid.UUID) -> bool:
    async with pool.connection() as conn:
        cur = await conn.execute(
            f"SELECT {eligibility.BACKGROUND_BUSY} FROM agents a"
            " LEFT JOIN agent_status s ON s.agent_id = a.id WHERE a.id = %s",
            (agent_id,),
        )
        return (await cur.fetchone())[0]


async def test_background_entry_stale_after_two_hours(pool) -> None:
    assert eligibility.BACKGROUND_STALE_SECONDS == 7200
    session = await insert_session(pool)
    agent = await insert_agent(pool, session["id"], ready=True)
    assert not await _busy(pool, agent["id"])  # no status row

    async def with_map(entries: dict[str, str]) -> bool:
        await set_agent_status(pool, agent["id"], background_subagents=entries)
        return await _busy(pool, agent["id"])

    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT (now() - interval '1 hour')::text, (now() - interval '2 hours 1 second')::text"
        )
        fresh, old = await cur.fetchone()
    assert not await with_map({})
    assert await with_map({"a": fresh})
    assert not await with_map({"a": old})
    assert await with_map({"a": old, "b": fresh})
