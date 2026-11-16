"""handoff_profile.detect: preset override, then chat traffic in the tenure, else work."""

from datetime import UTC, datetime, timedelta

from factories import insert_agent, insert_session

from agents_orchestrator import handoff_profile
from agents_orchestrator.db import chat_activity


async def _setup(pool, spec=None):
    session = await insert_session(pool, spec=spec or {})
    agent = await insert_agent(pool, session["id"])
    return session, agent["id"]


async def _detect(pool, session, agent_id, tenure):
    async with pool.connection() as conn:
        return await handoff_profile.detect(conn, session, agent_id, tenure)


async def test_profile_override_wins(pool):
    for override, traffic in (("work", True), ("chat", False)):
        session, agent = await _setup(pool, {"handoff_profile": override})
        async with pool.connection() as conn:
            if traffic:
                await chat_activity.record(conn, session["id"], agent, "out")
        got = await _detect(pool, session, agent, datetime.now(UTC) - timedelta(hours=1))
        assert got == override


async def test_profile_chat_when_posted_in_tenure(pool):
    session, agent = await _setup(pool)
    start = datetime.now(UTC) - timedelta(minutes=5)
    async with pool.connection() as conn:
        await chat_activity.record(conn, session["id"], agent, "out")
    assert await _detect(pool, session, agent, start) == "chat"


async def test_profile_work_when_chat_before_tenure_only(pool):
    session, agent = await _setup(pool)
    async with pool.connection() as conn:
        await chat_activity.record(conn, session["id"], agent, "out")
    later = datetime.now(UTC) + timedelta(seconds=5)
    assert await _detect(pool, session, agent, later) == "work"


async def test_profile_work_without_any_traffic(pool):
    session, agent = await _setup(pool)
    assert await _detect(pool, session, agent, None) == "work"


async def test_profile_chat_on_inbound_connector_message(pool):
    session, agent = await _setup(pool)
    start = datetime.now(UTC) - timedelta(minutes=5)
    async with pool.connection() as conn:
        await chat_activity.record(conn, session["id"], None, "in")
    assert await _detect(pool, session, agent, start) == "chat"


async def test_profile_other_agents_post_does_not_count(pool):
    session, agent = await _setup(pool)
    other = await insert_agent(pool, session["id"])
    async with pool.connection() as conn:
        await chat_activity.record(conn, session["id"], other["id"], "out")
    assert await _detect(pool, session, agent, datetime.now(UTC) - timedelta(hours=1)) == "work"
