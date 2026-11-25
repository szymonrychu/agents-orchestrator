"""The request-free cores of session_send and the project post: refusals raise ApiError and
write nothing; a call inside an outer transaction commits or rolls back with it."""

from __future__ import annotations

import pytest
from test_agent_session_messaging import _exec, _rows, hive
from test_api_agent import make_settings

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.project_post_core import project_post_core
from agents_orchestrator.api.session_core import session_send_core
from agents_orchestrator.db import agents as agents_repo
from agents_orchestrator.db import sessions as sessions_repo


async def _caller(conn, sess):
    own = await sessions_repo.get(conn, sess.id)
    agent = await agents_repo.get_agent(conn, sess.leader_id)
    assert own is not None and agent is not None
    return own, agent


async def _count(pool, table: str) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute(f"SELECT count(*) FROM {table}")
        row = await cur.fetchone()
        return row[0]


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("interrupt", "lane_forbidden"),
        ("not_found", "not_found"),
        ("self", "self_send"),
        ("hop", "hop_limit"),
    ],
)
async def test_session_send_core_refusal_writes_nothing(pool, dsn, case, code):
    settings = make_settings(dsn)
    a = await hive(pool, "core-a")
    kwargs = {"lane": "regular", "hop": 0, "target": "core-b"}
    await hive(pool, "core-b")
    if case == "interrupt":
        kwargs["lane"] = "interrupt"
    elif case == "not_found":
        kwargs["target"] = "core-nobody"
    elif case == "self":
        kwargs["target"] = "core-a"
    else:
        kwargs["hop"] = settings.session_message_max_hops
    async with pool.connection() as conn:
        own, agent = await _caller(conn, a)
        with pytest.raises(ApiError) as exc:
            await session_send_core(
                conn,
                own,
                agent,
                kwargs["target"],
                "hi",
                settings=settings,
                lane=kwargs["lane"],
                handled=[],
                hop=kwargs["hop"],
                allow_interrupt=False,
            )
    assert exc.value.code == code
    assert await _rows(pool) == []


async def test_session_send_core_in_outer_transaction(pool, dsn):
    settings = make_settings(dsn)
    a = await hive(pool, "core-c")
    await hive(pool, "core-d")

    async def send(conn):
        own, agent = await _caller(conn, a)
        return await session_send_core(
            conn, own, agent, "core-d", "hi", settings=settings, lane="regular", handled=[],
            hop=0, envelope_ref="a7",
        )  # fmt: skip

    async with pool.connection() as conn, conn.transaction(force_rollback=True):
        await send(conn)
        assert len(await _rows(pool)) == 0  # not visible to other connections yet
    assert await _rows(pool) == []
    async with pool.connection() as conn, conn.transaction():
        sent = await send(conn)
    rows = await _rows(pool)
    assert [r["id"] for r in rows] == [sent.message_id]
    assert rows[0]["envelope_ref"] == "a7"
    assert rows[0]["hop"] == 1


@pytest.mark.parametrize(
    ("case", "code"),
    [
        ("not_leader", "not_leader"),
        ("unknown_project", "unknown_project"),
        ("not_in_project", "not_in_project"),
        ("invalid_reply_to", "invalid_reply_to"),
        ("hop", "hop_limit"),
    ],
)
async def test_project_post_core_refusal_writes_nothing(pool, dsn, case, code):
    settings = make_settings(dsn)
    await _exec(pool, "INSERT INTO projects (slug, name) VALUES ('pa', 'PA')")
    a = await hive(pool, "pc-a", project="pa", leader=case != "not_leader")
    await hive(pool, "pc-b", project="pa")
    kwargs = {"project": None, "to": [], "reply_to": None, "hop": 0}
    if case == "unknown_project":
        kwargs["project"] = "nope"
        await _exec(pool, "UPDATE sessions SET spec = spec || '{\"session_messaging\": true}'")
    elif case == "not_in_project":
        kwargs["to"] = ["pc-ghost"]
    elif case == "invalid_reply_to":
        kwargs["reply_to"] = 999
    elif case == "hop":
        kwargs["hop"] = settings.session_message_max_hops
    async with pool.connection() as conn:
        own, agent = await _caller(conn, a)
        with pytest.raises(ApiError) as exc:
            await project_post_core(
                conn, own, agent, settings=settings, text="hi", handled=[], **kwargs
            )
    assert exc.value.code == code
    assert await _count(pool, "project_posts") == 0
    assert await _rows(pool) == []


async def test_project_post_core_in_outer_transaction(pool, dsn):
    settings = make_settings(dsn)
    await _exec(pool, "INSERT INTO projects (slug, name) VALUES ('pb', 'PB')")
    a = await hive(pool, "pd-a", project="pb")
    await hive(pool, "pd-b", project="pb")

    async def post(conn):
        own, agent = await _caller(conn, a)
        return await project_post_core(
            conn, own, agent, settings=settings, text="hi", to=[], reply_to=None, handled=[],
            project=None, hop=0, envelope_ref="a9",
        )  # fmt: skip

    async with pool.connection() as conn, conn.transaction(force_rollback=True):
        await post(conn)
    assert await _count(pool, "project_posts") == 0
    assert await _rows(pool) == []
    async with pool.connection() as conn, conn.transaction():
        posted = await post(conn)
    assert posted.delivered == ["pd-b"]
    rows = await _rows(pool)
    assert len(rows) == 1 and rows[0]["envelope_ref"] == "a9"
    assert await _count(pool, "project_posts") == 1
