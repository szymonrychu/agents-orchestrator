"""POST /agent/v1/admin/sessions/{slug}/locks/reassign: a superleader gives a file lock of a
manageable session to one of its agents or discards it; paths are read against the target's
workdir and the caller's own session is refused (422 `own_session`)."""

from __future__ import annotations

import pytest
from factories import attach_preset
from test_admin_audit_detail import admin_rows
from test_agent_session_messaging import hive
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.spec import WORKDIR_ROOT

S = f"{ADMIN_PREFIX}/sessions"


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


async def pods(pool, session_id):
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT pod_name FROM agents WHERE session_id = %s ORDER BY ordinal", (session_id,)
        )
        return [r[0] for r in await cur.fetchall()]


async def holders(pool, session_id):
    async with pool.connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT f.path, a.pod_name FROM file_locks f JOIN agents a ON a.id = f.agent_id"
            " WHERE f.session_id = %s ORDER BY f.path",
            (session_id,),
        )
        return dict(await cur.fetchall())


async def workdir_of(pool, session_id):
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, session_id)
    return session.spec.workdir


@pytest.fixture
async def world(pool):
    boss = await hive(pool, "lk-boss", superleader=True)
    other = await hive(pool, "lk-other")
    await attach_preset(pool, other.id, manageable=True)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET spec = jsonb_set(spec, '{workdir}', '\"other-wd\"') WHERE id = %s",
            (other.id,),
        )
        await conn.execute(
            "INSERT INTO file_locks (session_id, path, agent_id) VALUES (%s, 'src/a.py', %s)",
            (other.id, other.leader_id),
        )
    return boss, other, await pods(pool, other.id), await pods(pool, boss.id)


async def reassign(app, who, slug, body, ip=None):
    async with client_for(app, ip or who.leader_ip) as http:
        return await http.post(f"{S}/{slug}/locks/reassign", headers=headers(who.id), json=body)


async def test_reassign_lock_to_other_member(app, pool, world):
    boss, other, (a0, a1), _ = world
    r = await reassign(app, boss, other.slug, {"path": "src/a.py", "to": a1})
    assert r.status_code == 200, r.text
    assert r.json() == {"path": "src/a.py", "agent": a1, "previous": a0}
    async with client_for(app, boss.leader_ip) as http:
        got = await http.get(f"{S}/{other.slug}/locks", headers=headers(boss.id))
    assert [(x["path"], x["agent"]) for x in got.json()["locks"]] == [("src/a.py", a1)]


async def test_release_with_to_null(app, pool, world):
    boss, other, (a0, _), _ = world
    r = await reassign(app, boss, other.slug, {"path": "src/a.py", "to": None})
    assert r.status_code == 200, r.text
    assert r.json() == {"path": "src/a.py", "agent": None, "previous": a0}
    assert await holders(pool, other.id) == {}


async def test_reassign_free_path_previous_none(app, pool, world):
    boss, other, (_, a1), _ = world
    r = await reassign(app, boss, other.slug, {"path": "src/new.py", "to": a1})
    assert r.status_code == 200, r.text
    assert r.json()["previous"] is None
    assert (await holders(pool, other.id))["src/new.py"] == a1


async def test_absolute_path_under_target_workdir_is_normalised(app, pool, world):
    boss, other, (_, a1), _ = world
    r = await reassign(
        app, boss, other.slug, {"path": f"{WORKDIR_ROOT}/other-wd/src/a.py", "to": a1}
    )
    assert r.status_code == 200, r.text
    assert r.json()["path"] == "src/a.py"


async def test_path_under_caller_workdir_400(app, pool, world):
    boss, other, (a0, a1), _ = world
    mine = await workdir_of(pool, boss.id)
    assert mine != "other-wd"
    for path in (f"{WORKDIR_ROOT}/{mine}/src/a.py", "../x"):
        r = await reassign(app, boss, other.slug, {"path": path, "to": a1})
        assert_error(r, 400, "invalid_path")
    assert await holders(pool, other.id) == {"src/a.py": a0}


async def test_to_pod_of_another_session_404(app, pool, world):
    boss, other, (a0, _), _ = world
    async with pool.connection() as conn:  # pod names repeat across sessions in the fixture
        await conn.execute(
            "UPDATE agents SET pod_name = 'boss-pod' WHERE id = %s", (boss.leader_id,)
        )
    r = await reassign(app, boss, other.slug, {"path": "src/a.py", "to": "boss-pod"})
    assert_error(r, 404, "not_found")
    assert await holders(pool, other.id) == {"src/a.py": a0}


async def test_unmanageable_session_403(app, pool, world):
    boss, other, (a0, a1), _ = world
    await attach_preset(pool, other.id, manageable=False)
    r = await reassign(app, boss, other.slug, {"path": "src/a.py", "to": a1})
    assert_error(r, 403, "not_manageable")
    assert await holders(pool, other.id) == {"src/a.py": a0}


async def test_own_session_422(app, pool, world):
    boss, _, _, (b0, b1) = world
    body = {"path": "x.py", "to": b1}
    leader = await reassign(app, boss, boss.slug, body)
    assert_error(leader, 422, "own_session")
    member = await reassign(app, boss, boss.slug, body, ip=boss.member_ip)
    assert_error(member, 422, "own_session")


async def test_regular_leader_403(app, pool, world):
    _, other, (_, a1), _ = world
    plain = await hive(pool, "lk-plain")
    r = await reassign(app, plain, other.slug, {"path": "src/a.py", "to": a1})
    assert_error(r, 403, "not_superleader")


async def test_reassign_audit_detail(app, pool, world):
    boss, other, (a0, a1), _ = world
    await reassign(app, boss, other.slug, {"path": "src/a.py", "to": a1})
    row = (await admin_rows(pool))[-1]
    assert row["fields"] == ["path", "to"]
    assert row["detail"] == {"path": "src/a.py", "previous": a0, "to": a1}
