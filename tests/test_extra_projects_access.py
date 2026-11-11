"""Full membership in every project of a session (T-120 slice 1, user answer Q5): a session with
extra projects has the access of a member of each, read live on every request (no restart)."""

from __future__ import annotations

from factories import two_project_fixture
from test_agent_project_chat import PATH as ROOM
from test_agent_scope import _client, _projects, _taskboard_app
from test_agent_session_messaging import _exec, alone, hive
from test_api_agent import headers, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.scope import PROJECTS_HEADER, scope_of
from agents_orchestrator.api.session_limits import may_message
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo


async def _grant(pool, session_id, *projects: str) -> None:
    async with pool.connection() as conn:
        await sessions_repo.set_extra_projects(conn, session_id, list(projects))


async def test_scope_lists_home_then_extra_projects(pool, dsn):
    seats = await two_project_fixture(pool)
    await _grant(pool, seats["B1"].id, "a")
    await _grant(pool, seats["S"].id, "b")
    async with pool.connection() as conn:
        b1 = await sessions_repo.get(conn, seats["B1"].id)
        s = await sessions_repo.get(conn, seats["S"].id)
    assert b1 is not None and s is not None
    assert scope_of(b1) == "b,a"
    assert scope_of(s) == "*"


async def test_relay_header_follows_grant_and_revoke_live(pool, dsn):
    seats = await two_project_fixture(pool)
    app, seen = _taskboard_app(pool, dsn)
    b1 = seats["B1"]
    for extra in (["a"], []):
        await _grant(pool, b1.id, *extra)
        async with _client(app, b1.ip) as http:
            r = await http.get("/agent/v1/tasks", headers=headers(b1.id))
        assert r.status_code == 200, r.text
    assert [s.headers[PROJECTS_HEADER] for s in seen] == ["b,a", "b"]


async def test_me_and_projects_show_extra_projects(pool, dsn):
    seats = await two_project_fixture(pool)
    await _grant(pool, seats["B1"].id, "a")
    app = create_app(make_settings(dsn), pool)
    async with _client(app, seats["B1"].ip) as http:
        r = await http.get("/agent/v1/me", headers=headers(seats["B1"].id))
    assert r.status_code == 200, r.text
    me = r.json()
    assert (me["project"], me["extra_projects"], me["projects_scope"]) == ("b", ["a"], "b,a")
    assert await _projects(app, seats["B1"]) == ["b", "a"]
    async with _client(app, seats["A1"].ip) as http:
        r = await http.get("/agent/v1/me", headers=headers(seats["A1"].id))
    assert r.json()["extra_projects"] == []


async def test_peers_and_messaging_cover_every_shared_project(pool, dsn):
    seats = await two_project_fixture(pool)
    app = create_app(make_settings(dsn), pool)

    async def peers(seat) -> list[str]:
        async with _client(app, seat.ip) as http:
            r = await http.get("/agent/v1/sessions", headers=headers(seat.id))
        assert r.status_code == 200, r.text
        return [p["slug"] for p in r.json()["sessions"]]

    assert "two-a1" not in await peers(seats["B1"])
    await _grant(pool, seats["B1"].id, "a")
    assert await peers(seats["B1"]) == ["two-a1", "two-a2", "two-s"]
    assert "two-b1" in await peers(seats["A1"])
    async with pool.connection() as conn:
        a1 = await sessions_repo.get(conn, seats["A1"].id)
        b1 = await sessions_repo.get(conn, seats["B1"].id)
        assert a1 is not None and b1 is not None
        settings = make_settings(dsn).model_copy(update={"session_reply_window_seconds": 0})
        assert await may_message(conn, a1, b1, settings)
        assert await may_message(conn, b1, a1, settings)
        await sessions_repo.set_extra_projects(conn, b1.id, [])
        b1 = await sessions_repo.get(conn, seats["B1"].id)
        assert b1 is not None
        assert not await may_message(conn, a1, b1, settings)


async def test_project_room_of_an_extra_project(pool, dsn):
    await _exec(pool, "INSERT INTO projects (slug, name) VALUES ('a', 'A'), ('b', 'B')")
    alpha = await hive(pool, "room-alpha", project="a")
    delta = await hive(pool, "room-delta", project="b")
    app = create_app(make_settings(dsn), pool)

    async def post(caller, **body):
        async with _client(app, caller.leader_ip) as http:
            return await http.post(ROOM, json={"text": "hi", **body}, headers=headers(caller.id))

    r = await post(delta, project="a")
    assert r.status_code == 403, r.text
    await _grant(pool, delta.id, "a")
    r = await post(delta, project="a")
    assert r.status_code == 202, r.text
    assert r.json()["delivered"] == ["room-alpha"]
    r = await post(alpha)
    assert r.status_code == 202, r.text
    assert r.json()["delivered"] == ["room-delta"]
    async with _client(app, delta.leader_ip) as http:
        r = await http.get(ROOM, params={"project": "a"}, headers=headers(delta.id))
        assert r.status_code == 200, r.text
        assert len(r.json()["posts"]) == 2
        r = await http.get(ROOM, headers=headers(delta.id))
        assert r.status_code == 200 and r.json()["posts"] == []  # home room stays the default


async def test_init_scripts_write_a_preset_of_an_extra_project(pool):
    await _exec(pool, "INSERT INTO projects (slug, name) VALUES ('a', 'A'), ('b', 'B')")
    seat = await alone(pool, "init-extra", project="b")
    async with pool.connection() as conn:
        preset = await presets_repo.create(conn, "init-extra-p", {}, project="a")
        await conn.execute("UPDATE sessions SET preset_id = %s WHERE id = %s", (preset.id, seat.id))
        _, written = await sessions_repo.set_init_scripts(
            conn, seat.id, ["echo one"], same_project_preset=True
        )
        assert not written
        await sessions_repo.set_extra_projects(conn, seat.id, ["a"])
        _, written = await sessions_repo.set_init_scripts(
            conn, seat.id, ["echo two"], same_project_preset=True
        )
        assert written
        stored = await presets_repo.get(conn, preset.id)
    assert stored is not None and stored.spec["init_scripts"] == ["echo two"]
