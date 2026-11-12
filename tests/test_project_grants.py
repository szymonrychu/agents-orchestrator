"""Project grants (T-120 slice 1, user answers Q3 A, Q4 A, Q5): a superleader files a grant of an
extra project to a session or a preset as a change proposal that the human approves on the main
port; the approval applies it at once (no restart). Revoking is the superleader's alone. A human
sets extra projects directly on the main port."""

from __future__ import annotations

import pytest
from factories import attach_preset
from psycopg.rows import dict_row
from test_agent_session_messaging import _exec, alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api import proposal_sweep
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo


@pytest.fixture
async def world(pool):
    """Projects a, b, c (c archived); the superleader S in a; the target T in b on a manageable
    preset; W a plain session in a."""
    await _exec(
        pool,
        "INSERT INTO projects (slug, name, archived_at) VALUES ('a', 'A', NULL), ('b', 'B', NULL),"
        " ('c', 'C', now())",
    )
    boss = await alone(pool, "grant-boss", superleader=True, project="a")
    target = await alone(pool, "grant-target", project="b")
    await attach_preset(pool, target.id)
    plain = await alone(pool, "grant-plain", project="a")
    return {"S": boss, "T": target, "W": plain}


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


async def _agent(app, who, method, path, **kw):
    async with client_for(app, who.leader_ip) as http:
        return await http.request(method, f"{ADMIN_PREFIX}{path}", headers=headers(who.id), **kw)


async def _grant(app, who, slug, project, **extra):
    body = {"project": project, "reason": "T-120, user-approved", **extra}
    return await _agent(app, who, "POST", f"/sessions/{slug}/projects", json=body)


async def _review(app, proposal, decision="approve"):
    items = [{"path": i["path"], "decision": decision} for i in proposal["revisions"][0]["items"]]
    async with main_client(app) as http:
        return await http.post(
            f"/api/v1/proposals/{proposal['id']}/review",
            json={"revision": proposal["revision"], "items": items},
        )


async def _extra(pool, session_id) -> tuple[str, ...]:
    async with pool.connection() as conn:
        found = await sessions_repo.get(conn, session_id)
    assert found is not None
    return found.extra_projects


async def _notices(pool, session_id, kind) -> list[str]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT text FROM messages WHERE session_id = %s AND notice = %s ORDER BY id",
            (session_id, kind),
        )
        return [r["text"] for r in await cur.fetchall()]


async def test_grant_files_a_proposal_and_approval_applies_it(pool, app, world):
    boss, target = world["S"], world["T"]
    r = await _grant(app, boss, target.slug, "a")
    assert r.status_code == 202, r.text
    proposal = r.json()
    assert (proposal["kind"], proposal["target_name"], proposal["status"]) == (
        "session_projects",
        target.slug,
        "open",
    )
    [item] = proposal["revisions"][0]["items"]
    assert (item["path"], item["before"], item["after"]) == ("projects.a", None, True)
    assert (item["security"], item["grant"]) == (True, True)
    assert await _extra(pool, target.id) == ()  # nothing granted before the review
    r = await _review(app, proposal)
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "applied"
    assert await _extra(pool, target.id) == ("a",)
    [notice] = await _notices(pool, target.id, "project_grant")
    assert "project a" in notice
    assert await _notices(pool, boss.id, "proposal")
    async with client_for(app, target.leader_ip) as http:
        me = (await http.get("/agent/v1/me", headers=headers(target.id))).json()
    assert me["projects_scope"] == "b,a"


async def test_deny_grants_nothing(pool, app, world):
    r = await _grant(app, world["S"], world["T"].slug, "a")
    r = await _review(app, r.json(), "deny")
    assert r.status_code == 200 and r.json()["status"] == "denied"
    assert await _extra(pool, world["T"].id) == ()
    assert await _notices(pool, world["T"].id, "project_grant") == []


@pytest.mark.parametrize(
    ("project", "code"),
    [
        ("nope", "unknown_project"),
        ("c", "project_archived"),
        ("b", "own_project"),
    ],
)
async def test_grant_refusals(pool, app, world, project, code):
    r = await _grant(app, world["S"], world["T"].slug, project)
    assert_error(r, 422, code)


async def test_already_granted_and_cap(pool, app, world):
    target = world["T"]
    async with pool.connection() as conn:
        for n in range(9):
            await conn.execute(
                "INSERT INTO projects (slug, name) VALUES (%s, %s)", (f"x{n}", f"X{n}")
            )
        await sessions_repo.set_extra_projects(conn, target.id, ["a"])
    assert_error(await _grant(app, world["S"], target.slug, "a"), 422, "already_granted")
    async with pool.connection() as conn:
        await sessions_repo.set_extra_projects(conn, target.id, [f"x{n}" for n in range(9)])
    assert_error(await _grant(app, world["S"], target.slug, "a"), 422, "too_many_projects")


async def test_only_a_superleader_grants(app, world):
    r = await _grant(app, world["W"], world["T"].slug, "a")
    assert_error(r, 403, "not_superleader")


async def test_unmanageable_session_grant_is_403(pool, app, world):
    hidden = await alone(pool, "grant-hidden", project="b")
    await attach_preset(pool, hidden.id, manageable=False)
    assert_error(await _grant(app, world["S"], hidden.slug, "a"), 403, "not_manageable")


async def test_client_key_replays_the_first(app, world):
    first = await _grant(app, world["S"], world["T"].slug, "a", client_key="k1")
    again = await _grant(app, world["S"], world["T"].slug, "a", client_key="k1")
    assert (first.status_code, again.status_code) == (202, 200)
    assert again.json()["id"] == first.json()["id"]


async def test_changed_target_makes_the_grant_stale(pool, app, world):
    proposal = (await _grant(app, world["S"], world["T"].slug, "a")).json()
    async with pool.connection() as conn:
        await sessions_repo.set_project(conn, world["T"].id, "a")
    r = await _review(app, proposal)
    assert_error(r, 409, "proposal_stale")
    assert await _extra(pool, world["T"].id) == ()


async def test_sweeper_marks_stale_and_target_gone(pool, app, world):
    stale = (await _grant(app, world["S"], world["T"].slug, "a")).json()
    async with pool.connection() as conn:
        await sessions_repo.set_extra_projects(conn, world["T"].id, ["a"])
    await proposal_sweep.sweep_once(pool)
    async with main_client(app) as http:
        got = (await http.get(f"/api/v1/proposals/{stale['id']}")).json()
    assert got["status"] == "stale"
    other = await alone(pool, "grant-gone", project="b")
    await attach_preset(pool, other.id)
    gone = (await _grant(app, world["S"], other.slug, "a")).json()
    await _exec(pool, "UPDATE sessions SET desired = 'deleting' WHERE id = %s", (other.id,))
    await proposal_sweep.sweep_once(pool)
    async with main_client(app) as http:
        got = (await http.get(f"/api/v1/proposals/{gone['id']}")).json()
    assert got["status"] == "target_gone"


async def test_grant_proposals_are_not_revised_but_withdrawn(app, world):
    proposal = (await _grant(app, world["S"], world["T"].slug, "a")).json()
    r = await _agent(
        app, world["S"], "POST", f"/proposals/{proposal['id']}/revise", json={"patch": {}}
    )
    assert_error(r, 422, "not_revisable")
    r = await _agent(app, world["S"], "POST", f"/proposals/{proposal['id']}/withdraw")
    assert r.status_code == 200 and r.json()["status"] == "withdrawn"
    r = await _agent(app, world["S"], "GET", "/proposals", params={"status": "all"})
    assert [p["kind"] for p in r.json()] == ["session_projects"]


async def test_grant_is_audited_with_field_names(pool, app, world):
    await _grant(app, world["S"], world["T"].slug, "a")
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            "SELECT data FROM events WHERE session_id = %s AND type = 'admin_call'",
            (world["S"].id,),
        )
        [row] = await cur.fetchall()
    assert row["data"]["status"] == 202
    assert row["data"]["fields"] == ["projects"]


async def test_revoke_is_direct_and_live(pool, app, world):
    target = world["T"]
    async with pool.connection() as conn:
        await sessions_repo.set_extra_projects(conn, target.id, ["a"])
    r = await _agent(app, world["S"], "DELETE", f"/sessions/{target.slug}/projects/a")
    assert r.status_code == 200, r.text
    assert r.json()["extra_projects"] == []
    assert await _extra(pool, target.id) == ()
    [notice] = await _notices(pool, target.id, "project_grant")
    assert "no longer" in notice
    r = await _agent(app, world["S"], "DELETE", f"/sessions/{target.slug}/projects/a")
    assert_error(r, 404, "not_found")
    r = await _agent(app, world["W"], "DELETE", f"/sessions/{target.slug}/projects/a")
    assert_error(r, 403, "not_superleader")


# ---------------------------------------------------------------- presets


async def _preset(pool, project="b", manageable=True) -> presets_repo.Preset:
    async with pool.connection() as conn:
        return await presets_repo.create(
            conn, f"grant-{project}-{manageable}", {}, manageable, project=project
        )


async def test_preset_grant_proposal_and_revoke(pool, app, world):
    preset = await _preset(pool)
    body = {"project": "a", "reason": "T-120"}
    r = await _agent(app, world["S"], "POST", f"/presets/{preset.name}/projects", json=body)
    assert r.status_code == 202, r.text
    proposal = r.json()
    assert (proposal["kind"], proposal["target_name"]) == ("preset_projects", preset.name)
    assert (await _review(app, proposal)).json()["status"] == "applied"
    async with pool.connection() as conn:
        stored = await presets_repo.get(conn, preset.id)
    assert stored is not None and stored.extra_projects == ("a",)
    r = await _agent(app, world["S"], "GET", f"/presets/{preset.id}")
    assert r.json()["extra_projects"] == ["a"]
    r = await _agent(app, world["S"], "DELETE", f"/presets/{preset.id}/projects/a")
    assert r.status_code == 200, r.text
    assert r.json()["extra_projects"] == []
    r = await _agent(app, world["S"], "POST", f"/presets/{preset.id}/projects", json=body)
    assert r.status_code == 202
    hidden = await _preset(pool, manageable=False)
    r = await _agent(app, world["S"], "POST", f"/presets/{hidden.id}/projects", json=body)
    assert_error(r, 403, "not_manageable")


# ---------------------------------------------------------------- main port (the human)


async def test_human_sets_extra_projects_directly(pool, app, world):
    target = world["T"]
    url = f"/api/v1/sessions/{target.id}/extra-projects"
    async with main_client(app) as http:
        r = await http.put(url, json={"extra_projects": ["a"]})
        assert r.status_code == 200, r.text
        assert r.json()["extra_projects"] == ["a"]
        assert_error(await http.put(url, json={"extra_projects": ["c"]}), 422, "project_archived")
        assert_error(await http.put(url, json={"extra_projects": ["b"]}), 422, "own_project")
        assert_error(await http.put(url, json={"extra_projects": ["zz"]}), 422, "unknown_project")
        r = await http.put(url, json={"extra_projects": ["a", "a"]})
        assert_error(r, 422, "invalid_request")
    await _exec(pool, "UPDATE projects SET archived_at = now() WHERE slug = 'a'")
    async with main_client(app) as http:
        r = await http.put(url, json={"extra_projects": ["a"]})  # kept, though archived now
        assert r.status_code == 200, r.text
        r = await http.put(url, json={"extra_projects": []})
        assert r.status_code == 200 and r.json()["extra_projects"] == []
    preset = await _preset(pool)
    async with main_client(app) as http:
        r = await http.put(
            f"/api/v1/presets/{preset.id}/extra-projects", json={"extra_projects": ["a"]}
        )
    assert_error(r, 422, "project_archived")
    await _exec(pool, "UPDATE projects SET archived_at = NULL WHERE slug = 'a'")
    async with main_client(app) as http:
        r = await http.put(
            f"/api/v1/presets/{preset.id}/extra-projects", json={"extra_projects": ["a"]}
        )
    assert r.status_code == 200 and r.json()["extra_projects"] == ["a"]


async def test_human_routes_are_not_on_the_agent_port(app, world):
    target = world["T"]
    async with client_for(app, world["S"].leader_ip) as http:
        r = await http.put(
            f"/api/v1/sessions/{target.id}/extra-projects",
            json={"extra_projects": ["a"]},
            headers=headers(world["S"].id),
        )
    assert r.status_code == 404
