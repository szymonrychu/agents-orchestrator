"""S-83 step 6: the reads every agent may make, scoped to its projects (decisions 859-868, 943).

Roles: M a hive member pod, L a hive leader, S a superleader (the leader of a
`session_messaging` hive) and SM a member pod of that superleader hive, which reads like M
(decision 919). Every caller sits in project `a`; the in-scope target is in `a`, the out-of-scope
one in `b`.
"""

from __future__ import annotations

import io
import json

import httpx
import pytest
from factories import insert_agent, insert_session
from fakes import fake_upstream as fu
from test_admin_relay import make_app
from test_agent_session_messaging import Sess, hive, other_project
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator import history_backfill
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import handoffs
from agents_orchestrator.db.export import export_session_history

ROLES = ["M", "L", "S", "SM"]


async def callers(pool) -> dict[str, tuple[Sess, str]]:
    """role -> (session, pod ip) in project `a`; also creates projects `a` and `b`."""
    await other_project(pool, "a")
    await other_project(pool, "b")
    plain = await hive(pool, "sr-plain", project="a")
    boss = await hive(pool, "sr-boss", project="a", superleader=True)
    return {
        "M": (plain, plain.member_ip),
        "L": (plain, plain.leader_ip),
        "S": (boss, boss.leader_ip),
        "SM": (boss, boss.member_ip),
    }


async def targets(pool) -> dict[str, Sess]:
    return {
        "in": await hive(pool, "sr-in", project="a"),
        "out": await hive(pool, "sr-out", project="b"),
    }


async def get(app, caller: tuple[Sess, str], path: str, **params) -> httpx.Response:
    sess, ip = caller
    async with client_for(app, ip) as http:
        return await http.get(path, headers=headers(sess.id), params=params)


@pytest.mark.parametrize("role", ROLES)
async def test_history_relay_sends_caller_projects(pool, dsn, role):
    seats = await callers(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET extra_projects = '{b}' WHERE id = ANY(%s)",
            ([seats["L"][0].id, seats["S"][0].id],),
        )
    app, fakes, *_ = make_app(dsn, pool)
    key = ("GET", "/history/v1/sessions")
    fakes["history"].answers[key] = fu.success_sample(fakes["history"], key)
    r = await get(app, seats[role], f"{ADMIN_PREFIX}/history/sessions")
    assert r.status_code == 200, r.text
    (call,) = fakes["history"].calls
    assert call.headers["x-orch-projects"] == ("*" if role == "S" else "a,b")


async def test_export_carries_project_fields(pool):
    await other_project(pool, "a")
    row = await insert_session(pool, slug="sr-export", project="a")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET extra_projects = '{x,y}' WHERE id = %s", (row["id"],)
        )
        lines = [line async for line in export_session_history(conn, row["id"])]
    record = json.loads(lines[0])
    assert (record["kind"], record["project"], record["extra_projects"]) == (
        "session",
        "a",
        ["x", "y"],
    )


async def _handoff(pool, sess: Sess) -> int:
    async with pool.connection() as conn:
        row = await handoffs.insert(
            conn, sess.id, sess.leader_id, "voluntary", "done", handoff_text="t"
        )
    return row.id


@pytest.mark.parametrize("role", ROLES)
async def test_handoffs_scoped(pool, dsn, role):
    seats = await callers(pool)
    tg = await targets(pool)
    ids = {where: await _handoff(pool, sess) for where, sess in tg.items()}
    app = create_app(make_settings(dsn), pool)
    listed = await get(app, seats[role], f"{ADMIN_PREFIX}/handoffs")
    assert listed.status_code == 200, listed.text
    seen = {h["id"] for h in listed.json()["handoffs"]}
    out = await get(app, seats[role], f"{ADMIN_PREFIX}/handoffs/{ids['out']}")
    inside = await get(app, seats[role], f"{ADMIN_PREFIX}/handoffs/{ids['in']}")
    assert inside.status_code == 200, inside.text
    if role == "S":
        assert seen == set(ids.values())
        assert out.status_code == 200, out.text
    else:
        assert seen == {ids["in"]}
        assert_error(out, 404, "handoff_not_found")


@pytest.mark.parametrize("role", ROLES)
async def test_sessions_admin_list_scoped(pool, dsn, role):
    seats = await callers(pool)
    await targets(pool)
    granted = await insert_session(pool, slug="sr-granted", project="b")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET extra_projects = '{a}' WHERE id = %s", (granted["id"],)
        )
    app = create_app(make_settings(dsn), pool)
    r = await get(app, seats[role], f"{ADMIN_PREFIX}/sessions")
    assert r.status_code == 200, r.text
    slugs = {s["slug"] for s in r.json()}
    in_a = {"sr-plain", "sr-boss", "sr-in", "sr-granted"}
    assert slugs == (in_a | {"sr-out"} if role == "S" else in_a)
    # the `project` query parameter narrows further, never widens
    r = await get(app, seats[role], f"{ADMIN_PREFIX}/sessions", project="b")
    want = {"sr-out", "sr-granted"} if role == "S" else {"sr-granted"}
    assert {s["slug"] for s in r.json()} == want


@pytest.mark.parametrize("where", ["in", "out"])
@pytest.mark.parametrize("role", ROLES)
async def test_session_events_scoped(pool, dsn, role, where):
    seats = await callers(pool)
    tg = await targets(pool)
    app = create_app(make_settings(dsn), pool)
    r = await get(app, seats[role], f"{ADMIN_PREFIX}/sessions/{tg[where].slug}/events")
    if where == "out" and role != "S":
        assert_error(r, 403, "project_forbidden")
    else:
        assert r.status_code == 200, r.text


@pytest.mark.parametrize(
    ("role", "target", "status"),
    [
        ("L", "own", 200),
        ("L", "in", 200),
        ("L", "superleader_elsewhere", 200),
        ("L", "out", 403),
        ("L", "in_deleting", 403),
        ("M", "in", 403),
        ("SM", "in", 403),
        ("S", "out", 200),
    ],
)
async def test_session_get_leader_sessions_list_set(pool, dsn, role, target, status):
    """L reads the sessions its `sessions_list` shows (active, in its projects, or a superleader
    session) and its own (decision 863); a member pod is refused; S reads any."""
    seats = await callers(pool)
    tg = await targets(pool)
    tg["own"] = seats["L"][0]
    tg["superleader_elsewhere"] = await hive(pool, "sr-boss-b", project="b", superleader=True)
    tg["in_deleting"] = await hive(pool, "sr-in-del", project="a", desired="deleting")
    app = create_app(make_settings(dsn), pool)
    r = await get(app, seats[role], f"{ADMIN_PREFIX}/sessions/{tg[target].slug}")
    if status == 200:
        assert r.status_code == 200, r.text
        assert r.json()["slug"] == tg[target].slug
    else:
        assert_error(r, 403, "not_leader" if role in ("M", "SM") else "project_forbidden")


async def test_history_backfill_command_posts_known_projects(pool, dsn, monkeypatch):
    await other_project(pool, "a")
    rows = [await insert_session(pool, slug=f"sr-bf-{n}", project="a") for n in range(3)]
    await insert_agent(pool, rows[0]["id"], ordinal=0)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET extra_projects = '{b}' WHERE id = %s", (rows[1]["id"],)
        )
    seen: list[dict] = []
    auth: set[str] = set()

    def handler(request: httpx.Request) -> httpx.Response:
        auth.add(request.headers["authorization"])
        items = json.loads(request.content)["items"]
        seen.extend(items)
        return httpx.Response(200, json={"updated": len(items)})

    monkeypatch.setattr(history_backfill, "BATCH", 2)
    out = io.StringIO()
    url = history_backfill.projects_url("http://history.test/history/v1/ingest/")
    code = await history_backfill.run(
        dsn, url, "ingest-test-token", out, transport=httpx.MockTransport(handler)
    )
    assert code == 0
    assert url == "http://history.test/history/v1/ingest/projects"
    assert auth == {"Bearer ingest-test-token"}
    assert sorted(seen, key=lambda i: i["session_id"]) == sorted(
        (
            {
                "session_id": str(r["id"]),
                "project": "a",
                "extra_projects": ["b"] if r is rows[1] else [],
            }
            for r in rows
        ),
        key=lambda i: i["session_id"],
    )
    assert "3 sessions sent, 3 history sessions updated" in out.getvalue()
    assert "ingest-test-token" not in out.getvalue()


@pytest.mark.parametrize("role", ROLES)
async def test_agent_scope_star_only_for_superleader_leader_pod(pool, dsn, role):
    """The task relay header, `/me` projects_scope and `/projects` give `*` to S only; a member
    pod of the superleader session (SM) gets its session's projects (decision 919); L and M are
    unchanged."""
    seats = await callers(pool)
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET extra_projects = '{b}'")
    sent: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        return httpx.Response(200, json={"items": [], "next_cursor": None})

    settings = make_settings(dsn).model_copy(
        update={"taskboard_url": "http://taskboard.test:9002", "taskboard_token": "tb-token"}
    )
    app = create_app(settings, pool)
    app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    want = "*" if role == "S" else "a,b"
    r = await get(app, seats[role], "/agent/v1/tasks")
    assert r.status_code == 200, r.text
    assert sent[0].headers["x-orch-projects"] == want
    me = await get(app, seats[role], "/agent/v1/me")
    assert me.json()["projects_scope"] == want
    projects = await get(app, seats[role], "/agent/v1/projects")
    slugs = [p["slug"] for p in projects.json()]
    assert (len(slugs) > 2) == (role == "S")


@pytest.mark.parametrize(
    ("role", "sees_out"), [("M", False), ("L", False), ("SM", False), ("S", True)]
)
async def test_sessions_list_scope_follows_the_leader_check(pool, dsn, role, sees_out):
    """GET /agent/v1/sessions lists every project only for the superleader's leader; a member pod
    of a superleader session sees its own projects, like M and L (decision 919)."""
    seats = await callers(pool)
    tg = await targets(pool)
    app = create_app(make_settings(dsn), pool)
    r = await get(app, seats[role], "/agent/v1/sessions")
    assert r.status_code == 200, r.text
    slugs = {p["slug"] for p in r.json()["sessions"]}
    assert tg["in"].slug in slugs
    assert (tg["out"].slug in slugs) is sees_out
