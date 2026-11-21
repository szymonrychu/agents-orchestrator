"""Retrospective sessions (S-68 step 13; decisions 769, 770, 775): a human-only spec flag that
opens the read set over histories, handoffs, chats, presets, usage and backups (`require_reader`),
the backup copy, and the room notes update scoped to rooms bound to a project the session holds
(`require_notes_writer`). Every other admin route stays superleader-only, and a retrospective
session's project scope is `*` on GET relays only."""

from __future__ import annotations

import dataclasses
import json
import uuid

import httpx
import pytest
from fakes import fake_upstream as fu
from psycopg.types.json import Jsonb
from test_admin_audit_detail import admin_rows
from test_admin_gate import concrete
from test_admin_relay import DOCS, UPSTREAM_PREFIX, _body_schema, rig  # noqa: F401
from test_agent_session_messaging import Sess, alone, other_project
from test_agent_tasks import make_rig
from test_api_agent import AGENT_PORT, KEY, client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api import (
    admin,
    mindmap,  # noqa: F401
)
from agents_orchestrator.api import app as _app  # noqa: F401  (registers every admin route)
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.scope import scope_of
from agents_orchestrator.preset_policy import GRANTS, HUMAN_ONLY_PATHS, human_only_changes
from agents_orchestrator.spec import SessionSpec

NOTES = "/rooms/{room_id}/notes/sections/{section}"


async def _exec(pool, query: str, params: tuple = ()) -> None:
    async with pool.connection() as conn:
        await conn.execute(query, params)


async def retro(pool, slug: str, *, project: str = "default", extra: tuple[str, ...] = ()) -> Sess:
    """An alone session flagged retrospective, in `project` with `extra` granted projects."""
    s = await alone(pool, slug, project=project)
    await _exec(
        pool,
        "UPDATE sessions SET spec = spec || %s, extra_projects = %s WHERE id = %s",
        (Jsonb({"retrospective": True}), list(extra), s.id),
    )
    return s


async def add_preset(pool, project: str = "default") -> uuid.UUID:
    pid = uuid.uuid4()
    await _exec(
        pool,
        "INSERT INTO presets (id, name, spec, superleader_manageable, project)"
        " VALUES (%s, %s, %s, true, %s)",
        (pid, f"r-{pid.hex[:8]}", Jsonb({}), project),
    )
    return pid


async def call(app, caller: Sess, method: str, path: str, **kw) -> httpx.Response:
    async with client_for(app, caller.leader_ip) as http:
        return await http.request(method, path, headers=headers(caller.id), **kw)


# ------------------------------------------------------------------------------ the flag


async def test_retrospective_human_only(pool, dsn):
    """The flag is a human-only grant: listed in the policy, refused on an agent patch (turning
    it on), allowed toward safe (off), and stripped from a superleader's preset copy."""
    assert "retrospective" in GRANTS and "retrospective" in HUMAN_ONLY_PATHS
    assert human_only_changes({}, {"retrospective": True}) == ["retrospective"]
    assert human_only_changes({"retrospective": True}, {}) == []
    app = create_app(make_settings(dsn), pool)
    plain = await alone(pool, "rh-plain")
    r = await call(
        app,
        plain,
        "PATCH",
        f"{ADMIN_PREFIX}/sessions/{plain.slug}",
        json={"spec": {"retrospective": True}},
    )
    assert_error(r, 403, "human_only_field")
    assert "retrospective" in r.json()["error"]["message"]
    boss = await alone(pool, "rh-boss", superleader=True)
    r = await call(
        app,
        boss,
        "PATCH",
        f"{ADMIN_PREFIX}/sessions/{boss.slug}",
        json={"spec": {"retrospective": True}},
    )
    assert_error(r, 403, "human_only_field")


# ------------------------------------------------------------------------------ the reader gate


def _reader_routes() -> list[tuple[str, str]]:
    return sorted(
        (m, route.path)
        for route in admin.reader_router.routes
        for m in route.methods - {"HEAD", "OPTIONS"}
    )


def test_reader_routes_are_exactly_the_read_set_and_the_copy():
    p = ADMIN_PREFIX
    assert set(_reader_routes()) == {
        ("GET", f"{p}/presets"),
        ("GET", f"{p}/presets/{{ref}}"),
        ("GET", f"{p}/sessions"),
        ("GET", f"{p}/sessions/{{slug}}/activity"),
        ("GET", f"{p}/sessions/{{slug}}/events"),
        ("GET", f"{p}/sessions/{{slug}}/messages"),
        ("GET", f"{p}/usage"),
        ("GET", f"{p}/usage/history"),
        ("GET", f"{p}/backups"),
        ("POST", f"{p}/backups/copy"),
        ("GET", f"{p}/backups/{{name:path}}/versions"),
        ("GET", f"{p}/backups/{{name:path}}/versions/{{version_id}}/manifest"),
        ("GET", f"{p}/handoffs"),
        ("GET", f"{p}/handoffs/{{id}}"),
        ("GET", f"{p}/chats/rooms"),
        ("GET", f"{p}/chats/rooms/{{room_id}}"),
        ("GET", f"{p}/chats/rooms/{{room_id}}/messages"),
        ("GET", f"{p}/chats/rooms/{{room_id}}/notes"),
        ("GET", f"{p}/history/sessions"),
        ("GET", f"{p}/history/sessions/{{session_id}}"),
        ("GET", f"{p}/history/sessions/{{session_id}}/records"),
    }
    assert [(m, r.path) for r in admin.notes_router.routes for m in r.methods] == [
        ("PUT", f"{p}/chats/rooms/{{room_id}}/notes/sections/{{section}}")
    ]


async def test_retro_reader_routes_ok_and_redacted(rig):  # noqa: F811
    """A retrospective session passes the reader gate on every reader route and reads any
    session's spec; it gets the superleader's redacted views, never a secret touch (the audit
    rows list no secrets) and no vault list."""
    reader = await retro(rig.pool, "rr-reader")
    target = await alone(rig.pool, "rr-target")
    for method, path in _reader_routes():
        url = concrete(path).replace("/x/", f"/{target.slug}/")
        body = {"source": "a", "target": "b"} if method == "POST" else None
        r = await call(rig.app, reader, method, url, json=body)
        assert r.status_code not in (401, 403), (method, path, r.text)
    got = await call(rig.app, reader, "GET", f"{ADMIN_PREFIX}/sessions/{target.slug}")
    assert got.status_code == 200, got.text
    assert got.json()["slug"] == target.slug
    preset = await call(rig.app, reader, "GET", f"{ADMIN_PREFIX}/presets/{rig.vis}")
    assert preset.status_code == 200, preset.text
    assert_error(
        await call(rig.app, reader, "GET", f"{ADMIN_PREFIX}/vault"), 403, "not_superleader"
    )
    rows = [row for row in await admin_rows(rig.pool) if row["session"] == reader.slug]
    assert rows and all(row["secrets"] == [] and "vault" not in row for row in rows)


async def test_plain_session_refused_on_reader_routes(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    plain = await alone(pool, "rp-plain")
    for method, path in _reader_routes():
        r = await call(app, plain, method, concrete(path), json={})
        assert_error(r, 403, "not_superleader")


def _superleader_routes() -> list[tuple[str, str]]:
    return sorted(
        (m, route.path)
        for route in admin.router.routes
        for m in route.methods - {"HEAD", "OPTIONS"}
    )


@pytest.mark.parametrize(("method", "path"), _superleader_routes())
async def test_retro_forbidden_on_other_admin_routes(pool, dsn, method, path):
    """Every admin route outside the reader set (preset writes, session create, discard,
    restart, scale, cleanup settings, vault, webhooks, schedules, chat writes...) is 403 for a
    retrospective session."""
    app = create_app(make_settings(dsn), pool)
    reader = await retro(pool, f"rf-{uuid.uuid4().hex[:8]}")
    r = await call(app, reader, method, concrete(path), json={})
    assert_error(r, 403, "not_superleader")


async def test_retro_cannot_patch_or_withdraw_another_session(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    reader = await retro(pool, "rw-reader")
    target = await alone(pool, "rw-target")
    r = await call(
        app,
        reader,
        "PATCH",
        f"{ADMIN_PREFIX}/sessions/{target.slug}",
        json={"spec": {"model": "x"}},
    )
    assert_error(r, 403, "not_superleader")
    r = await call(
        app, reader, "POST", f"{ADMIN_PREFIX}/sessions/{target.slug}/leadership/withdraw", json={}
    )
    assert_error(r, 403, "not_superleader")


# ------------------------------------------------------------------------------ the scope


async def test_retro_scope_star_get_only(monkeypatch):
    """`*` on GET relays only; a write carries the session's own and granted projects."""
    rig_ = make_rig(monkeypatch)
    session = dataclasses.replace(
        rig_.session,
        spec=SessionSpec(replicas=2, max_replicas=2, retrospective=True),
        project="a",
        extra_projects=("b",),
    )
    rig_.db.sessions[session.id] = session
    assert scope_of(session) == "a,b"
    assert scope_of(session, "GET") == "*"
    assert scope_of(session, "POST") == "a,b"
    for method in ("GET", "POST", "PATCH"):
        rig_.upstream.seen.clear()
        body = {} if method == "GET" else {"json": {"title": "x"}}
        r = await rig_.call(
            method, "/agent/v1/tasks" + ("" if method != "PATCH" else "/T-1"), **body
        )
        assert r.status_code == 200, r.text
        (sent,) = rig_.upstream.seen
        assert sent.headers["x-orch-projects"] == ("*" if method == "GET" else "a,b"), method


async def test_retro_mindmap_write_in_granted_project_ok_else_403(monkeypatch):
    """The mind map service applies the project rule from `X-Orch-Projects` (stand-in below): a
    retrospective session reads every project and writes only in its own and granted ones."""
    from test_mindmap_relay import rig_for

    rig_ = rig_for(monkeypatch)
    session = dataclasses.replace(
        rig_.session,
        spec=SessionSpec(replicas=2, max_replicas=2, retrospective=True),
        project="a",
        extra_projects=("b",),
    )
    rig_.db.sessions[session.id] = session

    def handler(request: httpx.Request) -> httpx.Response:
        scope = request.headers["x-orch-projects"]
        if request.method != "GET":
            project = json.loads(request.content).get("project")
            if scope != "*" and project not in scope.split(","):
                return httpx.Response(403, json={"error": {"code": "forbidden", "message": "x"}})
        return httpx.Response(200, json={"scope": scope})

    rig_.app.state.mindmap_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    got = await rig_.call("GET", "/agent/v1/mindmap/search?q=x")
    assert got.status_code == 200 and got.json() == {"scope": "*"}
    for project, status in (("a", 200), ("b", 200), ("c", 403)):
        r = await rig_.call("POST", "/agent/v1/mindmap/maps", json={"project": project})
        assert r.status_code == status, (project, r.text)


# ------------------------------------------------------------------------------ room notes


def _room(n: int, preset, bound_session=None) -> dict:
    room = fu.sample(DOCS["connector"], {"$ref": "#/components/schemas/RoomDetail"})
    room.update(room_id=str(n), preset_id=preset, effective_preset_id=preset, binding=None)
    if bound_session is not None:
        room["binding"] = {
            "session_id": str(bound_session),
            "slug": "bound",
            "origin": "inbound",
            "since": "2026-11-16T00:00:00Z",
        }
    return room


async def test_retro_notes_update_scoped_to_granted_project(rig):  # noqa: F811
    """A retrospective session updates the notes of a room whose bound session's preset (else
    the configured preset) sits in its home or a granted project; any other room is 403
    `forbidden` and never reaches the connector. A plain session is 403 `not_superleader`; a
    superleader writes any room."""
    pool = rig.pool
    await other_project(pool, "rp")
    await other_project(pool, "granted")
    home, elsewhere, granted = (
        str(await add_preset(pool, "rp")),
        str(await add_preset(pool, "default")),
        str(await add_preset(pool, "granted")),
    )
    bound = await alone(pool, "rn-bound")
    await _exec(pool, "UPDATE sessions SET preset_id = %s WHERE id = %s", (home, bound.id))
    rooms = {
        "21": _room(21, home),
        "22": _room(22, elsewhere),
        "23": _room(23, elsewhere, bound.id),  # bound to a session of an rp preset
        "24": _room(24, None),
        "25": _room(25, granted),
    }
    fake = rig.fakes["connector"]
    fake.answers[("GET", "/chat/v1/admin/rooms/{room_id}")] = lambda c, p: (
        200,
        rooms[p["room_id"]],
    )
    key = ("PUT", UPSTREAM_PREFIX["connector"] + NOTES)
    body = fu.sample(DOCS["connector"], _body_schema(fake, key))
    reader = await retro(pool, "rn-reader", project="rp", extra=("granted",))
    url = ADMIN_PREFIX + "/chats" + NOTES.replace("{section}", "preferences")
    for room, status in (("21", 200), ("22", 403), ("23", 200), ("24", 403), ("25", 200)):
        r = await call(rig.app, reader, "PUT", url.replace("{room_id}", room), json=body)
        if status == 403:
            assert_error(r, 403, "forbidden")
        else:
            assert r.status_code == 200, (room, r.text)
    puts = [c.path for c in fake.calls if c.method == "PUT"]
    assert puts == [
        f"/chat/v1/admin/rooms/{n}/notes/sections/preferences" for n in ("21", "23", "25")
    ]
    plain = await alone(pool, "rn-plain")
    r = await call(rig.app, plain, "PUT", url.replace("{room_id}", "21"), json=body)
    assert_error(r, 403, "not_superleader")
    r = await call(rig.app, rig.boss, "PUT", url.replace("{room_id}", "22"), json=body)
    assert r.status_code == 200, r.text


# ------------------------------------------------------------------------------ the copy scope


async def _copy_rig(pool, dsn, monkeypatch, *, retro_project="default", extra=()):
    from test_api_backups import FakeS3, add_version, settings_for

    from agents_orchestrator.api import backups as backups_mod

    fake = FakeS3()
    monkeypatch.setattr(backups_mod.s3, "copy_object", fake.copy_object)
    monkeypatch.setattr(backups_mod.s3, "delete_keys", fake.delete_keys)
    await add_version(pool, "src/a")
    caller = await retro(pool, "rc-r", project=retro_project, extra=extra)
    app = create_app(settings_for(dsn, master_key=KEY, agent_port=AGENT_PORT), pool)
    return app, caller, fake


async def _preset_with_backup(pool, project: str, name: str) -> None:
    pid = uuid.uuid4()
    await _exec(
        pool,
        "INSERT INTO presets (id, name, spec, superleader_manageable, project)"
        " VALUES (%s, %s, %s, true, %s)",
        (
            pid,
            f"b-{pid.hex[:8]}",
            Jsonb({"backup": [{"root": "workspace", "path": ".", "mode": "both", "name": name}]}),
            project,
        ),
    )


async def test_retrospective_copy_into_a_foreign_project_name_is_refused(pool, dsn, monkeypatch):
    app, caller, fake = await _copy_rig(pool, dsn, monkeypatch)
    await _preset_with_backup(pool, await other_project(pool), "tgt/live")
    r = await call(
        app,
        caller,
        "POST",
        f"{ADMIN_PREFIX}/backups/copy",
        json={"source": "src/a", "target": "tgt/live"},
    )
    assert_error(r, 403, "forbidden")
    assert fake.copied == []


async def test_retrospective_copy_into_an_own_project_name_is_allowed(pool, dsn, monkeypatch):
    app, caller, fake = await _copy_rig(pool, dsn, monkeypatch)
    await _preset_with_backup(pool, "default", "tgt/mine")
    r = await call(
        app,
        caller,
        "POST",
        f"{ADMIN_PREFIX}/backups/copy",
        json={"source": "src/a", "target": "tgt/mine"},
    )
    assert r.status_code == 201, r.text
    assert len(fake.copied) == 1
