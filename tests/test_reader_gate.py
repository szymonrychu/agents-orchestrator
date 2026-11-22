"""The reader gate (`require_reader`), the notes writer gate and the project scope: a superleader
only (S-83 step 4 removed the retrospective flag; steps 6 and 7 reshape the gates)."""

from __future__ import annotations

import dataclasses

import pytest
from fakes import fake_upstream as fu
from test_admin_gate import concrete
from test_admin_relay import DOCS, UPSTREAM_PREFIX, _body_schema, rig  # noqa: F401
from test_agent_session_messaging import Sess, alone
from test_agent_tasks import make_rig
from test_api_agent import AGENT_PORT, KEY, client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api import admin
from agents_orchestrator.api import app as _app  # noqa: F401  (registers every admin route)
from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.scope import scope_of
from agents_orchestrator.spec import SessionSpec

NOTES = "/rooms/{room_id}/notes/sections/{section}"


async def call(app, caller: Sess, method: str, path: str, **kw):
    async with client_for(app, caller.leader_ip) as http:
        return await http.request(method, path, headers=headers(caller.id), **kw)


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
        ("GET", f"{p}/volumes"),
        ("GET", f"{p}/volumes/{{volume_id}}"),
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


async def test_plain_session_refused_on_reader_routes(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    plain = await alone(pool, "rp-plain")
    for method, path in _reader_routes():
        r = await call(app, plain, method, concrete(path), json={})
        assert_error(r, 403, "not_superleader")


@pytest.mark.parametrize("superleader", [True, False])
async def test_scope_of_star_only_superleader(monkeypatch, superleader):
    """`*` for a superleader only, on every method; a leader (the former retrospective caller
    included) carries its own and granted projects, on GET relays too."""
    rig_ = make_rig(monkeypatch)
    session = dataclasses.replace(
        rig_.session,
        spec=SessionSpec(replicas=2, max_replicas=2, session_messaging=superleader or None),
        project="a",
        extra_projects=("b",),
    )
    rig_.db.sessions[session.id] = session
    want = "*" if superleader else "a,b"
    assert scope_of(session) == want
    for method in ("GET", "POST"):
        rig_.upstream.seen.clear()
        body = {} if method == "GET" else {"json": {"title": "x"}}
        r = await rig_.call(method, "/agent/v1/tasks", **body)
        assert r.status_code == 200, r.text
        (sent,) = rig_.upstream.seen
        assert sent.headers["x-orch-projects"] == want, method


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


async def test_notes_update_superleader_only(rig):  # noqa: F811
    """A superleader writes any room's notes; any other session is 403 `not_superleader` and
    never reaches the connector."""
    fake = rig.fakes["connector"]
    fake.answers[("GET", "/chat/v1/admin/rooms/{room_id}")] = lambda c, p: (
        200,
        _room(int(p["room_id"]), None),
    )
    key = ("PUT", UPSTREAM_PREFIX["connector"] + NOTES)
    body = fu.sample(DOCS["connector"], _body_schema(fake, key))
    url = ADMIN_PREFIX + "/chats" + NOTES.replace("{section}", "preferences")
    plain = await alone(rig.pool, "rn-plain")
    r = await call(rig.app, plain, "PUT", url.replace("{room_id}", "21"), json=body)
    assert_error(r, 403, "not_superleader")
    r = await call(rig.app, rig.boss, "PUT", url.replace("{room_id}", "22"), json=body)
    assert r.status_code == 200, r.text
    assert [c.path for c in fake.calls if c.method == "PUT"] == [
        "/chat/v1/admin/rooms/22/notes/sections/preferences"
    ]


async def test_backups_copy_superleader_only(pool, dsn, monkeypatch):
    from test_api_backups import FakeS3, add_version, settings_for

    from agents_orchestrator.api import backups as backups_mod

    fake = FakeS3()
    monkeypatch.setattr(backups_mod.s3, "copy_object", fake.copy_object)
    monkeypatch.setattr(backups_mod.s3, "delete_keys", fake.delete_keys)
    await add_version(pool, "src/a")
    app = create_app(settings_for(dsn, master_key=KEY, agent_port=AGENT_PORT), pool)
    url, body = f"{ADMIN_PREFIX}/backups/copy", {"source": "src/a", "target": "tgt/x"}
    leader = await alone(pool, "rc-leader")
    assert_error(await call(app, leader, "POST", url, json=body), 403, "not_superleader")
    assert fake.copied == []
    boss = await alone(pool, "rc-boss", superleader=True)
    r = await call(app, boss, "POST", url, json=body)
    assert r.status_code == 201, r.text
    assert len(fake.copied) == 1
