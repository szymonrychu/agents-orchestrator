"""The reader gate (`require_reader`, a superleader only), the leader gate's route set and the
project scope (S-83 step 4 removed the retrospective flag; steps 6 and 7 reshaped the gates)."""

from __future__ import annotations

import dataclasses

import pytest
from test_admin_gate import concrete
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
        ("GET", f"{p}/sessions/{{slug}}/activity"),
        ("GET", f"{p}/sessions/{{slug}}/messages"),
        ("GET", f"{p}/usage/history"),
        ("GET", f"{p}/backups"),
        ("POST", f"{p}/backups/copy"),
        ("GET", f"{p}/backups/{{name:path}}/versions"),
        ("GET", f"{p}/backups/{{name:path}}/versions/{{version_id}}/manifest"),
        ("GET", f"{p}/volumes"),
        ("GET", f"{p}/volumes/{{volume_id}}"),
        ("GET", f"{p}/chats/rooms/{{room_id}}"),
    }
    assert {(m, r.path) for r in admin.leader_router.routes for m in r.methods} == {
        ("GET", f"{p}/chats/rooms"),
        ("PUT", f"{p}/chats/rooms/{{room_id}}/notes/sections/{{section}}"),
        ("GET", f"{p}/presets/{{ref}}"),
        ("GET", f"{p}/usage"),
    }


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
    assert scope_of(session, leader=True) == want
    for method in ("GET", "POST"):
        rig_.upstream.seen.clear()
        body = {} if method == "GET" else {"json": {"title": "x"}}
        r = await rig_.call(method, "/agent/v1/tasks", **body)
        assert r.status_code == 200, r.text
        (sent,) = rig_.upstream.seen
        assert sent.headers["x-orch-projects"] == want, method


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
