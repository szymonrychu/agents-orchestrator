"""S-83 step 7: chat admin reads, the room notes update, preset_get and usage, scoped by role and
by the caller's projects (decisions 869, 871-873, 933, 934).

Roles: M a hive member pod, L a hive leader, S a superleader. Every caller sits in project `a`. A
room belongs to the projects of the preset it routes to (`api/room_projects.py`).
"""

from __future__ import annotations

import uuid

import httpx
import pytest
from fakes import fake_upstream as fu
from psycopg.types.json import Jsonb
from test_admin_relay import DOCS, UPSTREAM_PREFIX, _body_schema, make_app
from test_agent_session_messaging import Sess, hive, other_project
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client
from test_api_usage_gate import Upstream, close_usage_sources, gated, windows  # noqa: F401

from agents_orchestrator.api import admin_relay
from agents_orchestrator.api.admin import ADMIN_PREFIX

ROOMS = f"{ADMIN_PREFIX}/chats/rooms"
NOTES_PUT = "/rooms/{room_id}/notes/sections/{section}"


async def callers(pool) -> dict[str, tuple[Sess, str]]:
    """role -> (session, pod ip) in project `a`; also creates projects `a` and `b`."""
    await other_project(pool, "a")
    await other_project(pool, "b")
    plain = await hive(pool, "sc-plain", project="a")
    boss = await hive(pool, "sc-boss", project="a", superleader=True)
    return {
        "M": (plain, plain.member_ip),
        "L": (plain, plain.leader_ip),
        "S": (boss, boss.leader_ip),
    }


async def add_preset(pool, project: str, extra: tuple[str, ...] = ()) -> uuid.UUID:
    pid = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO presets (id, name, spec, project, extra_projects, superleader_manageable)"
            " VALUES (%s, %s, %s, %s, %s, true)",
            (pid, f"sc-{pid.hex[:8]}", Jsonb({}), project, list(extra)),
        )
    return pid


def room(room_id: str, preset: uuid.UUID | None, bound: uuid.UUID | None = None) -> dict:
    obj = fu.sample(DOCS["connector"], {"$ref": "#/components/schemas/RoomView"})
    obj.update(
        room_id=room_id,
        preset_id=None if preset is None else str(preset),
        effective_preset_id=None if preset is None else str(preset),
        binding=None,
    )
    if bound is not None:
        obj["binding"] = {
            "session_id": str(bound),
            "slug": "bound",
            "origin": "inbound",
            "since": "2026-11-16T00:00:00Z",
        }
    return obj


async def rig(pool, dsn):
    """The callers, an app with fake upstreams and rooms `in` (preset in `a`) and `out` (preset
    in `b`); the connector serves its rooms list from `rooms`."""
    seats = await callers(pool)
    app, fakes, *_ = make_app(dsn, pool)
    in_p, out_p = await add_preset(pool, "a"), await add_preset(pool, "b")
    rooms = [room("in", in_p), room("out", out_p)]
    conn = fakes["connector"]
    conn.answers[("GET", "/chat/v1/admin/rooms")] = lambda c, p: (200, rooms)
    for key in (
        ("GET", "/chat/v1/admin/rooms/{room_id}/messages"),
        ("GET", "/chat/v1/admin/rooms/{room_id}/notes"),
        ("PUT", "/chat/v1/admin" + NOTES_PUT),
    ):
        conn.answers[key] = fu.success_sample(conn, key)
    return seats, app, fakes, rooms, (in_p, out_p)


async def call(app, caller: tuple[Sess, str], method: str, path: str, **kw) -> httpx.Response:
    sess, ip = caller
    async with client_for(app, ip) as http:
        return await http.request(method, path, headers=headers(sess.id), **kw)


@pytest.mark.parametrize("kind", ["bound_in_p", "other_project", "unbound"])
@pytest.mark.parametrize("role", ["L", "S"])
async def test_rooms_list_filtered_by_project(pool, dsn, kind, role):
    """L sees a room only when its resolved project overlaps P: the bound session's preset wins
    over the room's own preset, and a room with no preset is in no project. S sees every room."""
    seats, app, fakes, rooms, (in_p, out_p) = await rig(pool, dsn)
    bound = await hive(pool, "sc-bound", project="a")
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET preset_id = %s WHERE id = %s", (in_p, bound.id))
    rooms[:] = [
        {
            "bound_in_p": room("x", out_p, bound=bound.id),
            "other_project": room("x", out_p),
            "unbound": room("x", None),
        }[kind]
    ]
    r = await call(app, seats[role], "GET", ROOMS)
    assert r.status_code == 200, r.text
    listed = [x["room_id"] for x in r.json()]
    assert listed == (["x"] if role == "S" or kind == "bound_in_p" else [])
    assert len([c for c in fakes["connector"].calls if c.path.endswith("/rooms")]) == 1


async def test_rooms_list_refuses_member(pool, dsn):
    seats, app, fakes, *_ = await rig(pool, dsn)
    assert_error(await call(app, seats["M"], "GET", ROOMS), 403, "not_leader")
    assert not fakes["connector"].calls


@pytest.mark.parametrize("what", ["messages", "notes"])
@pytest.mark.parametrize("role", ["M", "L", "S"])
async def test_room_history_notes_scoped(pool, dsn, what, role):
    """M and L read the history and notes of a room of P and are refused 403 project_forbidden
    on one outside P (nothing forwarded); S reads every room."""
    seats, app, fakes, *_ = await rig(pool, dsn)
    inside = await call(app, seats[role], "GET", f"{ROOMS}/in/{what}")
    assert inside.status_code == 200, inside.text
    out = await call(app, seats[role], "GET", f"{ROOMS}/out/{what}")
    forwarded = {c.path for c in fakes["connector"].calls if c.path.endswith(what)}
    if role == "S":
        assert out.status_code == 200, out.text
    else:
        assert_error(out, 403, "project_forbidden")
        assert forwarded == {f"/chat/v1/admin/rooms/in/{what}"}


@pytest.mark.parametrize(
    ("role", "room_id", "error"),
    [
        ("M", "in", "not_leader"),
        ("L", "in", None),
        ("L", "out", "project_forbidden"),
        ("L", "unknown", "project_forbidden"),
        ("S", "out", None),
    ],
)
async def test_notes_update_leader_rooms_of_p(pool, dsn, role, room_id, error):
    seats, app, fakes, *_ = await rig(pool, dsn)
    fake = fakes["connector"]
    key = ("PUT", UPSTREAM_PREFIX["connector"] + NOTES_PUT)
    body = fu.sample(DOCS["connector"], _body_schema(fake, key))
    url = f"{ROOMS}/{room_id}/notes/sections/preferences"
    r = await call(app, seats[role], "PUT", url, json=body)
    puts = [c.path for c in fake.calls if c.method == "PUT"]
    if error is None:
        assert r.status_code == 200, r.text
        assert puts == [f"/chat/v1/admin/rooms/{room_id}/notes/sections/preferences"]
    else:
        assert_error(r, 403, error)
        assert puts == []


async def test_room_project_cache_invalidated_on_preset_patch(pool, dsn):
    """A superleader's preset patch moving the room's preset to another project takes the room
    out of L's list and reads at once, though the room map was just built."""
    seats, app, _, _, (in_p, _) = await rig(pool, dsn)
    assert [x["room_id"] for x in (await call(app, seats["L"], "GET", ROOMS)).json()] == ["in"]
    assert (await call(app, seats["L"], "GET", f"{ROOMS}/in/notes")).status_code == 200
    moved = await call(
        app, seats["S"], "PATCH", f"{ADMIN_PREFIX}/presets/{in_p}", json={"project": "b"}
    )
    assert moved.status_code == 200, moved.text
    assert (await call(app, seats["L"], "GET", ROOMS)).json() == []
    assert_error(await call(app, seats["L"], "GET", f"{ROOMS}/in/notes"), 403, "project_forbidden")


@pytest.mark.parametrize(
    ("role", "where", "error"),
    [
        ("L", "home", None),
        ("L", "extra", None),
        ("L", "out", "project_forbidden"),
        ("M", "home", "not_leader"),
        ("S", "out", None),
    ],
)
async def test_preset_get_scoped_redacted(pool, dsn, role, where, error):
    """L reads a preset whose project or extra projects overlap P, the same redacted view the
    main port answers; one outside P is 403 project_forbidden; M is refused."""
    seats = await callers(pool)
    await other_project(pool, "c")
    app = make_app(dsn, pool)[0]
    pid = {
        "home": await add_preset(pool, "a"),
        "extra": await add_preset(pool, "c", ("a",)),
        "out": await add_preset(pool, "b"),
    }[where]
    r = await call(app, seats[role], "GET", f"{ADMIN_PREFIX}/presets/{pid}")
    if error is None:
        async with main_client(app) as main:
            today = await main.get(f"/api/v1/presets/{pid}")
        assert r.status_code == 200, r.text
        assert r.json() == today.json()
    else:
        assert_error(r, 403, error)


@pytest.mark.parametrize("role", ["M", "L", "S"])
async def test_usage_current_leader_history_superleader(pool, dsn, role):
    """The current window is for any leader (L and S); M is refused. Usage history and sessions
    stay superleader only."""
    seats = await callers(pool)
    upstream = Upstream(windows(five_hour=12))
    app = gated(make_settings(dsn), pool, upstream)
    current = await call(app, seats[role], "GET", f"{ADMIN_PREFIX}/usage")
    history = await call(app, seats[role], "GET", f"{ADMIN_PREFIX}/usage/history")
    sessions = await call(app, seats[role], "GET", f"{ADMIN_PREFIX}/usage/sessions")
    if role == "M":
        assert_error(current, 403, "not_leader")
    else:
        assert current.status_code == 200, current.text
        assert current.json()["five_hour"]["utilization_pct"] == 12.0
    for r in (history, sessions):
        if role == "S":
            assert r.status_code not in (401, 403), r.text
        else:
            assert r.status_code == 403, r.text


async def test_notes_update_reads_the_binding_fresh(pool, dsn):
    """A room the upstream moved to another project is refused for a write at once, though the
    room map (kept for reads) still holds the old binding."""
    seats, app, fakes, rooms, (_, out_p) = await rig(pool, dsn)
    assert (await call(app, seats["L"], "GET", f"{ROOMS}/in/notes")).status_code == 200
    rooms[0] = room("in", out_p)
    fake = fakes["connector"]
    body = fu.sample(DOCS["connector"], _body_schema(fake, ("PUT", "/chat/v1/admin" + NOTES_PUT)))
    r = await call(app, seats["L"], "PUT", f"{ROOMS}/in/notes/sections/preferences", json=body)
    assert_error(r, 403, "project_forbidden")
    assert [c for c in fake.calls if c.method == "PUT"] == []


async def test_unknown_room_reads_do_not_refetch_every_call(pool, dsn):
    """Reads of a room the upstream does not list refetch the rooms list at most once per
    miss interval, not on every call."""
    seats, app, fakes, *_ = await rig(pool, dsn)
    for _ in range(3):
        r = await call(app, seats["L"], "GET", f"{ROOMS}/nope/notes")
        assert_error(r, 403, "project_forbidden")
    assert len([c for c in fakes["connector"].calls if c.path.endswith("/rooms")]) == 1


async def test_rooms_list_non_json_answer_is_502(pool, dsn, monkeypatch):
    seats, app, *_ = await rig(pool, dsn)

    async def not_json(*args, **kwargs):
        return 200, b"<html>"

    monkeypatch.setattr(admin_relay.forward_core, "forward", not_json)
    assert_error(await call(app, seats["L"], "GET", ROOMS), 502, "upstream_shape")
