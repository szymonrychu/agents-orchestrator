"""Preset saves with spec.volumes: existence, project scope, default mount path, audit."""

from __future__ import annotations

import uuid

import pytest_asyncio
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import volumes

P = "/api/v1/presets"


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        for slug in ("a", "b"):
            r = await c.post("/api/v1/projects", json={"slug": slug, "name": slug.upper()})
            assert r.status_code == 201, r.text
        yield c


async def volume(pool, name="data", projects=("default",)):
    async with pool.connection() as conn:
        return await volumes.create(
            conn,
            name=name,
            projects=list(projects),
            size="1Gi",
            description="",
            storage_class="sc",
            created_by="alice",
            actor_kind="user",
            actor="alice",
        )


def att(v, **over):
    return {"volume": str(v.id), "mode": "ro", **over}


async def save(http, atts, project=None, name="p"):
    body = {"name": name, "spec": {"volumes": atts}}
    if project:
        body["project"] = project
    return await http.post(P, json=body)


async def kinds(pool, v):
    async with pool.connection() as conn:
        return [
            (e.kind, e.detail) for e in reversed((await volumes.events(conn, v.id, limit=50)).items)
        ]


async def test_save_fills_the_default_mount_path(http, pool):
    v = await volume(pool)
    r = await save(http, [att(v)])
    assert r.status_code == 201, r.text
    assert r.json()["spec"]["volumes"][0]["mount_path"] == "/volumes/data"


async def test_arbitrary_mount_path_is_accepted(http, pool):
    v = await volume(pool)
    r = await save(http, [att(v, mount_path="/claude")])
    assert r.status_code == 201, r.text
    assert r.json()["spec"]["volumes"][0]["mount_path"] == "/claude"


async def test_unknown_deleted_and_out_of_scope_volumes_are_refused(http, pool):
    gone = await volume(pool, "gone")
    going = await volume(pool, "going")
    async with pool.connection() as conn:
        await volumes.mark_deleting(conn, gone.id)
        await volumes.finish_delete(conn, gone.id)
        await volumes.mark_deleting(conn, going.id)
    elsewhere = await volume(pool, "elsewhere", ("b",))
    for atts, text in [
        ([{"volume": str(uuid.uuid4()), "mode": "ro"}], "unknown volume"),
        ([att(gone)], "unknown volume"),
        ([att(going)], "being deleted"),
        ([att(elsewhere)], "project scope"),
    ]:
        r = await save(http, atts)
        assert_error(r, 422, "volumes_invalid")
        assert text in r.json()["error"]["message"]


async def test_default_path_collision_is_refused(http, pool):
    v = await volume(pool, "data")
    other = await volume(pool, "other")
    r = await save(http, [att(v), att(other, mount_path="/volumes/data")])
    assert_error(r, 422, "volumes_invalid")
    assert "already used" in r.json()["error"]["message"]


async def test_volume_in_several_projects_attaches_from_one_of_them(http, pool):
    v = await volume(pool, "shared", ("a", "b"))
    assert (await save(http, [att(v)], project="b")).status_code == 201


async def test_raw_session_create_with_volumes_is_refused(http, pool):
    v = await volume(pool)
    r = await http.post(
        "/api/v1/sessions",
        json={"slug": "raw", "spec": {"volumes": [{**att(v), "mount_path": "/x"}]}},
    )
    assert_error(r, 422, "volumes_preset_only")


async def test_attach_and_detach_each_write_one_event(http, pool):
    v = await volume(pool)
    made = (await save(http, [att(v, mode="rw")])).json()
    r = await http.put(f"{P}/{made['id']}", json={"name": "p", "spec": {}})
    assert r.status_code == 200, r.text
    got = await kinds(pool, v)
    assert [k for k, _ in got if k in ("attached", "detached")] == ["attached", "detached"]
    assert got[-2][1]["mode"] == "rw" and got[-2][1]["preset_id"] == made["id"]
    assert got[-1][1]["preset_id"] == made["id"]
