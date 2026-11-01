"""Init scripts public API: repo write-back to session and source preset, both routes, views."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from factories import insert_session
from psycopg.types.json import Jsonb
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as repo
from agents_orchestrator.spec import init_scripts_hash

DEFAULT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        yield c


def url(session_id) -> str:
    return f"/api/v1/sessions/{session_id}/init-scripts"


async def session_from_preset(http, preset_id=None, name="p1"):
    if preset_id is None:
        r = await http.post("/api/v1/presets", json={"name": name, "spec": {"replicas": 1}})
        assert r.status_code == 201, r.text
        preset_id = r.json()["id"]
    r = await http.post("/api/v1/sessions", json={"preset_id": str(preset_id)})
    assert r.status_code == 201, r.text
    return r.json()


async def preset_spec(pool, preset_id):
    async with pool.connection() as conn:
        return (await presets_repo.get(conn, uuid.UUID(str(preset_id)))).spec


async def test_put_writes_session_and_preset(http, pool):
    cl = await session_from_preset(http)
    async with pool.connection() as conn:
        before = (await presets_repo.get(conn, uuid.UUID(cl["preset_id"]))).updated_at
    r = await http.put(url(cl["id"]), json={"scripts": ["echo a", "echo b"]})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["preset_updated"] is True
    assert body["scripts"] == ["echo a", "echo b"]
    assert body["preset_id"] == cl["preset_id"]
    assert body["preset_name"] == "p1"
    got = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    assert got["spec"]["init_scripts"] == ["echo a", "echo b"]
    assert got["generation"] == cl["generation"]
    spec = await preset_spec(pool, cl["preset_id"])
    assert spec["init_scripts"] == ["echo a", "echo b"]
    assert spec["replicas"] == 1
    async with pool.connection() as conn:
        assert (await presets_repo.get(conn, uuid.UUID(cl["preset_id"]))).updated_at > before


async def test_put_without_preset(http, pool):
    raw = (await http.post("/api/v1/sessions", json={"slug": "raw", "spec": {}})).json()
    r = await http.put(url(raw["id"]), json={"scripts": ["echo x"]})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is False
    assert r.json()["preset_id"] is None
    # preset deleted: preset_id is null again
    cl = await session_from_preset(http, name="gone")
    assert (await http.delete(f"/api/v1/presets/{cl['preset_id']}")).status_code in (200, 204)
    r = await http.put(url(cl["id"]), json={"scripts": ["echo y"]})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is False
    got = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    assert got["spec"]["init_scripts"] == ["echo y"]


async def test_put_writes_default_preset(http, pool):
    cl = await session_from_preset(http, preset_id=DEFAULT_ID)
    r = await http.put(url(cl["id"]), json={"scripts": ["echo d"]})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is True
    assert (await preset_spec(pool, DEFAULT_ID))["init_scripts"] == ["echo d"]


async def test_get_view_and_edited_since_run(http, pool):
    cl = await session_from_preset(http, name="viewp")
    cid = cl["id"]
    r = await http.get(url(cid))
    assert r.status_code == 200, r.text
    v = r.json()
    assert v["scripts"] == [] and v["status"] is None and v["edited_since_run"] is False
    assert v["script_path"] == f"/workdir/{cl['spec']['workdir']}/.agent-init/init.sh"
    assert v["log_path"] == f"/workdir/{cl['spec']['workdir']}/.agent-init/init.log"
    assert v["preset_updated"] is False
    await http.put(url(cid), json={"scripts": ["echo 1"]})
    status = {
        "init": {
            "state": "succeeded",
            "scripts_hash": init_scripts_hash(["echo 1"]),
            "started_at": "2026-10-24T10:00:00Z",
            "finished_at": "2026-10-24T10:00:05Z",
            "exit_code": 0,
            "log_tail": "ok",
        }
    }
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET status = %s WHERE id = %s", (Jsonb(status), uuid.UUID(cid))
        )
    v = (await http.get(url(cid))).json()
    assert v["status"]["state"] == "succeeded"
    assert v["status"]["exit_code"] == 0
    assert v["status"]["reason"] is None
    assert v["edited_since_run"] is False
    v = (await http.put(url(cid), json={"scripts": ["echo 2"]})).json()
    assert v["edited_since_run"] is True
    assert (await http.get(url(cid))).json()["edited_since_run"] is True
    # skipped runs are never "edited since run"
    status["init"]["state"] = "skipped"
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET status = %s WHERE id = %s", (Jsonb(status), uuid.UUID(cid))
        )
    assert (await http.get(url(cid))).json()["edited_since_run"] is False


async def test_put_errors(http, pool):
    cl = await session_from_preset(http, name="errp")
    cid = cl["id"]
    for bad in (["  "], ["x"] * 21, ["y" * 16385]):
        assert_error(await http.put(url(cid), json={"scripts": bad}), 422, "invalid_request")
    assert_error(await http.put(url(cid), json={"nope": []}), 422, "invalid_request")
    assert_error(await http.put(url(uuid.uuid4()), json={"scripts": []}), 404, "not_found")
    assert_error(await http.get(url(uuid.uuid4())), 404, "not_found")
    deleting = await insert_session(pool, desired="deleting")
    assert_error(await http.put(url(deleting["id"]), json={"scripts": ["echo"]}), 409, "conflict")
    assert (await http.get(f"/api/v1/sessions/{cid}")).json()["spec"]["init_scripts"] == []
    assert "init_scripts" not in await preset_spec(pool, cl["preset_id"])
    async with pool.connection() as conn:
        row = await (
            await conn.execute("SELECT spec FROM sessions WHERE id = %s", (deleting["id"],))
        ).fetchone()
    assert "init_scripts" not in row[0]


async def test_set_init_scripts_unknown(pool):
    async with pool.connection() as conn:
        with pytest.raises(repo.SessionNotFound):
            await repo.set_init_scripts(conn, uuid.uuid4(), ["x"])


async def test_session_view_edited_flag(http, pool):
    cl = await session_from_preset(http, name="flagp")
    cid = cl["id"]
    assert cl["init_edited_since_run"] is False
    status = {"init": {"state": "failed", "scripts_hash": init_scripts_hash([])}}
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET status = %s WHERE id = %s", (Jsonb(status), uuid.UUID(cid))
        )
    await http.put(url(cid), json={"scripts": ["echo z"]})
    assert (await http.get(f"/api/v1/sessions/{cid}")).json()["init_edited_since_run"] is True
    listed = {c["id"]: c for c in (await http.get("/api/v1/sessions")).json()}
    assert listed[cid]["init_edited_since_run"] is True
    from agents_orchestrator.api.sessions import _view
    from agents_orchestrator.settings import Settings

    async with pool.connection() as conn:
        c = await repo.get(conn, uuid.UUID(cid))
    snap = _view(c, None, Settings(database_url="postgresql://x/y")).model_dump(mode="json")
    assert snap["init_edited_since_run"] is True


async def test_put_rejects_a_bad_template_and_keeps_edited_flag_stable(http, pool):
    r = await http.post(
        "/api/v1/presets", json={"name": "tpl", "spec": {"env": {"A": {"value": "1"}}}}
    )
    cl = await session_from_preset(http, r.json()["id"])
    cid = cl["id"]
    r = await http.put(url(cid), json={"scripts": ["echo {{ nope }}"]})
    assert r.status_code == 422 and "nope" in r.text
    r = await http.put(url(cid), json={"scripts": ["awk '{{print}}'"]})
    assert r.status_code == 422 and "init_scripts[0]" in r.text
    r = await http.put(url(cid), json={"scripts": ["echo {{ env.A }} {{ session_slug }}"]})
    assert r.status_code == 200, r.text
    rendered = [f"echo 1 {cl['slug']}"]
    status = {
        "init": {
            "state": "succeeded",
            "scripts_hash": init_scripts_hash(["echo {{ env.A }} {{ session_slug }}"], rendered),
        }
    }
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET status = %s WHERE id = %s", (Jsonb(status), uuid.UUID(cid))
        )
    assert (await http.get(url(cid))).json()["edited_since_run"] is False
