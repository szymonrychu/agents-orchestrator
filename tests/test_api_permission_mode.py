"""PUT /sessions/{id}/permission-mode: in-place edit of a session and its source preset."""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from factories import insert_session
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as repo

DEFAULT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        yield c


def url(session_id) -> str:
    return f"/api/v1/sessions/{session_id}/permission-mode"


async def session_from_preset(http, name="p1", spec=None, preset_id=None):
    if preset_id is None:
        spec = {"replicas": 1} if spec is None else spec
        r = await http.post("/api/v1/presets", json={"name": name, "spec": spec})
        assert r.status_code == 201, r.text
        preset_id = r.json()["id"]
    r = await http.post("/api/v1/sessions", json={"preset_id": str(preset_id)})
    assert r.status_code == 201, r.text
    return r.json()


async def preset_spec(pool, preset_id):
    async with pool.connection() as conn:
        return (await presets_repo.get(conn, uuid.UUID(str(preset_id)))).spec


async def test_put_changes_only_that_field_and_writes_preset(http, pool):
    wrapper = {"turn_max_seconds": 5, "permission_mode": "bypassPermissions"}
    cl = await session_from_preset(
        http, spec={"replicas": 1, "model": "claude-opus-5-5", "wrapper": wrapper}
    )
    async with pool.connection() as conn:
        before = (await presets_repo.get(conn, uuid.UUID(cl["preset_id"]))).updated_at
    r = await http.put(url(cl["id"]), json={"permission_mode": "auto"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["permission_mode"] == "auto"
    assert body["preset_updated"] is True
    assert body["preset_id"] == cl["preset_id"]
    assert body["preset_name"] == "p1"
    got = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    expected = {**cl["spec"], "wrapper": {**cl["spec"]["wrapper"], "permission_mode": "auto"}}
    assert got["spec"] == expected
    assert got["spec"]["wrapper"]["turn_max_seconds"] == 5
    assert got["generation"] == cl["generation"] + 1
    spec = await preset_spec(pool, cl["preset_id"])
    assert spec["wrapper"] == {"turn_max_seconds": 5, "permission_mode": "auto"}
    assert spec["model"] == "claude-opus-5-5"
    async with pool.connection() as conn:
        assert (await presets_repo.get(conn, uuid.UUID(cl["preset_id"]))).updated_at > before


async def test_put_creates_missing_wrapper_block(http, pool):
    cl = await session_from_preset(http, name="nowrap")
    assert "wrapper" not in cl["spec"] or cl["spec"]["wrapper"] is None
    r = await http.put(url(cl["id"]), json={"permission_mode": "bypassPermissions"})
    assert r.status_code == 200, r.text
    got = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    assert got["spec"]["wrapper"]["permission_mode"] == "bypassPermissions"
    assert (await preset_spec(pool, cl["preset_id"]))["wrapper"] == {
        "permission_mode": "bypassPermissions"
    }


async def test_unchanged_value_does_not_bump_generation(http):
    cl = await session_from_preset(http, name="same")
    await http.put(url(cl["id"]), json={"permission_mode": "auto"})
    first = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    await http.put(url(cl["id"]), json={"permission_mode": "auto"})
    second = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    assert second["generation"] == first["generation"]


async def test_put_without_preset(http):
    raw = (await http.post("/api/v1/sessions", json={"slug": "raw", "spec": {}})).json()
    r = await http.put(url(raw["id"]), json={"permission_mode": "auto"})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is False
    assert r.json()["preset_id"] is None
    cl = await session_from_preset(http, name="gone")
    assert (await http.delete(f"/api/v1/presets/{cl['preset_id']}")).status_code in (200, 204)
    r = await http.put(url(cl["id"]), json={"permission_mode": "auto"})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is False
    got = (await http.get(f"/api/v1/sessions/{cl['id']}")).json()
    assert got["spec"]["wrapper"]["permission_mode"] == "auto"


async def test_put_writes_default_preset(http, pool):
    cl = await session_from_preset(http, preset_id=DEFAULT_ID)
    r = await http.put(url(cl["id"]), json={"permission_mode": "auto"})
    assert r.status_code == 200, r.text
    assert r.json()["preset_updated"] is True
    assert (await preset_spec(pool, DEFAULT_ID))["wrapper"]["permission_mode"] == "auto"


async def test_put_errors(http, pool):
    cl = await session_from_preset(http, name="errp")
    cid = cl["id"]
    for bad in ("plan", "default", "", None, 1, ["auto"]):
        assert_error(
            await http.put(url(cid), json={"permission_mode": bad}), 422, "invalid_request"
        )
    assert_error(await http.put(url(cid), json={"nope": "auto"}), 422, "invalid_request")
    assert_error(await http.put(url(cid), json={}), 422, "invalid_request")
    assert_error(
        await http.put(url(uuid.uuid4()), json={"permission_mode": "auto"}), 404, "not_found"
    )
    deleting = await insert_session(pool, desired="deleting")
    assert_error(
        await http.put(url(deleting["id"]), json={"permission_mode": "auto"}), 409, "conflict"
    )
    got = (await http.get(f"/api/v1/sessions/{cid}")).json()
    assert got["spec"] == cl["spec"] and got["generation"] == cl["generation"]
    assert "wrapper" not in await preset_spec(pool, cl["preset_id"])


async def test_other_spec_fields_stay_immutable(http):
    cl = await session_from_preset(http, name="immut")
    r = await http.patch(f"/api/v1/sessions/{cl['id']}", json={"spec": {"model": "x"}})
    assert r.status_code == 405
    r = await http.put(f"/api/v1/sessions/{cl['id']}", json={"model": "x"})
    assert r.status_code in (404, 405)


async def test_set_permission_mode_unknown(pool):
    async with pool.connection() as conn:
        with pytest.raises(repo.SessionNotFound):
            await repo.set_permission_mode(conn, uuid.uuid4(), "auto")


async def test_set_permission_mode_with_null_wrapper(pool):
    s = await insert_session(pool, spec={"replicas": 1, "wrapper": None})
    async with pool.connection() as conn:
        got, _ = await repo.set_permission_mode(conn, s["id"], "auto")
    assert got.spec.wrapper.permission_mode == "auto"
