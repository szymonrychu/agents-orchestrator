"""Project routes: create, list, get and patch on the main port."""

from __future__ import annotations

import pytest
import pytest_asyncio
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.errors import project_error
from agents_orchestrator.db import projects as repo


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        yield c


async def test_create_list_get_patch_roundtrip(http):
    r = await http.post("/api/v1/projects", json={"slug": "alpha", "name": "Alpha"})
    assert r.status_code == 201, r.text
    created = r.json()
    assert created["slug"] == "alpha"
    assert created["name"] == "Alpha"
    assert created["description"] == ""
    assert created["archived"] is False
    assert created["created_at"] and created["updated_at"]

    r = await http.get("/api/v1/projects")
    assert r.status_code == 200
    rows = {p["slug"]: p for p in r.json()}
    assert set(rows) == {"default", "alpha"}
    assert rows["alpha"]["presets"] == 0 and rows["alpha"]["sessions"] == 0
    assert rows["default"]["presets"] >= 1

    r = await http.get("/api/v1/projects/alpha")
    assert r.status_code == 200
    assert r.json()["presets"] == 0 and r.json()["sessions"] == 0

    r = await http.patch("/api/v1/projects/alpha", json={"name": "Alpha 2", "description": "notes"})
    assert r.status_code == 200, r.text
    assert r.json()["name"] == "Alpha 2" and r.json()["description"] == "notes"

    r = await http.patch("/api/v1/projects/alpha", json={"archived": True})
    assert r.status_code == 200
    assert r.json()["archived"] is True
    r = await http.patch("/api/v1/projects/alpha", json={"archived": False})
    assert r.json()["archived"] is False


@pytest.mark.parametrize("slug", ["Bad", "-x", "has space", "a" * 41, ""])
async def test_create_bad_slug_422(http, slug):
    r = await http.post("/api/v1/projects", json={"slug": slug, "name": "X"})
    assert_error(r, 422, "invalid_request")


async def test_create_bad_name_422(http):
    r = await http.post("/api/v1/projects", json={"slug": "ok", "name": ""})
    assert_error(r, 422, "invalid_request")


async def test_create_duplicate_409_project_exists(http):
    body = {"slug": "dup", "name": "Dup"}
    assert (await http.post("/api/v1/projects", json=body)).status_code == 201
    assert_error(await http.post("/api/v1/projects", json=body), 409, "project_exists")


async def test_patch_slug_in_body_422(http):
    await http.post("/api/v1/projects", json={"slug": "beta", "name": "Beta"})
    r = await http.patch("/api/v1/projects/beta", json={"slug": "other"})
    assert_error(r, 422, "invalid_request")


async def test_archive_default_409_default_project(http):
    r = await http.patch("/api/v1/projects/default", json={"archived": True})
    assert_error(r, 409, "default_project")


async def test_get_unknown_404(http):
    assert_error(await http.get("/api/v1/projects/nope"), 404, "not_found")
    assert_error(await http.patch("/api/v1/projects/nope", json={"name": "x"}), 404, "not_found")


async def test_list_archived_param(http):
    await http.post("/api/v1/projects", json={"slug": "old", "name": "Old"})
    await http.patch("/api/v1/projects/old", json={"archived": True})
    assert {p["slug"] for p in (await http.get("/api/v1/projects")).json()} == {"default"}
    r = await http.get("/api/v1/projects", params={"archived": "true"})
    assert [p["slug"] for p in r.json()] == ["old"]
    assert r.json()[0]["archived"] is True


@pytest.mark.parametrize(
    ("exc", "status", "code"),
    [
        (repo.UnknownProject("x"), 422, "unknown_project"),
        (repo.ProjectArchived("x"), 422, "project_archived"),
        (repo.DefaultProject("x"), 409, "default_project"),
        (repo.ProjectExists("x"), 409, "project_exists"),
    ],
)
def test_project_error_mapping(exc, status, code):
    err = project_error(exc)
    assert (err.status, err.code) == (status, code)


def test_project_error_rejects_other_exceptions():
    with pytest.raises(TypeError):
        project_error(ValueError("x"))
