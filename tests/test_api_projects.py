"""Project routes: create, list, get and patch on the main port."""

from __future__ import annotations

import hashlib
import uuid

import httpx
import pytest
import pytest_asyncio
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.errors import project_error
from agents_orchestrator.db import project_posts
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


async def test_list_counts_presets_and_active_sessions(http, pool):
    await http.post("/api/v1/projects", json={"slug": "busy", "name": "Busy"})
    for name in ("p1", "p2"):
        r = await http.post("/api/v1/presets", json={"name": name, "project": "busy"})
        assert r.status_code == 201, r.text
    for slug in ("s1", "s2", "s3"):
        r = await http.post("/api/v1/sessions", json={"slug": slug, "spec": {}, "project": "busy"})
        assert r.status_code == 201, r.text
    async with pool.connection() as conn:
        await conn.execute("UPDATE sessions SET desired = 'deleting' WHERE slug = 's3'")
    rows = {p["slug"]: p for p in (await http.get("/api/v1/projects")).json()}
    assert (rows["busy"]["presets"], rows["busy"]["sessions"]) == (2, 2)
    assert (rows["default"]["presets"], rows["default"]["sessions"]) == (1, 0)


async def test_unknown_project_error_body(http):
    r = await http.post("/api/v1/sessions", json={"slug": "x", "spec": {}, "project": "nope"})
    assert r.status_code == 422
    assert r.json() == {
        "error": {"code": "unknown_project", "message": "project nope does not exist"}
    }
    r = await http.get("/api/v1/sessions", params={"project": "nope"})
    assert r.status_code == 422
    assert r.json() == {
        "error": {"code": "unknown_project", "message": "project nope does not exist"}
    }


# ------------------------------------------------------------------ project room read (T-117)


async def _seed_posts(pool, project: str, n: int) -> list[int]:
    ids = []
    async with pool.connection() as conn:
        for i in range(n):
            ids.append(
                await project_posts.insert_post(
                    conn,
                    project=project,
                    sender_kind="leader",
                    sender_session_id=uuid.uuid4(),
                    sender_slug="a-alpha",
                    text=f"post {i}",
                    reply_to=None,
                    addressed=[],
                )
            )
    return ids


async def test_project_messages_main_port_pages(http, pool):
    ids = await _seed_posts(pool, "default", 5)
    r = await http.get("/api/v1/projects/default/messages", params={"limit": 2})
    assert r.status_code == 200, r.text
    body = r.json()
    assert [p["id"] for p in body["posts"]] == [ids[4], ids[3]]
    assert body["next_before_id"] == ids[3]
    assert body["posts"][0]["sender_session_id"]
    r = await http.get(
        "/api/v1/projects/default/messages", params={"limit": 2, "before_id": ids[3]}
    )
    assert [p["id"] for p in r.json()["posts"]] == [ids[2], ids[1]]
    r = await http.get(
        "/api/v1/projects/default/messages", params={"limit": 10, "after_id": ids[2]}
    )
    assert [p["id"] for p in r.json()["posts"]] == [ids[3], ids[4]]
    assert r.json()["next_before_id"] is None
    r = await http.get("/api/v1/projects/default/messages", params={"before_id": 1, "after_id": 1})
    assert_error(r, 422, "invalid_cursor")
    r = await http.get("/api/v1/projects/default/messages", params={"limit": 101})
    assert r.status_code == 422


async def test_project_messages_unknown_project_404(http):
    r = await http.get("/api/v1/projects/nope/messages")
    assert_error(r, 404, "not_found")


async def test_project_messages_archived_readable(http, pool):
    await http.post("/api/v1/projects", json={"slug": "old", "name": "Old"})
    ids = await _seed_posts(pool, "old", 1)
    r = await http.patch("/api/v1/projects/old", json={"archived": True})
    assert r.status_code == 200, r.text
    r = await http.get("/api/v1/projects/old/messages")
    assert r.status_code == 200, r.text
    assert [p["id"] for p in r.json()["posts"]] == ids


async def test_project_messages_not_on_service_port(pool, dsn):
    token = "svc-example-token-value"
    digest = hashlib.sha256(token.encode()).hexdigest()
    app = create_app(make_settings(dsn, service_tokens=f"agents-webhook:{digest}"), pool)
    transport = httpx.ASGITransport(app=app, client=("203.0.113.9", 4444))
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://orch:8002",
        headers={"Authorization": f"Bearer {token}"},
    ) as c:
        r = await c.get("/api/v1/projects/default/messages")
    assert r.status_code == 403, r.text
