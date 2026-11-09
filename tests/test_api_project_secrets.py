"""Project secret routes on the main port: write-only values, metadata reads (projects 3.16)."""

from __future__ import annotations

import logging
import secrets as stdlib_secrets

import httpx
import pytest
from factories import insert_session
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from test_api_sessions import assert_error, client_for, make_settings
from test_service_port import TOKEN, TOKENS

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.projects import PROJECT_SECRETS_MAX
from agents_orchestrator.api.service_auth import route_allowed
from agents_orchestrator.k8s import secrets as store

NS = "agents"
SLUG = "alpha"
S = f"/api/v1/projects/{SLUG}/secrets"


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


@pytest.fixture
def app(pool, dsn, kube):
    return create_app(make_settings(dsn, namespace=NS, service_tokens=TOKENS), pool, kube=kube)


@pytest.fixture
async def http(app):
    async with client_for(app) as c:
        r = await c.post("/api/v1/projects", json={"slug": SLUG, "name": "Alpha"})
        assert r.status_code == 201, r.text
        yield c


def live_secrets(kube) -> set[str]:
    return {k[2] for k in kube.objects if k[0] == "Secret"}


async def put(http, name, value, slug=SLUG):
    return await http.put(f"/api/v1/projects/{slug}/secrets/{name}", json={"value": value})


async def test_set_list_rotate_delete_roundtrip(http, kube):
    r = await put(http, "API_KEY", "one")
    assert r.status_code == 200, r.text
    assert r.json() == {"name": "API_KEY", "version": 1}
    first = store.secret_name(store.Owner.project(SLUG), "API_KEY", 1)
    assert live_secrets(kube) == {first}

    r = await http.get(S)
    assert r.status_code == 200, r.text
    [row] = r.json()
    assert set(row) == {"name", "version", "updated_at", "sessions_on_old_version"}
    assert row["name"] == "API_KEY" and row["version"] == 1
    assert row["sessions_on_old_version"] == []

    r = await put(http, "API_KEY", "two")
    assert r.json() == {"name": "API_KEY", "version": 2}
    # no session names v1, so the rotation collected it
    assert live_secrets(kube) == {store.secret_name(store.Owner.project(SLUG), "API_KEY", 2)}
    assert [s["version"] for s in (await http.get(S)).json()] == [2]

    r = await http.delete(f"{S}/API_KEY")
    assert r.status_code == 204, r.text
    assert (await http.get(S)).json() == []
    assert live_secrets(kube) == set()
    assert_error(await http.delete(f"{S}/API_KEY"), 404, "not_found")


async def test_value_never_returned(http, caplog, canary):
    caplog.set_level(logging.DEBUG)
    texts = [(await put(http, "TOKEN_A", canary)).text]
    texts.append((await http.get(S)).text)
    texts.append((await put(http, "TOKEN_A", canary + "-2")).text)
    texts.append((await http.get(S)).text)
    texts.append((await http.delete(f"{S}/TOKEN_A")).text)
    texts.append((await http.get(S)).text)
    for text in texts:
        assert canary not in text
    assert canary not in caplog.text


async def test_cap_413(http, kube):
    r = await put(http, "BIG", "x" * (1024 * 1024 + 1))
    assert_error(r, 413, "secret_too_large")
    assert live_secrets(kube) == set()
    assert (await put(http, "BIG", "x" * (1024 * 1024))).status_code == 200


async def test_101st_secret_422(http, pool):
    async with pool.connection() as conn:
        for i in range(PROJECT_SECRETS_MAX - 1):
            await conn.execute(
                "INSERT INTO project_secrets (project, name, version) VALUES (%s, %s, 1)",
                (SLUG, f"S_{i:03d}"),
            )
    assert (await put(http, "LAST", "v")).status_code == 200
    assert_error(await put(http, "ONE_TOO_MANY", "v"), 422, "too_many_secrets")
    # rotating an existing secret at the cap is not a new one
    assert (await put(http, "LAST", "w")).json() == {"name": "LAST", "version": 2}
    # a retired secret frees its slot
    assert (await http.delete(f"{S}/LAST")).status_code == 204
    assert (await put(http, "ONE_TOO_MANY", "v")).status_code == 200


@pytest.mark.parametrize("name", ["lower", "1ABC", "CLAUDE_X", "ORCH_X", "POD_NAME", "A_PROXY"])
async def test_name_outside_env_rule_422(http, name):
    assert_error(await put(http, name, "v"), 422, "invalid_request")


async def test_archived_project_422(http, kube):
    assert (await put(http, "KEEP", "v")).status_code == 200
    r = await http.patch(f"/api/v1/projects/{SLUG}", json={"archived": True})
    assert r.status_code == 200, r.text
    assert_error(await put(http, "KEEP", "w"), 422, "project_archived")
    assert_error(await put(http, "NEW", "w"), 422, "project_archived")
    assert_error(await http.delete(f"{S}/KEEP"), 422, "project_archived")
    # reads keep working and the stored secret is untouched
    r = await http.get(S)
    assert r.status_code == 200
    assert [(s["name"], s["version"]) for s in r.json()] == [("KEEP", 1)]
    assert len(live_secrets(kube)) == 1


async def test_unknown_project_404(http, kube):
    assert_error(await put(http, "A", "v", slug="nope"), 404, "not_found")
    assert_error(await http.get("/api/v1/projects/nope/secrets"), 404, "not_found")
    assert_error(await http.delete("/api/v1/projects/nope/secrets/A"), 404, "not_found")
    assert_error(await http.delete(f"{S}/NEVER_SET"), 404, "not_found")
    assert live_secrets(kube) == set()


async def test_sessions_on_old_version_listed(http, pool, kube):
    owner = store.Owner.project(SLUG)
    assert (await put(http, "API_KEY", "one")).status_code == 200
    assert (await put(http, "OTHER", "x")).status_code == 200
    old = await insert_session(pool, slug="on-v1", project=SLUG)
    gone = await insert_session(pool, slug="deleting", project=SLUG, desired="deleting")
    current = await insert_session(pool, slug="on-v2")
    # a preset variable of the same name (another Secret) is not this project's secret
    preset_owner = store.Owner.preset(old["id"])
    async with pool.connection() as conn:
        for sess, versions in [
            (old, {"API_KEY": store.secret_name(owner, "API_KEY", 1)}),
            (gone, {"API_KEY": store.secret_name(owner, "API_KEY", 1)}),
        ]:
            await conn.execute(
                "UPDATE sessions SET secret_versions = %s WHERE id = %s",
                (Jsonb(versions), sess["id"]),
            )
    assert (await put(http, "API_KEY", "two")).json()["version"] == 2
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (
                Jsonb(
                    {
                        "API_KEY": store.secret_name(owner, "API_KEY", 2),
                        "OTHER": store.secret_name(preset_owner, "OTHER", 1),
                    }
                ),
                current["id"],
            ),
        )
    rows = {r["name"]: r for r in (await http.get(S)).json()}
    assert rows["API_KEY"]["version"] == 2
    assert rows["API_KEY"]["sessions_on_old_version"] == [
        {"id": str(old["id"]), "slug": "on-v1", "version": 1}
    ]
    assert rows["OTHER"]["sessions_on_old_version"] == []
    # v1 is still named by a session snapshot, so it was kept
    assert store.secret_name(owner, "API_KEY", 1) in live_secrets(kube)


async def test_service_and_agent_ports_have_no_secret_routes(app, http, kube):
    assert (await put(http, "API_KEY", "v")).status_code == 200
    for method in ("GET", "PUT", "DELETE"):
        assert not route_allowed(method, S)
        assert not route_allowed(method, f"{S}/API_KEY")
    calls = [("GET", S), ("PUT", f"{S}/API_KEY"), ("DELETE", f"{S}/API_KEY")]
    settings = app.state.settings
    for port, status in [(settings.service_port, 403), (settings.agent_port, 404)]:
        transport = httpx.ASGITransport(app=app, client=("10.1.2.3", 5555))
        headers = {"Authorization": f"Bearer {TOKEN}"}
        base = f"http://orch:{port}"
        async with httpx.AsyncClient(transport=transport, base_url=base, headers=headers) as c:
            for method, path in calls:
                r = await c.request(method, path, json={"value": "w"} if method == "PUT" else None)
                assert r.status_code == status, (port, method, path, r.text)
    agent_paths = [p for p in app.openapi()["paths"] if p.startswith("/agent/v1")]
    assert [p for p in agent_paths if "secrets" in p and "projects" in p] == []
    # nothing was written or retired through the other ports
    assert [s["version"] for s in (await http.get(S)).json()] == [1]
    assert len(live_secrets(kube)) == 1
