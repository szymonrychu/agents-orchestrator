"""O5: protected variables are redacted on every read and kept when a write leaves them alone."""

from __future__ import annotations

import json
import secrets as stdlib_secrets
import uuid

import pytest
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import export
from agents_orchestrator.k8s import secrets as store

P = "/api/v1/presets"
C = "/api/v1/sessions"
NS = "agents"


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def canary():
    return "canary-" + stdlib_secrets.token_hex(16)


@pytest.fixture
def make_http(pool, dsn, kube):
    def build(port_peer=None, **settings):
        app = create_app(make_settings(dsn, max_replicas=4, **settings), pool, kube=kube)
        return client_for(app) if port_peer is None else client_for(app, peer=port_peer)

    return build


def spec_with(value=None, **extra):
    entry = {"protected": True} if value is None else {"protected": True, "value": value}
    return {"env": {"API_KEY": entry, "PLAIN": {"value": "visible"}}, **extra}


def secret_values(kube):
    """Values of the Secrets that still exist (the fake drops stringData: read the applies)."""
    live = {k[2] for k in kube.objects if k[0] == "Secret"}
    return [
        a.obj["stringData"]["value"] for a in kube.applies if a.kind == "Secret" and a.name in live
    ]


async def create(http, name, spec):
    r = await http.post(P, json={"name": name, "spec": spec})
    assert r.status_code == 201, r.text
    return r.json()


async def test_create_with_a_value_stores_it_and_returns_metadata_only(make_http, kube, canary):
    async with make_http() as http:
        r = await http.post(P, json={"name": "p", "spec": spec_with(canary)})
        assert r.status_code == 201, r.text
        body = r.json()
        assert canary not in r.text
        entry = body["spec"]["env"]["API_KEY"]
        assert entry["protected"] is True and entry["set"] is True and entry["version"] == 1
        assert entry["expand"] is False and "updated_at" in entry and "value" not in entry
        assert body["spec"]["env"]["PLAIN"] == {"value": "visible"}
        got = await http.get(f"{P}/{body['id']}")
        assert got.json() == body and canary not in got.text
        assert canary not in (await http.get(P)).text
    assert secret_values(kube) == [canary]


async def test_unset_protected_entry_reads_set_false(make_http):
    async with make_http() as http:
        body = await create(http, "p", spec_with())
        entry = body["spec"]["env"]["API_KEY"]
        assert entry == {"protected": True, "expand": False, "set": False}


async def test_put_without_a_value_keeps_it_and_with_a_value_overwrites(make_http, kube, canary):
    other = "canary-" + stdlib_secrets.token_hex(16)
    async with make_http() as http:
        body = await create(http, "p", spec_with(canary))
        url = f"{P}/{body['id']}"
        kept = await http.put(url, json={"name": "p", "spec": spec_with(replicas=2)})
        assert kept.status_code == 200, kept.text
        assert kept.json()["spec"]["env"]["API_KEY"]["version"] == 1
        assert secret_values(kube) == [canary]
        over = await http.put(url, json={"name": "p", "spec": spec_with(other)})
        entry = over.json()["spec"]["env"]["API_KEY"]
        assert entry["version"] == 2 and entry["set"] is True
        assert canary not in over.text and other not in over.text
    assert other in secret_values(kube)


async def test_put_dropping_the_variable_deletes_it_and_its_secret(make_http, kube, canary):
    async with make_http() as http:
        body = await create(http, "p", spec_with(canary))
        r = await http.put(f"{P}/{body['id']}", json={"name": "p", "spec": {"replicas": 1}})
        assert r.status_code == 200, r.text
        assert "env" not in r.json()["spec"]
    assert secret_values(kube) == []


async def test_flipping_protected_to_false_needs_a_plaintext_value(make_http, canary):
    async with make_http() as http:
        body = await create(http, "p", spec_with(canary))
        url = f"{P}/{body['id']}"
        bad = await http.put(url, json={"name": "p", "spec": {"env": {"API_KEY": {}}}})
        assert_error(bad, 422, "invalid_request")
        ok = await http.put(
            url, json={"name": "p", "spec": {"env": {"API_KEY": {"value": "now-visible"}}}}
        )
        assert ok.status_code == 200, ok.text
        assert ok.json()["spec"]["env"]["API_KEY"] == {"value": "now-visible"}


async def test_secret_route_sets_a_protected_variable_and_returns_metadata(make_http, kube, canary):
    async with make_http() as http:
        body = await create(http, "p", spec_with())
        url = f"{P}/{body['id']}/secrets/API_KEY"
        r = await http.put(url, json={"value": canary})
        assert r.status_code == 200, r.text
        assert canary not in r.text
        meta = r.json()
        assert set(meta) == {"name", "set", "version", "updated_at"}
        assert (meta["name"], meta["set"], meta["version"]) == ("API_KEY", True, 1)
        by_name = await http.put(f"{P}/p/secrets/API_KEY", json={"value": canary + "2"})
        assert by_name.status_code == 200 and by_name.json()["version"] == 2
        assert (await http.get(f"{P}/{body['id']}")).json()["spec"]["env"]["API_KEY"][
            "version"
        ] == 2
    assert secret_values(kube) == [canary + "2"]


@pytest.mark.parametrize("name", ["lower", "1BAD", "A-B", "POD_NAME", "ORCH_X", "PLAIN", "NOPE"])
async def test_secret_route_rejects_invalid_or_undeclared_names(make_http, canary, name):
    async with make_http() as http:
        body = await create(http, "p", spec_with())
        r = await http.put(f"{P}/{body['id']}/secrets/{name}", json={"value": canary})
        assert r.status_code in (404, 422), r.text
        assert canary not in r.text


async def test_secret_route_unknown_preset_404(make_http, canary):
    async with make_http() as http:
        r = await http.put(f"{P}/{uuid.uuid4()}/secrets/API_KEY", json={"value": canary})
        assert_error(r, 404, "not_found")
        assert canary not in r.text


async def test_secret_route_size_cap_and_errors_echo_nothing(make_http, kube):
    big = "x" * (1024 * 1024 + 1)
    async with make_http() as http:
        body = await create(http, "p", spec_with())
        url = f"{P}/{body['id']}/secrets/API_KEY"
        r = await http.put(url, json={"value": big})
        assert r.status_code == 413, r.text[:200]
        assert big[:50] not in r.text
        at_cap = await http.put(url, json={"value": "y" * (1024 * 1024)})
        assert at_cap.status_code == 200
        for bad in ({}, {"value": ""}, {"value": 5}, {"value": "v", "x": 1}):
            r = await http.put(url, json=bad)
            assert_error(r, 422, "invalid_request")


async def test_secret_store_failure_is_generic(make_http, kube, canary):
    from agents_orchestrator.k8s.kube import KubeError

    async with make_http() as http:
        body = await create(http, "p", spec_with())
        kube.fail(
            "Secret",
            store.secret_name(uuid.UUID(body["id"]), "API_KEY", 1),
            KubeError(f"boom {canary}"),
        )
        r = await http.put(f"{P}/{body['id']}/secrets/API_KEY", json={"value": canary})
        assert r.status_code == 502, r.text
        assert canary not in r.text


async def test_copy_yields_set_false(make_http, canary):
    async with make_http() as http:
        body = await create(http, "p", spec_with(canary))
        copy = await http.post(f"{P}/{body['id']}/copy")
        assert copy.status_code == 201, copy.text
        assert copy.json()["spec"]["env"]["API_KEY"]["set"] is False
        assert canary not in copy.text


async def test_delete_preset_collects_its_secrets(make_http, kube, canary):
    async with make_http() as http:
        await create(http, "keep", {})
        body = await create(http, "gone", spec_with(canary))
        assert (await http.delete(f"{P}/{body['id']}")).status_code == 204
    assert secret_values(kube) == []


async def test_every_read_shape_redacts_even_a_value_in_the_database(make_http, pool, kube, canary):
    """A value in a stored preset (sessions refuse to load one) is still never read back."""
    async with make_http() as http:
        body = await create(http, "p", spec_with(canary))
        pid = body["id"]
        made = (await http.post(C, json={"preset_id": pid, "slug": "s1"})).json()
        async with pool.connection() as conn:
            for table, key in (("presets", pid),):
                cur = await conn.execute(f"SELECT spec FROM {table} WHERE id = %s", (key,))  # noqa: S608
                spec = (await cur.fetchone())[0]
                spec["env"]["API_KEY"]["value"] = canary
                await conn.execute(
                    f"UPDATE {table} SET spec = %s WHERE id = %s",  # noqa: S608
                    (Jsonb(spec), key),
                )
        for url in (P, f"{P}/{pid}", C, f"{C}/{made['id']}"):
            r = await http.get(url)
            assert r.status_code == 200 and canary not in r.text, url
        session = (await http.get(f"{C}/{made['id']}")).json()
        entry = session["spec"]["env"]["API_KEY"]
        assert entry["protected"] is True and entry["set"] is True and entry["version"] == 1
        # the export a pre-discard hook receives
        async with pool.connection() as conn:
            lines = [ln async for ln in export.export_session_history(conn, uuid.UUID(made["id"]))]
        assert canary not in "".join(lines)
        first = json.loads(lines[0])
        assert first["spec"]["env"]["API_KEY"]["set"] is True


async def test_service_port_reads_are_redacted(pool, dsn, kube, canary):
    import hashlib

    token = "svc-" + stdlib_secrets.token_hex(8)
    digest = hashlib.sha256(token.encode()).hexdigest()
    app = create_app(make_settings(dsn, service_tokens=f"agents-webhook:{digest}"), pool, kube=kube)
    async with make_http_for(app) as http:
        body = await create(http, "p", spec_with(canary))
        made = (await http.post(C, json={"preset_id": body["id"], "slug": "s1"})).json()
    headers = {"Authorization": f"Bearer {token}"}
    async with make_http_for(app, 8002, ("203.0.113.9", 4444), headers) as svc:
        for url in (P, f"{P}/{body['id']}", C, f"{C}/{made['id']}"):
            r = await svc.get(url)
            assert r.status_code == 200, (url, r.text)
            assert canary not in r.text
            assert "API_KEY" in r.text


def make_http_for(app, port=8000, peer=None, headers=None):
    import httpx
    from test_api_sessions import PEER

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, client=peer or PEER),
        base_url=f"http://orch:{port}",
        headers=headers or {},
    )


async def test_plain_string_env_entry_gets_a_clear_422(make_http):
    async with make_http() as http:
        bad = {"env": {"FOO": "bar", "OK": {"value": "v"}}}
        r = await http.post(P, json={"name": "p", "spec": bad})
        assert_error(r, 422, "invalid_request")
        msg = r.json()["error"]["message"]
        assert "env.FOO must be an object {value, protected, expand}" in msg
        assert "env.OK" not in msg
        r = await http.post(C, json={"slug": "x", "spec": {"env": {"FOO": "bar"}}})
        assert (
            "env.FOO must be an object {value, protected, expand}" in r.json()["error"]["message"]
        )
