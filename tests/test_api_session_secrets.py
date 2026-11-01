"""O4: session create snapshots the preset's protected-variable Secret versions (R6)."""

from __future__ import annotations

import asyncio
import secrets as stdlib_secrets

from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from pydantic import SecretStr
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import sessions as sessions_repo
from agents_orchestrator.k8s import secrets as store
from agents_orchestrator.spec import SessionSpec

C = "/api/v1/sessions"
NS = "agents"
ENV = {"API_KEY": {"protected": True}, "PLAIN": {"value": "v"}}


def app_client(pool, dsn):
    return client_for(create_app(make_settings(dsn), pool))


async def preset_with_env(pool, name="p"):
    async with pool.connection() as conn:
        return await presets_repo.create(conn, name, {"env": ENV})


async def put(pool, kube, preset_id, value):
    async with pool.connection() as conn:
        return await store.set_secret(conn, kube, NS, preset_id, "API_KEY", SecretStr(value))


async def stored_versions(pool, slug):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT secret_versions FROM sessions WHERE slug = %s", (slug,))
        return (await cur.fetchone())[0]


async def test_create_refuses_a_protected_variable_with_no_value(pool, dsn):
    preset = await preset_with_env(pool)
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"preset_id": str(preset.id), "slug": "unset"})
        assert_error(r, 422, "secret_unset")
        assert "API_KEY" in r.json()["error"]["message"]
        assert (await http.get(C)).json() == []


async def test_create_without_a_preset_refuses_protected_variables(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "raw", "spec": {"env": ENV}})
        assert_error(r, 422, "secret_unset")


async def test_create_snapshots_the_current_secret_version(pool, dsn):
    kube = FakeKube(namespace=NS)
    preset = await preset_with_env(pool)
    await put(pool, kube, preset.id, stdlib_secrets.token_hex(8))
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"preset_id": str(preset.id), "slug": "first"})
        assert r.status_code == 201, r.text
        await put(pool, kube, preset.id, stdlib_secrets.token_hex(8))
        r = await http.post(C, json={"preset_id": str(preset.id), "slug": "second"})
        assert r.status_code == 201, r.text
    assert await stored_versions(pool, "first") == {
        "API_KEY": store.secret_name(preset.id, "API_KEY", 1)
    }
    assert await stored_versions(pool, "second") == {
        "API_KEY": store.secret_name(preset.id, "API_KEY", 2)
    }
    # the first session still names v1, so a rotation never deleted it
    assert store.secret_name(preset.id, "API_KEY", 1) in {k[2] for k in kube.objects}


async def test_session_row_loads_its_snapshot(pool, dsn):
    kube = FakeKube(namespace=NS)
    preset = await preset_with_env(pool)
    await put(pool, kube, preset.id, stdlib_secrets.token_hex(8))
    async with app_client(pool, dsn) as http:
        made = (await http.post(C, json={"preset_id": str(preset.id), "slug": "loaded"})).json()
    async with pool.connection() as conn:
        session = await sessions_repo.get_by_slug(conn, made["slug"])
    assert session.secret_versions == {"API_KEY": store.secret_name(preset.id, "API_KEY", 1)}
    assert SessionSpec.model_validate(made["spec"]).protected_env == ["API_KEY"]


async def test_rotation_waits_for_an_uncommitted_create_so_gc_keeps_its_version(pool, dsn):
    """The GC versus create race: v1 is read inside a create transaction that has not committed;
    a concurrent rotation must wait for it, and its collection must then see the reference."""
    kube = FakeKube(namespace=NS)
    preset = await preset_with_env(pool)
    await put(pool, kube, preset.id, stdlib_secrets.token_hex(8))
    v1 = store.secret_name(preset.id, "API_KEY", 1)
    async with pool.connection() as conn, conn.transaction():
        names = await store.snapshot_versions(conn, preset.id, ["API_KEY"])
        assert names == {"API_KEY": v1}
        rotation = asyncio.create_task(put(pool, kube, preset.id, stdlib_secrets.token_hex(8)))
        await asyncio.sleep(0.5)
        assert not rotation.done()
        await conn.execute(
            "INSERT INTO sessions (id, slug, spec, secret_versions)"
            " VALUES (gen_random_uuid(), 'racer', %s, %s)",
            (Jsonb({"env": ENV}), Jsonb(names)),
        )
    await asyncio.wait_for(rotation, 10)
    assert v1 in {k[2] for k in kube.objects}


async def test_gc_never_deletes_a_version_a_session_snapshots_after_it_read_the_references(
    pool, monkeypatch
):
    """The other half of the race: gc read the stored references, then a create snapshotted the
    current version and a rotation made it old. gc must decide from the current versions it read
    and then read the references, so the version is kept."""
    kube = FakeKube(namespace=NS)
    preset = await preset_with_env(pool)
    for _ in range(2):
        async with pool.connection() as conn:
            await store.set_secret(
                conn,
                kube,
                NS,
                preset.id,
                "API_KEY",
                SecretStr(stdlib_secrets.token_hex(8)),
                collect=False,
            )
    v2 = store.secret_name(preset.id, "API_KEY", 2)
    real = store._referenced

    async def referenced_then_create_and_rotate(conn):
        names = await real(conn)
        async with pool.connection() as other:
            await other.execute(
                "INSERT INTO sessions (id, slug, spec, secret_versions)"
                " VALUES (gen_random_uuid(), 'late', %s, %s)",
                (Jsonb({"env": ENV}), Jsonb({"API_KEY": v2})),
            )
            await store.set_secret(
                other, kube, NS, preset.id, "API_KEY", SecretStr("x" * 8), collect=False
            )
        return names

    monkeypatch.setattr(store, "_referenced", referenced_then_create_and_rotate)
    async with pool.connection() as conn:
        await store.gc(conn, kube, NS, preset_id=preset.id)
    assert v2 in {k[2] for k in kube.objects}
