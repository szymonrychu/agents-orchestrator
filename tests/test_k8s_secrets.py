"""The preset secret store: immutable versioned k8s Secrets, rows in preset_secrets (D5, D7)."""

from __future__ import annotations

import hashlib
import logging
import secrets as stdlib_secrets
import uuid

import pytest
from factories import insert_session
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from pydantic import SecretStr

from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.k8s import secrets as store
from agents_orchestrator.k8s.kube import KubeError

NS = "agents"


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def canary():
    return stdlib_secrets.token_hex(16)


async def make_preset(pool, name="p") -> uuid.UUID:
    async with pool.connection() as conn:
        return (await presets_repo.create(conn, name, {})).id


async def put(pool, kube, preset_id, name, value):
    async with pool.connection() as conn:
        return await store.set_secret(conn, kube, NS, preset_id, name, SecretStr(value))


def secret_objects(kube):
    return {k[2]: v for k, v in kube.objects.items() if k[0] == "Secret"}


async def rows(pool, preset_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT name, version, retired_at IS NOT NULL FROM preset_secrets"
            " WHERE preset_id = %s ORDER BY name",
            (preset_id,),
        )
        return await cur.fetchall()


def test_secret_name_shape():
    pid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    got = store.secret_name(pid, "API_TOKEN", 3)
    assert got == f"ps-12345678-{hashlib.sha256(b'API_TOKEN').hexdigest()[:6]}-v3"


async def test_set_writes_an_immutable_secret_and_returns_metadata_only(pool, kube, canary):
    pid = await make_preset(pool)
    meta = await put(pool, kube, pid, "TOK", canary)
    assert (meta.name, meta.set, meta.version) == ("TOK", True, 1)
    name = store.secret_name(pid, "TOK", 1)
    applied = [a for a in kube.applies if a.kind == "Secret"]
    assert [a.name for a in applied] == [name]
    obj = applied[0].obj
    assert obj["immutable"] is True
    assert obj["stringData"] == {"value": canary}
    assert obj["metadata"]["namespace"] == NS
    assert canary not in repr(meta)
    assert await rows(pool, pid) == [("TOK", 1, False)]


async def test_version_increments_and_old_unreferenced_version_is_collected(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    meta = await put(pool, kube, pid, "TOK", canary + "x")
    assert meta.version == 2
    assert set(secret_objects(kube)) == {store.secret_name(pid, "TOK", 2)}
    assert await rows(pool, pid) == [("TOK", 2, False)]


async def test_version_referenced_by_a_session_snapshot_survives_gc(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    v1 = store.secret_name(pid, "TOK", 1)
    s = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (Jsonb({"TOK": v1}), s["id"]),
        )
    await put(pool, kube, pid, "TOK", canary + "x")
    assert set(secret_objects(kube)) == {v1, store.secret_name(pid, "TOK", 2)}
    # the session goes away: the next pass collects the version
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (s["id"],))
        await store.gc(conn, kube, NS)
    assert set(secret_objects(kube)) == {store.secret_name(pid, "TOK", 2)}


async def test_failure_leaves_the_old_version_live(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    kube.fail("Secret", store.secret_name(pid, "TOK", 2), KubeError(f"422: body {canary}"))
    with pytest.raises(store.SecretStoreError) as err:
        await put(pool, kube, pid, "TOK", canary + "x")
    assert canary not in str(err.value) and canary not in repr(err.value)
    assert err.value.__cause__ is None and err.value.__suppress_context__
    assert await rows(pool, pid) == [("TOK", 1, False)]
    assert store.secret_name(pid, "TOK", 1) in secret_objects(kube)
    kube.heal("Secret", store.secret_name(pid, "TOK", 2))
    assert (await put(pool, kube, pid, "TOK", canary + "y")).version == 2


async def test_removing_a_declared_name_deletes_rows_and_secrets(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "KEEP", canary)
    await put(pool, kube, pid, "DROP", canary)
    async with pool.connection() as conn:
        await store.sync_declared(conn, kube, NS, pid, ["KEEP"])
    assert await rows(pool, pid) == [("KEEP", 1, False)]
    assert set(secret_objects(kube)) == {store.secret_name(pid, "KEEP", 1)}


async def test_removed_name_with_a_live_reference_is_retired_then_collected(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "DROP", canary)
    v1 = store.secret_name(pid, "DROP", 1)
    s = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s", (Jsonb({"DROP": v1}), s["id"])
        )
        await store.sync_declared(conn, kube, NS, pid, [])
    assert v1 in secret_objects(kube)
    assert await rows(pool, pid) == [("DROP", 1, True)]
    async with pool.connection() as conn:
        assert await store.list_secrets(conn, pid) == []
        await conn.execute("DELETE FROM sessions WHERE id = %s", (s["id"],))
        await store.gc(conn, kube, NS)
    assert secret_objects(kube) == {}
    assert await rows(pool, pid) == []


async def test_redeclared_retired_name_keeps_counting_up(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    s = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (Jsonb({"TOK": store.secret_name(pid, "TOK", 1)}), s["id"]),
        )
        await store.sync_declared(conn, kube, NS, pid, [])
    meta = await put(pool, kube, pid, "TOK", canary)
    assert meta.version == 2  # a live session still names v1, so the name is never reused
    assert await rows(pool, pid) == [("TOK", 2, False)]


async def test_gc_collects_everything_of_a_deleted_preset(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    async with pool.connection() as conn:
        await presets_repo.delete(conn, pid)
        await store.gc(conn, kube, NS)
    assert secret_objects(kube) == {}
    assert await rows(pool, pid) == []


async def test_list_secrets_is_masked_metadata(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "B", canary)
    await put(pool, kube, pid, "A", canary)
    async with pool.connection() as conn:
        got = await store.list_secrets(conn, pid)
    assert [(m.name, m.set, m.version) for m in got] == [("A", True, 1), ("B", True, 1)]
    assert all(m.updated_at is not None for m in got)
    assert canary not in repr(got)


async def test_nothing_logs_the_value(pool, kube, canary, caplog):
    caplog.set_level(logging.DEBUG)
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    kube.fail("Secret", store.secret_name(pid, "TOK", 2), KubeError(f"body {canary}"))
    with pytest.raises(store.SecretStoreError):
        await put(pool, kube, pid, "TOK", canary + "x")
    assert canary not in caplog.text
    assert not any(canary in str(r.args) or canary in r.getMessage() for r in caplog.records)


async def test_apply_response_is_dropped_not_returned(pool, kube, canary):
    pid = await make_preset(pool)
    meta = await put(pool, kube, pid, "TOK", canary)
    assert set(vars(meta)) == {"name", "set", "version", "updated_at"}
