"""The project as a secret owner: rows in project_secrets, Secrets `prs-*` (projects 3.16)."""

from __future__ import annotations

import hashlib
import secrets as stdlib_secrets
import uuid

import pytest
from factories import insert_session
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from pydantic import SecretStr

from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import projects as projects_repo
from agents_orchestrator.k8s import secrets as store
from agents_orchestrator.k8s.labels import MANAGED_BY, MANAGED_BY_LABEL

NS = "agents"
SLUG = "alpha"


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def canary():
    return stdlib_secrets.token_hex(16)


@pytest.fixture
async def owner(pool) -> store.Owner:
    async with pool.connection() as conn:
        await projects_repo.create_project(conn, SLUG, "Alpha")
    return store.Owner.project(SLUG)


async def put(pool, kube, owner, name, value):
    async with pool.connection() as conn:
        return await store.set_secret(conn, kube, NS, owner, name, SecretStr(value))


def secret_objects(kube):
    return {k[2]: v for k, v in kube.objects.items() if k[0] == "Secret"}


async def rows(pool, slug=SLUG):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT name, version, retired_at IS NOT NULL FROM project_secrets"
            " WHERE project = %s ORDER BY name",
            (slug,),
        )
        return await cur.fetchall()


def test_project_secret_name_shape():
    got = store.secret_name(store.Owner.project("alpha"), "API_TOKEN", 3)
    h8 = hashlib.sha256(b"alpha").hexdigest()[:8]
    h6 = hashlib.sha256(b"API_TOKEN").hexdigest()[:6]
    assert got == f"prs-{h8}-{h6}-v3"
    assert store.secret_version(got) == 3


def test_preset_secret_name_shape_unchanged():
    pid = uuid.UUID("12345678-1234-5678-1234-567812345678")
    got = store.secret_name(store.Owner.preset(pid), "API_TOKEN", 3)
    assert got == f"ps-12345678-{hashlib.sha256(b'API_TOKEN').hexdigest()[:6]}-v3"


async def test_set_project_secret_versions(pool, kube, owner, canary):
    first = await put(pool, kube, owner, "TOK", canary)
    second = await put(pool, kube, owner, "TOK", canary + "x")
    assert (first.version, second.version) == (1, 2)
    v1, v2 = (store.secret_name(owner, "TOK", v) for v in (1, 2))
    assert set(secret_objects(kube)) == {v2}  # no session names v1, so the write collected it
    applied = [a for a in kube.applies if a.kind == "Secret"]
    assert [a.name for a in applied] == [v1, v2]  # a new name per version, never a patch
    obj = applied[1].obj
    assert obj["immutable"] is True
    assert obj["stringData"] == {store.VALUE_KEY: canary + "x"}
    assert obj["metadata"]["labels"] == {MANAGED_BY_LABEL: MANAGED_BY, "project": SLUG}
    assert await rows(pool) == [("TOK", 2, False)]
    async with pool.connection() as conn:
        metas = await store.list_metas(conn, owner)
    assert [(m.name, m.set, m.version) for m in metas] == [("TOK", True, 2)]
    assert canary not in repr(metas)


async def test_set_secret_of_missing_project_writes_nothing(pool, kube, canary):
    with pytest.raises(store.ProjectNotFound):
        await put(pool, kube, store.Owner.project("ghost"), "TOK", canary)
    assert secret_objects(kube) == {}


async def test_project_and_preset_secrets_are_separate(pool, kube, owner, canary):
    async with pool.connection() as conn:
        pid = (await presets_repo.create(conn, "p", {}, project=SLUG)).id
    preset = store.Owner.preset(pid)
    await put(pool, kube, preset, "TOK", canary)
    await put(pool, kube, owner, "TOK", canary)
    await put(pool, kube, owner, "TOK", canary)
    async with pool.connection() as conn:
        assert [m.version for m in await store.list_metas(conn, preset)] == [1]
        assert [m.version for m in await store.list_metas(conn, owner)] == [2]
        got = await store.snapshot_versions(conn, owner, ["TOK"])
    assert got == {"TOK": store.secret_name(owner, "TOK", 2)}
    assert set(secret_objects(kube)) == {
        store.secret_name(preset, "TOK", 1),
        store.secret_name(owner, "TOK", 2),
    }


async def test_project_lock_key_distinct_from_preset(pool):
    pid = uuid.uuid4()
    keys = []
    async with pool.connection() as conn:
        for owner in (store.Owner.preset(pid), store.Owner.project("alpha")):
            async with conn.transaction():
                await store._lock(conn, owner, "TOK")
                cur = await conn.execute(
                    "SELECT objid::bigint FROM pg_locks"
                    " WHERE locktype = 'advisory' AND pid = pg_backend_pid()"
                )
                keys.append({r[0] for r in await cur.fetchall()})
    assert keys[0] and keys[1] and keys[0] != keys[1]
    assert store.Owner.preset(pid).lock_key("TOK") == f"{pid}/TOK"
    assert store.Owner.project("alpha").lock_key("TOK") == "project:alpha/TOK"


async def test_snapshot_versions_raises_for_unset(pool, kube, owner, canary):
    await put(pool, kube, owner, "A", canary)
    async with pool.connection() as conn:
        with pytest.raises(store.SecretUnset) as err:
            await store.snapshot_versions(conn, owner, iter(["A", "B"]))
    assert err.value.names == ["B"]


async def test_retire_then_gc_deletes_unreferenced_versions(pool, kube, owner, canary):
    await put(pool, kube, owner, "TOK", canary)
    await put(pool, kube, owner, "KEEP", canary)
    async with pool.connection() as conn:
        await store.retire(conn, owner, "TOK")
        assert [m.name for m in await store.list_metas(conn, owner)] == ["KEEP"]
        with pytest.raises(store.SecretUnset):
            await store.snapshot_versions(conn, owner, ["TOK"])
    assert await rows(pool) == [("KEEP", 1, False), ("TOK", 1, True)]
    async with pool.connection() as conn:
        await store.gc(conn, kube, NS)
    assert set(secret_objects(kube)) == {store.secret_name(owner, "KEEP", 1)}
    assert await rows(pool) == [("KEEP", 1, False)]


async def test_gc_keeps_version_named_by_session(pool, kube, owner, canary):
    await put(pool, kube, owner, "TOK", canary)
    v1 = store.secret_name(owner, "TOK", 1)
    s = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (Jsonb({"TOK": v1}), s["id"]),
        )
    await put(pool, kube, owner, "TOK", canary + "x")
    v2 = store.secret_name(owner, "TOK", 2)
    assert set(secret_objects(kube)) == {v1, v2}
    async with pool.connection() as conn:
        await store.retire(conn, owner, "TOK")
        await store.gc(conn, kube, NS, owner=owner)
    assert set(secret_objects(kube)) == {v1}  # retired: v2 goes, the session still names v1
    assert await rows(pool) == [("TOK", 2, True)]
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (s["id"],))
        await store.gc(conn, kube, NS)
    assert secret_objects(kube) == {}
    assert await rows(pool) == []


async def test_retired_name_set_again_keeps_counting_up(pool, kube, owner, canary):
    await put(pool, kube, owner, "TOK", canary)
    async with pool.connection() as conn:
        await store.retire(conn, owner, "TOK")
    meta = await put(pool, kube, owner, "TOK", canary)
    assert meta.version == 2
    assert await rows(pool) == [("TOK", 2, False)]
