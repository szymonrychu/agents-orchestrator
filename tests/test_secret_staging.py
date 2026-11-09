"""Staged secret versions reserved for change proposals (preset-change-proposals 3.6, Q13 B).

A staged value is written once as the immutable Secret version it will become; the number is
reserved in `change_proposal_secrets` until the proposal promotes, binds or releases it. Every
test checks that the canary value never reaches the log.
"""

from __future__ import annotations

import asyncio
import logging
import secrets as stdlib_secrets

import pytest
from factories import attach_preset, insert_session
from fakes.fake_kube import FakeKube
from psycopg.types.json import Jsonb
from pydantic import SecretStr
from test_k8s_secrets import NS, make_preset, put, secret_objects, stage

from agents_orchestrator.db import sessions
from agents_orchestrator.k8s import secrets as store
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.spec import SessionSpec


@pytest.fixture
def kube():
    return FakeKube(namespace=NS)


@pytest.fixture
def canary(caplog):
    caplog.set_level(logging.DEBUG)
    value = stdlib_secrets.token_hex(16)
    yield value
    assert value not in caplog.text
    assert not any(value in str(r.args) or value in r.getMessage() for r in caplog.records)


async def reservations(pool, preset_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT name, version, proposal_id, revision, state FROM change_proposal_secrets"
            " WHERE preset_id = %s ORDER BY name, version",
            (preset_id,),
        )
        return await cur.fetchall()


async def promote(pool, preset_id, name, *, proposal_id=7, revision=1) -> int:
    async with pool.connection() as conn, conn.transaction():
        return await store.promote_staged(
            conn, preset_id, name, proposal_id=proposal_id, revision=revision
        )


async def release(pool, kube, *, proposal_id=7, revision=None) -> list[str]:
    async with pool.connection() as conn:
        return await store.release(conn, kube, NS, proposal_id=proposal_id, revision=revision)


async def lock_waiter(pool, backend_pid: int | None = None) -> None:
    """Return once a backend of this database (`backend_pid`, or any) waits on an advisory lock."""

    async def poll() -> None:
        async with pool.connection() as conn:
            while True:
                cur = await conn.execute(
                    "SELECT 1 FROM pg_locks l JOIN pg_database d ON d.oid = l.database"
                    " WHERE d.datname = current_database() AND l.locktype = 'advisory'"
                    " AND NOT l.granted AND (%s::int IS NULL OR l.pid = %s)",
                    (backend_pid, backend_pid),
                )
                if await cur.fetchone() is not None:
                    return
                await conn.rollback()
                await asyncio.sleep(0.01)

    await asyncio.wait_for(poll(), timeout=10)


async def test_stage_writes_the_next_version_with_the_value_only_in_string_data(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary + "live")
    got = await stage(pool, kube, pid, "TOK", canary, proposal_id=7, revision=2)
    assert got == store.secret_name(store.Owner.preset(pid), "TOK", 2)
    applied = [a for a in kube.applies if a.kind == "Secret" and a.name == got]
    assert len(applied) == 1
    obj = applied[0].obj
    assert obj["immutable"] is True
    assert obj["stringData"] == {"value": canary}
    assert obj["metadata"]["labels"] == {
        "app.kubernetes.io/managed-by": "agents-orchestrator",
        "orchestrator.proposal": "7",
    }
    assert canary not in repr({k: v for k, v in obj.items() if k != "stringData"})
    assert await reservations(pool, pid) == [("TOK", 2, 7, 2, "staged")]
    async with pool.connection() as conn:
        live = await store.list_metas(conn, store.Owner.preset(pid))
    assert [(m.name, m.version) for m in live] == [("TOK", 1)]  # staging does not go live


async def test_stage_of_an_unset_name_starts_at_one(pool, kube, canary):
    pid = await make_preset(pool)
    assert await stage(pool, kube, pid, "NEW", canary) == store.secret_name(
        store.Owner.preset(pid), "NEW", 1
    )
    assert await reservations(pool, pid) == [("NEW", 1, 7, 1, "staged")]


async def test_second_stage_of_a_name_takes_the_next_number(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    first = await stage(pool, kube, pid, "TOK", canary, proposal_id=7)
    second = await stage(pool, kube, pid, "TOK", canary, proposal_id=8)
    assert (first, second) == (
        store.secret_name(store.Owner.preset(pid), "TOK", 2),
        store.secret_name(store.Owner.preset(pid), "TOK", 3),
    )
    assert [r[1] for r in await reservations(pool, pid)] == [2, 3]


async def test_stage_replaces_a_leftover_of_that_name(pool, kube, canary):
    pid = await make_preset(pool)
    leftover = store.secret_name(store.Owner.preset(pid), "TOK", 1)
    kube.seed({"apiVersion": "v1", "kind": "Secret", "metadata": {"name": leftover}})
    assert await stage(pool, kube, pid, "TOK", canary) == leftover
    assert ("Secret", NS, leftover) in kube.deletes
    assert leftover in secret_objects(kube)
    assert [a.obj["stringData"] for a in kube.applies if a.name == leftover] == [{"value": canary}]


async def test_stage_kube_error_is_generic_and_reserves_nothing(pool, kube, canary):
    pid = await make_preset(pool)
    kube.fail(
        "Secret",
        store.secret_name(store.Owner.preset(pid), "TOK", 1),
        KubeError(f"422: body {canary}"),
    )
    with pytest.raises(store.SecretStoreError) as err:
        await stage(pool, kube, pid, "TOK", canary)
    assert canary not in str(err.value) and canary not in repr(err.value)
    assert err.value.__cause__ is None and err.value.__suppress_context__
    assert await reservations(pool, pid) == []


async def test_promote_makes_the_reserved_version_live_and_gc_collects_the_previous(
    pool, kube, canary
):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    staged = await stage(pool, kube, pid, "TOK", canary + "s")
    assert await promote(pool, pid, "TOK") == 2
    async with pool.connection() as conn:
        live = await store.list_metas(conn, store.Owner.preset(pid))
        assert [(m.name, m.version) for m in live] == [("TOK", 2)]
        assert await reservations(pool, pid) == []
        await store.gc(conn, kube, NS, owner=store.Owner.preset(pid))
    assert set(secret_objects(kube)) == {staged}


async def test_promote_revives_a_retired_name(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    s = await insert_session(pool)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (Jsonb({"TOK": store.secret_name(store.Owner.preset(pid), "TOK", 1)}), s["id"]),
        )
        await store.sync_declared(conn, kube, NS, pid, [])
    await stage(pool, kube, pid, "TOK", canary)
    assert await promote(pool, pid, "TOK") == 2
    async with pool.connection() as conn:
        assert [m.version for m in await store.list_metas(conn, store.Owner.preset(pid))] == [2]


async def test_promote_at_or_below_the_live_version_is_stale_and_writes_nothing(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    await stage(pool, kube, pid, "TOK", canary)  # v2
    await put(pool, kube, pid, "TOK", canary)  # a human rotation after it: v3
    with pytest.raises(store.StagedStale):
        await promote(pool, pid, "TOK")
    async with pool.connection() as conn:
        assert [m.version for m in await store.list_metas(conn, store.Owner.preset(pid))] == [3]
    assert await reservations(pool, pid) == [("TOK", 2, 7, 1, "staged")]


@pytest.mark.parametrize(("proposal_id", "revision"), [(8, 1), (7, 2)])
async def test_promote_with_the_wrong_proposal_or_revision_raises(
    pool, kube, canary, proposal_id, revision
):
    pid = await make_preset(pool)
    await stage(pool, kube, pid, "TOK", canary, proposal_id=7, revision=1)
    with pytest.raises(store.StagedStale):
        await promote(pool, pid, "TOK", proposal_id=proposal_id, revision=revision)
    async with pool.connection() as conn:
        assert await store.list_metas(conn, store.Owner.preset(pid)) == []
    assert await reservations(pool, pid) == [("TOK", 1, 7, 1, "staged")]


async def test_promote_of_a_bound_reservation_raises(pool, kube, canary):
    pid = await make_preset(pool)
    await stage(pool, kube, pid, "TOK", canary)
    async with pool.connection() as conn:
        await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=1)
    with pytest.raises(store.StagedStale):
        await promote(pool, pid, "TOK")


async def test_bind_turns_the_reservation_bound_once(pool, kube, canary):
    pid = await make_preset(pool)
    staged = await stage(pool, kube, pid, "TOK", canary)
    async with pool.connection() as conn:
        with pytest.raises(store.StagedStale):
            await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=2)
        assert await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=1) == staged
        with pytest.raises(store.StagedStale):
            await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=1)
    assert await reservations(pool, pid) == [("TOK", 1, 7, 1, "bound")]


async def test_release_deletes_the_staged_secrets_of_one_revision_or_all(pool, kube, canary):
    pid = await make_preset(pool)
    a = await stage(pool, kube, pid, "A", canary, proposal_id=7, revision=1)
    b = await stage(pool, kube, pid, "B", canary, proposal_id=7, revision=1)
    c = await stage(pool, kube, pid, "C", canary, proposal_id=7, revision=2)
    d = await stage(pool, kube, pid, "D", canary, proposal_id=8, revision=1)
    e = await stage(pool, kube, pid, "E", canary, proposal_id=7, revision=2)
    async with pool.connection() as conn:
        await store.bind_staged(conn, pid, "E", proposal_id=7, revision=2)
    assert sorted(await release(pool, kube, revision=1)) == sorted([a, b])
    assert set(secret_objects(kube)) == {c, d, e}
    assert sorted(await release(pool, kube)) == [c]
    assert set(secret_objects(kube)) == {d, e}  # a bound version is not released
    assert [r[0] for r in await reservations(pool, pid)] == ["D", "E"]


async def test_release_keeps_the_row_when_the_kube_delete_fails(pool, kube, canary):
    pid = await make_preset(pool)
    staged = await stage(pool, kube, pid, "TOK", canary)
    kube.fail("Secret", staged, KubeError(f"500 {canary}"), verbs=["delete"])
    assert await release(pool, kube) == []
    assert await reservations(pool, pid) == [("TOK", 1, 7, 1, "staged")]
    kube.heal("Secret", staged)
    assert await release(pool, kube) == [staged]
    assert await reservations(pool, pid) == []
    assert secret_objects(kube) == {}


async def test_release_of_an_already_missing_secret_drops_the_row(pool, kube, canary):
    pid = await make_preset(pool)
    staged = await stage(pool, kube, pid, "TOK", canary)
    kube.objects.pop(("Secret", NS, staged))
    assert await release(pool, kube) == [staged]
    assert await reservations(pool, pid) == []


async def test_collect_bound_waits_until_no_session_names_it(pool, kube, canary):
    pid = await make_preset(pool)
    bound = await stage(pool, kube, pid, "TOK", canary, proposal_id=7)
    staged = await stage(pool, kube, pid, "TOK", canary, proposal_id=8)
    s = await insert_session(pool)
    async with pool.connection() as conn:
        await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=1)
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (Jsonb({"TOK": bound}), s["id"]),
        )
    assert await store.collect_bound(pool, kube, NS) == 0
    assert set(secret_objects(kube)) == {bound, staged}
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (s["id"],))
    assert await store.collect_bound(pool, kube, NS) == 1
    assert set(secret_objects(kube)) == {staged}  # a staged reservation is not collected
    assert await reservations(pool, pid) == [("TOK", 2, 8, 1, "staged")]


async def test_collect_bound_keeps_the_row_when_the_kube_delete_fails(pool, kube, canary):
    pid = await make_preset(pool)
    bound = await stage(pool, kube, pid, "TOK", canary)
    async with pool.connection() as conn:
        await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=1)
    kube.fail("Secret", bound, KubeError(f"500 {canary}"), verbs=["delete"])
    assert await store.collect_bound(pool, kube, NS) == 0
    assert await reservations(pool, pid) == [("TOK", 1, 7, 1, "bound")]


async def test_concurrent_stage_and_set_never_share_a_version(pool, kube, canary):
    pid = await make_preset(pool)
    await put(pool, kube, pid, "TOK", canary)
    async with pool.connection() as a:
        async with a.transaction():
            staged = await store.stage_secret(
                a, kube, NS, pid, "TOK", SecretStr(canary), proposal_id=7, revision=1
            )
            # the stage still holds the variable's lock: a concurrent set must wait for it
            writer = asyncio.create_task(put(pool, kube, pid, "TOK", canary))
            await lock_waiter(pool)
            assert not writer.done()
        meta = await writer
    assert staged == store.secret_name(store.Owner.preset(pid), "TOK", 2)
    assert meta.version == 3
    results = await asyncio.gather(
        *(stage(pool, kube, pid, "TOK", canary, proposal_id=10 + i) for i in range(3)),
        *(put(pool, kube, pid, "TOK", canary) for _ in range(3)),
    )
    versions = [int(r.rsplit("-v", 1)[1]) for r in results[:3]] + [m.version for m in results[3:]]
    assert len(set(versions)) == len(versions)


async def test_release_racing_a_promote_keeps_the_promoted_secret(pool, kube, canary):
    pid = await make_preset(pool)
    staged = await stage(pool, kube, pid, "TOK", canary)
    async with pool.connection() as promoter, pool.connection() as releaser:
        async with promoter.transaction():
            assert await store.promote_staged(promoter, pid, "TOK", proposal_id=7, revision=1) == 1
            # release reads the still staged row, then waits for the variable's lock
            releasing = asyncio.create_task(store.release(releaser, kube, NS, proposal_id=7))
            await lock_waiter(pool, releaser.info.backend_pid)
        assert await releasing == []
    assert staged in secret_objects(kube)  # the now live version survives
    async with pool.connection() as conn:
        assert [m.version for m in await store.list_metas(conn, store.Owner.preset(pid))] == [1]
    assert await reservations(pool, pid) == []


async def test_release_racing_a_bind_keeps_the_bound_secret(pool, kube, canary):
    pid = await make_preset(pool)
    staged = await stage(pool, kube, pid, "TOK", canary)
    async with pool.connection() as binder, pool.connection() as releaser:
        async with binder.transaction():
            assert await store.bind_staged(binder, pid, "TOK", proposal_id=7, revision=1) == staged
            releasing = asyncio.create_task(store.release(releaser, kube, NS, proposal_id=7))
            await lock_waiter(pool, releaser.info.backend_pid)
        assert await releasing == []
    assert staged in secret_objects(kube)  # the session-bound version survives
    assert await reservations(pool, pid) == [("TOK", 1, 7, 1, "bound")]


async def test_reap_deletes_unreserved_versions_up_to_the_probe_depth(pool, kube, canary):
    """Orphans of rolled-back stages sit above every row: the `ORPHAN_DEPTH` versions above the
    highest stored or reserved one are deleted for each known variable, a spec-declared one with
    no row included; the live and the reserved versions stay."""
    pid = await make_preset(pool)
    live = (await put(pool, kube, pid, "TOK", canary)).version
    staged = await stage(pool, kube, pid, "TOK", canary)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET spec = %s WHERE id = %s",
            (Jsonb({"env": {"NEW": {"protected": True}, "PLAIN": {"value": "x"}}}), pid),
        )
    top = 2 + store.ORPHAN_DEPTH
    orphans = [
        store.secret_name(store.Owner.preset(pid), "TOK", 3),
        store.secret_name(store.Owner.preset(pid), "TOK", top),
    ]
    orphans.append(store.secret_name(store.Owner.preset(pid), "NEW", 1))
    beyond = store.secret_name(store.Owner.preset(pid), "TOK", top + 1)
    for full in [*orphans, beyond]:
        await store._write(kube, NS, full, SecretStr(canary), {store.PROPOSAL_LABEL: "9"})
    assert sorted(await store.reap_unreserved(pool, kube, NS)) == sorted(orphans)
    assert set(secret_objects(kube)) == {
        store.secret_name(store.Owner.preset(pid), "TOK", live),
        staged,
        beyond,
    }


async def _bound_by_replace(pool, kube, value):
    """A session apply: stage TOK for a session's preset, bind it through `replace_spec`."""
    async with pool.connection() as conn:
        s = await sessions.create(conn, "bound", SessionSpec(image="img:1"))
    pid = await attach_preset(pool, s.id)
    await put(pool, kube, pid, "TOK", value + "live")
    bound = await stage(pool, kube, pid, "TOK", value)
    async with pool.connection() as conn:
        _, stored = await sessions.get_stored(conn, s.id)

        async def recheck(_locked):
            return None

        spec = SessionSpec(image="img:1", env={"TOK": {"protected": True}})
        async with conn.transaction():
            name = await store.bind_staged(conn, pid, "TOK", proposal_id=7, revision=1)
            await sessions.replace_spec(
                conn, s.id, stored, spec, recheck, secret_versions={"TOK": name}
            )
    assert name == bound
    return s, pid, bound


async def test_gc_keeps_a_version_bound_through_replace_spec(pool, kube, canary):
    s, pid, bound = await _bound_by_replace(pool, kube, canary)
    await put(pool, kube, pid, "TOK", canary + "next")  # v3 live; gc runs for TOK
    async with pool.connection() as conn:
        await store.gc(conn, kube, NS, owner=store.Owner.preset(pid))
    assert set(secret_objects(kube)) == {
        bound,
        store.secret_name(store.Owner.preset(pid), "TOK", 3),
    }
    assert await reservations(pool, pid) == [("TOK", 2, 7, 1, "bound")]


async def test_collect_bound_deletes_a_replace_spec_binding_only_after_the_session_row(
    pool, kube, canary
):
    s, pid, bound = await _bound_by_replace(pool, kube, canary)
    assert await store.collect_bound(pool, kube, NS) == 0
    assert bound in secret_objects(kube)
    async with pool.connection() as conn:
        await conn.execute("DELETE FROM sessions WHERE id = %s", (s.id,))
    assert await store.collect_bound(pool, kube, NS) == 1
    assert bound not in secret_objects(kube)
    assert await reservations(pool, pid) == []
