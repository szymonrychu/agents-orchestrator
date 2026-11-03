"""sessions.replace_spec: the whole-spec write under the row lock, with a caller recheck."""

import asyncio
import json
import uuid

import psycopg
import pytest
from factories import attach_preset
from psycopg.types.json import Jsonb

from agents_orchestrator.db import sessions
from agents_orchestrator.db.queue import SessionBeingDeleted
from agents_orchestrator.spec import SessionSpec

BASE = SessionSpec(replicas=2, max_replicas=4, image="img:1", model="m1")


async def _noop(_locked):
    return None


async def _row(pool, session_id):
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT spec, generation, observed_generation FROM sessions WHERE id = %s",
            (session_id,),
        )
        return await cur.fetchone()


async def _created(pool, spec=BASE):
    async with pool.connection() as conn:
        return await sessions.create(conn, f"s-{uuid.uuid4().hex[:8]}", spec)


class _Listener:
    def __init__(self, dsn):
        self.dsn = dsn

    async def __aenter__(self):
        self.conn = await psycopg.AsyncConnection.connect(self.dsn, autocommit=True)
        await self.conn.execute(f"LISTEN {sessions.CHANNEL}")
        return self

    async def __aexit__(self, *exc):
        await self.conn.close()

    async def payloads(self, wait=0.5):
        return [n.payload async for n in self.conn.notifies(timeout=wait)]


async def test_writes_bumps_once_and_notifies(pool, dsn):
    s = await _created(pool)
    stored, gen, obs = await _row(pool, s.id)
    new = BASE.model_copy(update={"image": "img:2"})
    async with _Listener(dsn) as lis, pool.connection() as conn:
        out, written = await sessions.replace_spec(conn, s.id, stored, new, _noop)
        got = await lis.payloads()
    assert written is True
    assert out.generation == gen + 1 and out.observed_generation == obs
    assert out.spec == new
    assert got == [json.dumps({"session_id": str(s.id)})]
    spec, gen2, _ = await _row(pool, s.id)
    assert spec == new.model_dump(mode="json", exclude_none=True)
    assert gen2 == gen + 1


async def test_equal_spec_writes_nothing(pool, dsn):
    s = await _created(pool)
    before = await _row(pool, s.id)
    async with _Listener(dsn) as lis, pool.connection() as conn:
        out, written = await sessions.replace_spec(conn, s.id, before[0], BASE, _noop)
        got = await lis.payloads()
    assert written is False
    assert out.generation == before[1] and out.spec == BASE
    assert got == []
    assert await _row(pool, s.id) == before


async def test_stale_expected_raises_spec_changed(pool):
    s = await _created(pool)
    stored = (await _row(pool, s.id))[0]
    async with pool.connection() as other:
        await sessions.set_replicas(other, s.id, 3)
    before = await _row(pool, s.id)
    called = []

    async def recheck(locked):
        called.append(locked)

    new = BASE.model_copy(update={"image": "img:2"})
    async with pool.connection() as conn:
        with pytest.raises(sessions.SpecChanged):
            await sessions.replace_spec(conn, s.id, stored, new, recheck)
    assert called == []
    assert await _row(pool, s.id) == before


async def test_deleting_session_refused(pool):
    s = await _created(pool)
    async with pool.connection() as conn:
        await sessions.request_delete(conn, s.id)
    before = await _row(pool, s.id)
    called = []

    async def recheck(locked):
        called.append(locked)

    async with pool.connection() as conn:
        with pytest.raises(SessionBeingDeleted):
            await sessions.replace_spec(conn, s.id, before[0], BASE, recheck)
    assert called == []
    assert await _row(pool, s.id) == before


async def test_unknown_session(pool):
    async with pool.connection() as conn:
        with pytest.raises(sessions.SessionNotFound):
            await sessions.replace_spec(conn, uuid.uuid4(), {}, BASE, _noop)


async def test_recheck_raise_rolls_back(pool, dsn):
    s = await _created(pool)
    before = await _row(pool, s.id)

    async def recheck(_locked):
        raise RuntimeError("refused")

    new = BASE.model_copy(update={"image": "img:2"})
    async with _Listener(dsn) as lis, pool.connection() as conn:
        with pytest.raises(RuntimeError, match="refused"):
            await sessions.replace_spec(conn, s.id, before[0], new, recheck)
        got = await lis.payloads()
    assert got == []
    assert await _row(pool, s.id) == before


async def test_recheck_gets_the_fresh_locked_row(pool):
    s = await _created(pool)
    preset_id = await attach_preset(pool, s.id)
    versions = {"API_KEY": "secret-abc"}
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET secret_versions = %s WHERE id = %s",
            (Jsonb(versions), s.id),
        )
    stored = (await _row(pool, s.id))[0]
    seen = []

    async def recheck(locked):
        seen.append(locked)

    async with pool.connection() as conn:
        await sessions.replace_spec(conn, s.id, stored, BASE, recheck)
    assert seen == [
        sessions.LockedSession(spec=stored, preset_id=preset_id, secret_versions=versions)
    ]


@pytest.mark.parametrize("change", [False, True])
async def test_recheck_mutating_the_locked_spec_does_not_move_the_decision(pool, change):
    """The written or not written decision is taken before the recheck, so a recheck that
    edits the dict it was handed cannot turn a real change into no write, or the reverse."""
    s = await _created(pool)
    stored, gen, _ = await _row(pool, s.id)
    new = BASE.model_copy(update={"image": "img:2"}) if change else BASE
    target = new.model_dump(mode="json", exclude_none=True)

    async def recheck(locked):
        if change:
            locked.spec.clear()
            locked.spec.update(target)  # now equal to the new spec
        else:
            locked.spec["image"] = "img:other"  # now differs from the new spec

    async with pool.connection() as conn:
        _, written = await sessions.replace_spec(conn, s.id, dict(stored), new, recheck)
    assert written is change
    spec, gen2, _ = await _row(pool, s.id)
    assert spec == target
    assert gen2 == gen + int(change)


async def _wait_for_lock_wait(pool, timeout=5.0):
    """Until some backend waits on a row lock."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'"
            )
            if (await cur.fetchone())[0]:
                return
        await asyncio.sleep(0.05)
    raise AssertionError("no backend waits on a lock")


async def test_concurrent_scale_waits_and_applies_on_top(pool):
    s = await _created(pool)
    stored, gen, _ = await _row(pool, s.id)
    entered, release = asyncio.Event(), asyncio.Event()

    async def recheck(_locked):
        entered.set()
        await release.wait()

    new = BASE.model_copy(update={"image": "img:2", "model": "m2"})

    async def patch():
        async with pool.connection() as conn:
            return await sessions.replace_spec(conn, s.id, stored, new, recheck)

    async def scale():
        async with pool.connection() as conn:
            return await sessions.set_replicas(conn, s.id, 3)

    patching = asyncio.create_task(patch())
    await asyncio.wait_for(entered.wait(), 5)
    scaling = asyncio.create_task(scale())
    await _wait_for_lock_wait(pool)
    assert not scaling.done()
    release.set()
    (_, written), scaled = await asyncio.gather(patching, scaling)
    assert written is True and scaled.previous_replicas == 2 and scaled.replicas == 3
    spec, gen2, _ = await _row(pool, s.id)
    assert spec == {**new.model_dump(mode="json", exclude_none=True), "replicas": 3}
    assert gen2 == gen + 1
