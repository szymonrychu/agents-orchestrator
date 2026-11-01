import asyncio
import uuid

import pytest

from agents_orchestrator.db import presets
from agents_orchestrator.db.presets import (
    LastPreset,
    PresetDefault,
    PresetExists,
    PresetNotFound,
    validate_preset_name,
)

DEFAULT_ID = uuid.UUID("00000000-0000-0000-0000-000000000001")


async def test_crud_round_trip_and_order(pool):
    spec = {"replicas": 3, "model": "opus", "nested": {"a": [1, 2]}}
    async with pool.connection() as conn:
        zed = await presets.create(conn, "zed", spec)
        alpha = await presets.create(conn, "Alpha", {})
        assert zed.spec == spec and not zed.is_default and zed.created_at == zed.updated_at
        assert await presets.get(conn, zed.id) == zed
        assert await presets.get(conn, uuid.uuid4()) is None
        names = [p.name for p in await presets.list_presets(conn)]
        assert names == ["Default", "Alpha", "zed"]
        shared = await presets.get_for_share(conn, alpha.id)
        assert shared == alpha and await presets.get_for_share(conn, uuid.uuid4()) is None

        new_spec = {"replicas": 0, "z": None}
    async with pool.connection() as conn:
        upd = await presets.update(conn, zed.id, "Beta", new_spec)
        assert (upd.id, upd.name, upd.spec, upd.is_default) == (zed.id, "Beta", new_spec, False)
        assert upd.created_at == zed.created_at and upd.updated_at > zed.updated_at
        assert await presets.get(conn, zed.id) == upd
        default = await presets.update(conn, DEFAULT_ID, "Default", {"replicas": 2})
        assert default.is_default and default.spec == {"replicas": 2}
        assert [p.name for p in await presets.list_presets(conn)] == ["Default", "Alpha", "Beta"]
        with pytest.raises(PresetNotFound):
            await presets.update(conn, uuid.uuid4(), "Nope", {})


def test_validate_preset_name():
    for ok in ("a", "Big Session", "a.b-c_d 1", "x" * 40):
        assert validate_preset_name(ok) == ok
    for bad in ("", "x" * 41, " lead", "trail ", " ", "bad/slash", "emoji!", "tab\tx", "a\n"):
        with pytest.raises(ValueError, match="name"):
            validate_preset_name(bad)


async def test_name_rules_and_unique(pool):
    async with pool.connection() as conn:
        one = await presets.create(conn, "Big", {})
        for dup in ("big", "BIG", "Default", "default"):
            with pytest.raises(PresetExists):
                await presets.create(conn, dup, {})
        other = await presets.create(conn, "Other", {})
        with pytest.raises(PresetExists):
            await presets.update(conn, other.id, "bIg", {})
        # renaming a preset to its own name in another case is allowed
        assert (await presets.update(conn, one.id, "BIG", {})).name == "BIG"
        for bad in ("", "x" * 41, " Big", "Big ", "no/slash"):
            with pytest.raises(ValueError):
                await presets.create(conn, bad, {})
            with pytest.raises(ValueError):
                await presets.update(conn, one.id, bad, {})
        assert len(await presets.list_presets(conn)) == 3


async def test_default_and_last_preset_guards(pool):
    async with pool.connection() as conn:
        with pytest.raises(PresetDefault):
            await presets.update(conn, DEFAULT_ID, "Renamed", {"replicas": 1})
        with pytest.raises(PresetDefault):
            await presets.update(conn, DEFAULT_ID, "default", {"replicas": 1})
        with pytest.raises(PresetDefault):
            await presets.delete(conn, DEFAULT_ID)
        with pytest.raises(PresetNotFound):
            await presets.delete(conn, uuid.uuid4())
        extra = await presets.create(conn, "Extra", {})
        await presets.delete(conn, extra.id)
        assert await presets.get(conn, extra.id) is None
        assert (await presets.get(conn, DEFAULT_ID)).name == "Default"
        # the default row is gone by raw SQL: the one remaining row is the last preset
        keep = await presets.create(conn, "Keep", {})
        await conn.execute("DELETE FROM presets WHERE is_default")
        with pytest.raises(LastPreset):
            await presets.delete(conn, keep.id)
        assert await presets.get(conn, keep.id) is not None


async def test_concurrent_deletes_keep_one_row(pool):
    async with pool.connection() as conn:
        a = await presets.create(conn, "A", {})
        b = await presets.create(conn, "B", {})
        await conn.execute("DELETE FROM presets WHERE is_default")

    async def go(pid):
        async with pool.connection() as conn:
            try:
                await presets.delete(conn, pid)
            except LastPreset:
                return "last"
            return "deleted"

    results = sorted(await asyncio.gather(go(a.id), go(b.id)))
    assert results == ["deleted", "last"]
    async with pool.connection() as conn:
        assert len(await presets.list_presets(conn)) == 1


async def test_create_defaults_not_manageable(pool):
    async with pool.connection() as conn:
        p = await presets.create(conn, "a", {})
    assert p.superleader_manageable is False


async def test_create_manageable(pool):
    async with pool.connection() as conn:
        p = await presets.create(conn, "a", {}, superleader_manageable=True)
    assert p.superleader_manageable is True


async def test_update_none_keeps_flag(pool):
    async with pool.connection() as conn:
        p = await presets.create(conn, "a", {}, superleader_manageable=True)
        upd = await presets.update(conn, p.id, "a", {}, None)
    assert upd.superleader_manageable is True


async def test_update_sets_flag(pool):
    async with pool.connection() as conn:
        p = await presets.create(conn, "a", {}, superleader_manageable=True)
        off = await presets.update(conn, p.id, "a", {}, False)
        on = await presets.update(conn, p.id, "a", {}, True)
    assert (off.superleader_manageable, on.superleader_manageable) == (False, True)


async def test_manageable_ids(pool):
    async with pool.connection() as conn:
        p1 = await presets.create(conn, "a", {}, superleader_manageable=True)
        p2 = await presets.create(conn, "b", {}, superleader_manageable=True)
        await presets.create(conn, "c", {})
        assert await presets.manageable_ids(conn) == {p1.id, p2.id}
