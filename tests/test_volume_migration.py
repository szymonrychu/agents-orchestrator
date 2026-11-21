"""One-shot migration of preset backup entries to volumes (S-77 step 10, section 13)."""

from __future__ import annotations

import logging
import uuid

import pytest
from psycopg.types.json import Jsonb
from test_api_backups import settings_for

from agents_orchestrator.db import backups, volumes
from agents_orchestrator.db import migrate_volumes as mv

PRE = "workspace-cache/"


class FakeS3:
    def __init__(self):
        self.copied: list[tuple[str, str]] = []
        self.deleted: list[str] = []
        self.missing: set[str] = set()

    def copy_object(self, settings, src, dst, *, client=None, credentials=None):
        if src in self.missing:
            raise RuntimeError("NoSuchKey")
        self.copied.append((src, dst))

    def delete_keys(self, settings, keys, *, credentials=None):
        self.deleted += list(keys)
        return []


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3()
    monkeypatch.setattr(mv.s3, "copy_object", fake.copy_object)
    monkeypatch.setattr(mv.s3, "delete_keys", fake.delete_keys)
    return fake


@pytest.fixture
def settings(dsn):
    return settings_for(dsn, volume_storage_class="sc-test")


def entry(name="proj/ws", root="workspace", path=".", mode="both", **kw):
    return {"root": root, "path": path, "mode": mode, "name": name, **kw}


async def add_preset(pool, name, backup, **spec):
    pid = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO presets (id, name, spec) VALUES (%s,%s,%s)",
            (pid, name, Jsonb({**spec, "backup": backup})),
        )
    return pid


async def add_version(pool, name, digest, preset_id=None, volume_writer=False):
    async with pool.connection() as conn, conn.transaction():
        _, v = await backups.record_save(
            conn,
            name=name,
            object_key=f"{PRE}b/{name}/{uuid.uuid4().hex}.tar.zst",
            size=10,
            digest=digest,
            manifest=None,
            base_version=None,
            origin="save",
            session_slug=None,
            preset_id=preset_id,
            project=None,
            created_by="t",
            orphan=False,
            volume_writer=volume_writer,
        )
    return v


async def spec_of(pool, pid):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT spec FROM presets WHERE id = %s", (pid,))
        return (await cur.fetchone())[0]


async def attached(pool, pid):
    """(volume row, attachment) of each attachment of the preset."""
    out = []
    async with pool.connection() as conn:
        for att in (await spec_of(pool, pid)).get("volumes") or []:
            out.append((await volumes.get(conn, uuid.UUID(att["volume"])), att))
    return out


async def volume_count(pool):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM volumes")
        return (await cur.fetchone())[0]


@pytest.mark.parametrize(
    ("ent", "spec", "path"),
    [
        (entry(), {}, "/workdir/workdir"),
        (entry(path="sub/dir"), {"workdir": "repo"}, "/workdir/repo/sub/dir"),
        (entry(path=""), {}, "/workdir/workdir"),
        (entry(root="claude"), {}, "/claude"),
        (entry(root="claude", path="skills"), {}, "/claude/skills"),
        (entry(mode="restore"), {}, "/workdir/workdir"),
        (entry(mode="backup"), {}, "/workdir/workdir"),
    ],
    ids=["ws-root", "ws-sub", "empty-path", "claude-root", "claude-sub", "restore", "backup"],
)
async def test_entry_becomes_rw_restoring_volume_at_old_path(pool, settings, s3, ent, spec, path):
    pid = await add_preset(pool, "Mtg Bot", [{**ent, "exclude": ["node_modules"]}], **spec)
    res = await mv.migrate(pool, settings)
    assert res.volumes == ["mtg-bot-proj-ws"]
    stored = await spec_of(pool, pid)
    assert "backup" not in stored
    [(vol, att)] = await attached(pool, pid)
    assert att == {"volume": str(vol.id), "mount_path": path, "mode": "rw", "backup_restore": True}
    assert (vol.name, vol.projects, vol.size, vol.storage_class, vol.exclude) == (
        "mtg-bot-proj-ws",
        ["default"],
        "10Gi",
        "sc-test",
        ["node_modules"],
    )
    async with pool.connection() as conn:
        page = await volumes.events(conn, vol.id, limit=10)
    assert sorted((e.kind, e.actor) for e in page.items) == [
        ("attached", "migration"),
        ("created", "migration"),
    ]


async def test_seed_copies_current_and_first_claim_restores_it(pool, settings, s3):
    old = await add_version(pool, "proj/ws", "sha:old")
    pid = await add_preset(pool, "Mtg Bot", [entry()])
    await mv.migrate(pool, settings)
    [(vol, _)] = await attached(pool, pid)
    async with pool.connection() as conn:
        seed = await backups.current(conn, "volume-mtg-bot-proj-ws")
        assert (seed.digest, seed.origin, vol.content_version) == ("sha:old", "copy", None)
        assert s3.copied == [(old.object_key, seed.object_key)]
        await volumes.set_state(conn, vol.id, "ready")
        decision = await volumes.claim(
            conn,
            volume_id=vol.id,
            session_id=uuid.uuid4(),
            preset_id=pid,
            mode="rw",
            backup_restore=True,
            session_slug="s-1",
            current_version=seed.id,
        )
    assert decision.decision == "restore"


async def test_templated_entry_collapses_to_one_volume_seeded_by_newest(pool, settings, s3):
    pid = await add_preset(pool, "Bot", [entry(name="{{ session.slug }}/ws")])
    await add_version(pool, "s-aaa/ws", "sha:a", pid)
    await add_version(pool, "s-bbb/ws", "sha:b", pid)
    await add_version(pool, "s-ccc/ws", "sha:other", uuid.uuid4())  # another preset's
    res = await mv.migrate(pool, settings)
    assert res.volumes == ["bot-ws"] and res.seeds == 1
    async with pool.connection() as conn:
        assert (await backups.current(conn, "volume-bot-ws")).digest == "sha:b"


async def test_entries_sharing_a_path_collapse_with_newest_seed_and_exclude_union(
    pool, settings, s3
):
    await add_version(pool, "seed", "sha:seed")
    await add_version(pool, "saved", "sha:saved")
    pid = await add_preset(
        pool,
        "Bot",
        [
            entry(name="seed", mode="restore", exclude=["a"], live=True),
            entry(name="saved", mode="backup", exclude=["b", "a"], after_init=True),
        ],
    )
    await mv.migrate(pool, settings)
    [(vol, att)] = await attached(pool, pid)
    assert (vol.name, vol.exclude, att["backup_restore"]) == ("bot-seed", ["a", "b"], True)
    async with pool.connection() as conn:
        assert (await backups.current(conn, "volume-bot-seed")).digest == "sha:saved"


async def test_two_presets_sharing_one_entry_name_get_one_volume_each(pool, settings, s3):
    await add_version(pool, "shared", "sha:s")
    a = await add_preset(pool, "a", [entry(name="shared")])
    b = await add_preset(pool, "b", [entry(name="shared")])
    await mv.migrate(pool, settings)
    [(va, _)], [(vb, _)] = await attached(pool, a), await attached(pool, b)
    assert (va.name, vb.name) == ("a-shared", "b-shared")
    async with pool.connection() as conn:
        digests = [
            (await backups.current(conn, f"volume-{n}")).digest for n in ("a-shared", "b-shared")
        ]
    assert digests == ["sha:s", "sha:s"]


async def test_taken_names_get_a_suffix(pool, settings, s3):
    async with pool.connection() as conn:
        await volumes.create(
            conn,
            name="a-b-c",
            projects=["p"],
            size="1Gi",
            storage_class="x",
            created_by="u",
            actor_kind="user",
            actor="u",
        )
    pid = await add_preset(pool, "a", [entry(name="b/c")])
    other = await add_preset(pool, "a-b", [entry(name="c")])
    res = await mv.migrate(pool, settings)
    assert sorted(res.volumes) == ["a-b-c-2", "a-b-c-3"]
    assert {v.name for v, _ in await attached(pool, pid) + await attached(pool, other)} == {
        "a-b-c-2",
        "a-b-c-3",
    }


async def test_preexisting_reserved_backup_names_never_crash_and_are_not_inherited(
    pool, settings, s3, caplog
):
    legacy = await add_version(pool, "volume-bot-ws", "sha:legacy", volume_writer=True)
    pid = await add_preset(pool, "Bot", [entry(name="volume-bot-ws"), entry(name="ws", path="x")])
    caplog.set_level(logging.INFO, logger=mv.__name__)
    res = await mv.migrate(pool, settings)
    assert res.reserved == ["volume-bot-ws"] and "volume-bot-ws" in caplog.text
    vols = {v.name: v for v, _ in await attached(pool, pid)}
    # neither volume takes the reserved name; the entry naming it seeds a fresh volume
    assert set(vols) == {"bot-volume-bot-ws", "bot-ws-2"}
    assert all(v.content_version is None for v in vols.values())
    async with pool.connection() as conn:
        assert (await backups.current(conn, "volume-bot-volume-bot-ws")).digest == "sha:legacy"
        assert (await backups.current(conn, "volume-bot-ws")).id == legacy.id


async def test_second_run_changes_nothing(pool, settings, s3):
    await add_version(pool, "proj/ws", "sha:1")
    pid = await add_preset(pool, "Bot", [entry()])
    await mv.migrate(pool, settings)
    after = await spec_of(pool, pid)
    res = await mv.migrate(pool, settings)
    assert (res.presets, res.volumes, res.seeds) == (0, [], 0)
    assert await spec_of(pool, pid) == after and await volume_count(pool) == 1
    assert len(s3.copied) == 1


async def test_dry_run_writes_nothing_and_logs_names_and_counts(pool, settings, s3, caplog):
    await add_version(pool, "proj/ws", "sha:1")
    pid = await add_preset(pool, "Bot", [entry(path="secret-dir", exclude=["CANARY-x"])])
    caplog.set_level(logging.INFO, logger=mv.__name__)
    res = await mv.migrate(pool, settings, dry_run=True)
    assert (res.presets, res.volumes, res.seeds) == (1, ["bot-proj-ws"], 1)
    assert "backup" in await spec_of(pool, pid)
    assert await volume_count(pool) == 0 and s3.copied == []
    assert "preset=Bot" in caplog.text and "bot-proj-ws" in caplog.text
    assert "secret-dir" not in caplog.text and "CANARY" not in caplog.text


async def test_failing_preset_is_left_untouched_and_others_migrate(pool, settings, s3):
    bad = await add_version(pool, "gone", "sha:g")
    s3.missing.add(bad.object_key)
    await add_version(pool, "fine", "sha:f")
    broken = await add_preset(pool, "a", [entry(name="fine", path="x"), entry(name="gone")])
    ok = await add_preset(pool, "b", [entry(name="fine")])
    res = await mv.migrate(pool, settings)
    assert res.skipped == ["a"] and res.volumes == ["b-fine"]
    assert "backup" in await spec_of(pool, broken)
    assert "backup" not in await spec_of(pool, ok)
    assert s3.deleted == [k for _, k in s3.copied[:1]]  # the copy made for "a" is cleaned up


async def test_lock_held_elsewhere_skips_the_run(pool, settings, s3):
    pid = await add_preset(pool, "Bot", [entry()])
    async with pool.connection() as holder:
        await holder.execute("SELECT pg_advisory_lock(%s)", (mv.LOCK_KEY,))
        try:
            assert await mv.migrate(pool, settings) is None
        finally:
            await holder.execute("SELECT pg_advisory_unlock(%s)", (mv.LOCK_KEY,))
    assert "backup" in await spec_of(pool, pid)


@pytest.mark.parametrize("dry_run", [False, True])
async def test_api_startup_runs_the_migration(pool, dsn, s3, dry_run):
    from agents_orchestrator.api.app import create_app

    pid = await add_preset(pool, "Bot", [entry()])
    app = create_app(settings_for(dsn, volume_migration_dry_run=dry_run), pool)
    async with app.router.lifespan_context(app):
        pass
    assert ("backup" in await spec_of(pool, pid)) is dry_run


async def test_api_startup_survives_a_failing_migration(pool, dsn, s3, monkeypatch):
    from agents_orchestrator.api.app import create_app

    async def boom(*a, **kw):
        raise RuntimeError("x")

    monkeypatch.setattr(mv, "migrate", boom)
    app = create_app(settings_for(dsn), pool)
    async with app.router.lifespan_context(app):
        pass


async def test_legacy_backup_rows_still_migrate_and_load_without_the_entry_model(
    pool, settings, s3
):
    # the entry model is gone: a stored preset with `backup` is migrated from its raw JSON, the
    # migrated spec loads, and a running session row that still carries the key still loads
    from agents_orchestrator.db import sessions
    from agents_orchestrator.spec import SessionSpec

    await add_version(pool, "proj/ws", "sha:1")
    pid = await add_preset(pool, "Bot", [entry()])
    async with pool.connection() as conn:
        created = await sessions.create(conn, "legacy", SessionSpec())
        await conn.execute(
            "UPDATE sessions SET spec = spec || %s WHERE id = %s",
            (Jsonb({"backup": [entry()]}), created.id),
        )
    res = await mv.migrate(pool, settings)
    assert res.volumes == ["bot-proj-ws"]
    stored = await spec_of(pool, pid)
    assert "backup" not in stored
    assert SessionSpec.model_validate(stored).volumes[0].backup_restore is True
    async with pool.connection() as conn:
        loaded = await sessions.get(conn, created.id)
    assert loaded is not None and "backup" not in loaded.spec.model_dump(exclude_unset=True)
