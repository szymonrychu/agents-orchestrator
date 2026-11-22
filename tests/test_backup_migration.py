"""Data migration of cache specs and archives to versioned backups (S-68 step 15, T-523)."""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import boto3
import psycopg
import pytest
from moto.server import ThreadedMotoServer
from psycopg.types.json import Jsonb

from agents_orchestrator import cli
from agents_orchestrator.db import migrate_backup_spec as bm
from agents_orchestrator.db.migrate import run_migrations
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, without_legacy_backup

GOLDEN = Path(__file__).parent / "golden" / "backup_migration"
CASES = sorted(p for p in GOLDEN.glob("*.json") if not p.name.startswith("unmapped_"))
UNMAPPED = sorted(GOLDEN.glob("unmapped_*.json"))
ACCESS_KEY = "TESTIDTESTCACHE"
SECRET = "placeholder-secret-value"


def _preset(raw):
    if raw is None:
        return None
    return SimpleNamespace(id=raw["id"] and uuid.UUID(raw["id"]), name=raw["name"])


@pytest.mark.parametrize("path", CASES, ids=lambda p: p.stem)
def test_map_cache_cases(path):
    case = json.loads(path.read_text())
    got = bm.map_cache(case["cache"], _preset(case["preset"]), is_preset=case["is_preset"])
    assert got == case["expected"]


@pytest.mark.parametrize("path", UNMAPPED, ids=lambda p: p.stem)
def test_map_cache_unmapped(path):
    case = json.loads(path.read_text())
    with pytest.raises(bm.Unmapped, match=case["unmapped"]):
        bm.map_cache(case["cache"], _preset(case["preset"]), is_preset=case["is_preset"])


def test_map_status():
    run = {"state": "succeeded", "job": "agent-s-cache-restore"}
    save = {"state": "succeeded"}
    tx = {"state": "skipped", "reason": "not_configured"}
    got = bm.map_status({"restore": run, "save": save, "sessions_restore": tx, "sessions_save": tx})
    assert got == {
        "entries": [],
        "runs": {"restore_pre": run, "save": save},
        "transcripts": {"restore": tx, "save": tx},
    }
    assert bm.map_status(None) is None
    assert bm.map_status({}) == {"entries": [], "runs": {}, "transcripts": {}}


WS = {"root": "workspace", "path": ".", "mode": "both", "name": "proj/workspace"}


@pytest.mark.parametrize(
    ("old", "new"),
    [
        ("restored", "restored"),
        ("miss", "missing"),
        ("reset", "unreadable"),
        ("failed", "failed"),
        ("running", "running"),
    ],
)
def test_map_status_seeds_the_restore_of_each_restored_entry(old, new):
    run = {"state": old, "finished_at": "2026-11-01T10:00:00+00:00", "reason": None}
    split = [{**WS, "mode": "restore"}, {**WS, "mode": "backup", "name": "other"}]
    got = bm.map_status({"restore": run}, split)
    first, second = got["entries"]
    assert first["index"] == 0 and first["root"] == "workspace" and first["path"] == "."
    assert first["restore"]["state"] == new
    assert first["restore"]["name"] == "proj/workspace"
    assert first["restore"]["at"] == "2026-11-01T10:00:00+00:00"
    assert first["save"] is None and first["version"] is None
    assert second["restore"] is None  # a backup-only entry never restored


def test_map_status_leaves_a_skipped_or_absent_restore_pending():
    # the old rule never saved after a skipped or absent workdir restore; the new one keeps that
    for cache in ({"restore": {"state": "skipped", "reason": "not_configured"}}, {}):
        assert bm.map_status(cache, [WS])["entries"][0]["restore"] is None


# ---------------------------------------------------------------------- stored specs


def _sql(dsn: str, query: str, params=()):
    with psycopg.connect(dsn, autocommit=True) as c:
        cur = c.execute(query, params)
        return cur.fetchall() if cur.description else None


def _preset_row(dsn: str, name: str, spec: dict) -> uuid.UUID:
    pid = uuid.uuid4()
    _sql(dsn, "INSERT INTO presets (id, name, spec) VALUES (%s,%s,%s)", (pid, name, Jsonb(spec)))
    return pid


def _session_row(
    dsn: str, spec: dict, preset: uuid.UUID | None, status=None, name: str | None = None
) -> uuid.UUID:
    sid = uuid.uuid4()
    _sql(
        dsn,
        "INSERT INTO sessions (id, slug, spec, status, preset_id, preset_name)"
        " VALUES (%s,%s,%s,%s,%s,%s)",
        (sid, f"s-{sid.hex[:8]}", Jsonb(spec), Jsonb(status or {}), preset, name),
    )
    return sid


def _spec_of(dsn: str, table: str, id: uuid.UUID) -> dict:
    return _sql(dsn, f"SELECT spec FROM {table} WHERE id = %s", (id,))[0][0]


def test_apply_rewrites_specs_and_status_idempotently(dsn):
    pid = _preset_row(dsn, "Mtg Bot", {"cache": {}})
    derived = bm.legacy_cache_key(pid)
    sid = _session_row(
        dsn,
        {"cache": {"restore_key": derived, "save_key": derived}},
        pid,
        {"cache": {"restore": {"state": "succeeded"}, "sessions_save": {"state": "skipped"}}},
    )
    plain = _session_row(dsn, {"replicas": 1}, None)
    orphan = _session_row(dsn, {"cache": {}}, None, name="Gone Preset")
    with psycopg.connect(dsn) as conn:
        bm.apply(conn)
    with psycopg.connect(dsn) as conn:
        bm.apply(conn)  # idempotent
    want = [{"root": "workspace", "path": ".", "mode": "both", "name": "mtg-bot/workspace"}]
    assert _spec_of(dsn, "presets", pid) == {"backup": want}
    assert _spec_of(dsn, "sessions", sid) == {"backup": want}
    assert _spec_of(dsn, "sessions", plain) == {"replicas": 1}
    # the preset is gone: the session keeps the name its preset_name slugs to
    want_gone = [{**want[0], "name": "gone-preset/workspace"}]
    assert _spec_of(dsn, "sessions", orphan) == {"backup": want_gone}
    status = _sql(dsn, "SELECT status FROM sessions WHERE id = %s", (sid,))[0][0]
    assert "cache" not in status
    assert status["backup"]["runs"] == {"restore_pre": {"state": "succeeded"}}
    assert status["backup"]["transcripts"] == {"save": {"state": "skipped"}}
    SessionSpec.model_validate(without_legacy_backup(_spec_of(dsn, "presets", pid)))


def test_run_migrations_runs_the_step_and_refuses_unmapped(dsn):
    pid = _preset_row(dsn, "Old", {"cache": {"restore_key": "k", "save_key": "k"}})
    assert run_migrations(dsn) == []
    assert _spec_of(dsn, "presets", pid)["backup"][0]["name"] == "k"
    _session_row(dsn, {"cache": {}}, None)  # no preset to name the default backup
    with pytest.raises(bm.MigrationError, match="unmapped"):
        run_migrations(dsn)


def _cli_settings(monkeypatch, dsn: str, **kw) -> Settings:
    settings = Settings(database_url=dsn, agent_image="img:1", **kw)
    monkeypatch.setattr(cli, "_settings", lambda: settings)
    return settings


def test_dry_run_reports_unmapped_exit_1(dsn, monkeypatch, capsys):
    _cli_settings(monkeypatch, dsn)
    pid = _preset_row(dsn, "Good", {"cache": {"restore_key": "a", "save_key": "b"}})
    assert cli.main(["backup", "migrate", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert f"preset {pid} " in out and " -> " in out
    assert "UNMAPPED" not in out
    # the dry run changed nothing
    assert "cache" in _spec_of(dsn, "presets", pid)
    bad = _session_row(dsn, {"cache": {}}, None)
    assert cli.main(["backup", "migrate", "--dry-run"]) == 1
    out = capsys.readouterr().out
    assert f"UNMAPPED session {bad} " in out


def test_dry_run_excludes_the_writing_modes(dsn, monkeypatch, capsys):
    _cli_settings(monkeypatch, dsn)
    for extra in (["--delete-old"], ["--add-entries", "--preset", "x=p"]):
        with pytest.raises(SystemExit) as exc:
            cli.main(["backup", "migrate", "--dry-run", *extra])
        assert exc.value.code == 2


def test_deleted_preset_derived_key_maps_spec_and_object_alike(dsn, s3, monkeypatch):
    """The preset is gone: spec and object keep the literal `preset-<hash>`, so they still meet."""
    _s3_settings(s3, dsn, monkeypatch)
    derived = bm.legacy_cache_key(uuid.uuid4())
    sid = _session_row(
        dsn, {"cache": {"restore_key": derived, "save_key": derived}}, None, name="Gone"
    )
    _put(s3, f"{P}{derived}.tar.zst")
    with psycopg.connect(dsn) as conn:
        bm.apply(conn)
    assert _spec_of(dsn, "sessions", sid)["backup"][0]["name"] == derived
    assert cli.main(["backup", "migrate"]) == 0
    assert _current(dsn, derived) == f"{P}{derived}.tar.zst"


def test_dry_run_flags_any_spec_the_strict_model_refuses(dsn, monkeypatch, capsys):
    _cli_settings(monkeypatch, dsn)
    bad = _preset_row(dsn, "Odd", {"not_a_field": 1})
    assert cli.main(["backup", "migrate", "--dry-run"]) == 1
    assert f"UNMAPPED preset {bad} " in capsys.readouterr().out


# ---------------------------------------------------------------------- add entries


def test_add_entries_skips_existing(dsn, monkeypatch, capsys):
    _cli_settings(monkeypatch, dsn)
    pid = _preset_row(
        dsn,
        "Dreaming",
        {"backup": [{"root": "claude", "path": "skills", "mode": "both", "name": "p1/skills"}]},
    )
    args = ["backup", "migrate", "--add-entries", "--preset", f"{pid}=p1"]
    assert cli.main(args) == 0
    entries = _spec_of(dsn, "presets", pid)["backup"]
    assert [(e["path"], e["name"]) for e in entries] == [
        ("skills", "p1/skills"),
        ("rules", "p1/rules"),
        ("scripts", "p1/scripts"),
    ]
    assert entries[1] == {
        "root": "claude",
        "path": "rules",
        "mode": "both",
        "name": "p1/rules",
        "live": True,
    }
    assert cli.main(args) == 0  # again: nothing new
    assert _spec_of(dsn, "presets", pid)["backup"] == entries


def test_add_entries_workspace_root_for_several_projects(dsn, monkeypatch):
    _cli_settings(monkeypatch, dsn)
    pid = _preset_row(dsn, "Dream", {})
    args = ["backup", "migrate", "--add-entries", "--root", "workspace"]
    args += ["--preset", "Dream=alpha", "--preset", f"{pid}=beta"]
    assert cli.main(args) == 0
    got = [(e["root"], e["path"], e["name"]) for e in _spec_of(dsn, "presets", pid)["backup"]]
    assert ("workspace", "alpha/skills", "alpha/skills") in got
    assert ("workspace", "beta/scripts", "beta/scripts") in got
    assert len(got) == 6
    entry = _spec_of(dsn, "presets", pid)["backup"][0]
    assert entry["mode"] == "both" and "live" not in entry


# ---------------------------------------------------------------------- objects


@pytest.fixture(scope="module")
def moto_endpoint() -> Iterator[str]:
    server = ThreadedMotoServer(ip_address="127.0.0.1", port=0, verbose=False)
    server.start()
    host, port = server.get_host_and_port()
    try:
        yield f"http://{host}:{port}"
    finally:
        server.stop()


@pytest.fixture
def s3(moto_endpoint, monkeypatch):
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    bucket = f"cache-{uuid.uuid4().hex[:12]}"
    client = boto3.client(
        "s3",
        endpoint_url=moto_endpoint,
        region_name="us-east-1",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET,
    )
    client.create_bucket(Bucket=bucket)
    return client, bucket, moto_endpoint


def _put(s3, key: str, body: bytes = b"x") -> None:
    client, bucket, _ = s3
    client.put_object(Bucket=bucket, Key=key, Body=body)


def _keys(s3) -> set[str]:
    client, bucket, _ = s3
    pages = client.get_paginator("list_objects_v2").paginate(Bucket=bucket)
    return {o["Key"] for p in pages for o in p.get("Contents", ())}


def _s3_settings(s3, dsn: str, monkeypatch) -> Settings:
    _, bucket, endpoint = s3
    return _cli_settings(
        monkeypatch,
        dsn,
        cache_s3_endpoint=endpoint,
        cache_s3_bucket=bucket,
        cache_s3_path_style=True,
        cache_s3_access_key_id=ACCESS_KEY,
        cache_s3_secret_access_key=SECRET,
    )


def _versions(dsn: str, name: str) -> list[tuple]:
    return _sql(
        dsn,
        "SELECT v.state, v.origin, v.manifest ->> 'migrated_from' FROM backup_versions v"
        " WHERE v.name = %s ORDER BY v.object_key",
        (name,),
    )


def _current(dsn: str, name: str) -> str:
    return _sql(
        dsn,
        "SELECT v.manifest ->> 'migrated_from' FROM backups b"
        " JOIN backup_versions v ON v.id = b.current_version WHERE b.name = %s",
        (name,),
    )[0][0]


P = "workspace-cache/"


def test_object_migration_groups_versions(dsn, s3, monkeypatch):
    settings = _s3_settings(s3, dsn, monkeypatch)
    pid = _preset_row(dsn, "Mtg Bot", {})
    derived = bm.legacy_cache_key(pid)
    _put(s3, f"{P}proj/20261030t100000z.tar.zst")
    _put(s3, f"{P}proj/20261031t100000z.tar.zst")
    _put(s3, f"{P}flat-name.tar.zst")
    _put(s3, f"{P}{derived}.tar.zst")
    _put(s3, f"{P}sessions/some-slug.tar.zst")
    assert cli.main(["backup", "migrate"]) == 0
    assert _current(dsn, "proj") == f"{P}proj/20261031t100000z.tar.zst"
    assert len(_versions(dsn, "proj")) == 2
    assert [v[0] for v in _versions(dsn, "proj")] == ["superseded", "current"]
    assert all(v[1] == "migration" for v in _versions(dsn, "proj"))
    assert len(_versions(dsn, "flat-name")) == 1
    assert len(_versions(dsn, "mtg-bot/workspace")) == 1
    assert _sql(dsn, "SELECT count(*) FROM backups WHERE name LIKE 'sessions%%'")[0][0] == 0
    keys = _keys(s3)
    new = {k for k in keys if k.startswith(f"{P}b/")}
    assert len(new) == 4
    assert all(k.endswith(".tar.zst") for k in new)
    assert any(k.startswith(f"{P}b/proj/20261031t100000z-migration-") for k in new)
    assert f"{P}proj/20261030t100000z.tar.zst" in keys  # old objects stay until --delete-old
    assert settings.cache_s3_bucket


def test_object_migration_idempotent(dsn, s3, monkeypatch):
    _s3_settings(s3, dsn, monkeypatch)
    _put(s3, f"{P}proj/20261030t100000z.tar.zst")
    _put(s3, f"{P}flat.tar.zst")
    assert cli.main(["backup", "migrate"]) == 0
    before = _keys(s3)
    rows = _sql(dsn, "SELECT count(*) FROM backup_versions")[0][0]
    assert cli.main(["backup", "migrate"]) == 0
    assert _keys(s3) == before
    assert _sql(dsn, "SELECT count(*) FROM backup_versions")[0][0] == rows == 2


def test_delete_old_spares_transcripts(dsn, s3, monkeypatch):
    _s3_settings(s3, dsn, monkeypatch)
    _put(s3, f"{P}proj/20261030t100000z.tar.zst")
    _put(s3, f"{P}not-migrated.tar.zst")
    _put(s3, f"{P}sessions/slug.tar.zst")
    assert cli.main(["backup", "migrate"]) == 0
    _put(s3, f"{P}later.tar.zst")  # appears after the migration: no row, so it stays
    assert cli.main(["backup", "migrate", "--delete-old"]) == 0
    keys = _keys(s3)
    assert f"{P}proj/20261030t100000z.tar.zst" not in keys
    assert f"{P}not-migrated.tar.zst" not in keys
    assert f"{P}sessions/slug.tar.zst" in keys
    assert f"{P}later.tar.zst" in keys
    assert len([k for k in keys if k.startswith(f"{P}b/")]) == 2


def test_migrated_versions_never_move_a_newer_current(dsn, s3, monkeypatch):
    _s3_settings(s3, dsn, monkeypatch)
    _put(s3, f"{P}proj/20261030t100000z.tar.zst")
    with psycopg.connect(dsn, autocommit=True) as c:
        c.execute("INSERT INTO backups (name, updated_at) VALUES ('proj', now())")
        vid = uuid.uuid4()
        c.execute(
            "INSERT INTO backup_versions (id, name, object_key, size, digest, origin,"
            " created_by, state) VALUES (%s,'proj',%s,1,'d','save','s','current')",
            (vid, f"{P}b/proj/x.tar.zst"),
        )
        c.execute("UPDATE backups SET current_version = %s WHERE name = 'proj'", (vid,))
    assert cli.main(["backup", "migrate"]) == 0
    assert _sql(dsn, "SELECT current_version FROM backups WHERE name='proj'")[0][0] == vid
    assert [v[0] for v in _versions(dsn, "proj") if v[1] == "migration"] == ["orphan"]


def test_apply_withdraws_open_proposals_that_patch_cache(dsn):
    pid = _preset_row(dsn, "Prop", {"cache": {"restore_key": "k", "save_key": "k"}})
    ids = []
    for path in ("cache", "env"):
        row = _sql(
            dsn,
            "INSERT INTO change_proposals (kind, target_id, target_name, proposer_session,"
            " proposer_slug, proposer_superleader, expires_at)"
            " VALUES ('preset', %s, 'Prop', gen_random_uuid(), 's', false,"
            " now() + interval '1 day')"
            " RETURNING id",
            (str(pid),),
        )
        ids.append(row[0][0])
        _sql(
            dsn,
            "INSERT INTO change_proposal_revisions (proposal_id, revision, reason, base,"
            " fingerprint) VALUES (%s, 1, 'r', '{}', 'f')",
            (ids[-1],),
        )
        _sql(
            dsn,
            'INSERT INTO change_proposal_items (proposal_id, revision, path, security, "grant")'
            " VALUES (%s, 1, %s, false, false)",
            (ids[-1], path),
        )
    with psycopg.connect(dsn) as conn:
        bm.apply(conn)
    got = dict(_sql(dsn, "SELECT id, status FROM change_proposals"))
    assert got[ids[0]] == "withdrawn" and got[ids[1]] == "open"
    outcomes = dict(_sql(dsn, "SELECT proposal_id, outcome FROM change_proposal_revisions"))
    assert outcomes == {ids[0]: "withdrawn", ids[1]: None}
    notes = dict(_sql(dsn, "SELECT proposal_id, note FROM change_proposal_items"))
    assert "spec.backup" in notes[ids[0]] and notes[ids[1]] is None


def test_object_migration_skips_reserved_volume_names(dsn, s3, monkeypatch):
    _s3_settings(s3, dsn, monkeypatch)
    _put(s3, f"{P}volume-x.tar.zst")
    _put(s3, f"{P}volume-y/20261030t100000z.tar.zst")
    assert cli.main(["backup", "migrate"]) == 0
    assert _sql(dsn, "SELECT count(*) FROM backups")[0][0] == 0
