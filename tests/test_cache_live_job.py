"""Job side of live delivery and on-demand save: the dirty-directory check (S-68, T-519)."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
from test_cache_cli import (
    _archive_of,
    _entries_env,
    _entries_out,
    _entry,
    _make_src,
    _Recorder,
    _tree,
)

from agents_orchestrator.cache import archive, transfer


def _digest(path: Path) -> str:
    return archive.digest(archive.manifest(path, []))


def _restore(tmp_path, capsys, *, skip_dirty=True, **kw):
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    entry = _entry(tmp_path, index=2, name="proj-skills", version="v2", **kw)
    if skip_dirty:
        entry["skip_dirty"] = True
    code = transfer.restore_entries(_entries_env(tmp_path, [entry]), transport=rec.transport)
    return code, _entries_out(capsys)[0], rec


def test_state_records_the_digest(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    _, out, _ = _restore(tmp_path, capsys, skip_dirty=False)
    state = json.loads((tmp_path / "state" / "2.json").read_text())
    assert out["state"] == "restored" and state["digest"] == out["digest"]


def test_clean_dir_is_overwritten(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "a.txt").write_text("old")
    (tmp_path / "state" / "2.json").write_text(json.dumps({"digest": _digest(work)}))
    _, out, rec = _restore(tmp_path, capsys)
    assert out["state"] == "restored" and len(rec.requests) == 1
    assert (work / "a.txt").read_text() == "alpha"


def test_dirty_dir_is_skipped_and_untouched(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "a.txt").write_text("old")
    (tmp_path / "state" / "2.json").write_text(json.dumps({"digest": _digest(work)}))
    (work / "a.txt").write_text("my unsaved edit")
    before = json.loads((tmp_path / "state" / "2.json").read_text())
    code, out, rec = _restore(tmp_path, capsys)
    assert code == 0 and out == {"index": 2, "state": "skipped_dirty"}
    assert _tree(work) == {"a.txt": "my unsaved edit"}
    assert json.loads((tmp_path / "state" / "2.json").read_text()) == before
    assert rec.requests == []  # nothing downloaded


def test_files_without_a_state_record_are_dirty(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "mine.txt").write_text("unsaved")
    _, out, _ = _restore(tmp_path, capsys)
    assert out["state"] == "skipped_dirty"
    assert _tree(work) == {"mine.txt": "unsaved"}


def test_absent_dir_without_a_state_record_is_restored(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    _, out, _ = _restore(tmp_path, capsys)
    assert out["state"] == "restored"


def test_skip_dirty_is_off_for_a_plain_restore(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "mine.txt").write_text("unsaved")
    _, out, _ = _restore(tmp_path, capsys, skip_dirty=False)
    assert out["state"] == "restored"


def test_save_writes_the_state_so_the_saver_is_not_dirty(tmp_path, capsys):
    (tmp_path / "state").mkdir()
    work = tmp_path / "root" / "work"
    _make_src(work)
    rec = _Recorder(lambda request: httpx.Response(200))
    entry = _entry(tmp_path, index=2, name="proj-skills")
    assert transfer.save_entries(_entries_env(tmp_path, [entry]), transport=rec.transport) == 0
    out = _entries_out(capsys)[0]
    state = json.loads((tmp_path / "state" / "2.json").read_text())
    assert out["state"] == "uploaded" and state["digest"] == out["digest"]
    entry["skip_dirty"] = True  # the next live delivery finds the saver's dir clean
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    transfer.restore_entries(_entries_env(tmp_path, [entry]), transport=rec.transport)
    assert _entries_out(capsys)[0]["state"] == "restored"
