"""Workspace cache archive: tar.zst pack with excludes and a cap, safe unpack (design D5, D6)."""

from __future__ import annotations

import io
import logging
import os
import socket
import stat
import tarfile
from pathlib import Path

import pytest
import zstandard

from agents_orchestrator.cache import archive
from agents_orchestrator.cache.archive import ExtractError, Packed, TooLarge, pack, unpack
from agents_orchestrator.spec import INIT_DIR, SESSIONS_EXCLUDE

BIG = 1 << 30


def _packed(workdir: Path, exclude: list[str] | None = None) -> bytes:
    out = io.BytesIO()
    packed = pack(workdir, out, exclude=exclude or [], max_bytes=BIG)
    assert packed.compressed_bytes == len(out.getvalue())
    return out.getvalue()


def _tar_bytes(data: bytes) -> bytes:
    return zstandard.ZstdDecompressor().stream_reader(io.BytesIO(data)).read()


def _members(data: bytes) -> dict[str, tarfile.TarInfo]:
    with tarfile.open(fileobj=io.BytesIO(_tar_bytes(data)), mode="r:") as tar:
        return {m.name: m for m in tar.getmembers()}


def _zst_tar(*members: tuple[tarfile.TarInfo, bytes | None]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:", format=tarfile.PAX_FORMAT) as tar:
        for info, content in members:
            tar.addfile(info, io.BytesIO(content) if content is not None else None)
    return zstandard.ZstdCompressor().compress(raw.getvalue())


def _info(name: str, kind: bytes = tarfile.REGTYPE, *, size: int = 0, link: str = ""):
    info = tarfile.TarInfo(name)
    info.type = kind
    info.size = size
    info.linkname = link
    info.mode = 0o644
    return info


def _snapshot(root: Path) -> set[str]:
    return {str(p.relative_to(root)) for p in root.rglob("*")}


def test_round_trip_keeps_files_modes_symlinks(tmp_path: Path) -> None:
    src = tmp_path / "src"
    (src / "repo" / "bin").mkdir(parents=True)
    (src / "empty-dir").mkdir()
    (src / "repo" / "README.md").write_text("hello\n")
    tool = src / "repo" / "bin" / "tool"
    tool.write_text("#!/bin/sh\necho hi\n")
    tool.chmod(0o755)
    secret = src / "repo" / "secret.txt"
    secret.write_text("s3cr3t")
    secret.chmod(0o600)
    os.symlink("README.md", src / "repo" / "readme-link")
    os.symlink("repo/bin", src / "bin-link")
    os.link(src / "repo" / "README.md", src / "repo" / "README.hard")

    dst = tmp_path / "restore" / "workdir"
    count = unpack(io.BytesIO(_packed(src)), dst)

    assert count == len(_snapshot(src))
    assert _snapshot(dst) == _snapshot(src)
    assert (dst / "repo" / "README.md").read_text() == "hello\n"
    assert (dst / "repo" / "README.hard").read_text() == "hello\n"
    assert stat.S_IMODE((dst / "repo" / "bin" / "tool").stat().st_mode) == 0o755
    assert stat.S_IMODE((dst / "repo" / "secret.txt").stat().st_mode) == 0o600
    assert (dst / "empty-dir").is_dir()
    assert os.readlink(dst / "repo" / "readme-link") == "README.md"
    assert os.readlink(dst / "bin-link") == "repo/bin"
    assert (dst / "bin-link" / "tool").read_text() == "#!/bin/sh\necho hi\n"


def test_excludes(tmp_path: Path) -> None:
    src = tmp_path / "src"
    (src / INIT_DIR).mkdir(parents=True)
    (src / INIT_DIR / "init.sh").write_text("echo init\n")
    (src / "node_modules" / "pkg" / "lib").mkdir(parents=True)
    (src / "node_modules" / "pkg" / "lib" / "index.js").write_text("x")
    (src / "app" / "node_modules" / "dep").mkdir(parents=True)
    (src / "app" / "node_modules" / "dep" / "a.js").write_text("x")
    (src / "app" / "main.py").write_text("print()")
    (src / "app" / "debug.log").write_text("noise")
    (src / "keep.txt").write_text("keep")

    names = set(_members(_packed(src, ["node_modules", "*/node_modules", "*.log"])))

    assert names == {"app", "app/main.py", "keep.txt"}
    assert set(_members(_packed(src))) >= {"node_modules/pkg/lib/index.js", "app/debug.log"}
    assert not any(n == INIT_DIR or n.startswith(INIT_DIR + "/") for n in _members(_packed(src)))


def test_sessions_exclude_drops_credentials(tmp_path: Path) -> None:
    src = tmp_path / "sessions"
    (src / "ses-1" / "config" / "projects" / "p").mkdir(parents=True)
    (src / "ses-1" / "config" / ".credentials.json").write_text('{"token": "secret"}')
    (src / "ses-1" / "config" / "projects" / "p" / "t.jsonl").write_text("{}\n")
    (src / "ses-1" / "config" / "projects" / "p" / ".credentials.json").write_text("deep")
    (src / ".credentials.json").write_text("top")
    (src / "ses-1" / "turns.jsonl").write_text("{}\n")

    names = set(_members(_packed(src, list(SESSIONS_EXCLUDE))))

    assert not any(n.rsplit("/", 1)[-1] == ".credentials.json" for n in names)
    assert {
        "ses-1/config/projects/p/t.jsonl",
        "ses-1/turns.jsonl",
    } <= names


def test_symlinks_not_followed_specials_skipped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "host-secret").write_text("TOP-SECRET-CONTENT")
    src = tmp_path / "w"
    src.mkdir()
    (src / "sub").mkdir()
    (src / "sub" / "inside.txt").write_text("inside")
    os.symlink(outside / "host-secret", src / "file-link")
    os.symlink(outside, src / "dir-link")
    os.symlink("../outside/host-secret", src / "dotdot-link")
    os.symlink("../dir-link/host-secret", src / "sub" / "via-escaping-link")
    os.symlink("sub/inside.txt", src / "inside-link")
    os.symlink("../inside-link", src / "sub" / "chained-inside-link")
    os.mkfifo(src / "fifo")
    monkeypatch.chdir(src)
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind("sock")
    out = io.BytesIO()
    try:
        packed = pack(src, out, exclude=[], max_bytes=BIG)
    finally:
        sock.close()

    data = out.getvalue()
    members = _members(data)
    assert packed == Packed(compressed_bytes=len(data), skipped_links=4)
    assert set(members) == {"sub", "sub/inside.txt", "inside-link", "sub/chained-inside-link"}
    assert members["inside-link"].issym()
    assert members["inside-link"].linkname == "sub/inside.txt"
    assert b"TOP-SECRET-CONTENT" not in _tar_bytes(data)
    assert str(outside).encode() not in _tar_bytes(data)


def test_venv_absolute_link_skipped(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    src = tmp_path / "src"
    (src / ".venv" / "bin").mkdir(parents=True)
    (src / ".venv" / "lib").mkdir()
    (src / ".venv" / "pyvenv.cfg").write_text("home = /usr/bin\n")
    os.symlink("/usr/bin/python3", src / ".venv" / "bin" / "python")
    os.symlink("lib", src / ".venv" / "lib64")
    out = io.BytesIO()

    with caplog.at_level(logging.INFO, logger="agents_orchestrator.cache.archive"):
        packed = pack(src, out, exclude=[], max_bytes=BIG)

    assert packed.skipped_links == 1
    assert packed.compressed_bytes == len(out.getvalue())
    lines = [
        r.getMessage() for r in caplog.records if r.name == "agents_orchestrator.cache.archive"
    ]
    assert len(lines) == 1
    assert "1" in lines[0]
    assert "/usr/bin/python3" not in lines[0]
    dst = tmp_path / "dst"
    assert unpack(io.BytesIO(out.getvalue()), dst) == 5
    assert not (dst / ".venv" / "bin" / "python").is_symlink()
    assert os.readlink(dst / ".venv" / "lib64") == "lib"
    assert (dst / ".venv" / "pyvenv.cfg").read_text() == "home = /usr/bin\n"


def test_too_large(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    (src / "noise.bin").write_bytes(os.urandom(1 << 20))
    out = io.BytesIO()

    with pytest.raises(TooLarge):
        pack(src, out, exclude=[], max_bytes=64 * 1024)
    assert len(out.getvalue()) <= 64 * 1024

    fits = io.BytesIO()
    packed = pack(src, fits, exclude=[], max_bytes=4 << 20)
    assert packed.compressed_bytes == len(fits.getvalue()) > 1 << 20


def test_unpack_refuses_unsafe_members(tmp_path: Path) -> None:
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "victim").write_text("original")
    cases = {
        "absolute path": [(_info(str(outside / "abs-escape"), size=3), b"bad")],
        "dot-dot": [(_info("../outside/dotdot-escape", size=3), b"bad")],
        "nested dot-dot": [(_info("a/../../outside/nested-escape", size=3), b"bad")],
        "relative link escaping": [(_info("up", tarfile.SYMTYPE, link="../outside"), None)],
        "absolute link": [(_info("abs", tarfile.SYMTYPE, link=str(outside)), None)],
        "write through escaping link": [
            (_info("via", tarfile.SYMTYPE, link="../outside"), None),
            (_info("via/through-link", size=3), b"bad"),
        ],
        "hard link outside": [
            (_info("hard", tarfile.LNKTYPE, link=str(outside / "victim")), None),
        ],
        "hard link dot-dot": [
            (_info("hard2", tarfile.LNKTYPE, link="../outside/victim"), None),
        ],
        "char device": [(_info("null", tarfile.CHRTYPE), None)],
        "block device": [(_info("disk", tarfile.BLKTYPE), None)],
        "fifo": [(_info("pipe", tarfile.FIFOTYPE), None)],
    }
    for label, members in cases.items():
        workdir = tmp_path / "work" / label.replace(" ", "-")
        with pytest.raises(ExtractError):
            unpack(io.BytesIO(_zst_tar(*members)), workdir)
        assert _snapshot(outside) == {"victim"}, label
        assert (outside / "victim").read_text() == "original", label
        assert not (tmp_path / "work" / "outside").exists(), label
        assert not any(p.name.endswith("escape") for p in tmp_path.rglob("*")), label


def test_unpack_refuses_decompression_bomb(tmp_path: Path) -> None:
    size = 64 << 20
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w:") as tar:
        info = _info("zeros", size=size)
        tar.addfile(info, io.BytesIO(bytes(size)))
    bomb = zstandard.ZstdCompressor(level=19).compress(raw.getvalue())
    assert len(bomb) < size // 1000
    workdir = tmp_path / "w"

    with pytest.raises(ExtractError):
        unpack(io.BytesIO(bomb), workdir, max_bytes=1 << 20)
    assert not (workdir / "zeros").exists()

    sparse = tarfile.TarInfo("holes")
    sparse.type = tarfile.GNUTYPE_SPARSE
    with pytest.raises(ExtractError):
        unpack(io.BytesIO(_zst_tar((sparse, None))), tmp_path / "s")

    header_flood = _zst_tar(*((_info(f"e{i}"), b"") for i in range(500)))
    with pytest.raises(ExtractError):
        unpack(io.BytesIO(header_flood), tmp_path / "flood", max_bytes=64 * 1024)
    assert len(list((tmp_path / "flood").iterdir())) < 500


def test_unpack_keeps_existing_files(tmp_path: Path) -> None:
    src = tmp_path / "src"
    (src / "repo").mkdir(parents=True)
    (src / "repo" / "cached.txt").write_text("from cache")
    data = _packed(src)
    dst = tmp_path / "dst"
    (dst / "repo").mkdir(parents=True)
    (dst / "repo" / "local.txt").write_text("local")
    (dst / "top.txt").write_text("top")

    assert unpack(io.BytesIO(data), dst) == 2

    assert (dst / "repo" / "local.txt").read_text() == "local"
    assert (dst / "top.txt").read_text() == "top"
    assert (dst / "repo" / "cached.txt").read_text() == "from cache"


def test_corrupt_stream(tmp_path: Path) -> None:
    src = tmp_path / "src"
    src.mkdir()
    (src / "f").write_bytes(os.urandom(4096))
    good = _packed(src)
    not_tar = zstandard.ZstdCompressor().compress(b"this is not a tar archive" * 100)
    for label, data in {
        "garbage": b"\x00not zstd at all" * 64,
        "truncated": good[: len(good) // 2],
        "zstd but not tar": not_tar,
    }.items():
        with pytest.raises(ExtractError):
            unpack(io.BytesIO(data), tmp_path / "dst" / label.replace(" ", "-"))


def test_refuses_empty_or_root_workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    def no_fs(*args, **kwargs):
        raise AssertionError("filesystem touched")

    for name in ("walk", "scandir", "lstat", "stat", "mkdir", "makedirs", "open"):
        monkeypatch.setattr(archive.os, name, no_fs, raising=False)
    monkeypatch.setattr(archive.Path, "mkdir", no_fs)
    monkeypatch.setattr(archive.Path, "lstat", no_fs)
    monkeypatch.setattr(archive.Path, "stat", no_fs)
    monkeypatch.setattr(archive.tarfile, "open", no_fs)

    for bad in (Path(""), Path("."), Path("/"), Path("//"), Path("/../")):
        out = io.BytesIO()
        with pytest.raises(ValueError):
            pack(bad, out, exclude=[], max_bytes=BIG)
        assert out.getvalue() == b""
        src = io.BytesIO(b"anything")
        with pytest.raises(ValueError):
            unpack(src, bad)
        assert src.tell() == 0


def test_pack_leaves_no_finalizer_error_after_out_is_closed(tmp_path, monkeypatch):
    """Closing `out` after pack must not make the cap wrapper's finalizer flush a closed file.

    The cache Job's outcome line is the last log line; a late "Exception ignored" on stderr
    would follow it.
    """
    import gc
    import sys

    work = tmp_path / "work"
    work.mkdir()
    (work / "a.txt").write_text("alpha")
    unraisable: list[object] = []
    monkeypatch.setattr(sys, "unraisablehook", unraisable.append)
    with (tmp_path / "out.tar.zst").open("wb") as out:
        pack(work, out, exclude=[], max_bytes=BIG)
    gc.collect()
    assert unraisable == []


# ------------------------------------------------ session config needs the redacted marker


def _session_dir(root: Path, pod: str, sid: str, *, marker: bool) -> Path:
    d = root / pod / sid
    (d / "config" / "projects" / "p").mkdir(parents=True)
    (d / "config" / "projects" / "p" / "t.jsonl").write_text("{}\n")
    (d / "config" / ".credentials.json").write_text("{}")
    (d / "session.json").write_text("{}")
    (d / "events.jsonl").write_text("e\n")
    if marker:
        (d / ".redacted").write_text('{"format": 1}')
    return d


def _names(data: bytes) -> set[str]:
    return set(_members(data))


def _pack_sessions(root: Path) -> bytes:
    out = io.BytesIO()
    pack(
        root,
        out,
        exclude=list(SESSIONS_EXCLUDE),
        max_bytes=BIG,
        require_redacted=True,
    )
    return out.getvalue()


def test_session_config_packed_only_with_marker(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    root = tmp_path / "sessions"
    _session_dir(root, "pod-0", "marked", marker=True)
    _session_dir(root, "pod-0", "plain", marker=False)
    with caplog.at_level(logging.WARNING):
        names = _names(_pack_sessions(root))

    assert "pod-0/marked/config/projects/p/t.jsonl" in names
    assert "pod-0/marked/.redacted" in names
    assert not any("/.credentials.json" in n for n in names)
    assert "pod-0/plain/session.json" in names and "pod-0/plain/events.jsonl" in names
    assert not any(n.startswith("pod-0/plain/config") for n in names)
    msgs = [r.getMessage() for r in caplog.records]
    assert msgs == ["session config left out of the archive: no redacted marker (session plain)"]


def test_session_config_marker_symlink_does_not_count(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    d = _session_dir(root, "pod-0", "linked", marker=False)
    os.symlink(d / "events.jsonl", d / ".redacted")
    assert not any(n.startswith("pod-0/linked/config") for n in _names(_pack_sessions(root)))


def test_config_dir_without_session_file_is_ordinary(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    (root / "pod-0" / "config").mkdir(parents=True)
    (root / "pod-0" / "config" / "x").write_text("1")
    assert "pod-0/config/x" in _names(_pack_sessions(root))


def test_marker_not_required_by_default(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _session_dir(root, "pod-0", "plain", marker=False)
    assert "pod-0/plain/config/projects/p/t.jsonl" in _names(_packed(root))


def test_restore_of_archive_without_config(tmp_path: Path) -> None:
    root = tmp_path / "sessions"
    _session_dir(root, "pod-0", "plain", marker=False)
    dst = tmp_path / "restored"
    count = unpack(io.BytesIO(_pack_sessions(root)), dst)
    assert count > 0
    assert (dst / "pod-0" / "plain" / "session.json").is_file()
    assert not (dst / "pod-0" / "plain" / "config").exists()
