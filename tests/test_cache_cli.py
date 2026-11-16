"""The Job-side transfer CLI `orch cache restore|save` (contract orch-cache-job-cli; D1, D11)."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import tarfile
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest
import zstandard

from agents_orchestrator import cli
from agents_orchestrator.cache import archive, transfer

SIGNATURE = "c2VjcmV0LXNpZ25hdHVyZQ"
URL = (
    "http://s3.example:9000/bucket/workspace-cache/preset-0123.tar.zst"
    f"?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Signature={SIGNATURE}"
)


def _tree(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): p.read_text()
        for p in sorted(root.rglob("*"))
        if p.is_file() and not p.is_symlink()
    }


def _make_src(root: Path) -> Path:
    (root / "sub").mkdir(parents=True)
    (root / "a.txt").write_text("alpha")
    (root / "sub" / "b.txt").write_text("beta")
    return root


def _archive_of(root: Path) -> bytes:
    buf = io.BytesIO()
    archive.pack(root, buf, exclude=[], max_bytes=archive.MAX_ARCHIVE_BYTES)
    return buf.getvalue()


def _outcome(out: str) -> dict:
    lines = [line for line in out.splitlines() if line.strip()]
    outcome = json.loads(lines[-1])
    assert {"outcome", "bytes", "reason"} <= outcome.keys()
    return outcome


def _env(tmp_path: Path, **extra: str) -> dict[str, str]:
    env = {
        "CACHE_URL": URL,
        "CACHE_DIR": str(tmp_path / "work"),
        "CACHE_MAX_BYTES": str(archive.MAX_ARCHIVE_BYTES),
        "CACHE_EXCLUDE": "[]",
        "CACHE_SCRATCH": str(tmp_path / "scratch"),
    }
    env.update(extra)
    return env


class _Recorder:
    """A MockTransport handler that records every request and answers from `respond`."""

    def __init__(self, respond) -> None:
        self.requests: list[httpx.Request] = []
        self.bodies: list[bytes] = []
        self._respond = respond

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        response = self._respond(request)
        return response

    @property
    def transport(self) -> httpx.MockTransport:
        return httpx.MockTransport(self)


def _no_signature(*texts: str) -> None:
    for text in texts:
        assert SIGNATURE not in text
        assert "X-Amz-Signature" not in text


# ---------------------------------------------------------------------- restore


def test_restore_restored(tmp_path, capsys):
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    env = _env(tmp_path)

    code = transfer.restore(env, transport=rec.transport)

    out = capsys.readouterr()
    assert code == 0
    assert _outcome(out.out) == {"outcome": "restored", "bytes": len(body), "reason": None}
    assert _tree(tmp_path / "work") == {"a.txt": "alpha", "sub/b.txt": "beta"}
    assert len(rec.requests) == 1
    assert rec.requests[0].method == "GET"
    assert str(rec.requests[0].url) == URL
    _no_signature(out.err)


def test_restore_streams_into_unpack(tmp_path, capsys):
    """Extraction starts before the body is complete: the archive is never buffered whole."""
    src = tmp_path / "src"
    src.mkdir()
    (src / "first.bin").write_bytes(os.urandom(256 * 1024))
    (src / "last.bin").write_bytes(os.urandom(256 * 1024))
    body = _archive_of(src)
    work = tmp_path / "work"
    seen_before_last: list[bool] = []

    def chunks() -> Iterator[bytes]:
        step = 16 * 1024
        for start in range(0, len(body), step):
            if start + step >= len(body):
                seen_before_last.append((work / "first.bin").exists())
            yield body[start : start + step]

    rec = _Recorder(lambda request: httpx.Response(200, content=chunks()))
    code = transfer.restore(_env(tmp_path), transport=rec.transport)

    assert code == 0
    assert _outcome(capsys.readouterr().out)["outcome"] == "restored"
    assert seen_before_last == [True]
    assert (work / "last.bin").read_bytes() == (src / "last.bin").read_bytes()


def test_restore_miss(tmp_path, capsys):
    rec = _Recorder(lambda request: httpx.Response(404, content=b"<Error>NoSuchKey</Error>"))

    code = transfer.restore(_env(tmp_path), transport=rec.transport)

    assert code == 0
    assert _outcome(capsys.readouterr().out) == {"outcome": "miss", "bytes": 0, "reason": None}
    assert not (tmp_path / "work").exists()

    work = tmp_path / "work"
    work.mkdir()
    (work / "keep.txt").write_text("kept")
    code = transfer.restore(_env(tmp_path), transport=rec.transport)
    assert code == 0
    assert _outcome(capsys.readouterr().out)["outcome"] == "miss"
    assert _tree(work) == {"keep.txt": "kept"}


def _connect_error(request: httpx.Request) -> httpx.Response:
    raise httpx.ConnectError(f"connection refused for {request.url}", request=request)


@pytest.mark.parametrize(
    "respond",
    [lambda request: httpx.Response(500, content=b"boom"), _connect_error],
    ids=["http-500", "connect-error"],
)
def test_restore_download_failed(tmp_path, capsys, respond):
    rec = _Recorder(respond)

    code = transfer.restore(_env(tmp_path), transport=rec.transport)

    out = capsys.readouterr()
    outcome = _outcome(out.out)
    assert code == 1
    assert outcome["outcome"] == "failed"
    assert outcome["bytes"] is None
    assert outcome["reason"].startswith("download_failed: ")
    assert not (tmp_path / "work").exists()
    _no_signature(out.out, out.err)


def test_restore_download_failed_reports_status(tmp_path, capsys):
    rec = _Recorder(lambda request: httpx.Response(403, content=b"denied"))
    assert transfer.restore(_env(tmp_path), transport=rec.transport) == 1
    assert _outcome(capsys.readouterr().out)["reason"] == "download_failed: HTTP 403"


def test_restore_corrupt_archive_resets_workdir(tmp_path, capsys):
    work = tmp_path / "work"
    (work / "sub").mkdir(parents=True)
    (work / "old.txt").write_text("stale")
    (work / ".hidden").write_text("stale")
    (work / "sub" / "deep.txt").write_text("stale")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.txt").write_text("keep")
    (work / "link").symlink_to(outside)
    rec = _Recorder(lambda request: httpx.Response(200, content=b"definitely not zstd" * 64))

    code = transfer.restore(_env(tmp_path), transport=rec.transport)

    outcome = _outcome(capsys.readouterr().out)
    assert code == 0
    assert outcome["outcome"] == "reset"
    assert outcome["reason"].startswith("extract_failed: ")
    assert work.is_dir() and list(work.iterdir()) == []
    assert (outside / "keep.txt").read_text() == "keep"


def test_restore_reset_failure_is_failed(tmp_path, capsys, monkeypatch):
    work = tmp_path / "work"
    work.mkdir()
    (work / "old.txt").write_text("stale")

    def boom(path):
        raise OSError("read-only file system")

    monkeypatch.setattr(transfer, "_clear_dir", boom)
    rec = _Recorder(lambda request: httpx.Response(200, content=b"definitely not zstd" * 64))

    code = transfer.restore(_env(tmp_path), transport=rec.transport)

    outcome = _outcome(capsys.readouterr().out)
    assert code == 1
    assert outcome["outcome"] == "failed"
    assert outcome["reason"].startswith("extract_failed: ")
    assert "; reset failed: " in outcome["reason"]


def test_restore_network_error_after_extraction_began_is_extract_failed(tmp_path, capsys):
    src = tmp_path / "src"
    src.mkdir()
    (src / "big.bin").write_bytes(os.urandom(512 * 1024))
    body = _archive_of(src)

    def chunks() -> Iterator[bytes]:
        yield body[: len(body) // 2]
        raise httpx.ReadError(f"connection reset reading {URL}")

    work = tmp_path / "work"
    work.mkdir()
    (work / "old.txt").write_text("stale")
    rec = _Recorder(lambda request: httpx.Response(200, content=chunks()))
    code = transfer.restore(_env(tmp_path), transport=rec.transport)

    out = capsys.readouterr()
    outcome = _outcome(out.out)
    assert code == 1
    assert outcome["outcome"] == "failed"
    assert outcome["reason"].startswith("extract_failed: ")
    assert (work / "old.txt").read_text() == "stale"
    _no_signature(out.out, out.err)


def test_restore_without_url_fails_before_any_request(tmp_path, capsys):
    rec = _Recorder(lambda request: httpx.Response(200))
    env = _env(tmp_path)
    del env["CACHE_URL"]
    assert transfer.restore(env, transport=rec.transport) == 1
    assert _outcome(capsys.readouterr().out)["reason"].startswith("download_failed: ")
    assert rec.requests == []


# ---------------------------------------------------------------------- save


def _store(rec_bodies: list[bytes]):
    def respond(request: httpx.Request) -> httpx.Response:
        rec_bodies.append(request.read())
        return httpx.Response(200)

    return respond


def test_save_saved(tmp_path, capsys):
    work = _make_src(tmp_path / "work")
    os.symlink("/etc/passwd", work / "escape")
    bodies: list[bytes] = []
    rec = _Recorder(_store(bodies))

    code = transfer.save(_env(tmp_path), transport=rec.transport)

    outcome = _outcome(capsys.readouterr().out)
    assert code == 0
    assert len(rec.requests) == 1
    request = rec.requests[0]
    assert request.method == "PUT"
    assert str(request.url) == URL
    assert request.headers["Content-Type"] == transfer.CONTENT_TYPE == "application/zstd"
    assert int(request.headers["Content-Length"]) == len(bodies[0])
    assert "Transfer-Encoding" not in request.headers
    assert outcome == {
        "outcome": "saved",
        "bytes": len(bodies[0]),
        "reason": None,
        "skipped_links": 1,
    }
    restored = tmp_path / "restored"
    archive.unpack(io.BytesIO(bodies[0]), restored)
    assert _tree(restored) == {"a.txt": "alpha", "sub/b.txt": "beta"}
    assert list((tmp_path / "scratch").iterdir()) == []


class _ChunkTransport(httpx.BaseTransport):
    """Reads the request body chunk by chunk, as a network transport does.

    MockTransport reads the whole body before its handler runs, so it cannot show streaming.
    """

    def __init__(self, scratch_file: Path) -> None:
        self.scratch_file = scratch_file
        self.chunk_sizes: list[int] = []
        self.buffered: list[bool] = []
        self.file_present: list[bool] = []

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self.buffered.append(isinstance(request.stream, httpx.ByteStream))
        self.file_present.append(self.scratch_file.is_file())
        assert isinstance(request.stream, httpx.SyncByteStream)
        for chunk in request.stream:
            self.chunk_sizes.append(len(chunk))
        return httpx.Response(200)


def test_save_streams_the_archive_file(tmp_path, capsys):
    """The PUT body is the scratch file read in chunks, never the archive held in memory."""
    work = tmp_path / "work"
    work.mkdir()
    (work / "noise.bin").write_bytes(os.urandom(1024 * 1024))
    transport = _ChunkTransport(tmp_path / "scratch" / "archive.tar.zst")

    code = transfer.save(_env(tmp_path), transport=transport)

    outcome = _outcome(capsys.readouterr().out)
    assert code == 0
    assert transport.buffered == [False]
    assert transport.file_present == [True]
    assert sum(transport.chunk_sizes) == outcome["bytes"]
    assert len(transport.chunk_sizes) > 1
    assert max(transport.chunk_sizes) < outcome["bytes"]
    assert not transport.scratch_file.exists()


def test_save_no_workdir(tmp_path, capsys):
    rec = _Recorder(lambda request: httpx.Response(200))

    code = transfer.save(_env(tmp_path), transport=rec.transport)

    assert code == 0
    assert _outcome(capsys.readouterr().out) == {
        "outcome": "skipped",
        "bytes": None,
        "reason": "no_workdir",
    }
    assert rec.requests == []


def test_save_too_large_no_put(tmp_path, capsys):
    work = tmp_path / "work"
    work.mkdir()
    (work / "noise.bin").write_bytes(os.urandom(64 * 1024))
    rec = _Recorder(lambda request: httpx.Response(200))

    code = transfer.save(_env(tmp_path, CACHE_MAX_BYTES="1024"), transport=rec.transport)

    assert code == transfer.EXIT_FAILED == 1
    assert _outcome(capsys.readouterr().out) == {
        "outcome": "failed",
        "bytes": None,
        "reason": "too_large",
    }
    assert rec.requests == []
    assert list((tmp_path / "scratch").iterdir()) == []


def test_save_applies_exclude(tmp_path, capsys):
    work = _make_src(tmp_path / "work")
    (work / "node_modules").mkdir()
    (work / "node_modules" / "x.js").write_text("x")
    bodies: list[bytes] = []
    rec = _Recorder(_store(bodies))

    code = transfer.save(_env(tmp_path, CACHE_EXCLUDE='["node_modules"]'), transport=rec.transport)

    assert code == 0
    restored = tmp_path / "restored"
    archive.unpack(io.BytesIO(bodies[0]), restored)
    assert _tree(restored) == {"a.txt": "alpha", "sub/b.txt": "beta"}
    capsys.readouterr()


@pytest.mark.parametrize(
    "respond",
    [lambda request: httpx.Response(403, content=f"<Error>{URL}</Error>".encode()), _connect_error],
    ids=["http-403", "connect-error"],
)
def test_save_upload_failed_hides_signature(tmp_path, capsys, caplog, respond):
    _make_src(tmp_path / "work")
    rec = _Recorder(respond)

    code = transfer.save(_env(tmp_path), transport=rec.transport)

    out = capsys.readouterr()
    outcome = _outcome(out.out)
    assert code == 1
    assert outcome["outcome"] == "failed"
    assert outcome["reason"].startswith("upload_failed: ")
    assert len(rec.requests) == 1
    _no_signature(out.out, out.err, caplog.text)
    assert list((tmp_path / "scratch").iterdir()) == []


@pytest.mark.parametrize(
    ("value", "verify"),
    [(None, True), ("", True), ("true", True), ("True", True), ("false", False), ("FALSE", False)],
)
@pytest.mark.parametrize("action", ["restore", "save"])
def test_verify_tls_reaches_httpx(tmp_path, monkeypatch, capsys, action, value, verify):
    _make_src(tmp_path / "work")
    seen: list[object] = []
    real_client = httpx.Client

    def client(*args, **kwargs):
        seen.append(kwargs.get("verify", True))
        kwargs["transport"] = httpx.MockTransport(lambda request: httpx.Response(404))
        return real_client(*args, **kwargs)

    monkeypatch.setattr(transfer.httpx, "Client", client)
    env = _env(tmp_path)
    if value is not None:
        env["CACHE_VERIFY_TLS"] = value
    getattr(transfer, action)(env)
    capsys.readouterr()
    assert seen == [verify]


@pytest.mark.parametrize("action", ["restore", "save"])
def test_bad_verify_tls_fails_before_any_request(tmp_path, capsys, action):
    _make_src(tmp_path / "work")
    rec = _Recorder(lambda request: httpx.Response(200))
    code = getattr(transfer, action)(
        _env(tmp_path, CACHE_VERIFY_TLS="maybe"), transport=rec.transport
    )
    assert code == 1
    assert "CACHE_VERIFY_TLS" in _outcome(capsys.readouterr().out)["reason"]
    assert rec.requests == []


def test_redact_strips_the_query_string():
    redacted = transfer.redact(f"failed for {URL} after retry", URL)
    assert redacted == (
        "failed for http://s3.example:9000/bucket/workspace-cache/preset-0123.tar.zst after retry"
    )
    _no_signature(redacted)


# ---------------------------------------------------------------------- command


def _bare_env(**extra: str) -> dict[str, str]:
    env = {k: v for k, v in os.environ.items() if not k.startswith(("ORCH_", "CACHE_"))}
    env.update(extra)
    return env


def test_cli_needs_no_settings(tmp_path, monkeypatch, capsys):
    from agents_orchestrator import settings as settings_module

    def forbidden(*args, **kwargs):
        raise AssertionError("orch cache must not construct Settings")

    for name in [k for k in os.environ if k.startswith(("ORCH_", "CACHE_"))]:
        monkeypatch.delenv(name)
    monkeypatch.setattr(settings_module.Settings, "__init__", forbidden)
    for key, value in _env(tmp_path).items():
        monkeypatch.setenv(key, value)

    assert cli.main(["cache", "save"]) == 0
    assert _outcome(capsys.readouterr().out)["reason"] == "no_workdir"

    proc = subprocess.run(
        [sys.executable, "-m", "agents_orchestrator.cli", "cache", "save"],
        env=_bare_env(**_env(tmp_path)),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert _outcome(proc.stdout) == {"outcome": "skipped", "bytes": None, "reason": "no_workdir"}


def test_cli_rejects_unknown_action(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["cache", "purge"])
    assert exc.value.code == 2
    capsys.readouterr()


class _ObjectStore(BaseHTTPRequestHandler):
    objects: dict[str, bytes] = {}

    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_PUT(self):
        length = int(self.headers["Content-Length"])
        self.objects[self.path.split("?")[0]] = self.rfile.read(length)
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        body = self.objects.get(self.path.split("?")[0])
        if body is None:
            self.send_response(404)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        self.send_response(200)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def test_cli_round_trip_over_http(tmp_path):
    _ObjectStore.objects = {}
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ObjectStore)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/o?X-Amz-Signature={SIGNATURE}"
    try:

        def run(action: str, workdir: Path) -> subprocess.CompletedProcess:
            env = _bare_env(**_env(tmp_path, CACHE_URL=url, CACHE_DIR=str(workdir)))
            return subprocess.run(
                [sys.executable, "-m", "agents_orchestrator.cli", "cache", action],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )

        miss = run("restore", tmp_path / "fresh")
        assert miss.returncode == 0, miss.stderr
        assert _outcome(miss.stdout)["outcome"] == "miss"

        saved = run("save", _make_src(tmp_path / "work"))
        assert saved.returncode == 0, saved.stderr
        saved_outcome = _outcome(saved.stdout)
        assert saved_outcome["outcome"] == "saved"
        assert saved_outcome["bytes"] == len(_ObjectStore.objects["/o"])

        restored = run("restore", tmp_path / "next")
        assert restored.returncode == 0, restored.stderr
        assert _outcome(restored.stdout) == {
            "outcome": "restored",
            "bytes": saved_outcome["bytes"],
            "reason": None,
        }
        assert _tree(tmp_path / "next") == {"a.txt": "alpha", "sub/b.txt": "beta"}
        for proc in (miss, saved, restored):
            _no_signature(proc.stdout, proc.stderr)
    finally:
        server.shutdown()
        server.server_close()


def test_sessions_target_always_requires_redacted_marker(tmp_path):
    from agents_orchestrator.cache import transfer

    assert transfer._require_redacted("/sessions") is True
    assert transfer._require_redacted("/sessions/") is True
    assert transfer._require_redacted(str(tmp_path / "work")) is False


# ------------------------------------------------------------- per-entry jobs


def _entries_env(tmp_path: Path, entries: list[dict], **extra: str) -> dict[str, str]:
    env = {
        "CACHE_ENTRIES": json.dumps(entries),
        "CACHE_SCRATCH": str(tmp_path / "scratch"),
        "CACHE_STATE_DIR": str(tmp_path / "state"),
    }
    env.update(extra)
    return env


def _entries_out(capsys) -> list[dict]:
    lines = [ln for ln in capsys.readouterr().out.splitlines() if ln.strip()]
    return json.loads(lines[-1])["entries"]


def _entry(tmp_path: Path, **kw) -> dict:
    return {
        "index": 0,
        "root_dir": str(tmp_path / "root"),
        "path": "work",
        "exclude": [],
        "url": URL,
        **kw,
    }


def test_restore_missing_leaves_dir(tmp_path, capsys):
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "keep.txt").write_text("mine")
    rec = _Recorder(lambda request: httpx.Response(404))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    assert _entries_out(capsys) == [{"index": 0, "state": "missing"}]
    assert _tree(work) == {"keep.txt": "mine"}


def test_restore_unreadable_leaves_dir(tmp_path, capsys):
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "keep.txt").write_text("mine")
    before = sorted(p.name for p in (tmp_path / "root").iterdir())
    rec = _Recorder(lambda request: httpx.Response(200, content=b"not a zstd stream at all"))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    out = _entries_out(capsys)
    assert out[0]["state"] == "unreadable"
    assert _tree(work) == {"keep.txt": "mine"}
    assert sorted(p.name for p in (tmp_path / "root").iterdir()) == before
    assert not (tmp_path / "state").exists()


def test_restore_writes_backup_state(tmp_path, capsys):
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    (tmp_path / "state").mkdir()
    entry = _entry(tmp_path, index=3, name="proj-skills", version=7)
    env = _entries_env(tmp_path, [entry])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    out = _entries_out(capsys)[0]
    assert out["state"] == "restored" and out["version"] == 7
    assert _tree(tmp_path / "root" / "work") == {"a.txt": "alpha", "sub/b.txt": "beta"}
    state = json.loads((tmp_path / "state" / "3.json").read_text())
    assert state["name"] == "proj-skills" and state["version"] == 7
    assert state["manifest"] == out["manifest"]
    assert out["digest"] == archive.digest(archive.manifest(tmp_path / "root" / "work", []))


def test_save_reports_manifest_and_digest(tmp_path, capsys):
    work = tmp_path / "root" / "work"
    _make_src(work)
    rec = _Recorder(lambda request: httpx.Response(200))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.save_entries(env, transport=rec.transport) == 0
    out = _entries_out(capsys)[0]
    assert out["state"] == "uploaded"
    assert out["digest"] == archive.digest(archive.manifest(work, []))
    assert [m["path"] for m in out["manifest"]] == ["a.txt", "sub/b.txt"]
    assert out["size"] == len(rec.requests[0].content)
    assert rec.requests[0].method == "PUT"
    assert not list((tmp_path / "scratch").iterdir())


def test_save_manifest_null_above_cap(tmp_path, capsys, monkeypatch):
    work = tmp_path / "root" / "work"
    _make_src(work)
    monkeypatch.setattr(archive, "MANIFEST_MAX_FILES", 1)
    rec = _Recorder(lambda request: httpx.Response(200))
    assert (
        transfer.save_entries(_entries_env(tmp_path, [_entry(tmp_path)]), transport=rec.transport)
        == 0
    )
    out = _entries_out(capsys)[0]
    assert out["manifest"] is None and len(out["digest"]) == 64


def test_claude_entry_with_secret_refused(tmp_path, capsys):
    work = tmp_path / "root" / "work"
    (work / "s1" / "config").mkdir(parents=True)
    rec = _Recorder(lambda request: httpx.Response(200))
    env = _entries_env(tmp_path, [_entry(tmp_path, root="claude")])
    assert transfer.save_entries(env, transport=rec.transport) == 0
    assert _entries_out(capsys) == [{"index": 0, "state": "refused_secret"}]
    assert rec.requests == []


def test_resolve_outputs_digests(tmp_path, capsys):
    import hashlib

    root = tmp_path / "root"
    root.mkdir()
    (root / "f.txt").write_text("alpha")
    env = {
        "CACHE_RESOLVE": json.dumps(
            [
                {"root_dir": str(root), "path": "f.txt", "length": 8, "exclude": []},
                {"root_dir": str(root), "path": "gone", "length": 8, "exclude": []},
            ]
        )
    }
    assert transfer.resolve(env) == 0
    digests = json.loads(capsys.readouterr().out.splitlines()[-1])["digests"]
    assert digests == {
        "f.txt:8": hashlib.sha256(b"alpha").hexdigest()[:8],
        "gone:8": "none",
    }


def test_cli_dispatches_entries_and_resolve(tmp_path, capsys, monkeypatch):
    root = tmp_path / "root"
    root.mkdir()
    (root / "f.txt").write_text("alpha")
    monkeypatch.setenv(
        "CACHE_RESOLVE",
        json.dumps([{"root_dir": str(root), "path": "f.txt", "length": 4, "exclude": []}]),
    )
    assert cli.main(["cache", "resolve"]) == 0
    assert "f.txt:4" in capsys.readouterr().out


# ------------------------------------------------- step 9 fix round 1: modes, atomicity, safety


def _tar_zst(members: list[tuple[tarfile.TarInfo, bytes | None]]) -> bytes:
    raw = io.BytesIO()
    with tarfile.open(fileobj=raw, mode="w") as tar:
        for info, data in members:
            tar.addfile(info, io.BytesIO(data) if data is not None else None)
    return zstandard.ZstdCompressor().compress(raw.getvalue())


def _file_member(name: str, data: bytes = b"x") -> tuple[tarfile.TarInfo, bytes]:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    return info, data


def _link_member(name: str, linkname: str) -> tuple[tarfile.TarInfo, None]:
    info = tarfile.TarInfo(name)
    info.type = tarfile.SYMTYPE
    info.linkname = linkname
    return info, None


def test_restore_into_absent_target_keeps_normal_mode(tmp_path, capsys):
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    assert _entries_out(capsys)[0]["state"] == "restored"
    mode = (tmp_path / "root" / "work").stat().st_mode & 0o777
    assert mode == 0o755


def test_restore_merge_conflict_leaves_target_untouched(tmp_path, capsys):
    work = tmp_path / "root" / "work"
    (work / "x").mkdir(parents=True)
    (work / "x" / "keep.txt").write_text("mine")
    (work / "other.txt").write_text("old")
    body = _archive_of(_src_with_file_x(tmp_path))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 1
    out = _entries_out(capsys)[0]
    assert out["state"] == "failed"
    assert _tree(work) == {"x/keep.txt": "mine", "other.txt": "old"}
    assert sorted(p.name for p in (tmp_path / "root").iterdir()) == ["work"]


def _src_with_file_x(tmp_path: Path) -> Path:
    src = tmp_path / "srcx"
    src.mkdir()
    (src / "x").write_text("file")
    (src / "other.txt").write_text("new")
    return src


def test_restore_state_write_error_stays_restored(tmp_path, capsys, monkeypatch):
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    (tmp_path / "state").mkdir()

    def boom(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(transfer, "_write_state", boom)
    env = _entries_env(tmp_path, [_entry(tmp_path, index=2, version=1)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    out = _entries_out(capsys)[0]
    assert out["state"] == "restored"
    assert "disk full" in out["warning"]
    assert _tree(tmp_path / "root" / "work") == {"a.txt": "alpha", "sub/b.txt": "beta"}


def test_restore_absent_index_does_not_fail_a_restored_entry(tmp_path, capsys):
    body = _archive_of(_make_src(tmp_path / "src"))
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    (tmp_path / "state").mkdir()
    entry = _entry(tmp_path, version=1)
    del entry["index"]
    env = _entries_env(tmp_path, [entry])
    transfer.restore_entries(env, transport=rec.transport)
    out = _entries_out(capsys)[0]
    # validated up front: nothing was requested, extracted or merged
    assert out["state"] == "failed" and "index" in out["reason"]
    assert rec.requests == []
    assert not (tmp_path / "root" / "work").exists()


@pytest.mark.parametrize(
    "members",
    [
        [_file_member("../escape.txt")],
        [_file_member("/abs-escape.txt")],
        [_link_member("l", "../../outside")],
    ],
    ids=["dotdot", "absolute", "symlink-outside"],
)
def test_restore_refuses_escaping_members(tmp_path, capsys, members):
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "keep.txt").write_text("mine")
    rec = _Recorder(lambda request: httpx.Response(200, content=_tar_zst(members)))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    assert _entries_out(capsys)[0]["state"] == "unreadable"
    assert _tree(work) == {"keep.txt": "mine"}
    assert not (tmp_path / "root" / "escape.txt").exists()
    assert not (tmp_path / "escape.txt").exists()
    assert sorted(p.name for p in (tmp_path / "root").iterdir()) == ["work"]


@pytest.mark.parametrize("path", ["work", "."])
def test_restore_does_not_write_through_existing_symlink(tmp_path, capsys, path):
    root = tmp_path / "root"
    target = root if path == "." else root / path
    outside = tmp_path / "outside"
    outside.mkdir()
    target.mkdir(parents=True, exist_ok=True)
    (target / "link").symlink_to(outside)
    (target / "keep.txt").write_text("mine")
    body = _tar_zst([_file_member("link/pwn.txt", b"pwn"), _file_member("fresh.txt", b"new")])
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    env = _entries_env(tmp_path, [_entry(tmp_path, path=path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 1
    assert _entries_out(capsys)[0]["state"] == "failed"
    assert list(outside.iterdir()) == []
    assert not (target / "fresh.txt").exists()
    assert (target / "link").is_symlink()
    assert not [p for p in root.iterdir() if p.name.startswith(".restore-")]


def test_restore_replaces_existing_symlink_leaf_without_following_it(tmp_path, capsys):
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    victim = tmp_path / "victim.txt"
    victim.write_text("safe")
    (work / "f.txt").symlink_to(victim)
    body = _tar_zst([_file_member("f.txt", b"new")])
    rec = _Recorder(lambda request: httpx.Response(200, content=body))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    assert victim.read_text() == "safe"
    assert not (work / "f.txt").is_symlink()
    assert (work / "f.txt").read_text() == "new"


def test_restore_truncated_archive_is_unreadable_and_leaves_dir(tmp_path, capsys):
    src = tmp_path / "src"
    src.mkdir()
    for i in range(6):
        (src / f"f{i}.bin").write_bytes(os.urandom(200_000))
    body = _archive_of(src)
    truncated = body[: int(len(body) * 0.6)]
    work = tmp_path / "root" / "work"
    work.mkdir(parents=True)
    (work / "keep.txt").write_text("mine")
    rec = _Recorder(lambda request: httpx.Response(200, content=truncated))
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    assert transfer.restore_entries(env, transport=rec.transport) == 0
    assert _entries_out(capsys)[0]["state"] == "unreadable"
    assert _tree(work) == {"keep.txt": "mine"}
    assert sorted(p.name for p in (tmp_path / "root").iterdir()) == ["work"]
    assert not (tmp_path / "state").exists()
