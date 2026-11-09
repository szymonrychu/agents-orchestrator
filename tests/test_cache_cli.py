"""The Job-side transfer CLI `orch cache restore|save` (contract orch-cache-job-cli; D1, D11)."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import httpx
import pytest

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
