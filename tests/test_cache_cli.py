"""The Job-side transfer CLI `orch cache restore|save` (contract orch-cache-job-cli; D1, D11)."""

from __future__ import annotations

import io
import json
import os
import subprocess
import sys
import threading
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


def test_redact_strips_the_query_string():
    redacted = transfer.redact(f"failed for {URL} after retry", URL)
    assert redacted == (
        "failed for http://s3.example:9000/bucket/workspace-cache/preset-0123.tar.zst after retry"
    )
    _no_signature(redacted)


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


# ---------------------------------------------------------- command and transport


@pytest.mark.parametrize(
    ("value", "verify"),
    [(None, True), ("", True), ("true", True), ("True", True), ("false", False), ("FALSE", False)],
)
@pytest.mark.parametrize("action", ["restore", "save"])
def test_verify_tls_reaches_httpx(tmp_path, monkeypatch, capsys, action, value, verify):
    _make_src(tmp_path / "root" / "work")
    seen: list[object] = []
    real_client = httpx.Client

    def client(*args, **kwargs):
        seen.append(kwargs.get("verify", True))
        kwargs["transport"] = httpx.MockTransport(lambda request: httpx.Response(404))
        return real_client(*args, **kwargs)

    monkeypatch.setattr(transfer.httpx, "Client", client)
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    if value is not None:
        env["CACHE_VERIFY_TLS"] = value
    getattr(transfer, f"{action}_entries")(env)
    capsys.readouterr()
    assert seen == [verify]


@pytest.mark.parametrize("action", ["restore", "save"])
def test_bad_verify_tls_fails_before_any_request(tmp_path, capsys, action):
    _make_src(tmp_path / "root" / "work")
    rec = _Recorder(lambda request: httpx.Response(200))
    env = _entries_env(tmp_path, [_entry(tmp_path)], CACHE_VERIFY_TLS="maybe")
    code = getattr(transfer, f"{action}_entries")(env, transport=rec.transport)
    assert code == 1
    assert "CACHE_VERIFY_TLS" in json.loads(capsys.readouterr().out)["reason"]
    assert rec.requests == []


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
    env = _entries_env(tmp_path, [_entry(tmp_path)])
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    assert cli.main(["cache", "save"]) == 0
    assert _entries_out(capsys)[0]["state"] == "skipped"

    proc = subprocess.run(
        [sys.executable, "-m", "agents_orchestrator.cli", "cache", "save"],
        env=_bare_env(**env),
        capture_output=True,
        text=True,
        timeout=60,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert json.loads(proc.stdout.splitlines()[-1]) == {
        "entries": [{"index": 0, "state": "skipped", "reason": "no_dir"}]
    }


def test_cli_rejects_unknown_action(capsys):
    with pytest.raises(SystemExit) as exc:
        cli.main(["cache", "purge"])
    assert exc.value.code == 2
    capsys.readouterr()


def test_cli_round_trip_over_http(tmp_path):
    _ObjectStore.objects = {}
    server = ThreadingHTTPServer(("127.0.0.1", 0), _ObjectStore)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}/o?X-Amz-Signature={SIGNATURE}"
    try:

        def run(action: str, root: Path) -> subprocess.CompletedProcess:
            entry = {"index": 0, "root_dir": str(root), "path": ".", "exclude": [], "url": url}
            env = _bare_env(
                CACHE_ENTRIES=json.dumps([entry]), CACHE_SCRATCH=str(tmp_path / "scratch")
            )
            return subprocess.run(
                [sys.executable, "-m", "agents_orchestrator.cli", "cache", action],
                env=env,
                capture_output=True,
                text=True,
                timeout=60,
                check=False,
            )

        def state(proc: subprocess.CompletedProcess) -> dict:
            return json.loads(proc.stdout.splitlines()[-1])["entries"][0]

        miss = run("restore", tmp_path / "fresh")
        assert miss.returncode == 0, miss.stderr
        assert state(miss)["state"] == "missing"

        saved = run("save", _make_src(tmp_path / "work"))
        assert saved.returncode == 0, saved.stderr
        assert state(saved)["state"] == "uploaded"
        assert state(saved)["size"] == len(_ObjectStore.objects["/o"])

        restored = run("restore", tmp_path / "next")
        assert restored.returncode == 0, restored.stderr
        assert state(restored)["state"] == "restored"
        assert state(restored)["digest"] == state(saved)["digest"]
        assert _tree(tmp_path / "next") == {"a.txt": "alpha", "sub/b.txt": "beta"}
        for proc in (miss, saved, restored):
            _no_signature(proc.stdout, proc.stderr)
    finally:
        server.shutdown()
        server.server_close()


def test_sessions_root_save_leaves_out_unmarked_config(tmp_path, capsys):
    root = tmp_path / "root" / "work"
    (root / "ses1" / "config").mkdir(parents=True)
    (root / "ses1" / "config" / "session.json").write_text("{}")
    (root / "ses1" / "t.jsonl").write_text("t")
    sent: list[bytes] = []

    def put(request: httpx.Request) -> httpx.Response:
        sent.append(request.read())
        return httpx.Response(200)

    rec = _Recorder(put)
    env = _entries_env(tmp_path, [_entry(tmp_path, root="sessions")])
    assert transfer.save_entries(env, transport=rec.transport) == 0
    assert _entries_out(capsys)[0]["state"] == "uploaded"
    import tarfile

    import zstandard

    with (
        zstandard.ZstdDecompressor().stream_reader(io.BytesIO(sent[0])) as zst,
        tarfile.open(fileobj=zst, mode="r|") as tar,
    ):
        names = [m.name for m in tar]
    assert "ses1/t.jsonl" in names
    assert not [n for n in names if "/config" in n]
