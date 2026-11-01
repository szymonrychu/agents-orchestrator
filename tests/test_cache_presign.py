"""Presigned cache object URLs signed in the reconciler (design D2, D8)."""

from __future__ import annotations

import json
import logging
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from urllib.parse import parse_qs, urlsplit

import boto3
import pytest
from moto.server import ThreadedMotoServer

from agents_orchestrator import cli
from agents_orchestrator.cache import presign as presign_module
from agents_orchestrator.cache import transfer
from agents_orchestrator.cache.presign import object_name, presign
from agents_orchestrator.settings import Settings

SECRET = "s3cr3t-Access-Key-Value"
ACCESS_KEY = "TESTIDTESTCACHE"


def _settings(**kw) -> Settings:
    kw.setdefault("database_url", "postgresql://x/y")
    kw.setdefault("agent_image", "img:1")
    kw.setdefault("cache_s3_endpoint", "http://rgw:80")
    kw.setdefault("cache_s3_bucket", "b")
    kw.setdefault("cache_s3_path_style", True)
    kw.setdefault("cache_s3_access_key_id", ACCESS_KEY)
    kw.setdefault("cache_s3_secret_access_key", SECRET)
    return Settings(**kw)


def _query(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}


@pytest.fixture(autouse=True)
def _no_ambient_aws(monkeypatch):
    for name in ("AWS_PROFILE", "AWS_DEFAULT_PROFILE", "AWS_CONFIG_FILE", "AWS_ENDPOINT_URL"):
        monkeypatch.delenv(name, raising=False)
    for name in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy"):
        monkeypatch.delenv(name, raising=False)


def test_object_name():
    s = _settings(cache_s3_prefix="workspace-cache/")
    assert object_name(s, "preset-0123") == "workspace-cache/preset-0123.tar.zst"
    assert object_name(_settings(cache_s3_prefix=""), "k") == "k.tar.zst"
    assert object_name(_settings(cache_s3_prefix="a/b/"), "k") == "a/b/k.tar.zst"


def test_path_style_url():
    url = presign(_settings(), "PUT", "k")
    parts = urlsplit(url)
    assert parts.scheme == "http"
    assert parts.netloc == "rgw:80"
    assert parts.path.startswith("/b/")
    assert parts.path == "/b/workspace-cache/k.tar.zst"
    q = _query(url)
    assert q["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"
    assert q["X-Amz-Credential"].startswith(f"{ACCESS_KEY}/")
    assert q["X-Amz-Credential"].endswith("/us-east-1/s3/aws4_request")
    # Host and Content-Type are signed; Content-Length is not, so it need not match.
    assert q["X-Amz-SignedHeaders"] == "content-type;host"
    assert SECRET not in url


def test_virtual_style_url():
    # botocore keeps a bucket name that is not a valid DNS label (like "b") in the path.
    url = presign(_settings(cache_s3_bucket="cache-bucket", cache_s3_path_style=False), "GET", "k")
    parts = urlsplit(url)
    assert parts.netloc == "cache-bucket.rgw:80"
    assert parts.path == "/workspace-cache/k.tar.zst"
    assert _query(url)["X-Amz-Algorithm"] == "AWS4-HMAC-SHA256"


def test_virtual_style_without_endpoint_is_aws():
    s = _settings(cache_s3_endpoint="", cache_s3_bucket="cache-bucket", cache_s3_path_style=False)
    parts = urlsplit(presign(s, "GET", "k"))
    assert parts.scheme == "https"
    assert parts.hostname is not None
    assert parts.hostname.startswith("cache-bucket.s3.")
    assert parts.hostname.endswith(".amazonaws.com")
    assert parts.path == "/workspace-cache/k.tar.zst"


def test_expiry():
    assert _query(presign(_settings(), "PUT", "k"))["X-Amz-Expires"] == "2400"
    assert _query(presign(_settings(cache_timeout_seconds=60), "GET", "k"))["X-Amz-Expires"] == (
        "660"
    )


def test_region_is_signed():
    q = _query(presign(_settings(cache_s3_region="eu-1"), "GET", "k"))
    assert q["X-Amz-Credential"].endswith("/eu-1/s3/aws4_request")


def test_get_and_put_sign_different_requests():
    s = _settings()
    get, put = presign(s, "GET", "k"), presign(s, "PUT", "k")
    assert urlsplit(get).path == urlsplit(put).path
    assert _query(get)["X-Amz-Signature"] != _query(put)["X-Amz-Signature"]


def test_put_signs_content_type():
    s = _settings()
    put_headers = _query(presign(s, "PUT", "k"))["X-Amz-SignedHeaders"].split(";")
    get_headers = _query(presign(s, "GET", "k"))["X-Amz-SignedHeaders"].split(";")
    assert "content-type" in put_headers
    assert "content-type" not in get_headers
    assert transfer.CONTENT_TYPE == "application/zstd"


def test_other_methods_are_refused():
    with pytest.raises(ValueError, match="GET or PUT"):
        presign(_settings(), cast(Any, "DELETE"), "k")


def test_secret_never_logged_or_in_repr(caplog):
    s = _settings()
    with caplog.at_level(logging.DEBUG):
        url = presign(s, "PUT", "k")
    assert SECRET not in url
    assert SECRET not in caplog.text
    assert SECRET not in repr(s)


def test_client_is_reused_for_the_same_settings():
    presign_module._client.cache_clear()
    s = _settings()
    presign(s, "GET", "a")
    presign(s, "PUT", "b")
    assert presign_module._client.cache_info().misses == 1
    presign(_settings(cache_s3_region="eu-1"), "GET", "a")
    assert presign_module._client.cache_info().misses == 2


# ---------------------------------------------------------------------- moto round trip


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
def moto_settings(moto_endpoint: str) -> Settings:
    bucket = f"cache-{uuid.uuid4().hex[:12]}"
    boto3.client(
        "s3",
        endpoint_url=moto_endpoint,
        region_name="us-east-1",
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET,
    ).create_bucket(Bucket=bucket)
    return _settings(cache_s3_endpoint=moto_endpoint, cache_s3_bucket=bucket)


def _tree(root: Path) -> dict[str, str]:
    return {
        p.relative_to(root).as_posix(): p.read_text()
        for p in sorted(root.rglob("*"))
        if p.is_file() and not p.is_symlink()
    }


def _run(monkeypatch, capsys, action: str, url: str, workdir: Path, scratch: Path) -> dict:
    monkeypatch.setenv("CACHE_URL", url)
    monkeypatch.setenv("CACHE_DIR", str(workdir))
    monkeypatch.setenv("CACHE_EXCLUDE", "[]")
    monkeypatch.setenv("CACHE_SCRATCH", str(scratch))
    code = cli.main(["cache", action])
    lines = [line for line in capsys.readouterr().out.splitlines() if line.strip()]
    outcome = json.loads(lines[-1])
    outcome["exit"] = code
    return outcome


def test_round_trip_through_moto(moto_settings, tmp_path, monkeypatch, capsys):
    src = tmp_path / "src"
    (src / "sub").mkdir(parents=True)
    (src / "a.txt").write_text("alpha")
    (src / "sub" / "b.txt").write_text("beta")

    saved = _run(
        monkeypatch, capsys, "save", presign(moto_settings, "PUT", "k"), src, tmp_path / "s"
    )
    assert saved["outcome"] == "saved", saved
    assert saved["exit"] == 0
    assert saved["bytes"] > 0

    dest = tmp_path / "dest"
    dest.mkdir()
    restored = _run(
        monkeypatch, capsys, "restore", presign(moto_settings, "GET", "k"), dest, tmp_path / "r"
    )
    assert restored["outcome"] == "restored", restored
    assert restored["exit"] == 0
    assert restored["bytes"] == saved["bytes"]
    assert _tree(dest) == {"a.txt": "alpha", "sub/b.txt": "beta"}


def test_missing_object_is_miss(moto_settings, tmp_path, monkeypatch, capsys):
    dest = tmp_path / "dest"
    dest.mkdir()
    (dest / "keep.txt").write_text("untouched")
    outcome = _run(
        monkeypatch,
        capsys,
        "restore",
        presign(moto_settings, "GET", "never-saved"),
        dest,
        tmp_path / "r",
    )
    assert outcome["outcome"] == "miss", outcome
    assert outcome["exit"] == 0
    assert _tree(dest) == {"keep.txt": "untouched"}
