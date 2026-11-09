"""Job-side workspace transfer: `orch cache restore|save` (contract orch-cache-job-cli; D1, D11).

Runs in the hardened cache Job with only the `CACHE_*` env: `CACHE_URL` (one presigned GET or
PUT URL), `CACHE_DIR` (the workdir), `CACHE_MAX_BYTES`, `CACHE_EXCLUDE` (a JSON list of globs),
`CACHE_SCRATCH` (where the save writes its archive) and `CACHE_VERIFY_TLS` (`false` turns off
certificate checks for a private-CA endpoint; unset is `true`). It never builds `Settings` or
reads `ORCH_*`. The last stdout line is one JSON object `{"outcome", "bytes", "reason"}` that the
reconciler reads from the pod log tail; nothing is printed after it.

Restore streams the GET body straight into `archive.unpack`; save packs into a scratch file and
streams that file as one PUT, so neither holds the archive in memory. The URL's query string is
the signature: every reason and log line has it stripped (`redact`).
"""

from __future__ import annotations

import io
import json
import os
import shutil
import sys
from collections.abc import Buffer, Iterator, Mapping
from pathlib import Path
from typing import Any, BinaryIO, cast

import httpx

from agents_orchestrator.cache import archive

EXIT_OK = 0
# Every failure, too_large included: the reconciler reads the reason from the outcome line, and
# a too_large save sends no request, so the previous archive is kept (gate Q2).
EXIT_FAILED = 1
ARCHIVE_NAME = "archive.tar.zst"
DEFAULT_SCRATCH = "/scratch"
CONTENT_TYPE = "application/zstd"
TIMEOUT = httpx.Timeout(60.0)


class _BadEnv(Exception):
    """A missing or malformed CACHE_* variable."""


def redact(text: str, url: str) -> str:
    """`text` with `url` shown without its query string and the query string itself removed."""
    if not url:
        return text
    try:
        parsed = httpx.URL(url)
    except httpx.InvalidURL:
        return text.replace(url, "<cache url>")
    bare = str(parsed.copy_with(query=None))
    text = text.replace(url, bare)
    query = parsed.query.decode("ascii", "replace")
    if query:
        text = text.replace(query, "<redacted>")
    return text


def _emit(
    outcome: str, *, nbytes: int | None = None, reason: str | None = None, **extra: Any
) -> None:
    line = {"outcome": outcome, "bytes": nbytes, "reason": reason, **extra}
    sys.stderr.flush()
    print(json.dumps(line), file=sys.stdout, flush=True)


def _error(exc: BaseException, url: str) -> str:
    detail = f"{type(exc).__name__}: {exc}" if str(exc) else type(exc).__name__
    return redact(detail, url)


def _url(env: Mapping[str, str]) -> str:
    url = env.get("CACHE_URL", "")
    if not url:
        raise _BadEnv("CACHE_URL is not set")
    return url


def _workdir(env: Mapping[str, str]) -> Path:
    workdir = env.get("CACHE_DIR", "")
    if not workdir:
        raise _BadEnv("CACHE_DIR is not set")
    return Path(workdir)


def _verify_tls(env: Mapping[str, str]) -> bool:
    raw = env.get("CACHE_VERIFY_TLS", "").strip().lower()
    if raw in ("", "true"):
        return True
    if raw == "false":
        return False
    raise _BadEnv(f"CACHE_VERIFY_TLS must be true or false: {raw!r}")


def _max_bytes(env: Mapping[str, str]) -> int:
    raw = env.get("CACHE_MAX_BYTES", "")
    if not raw:
        return archive.MAX_ARCHIVE_BYTES
    try:
        value = int(raw)
    except ValueError:
        raise _BadEnv(f"CACHE_MAX_BYTES is not an integer: {raw!r}") from None
    if not 1 <= value <= archive.MAX_ARCHIVE_BYTES:
        raise _BadEnv(f"CACHE_MAX_BYTES must be 1 to {archive.MAX_ARCHIVE_BYTES}")
    return value


def _require_redacted(env: Mapping[str, str]) -> bool:
    return env.get("CACHE_REQUIRE_REDACTED") == "1"


def _exclude(env: Mapping[str, str]) -> list[str]:
    raw = env.get("CACHE_EXCLUDE", "")
    if not raw:
        return []
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        raise _BadEnv("CACHE_EXCLUDE is not JSON") from None
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise _BadEnv("CACHE_EXCLUDE is not a JSON list of strings")
    return value


class _BodyReader(io.RawIOBase):
    """A readable over a streamed response body, counting the bytes handed on."""

    def __init__(self, chunks: Iterator[bytes]) -> None:
        super().__init__()
        self._chunks = chunks
        self._pending = b""
        self.delivered = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer, /) -> int:
        view = memoryview(buffer).cast("B")
        while not self._pending:
            chunk = next(self._chunks, None)
            if chunk is None:
                return 0
            self._pending = chunk
        size = min(len(view), len(self._pending))
        view[:size] = self._pending[:size]
        self._pending = self._pending[size:]
        self.delivered += size
        return size


def _clear_dir(path: Path) -> None:
    """Empty `path` but keep it; symlinks are unlinked, never followed."""
    for entry in path.iterdir():
        if entry.is_symlink() or not entry.is_dir():
            entry.unlink()
        else:
            shutil.rmtree(entry)


def restore(env: Mapping[str, str], *, transport: httpx.BaseTransport | None = None) -> int:
    """GET `CACHE_URL` and unpack it into `CACHE_DIR`; print the outcome, return the exit code.

    200 restores, 404 is a miss that leaves the workdir untouched. Another status, or an error
    before any body byte reached the extractor, is `download_failed`; any failure once extraction
    began is `extract_failed`. An ExtractError means the archive itself is unreadable or untrusted:
    the workdir is emptied and the outcome is `reset` (exit 0), so the save that follows replaces
    the bad object. Any other failure leaves the workdir alone: the stored archive may be good.
    """
    url = env.get("CACHE_URL", "")
    try:
        url = _url(env)
        workdir = _workdir(env)
        verify = _verify_tls(env)
    except _BadEnv as exc:
        _emit("failed", reason=f"download_failed: {exc}")
        return EXIT_FAILED
    reader: _BodyReader | None = None
    try:
        with (
            httpx.Client(transport=transport, timeout=TIMEOUT, verify=verify) as client,
            client.stream("GET", url) as response,
        ):
            if response.status_code == 404:
                _emit("miss", nbytes=0)
                return EXIT_OK
            if response.status_code != 200:
                _emit("failed", reason=f"download_failed: HTTP {response.status_code}")
                return EXIT_FAILED
            reader = _BodyReader(response.iter_bytes())
            archive.unpack(cast(BinaryIO, reader), workdir)
            while reader.read(io.DEFAULT_BUFFER_SIZE):
                pass  # the zstd epilogue past the tar end: count the whole object
            delivered = reader.delivered
    except archive.ExtractError as exc:
        reason = f"extract_failed: {_error(exc, url)}"
        try:
            _clear_dir(workdir)
        except Exception as wipe:  # noqa: BLE001
            _emit("failed", reason=f"{reason}; reset failed: {_error(wipe, url)}")
            return EXIT_FAILED
        _emit("reset", reason=reason)
        return EXIT_OK
    except Exception as exc:  # noqa: BLE001 - every failure becomes the one outcome line
        stage = "extract_failed" if reader is not None and reader.delivered else "download_failed"
        _emit("failed", reason=f"{stage}: {_error(exc, url)}")
        return EXIT_FAILED
    _emit("restored", nbytes=delivered)
    return EXIT_OK


def save(env: Mapping[str, str], *, transport: httpx.BaseTransport | None = None) -> int:
    """Pack `CACHE_DIR` into the scratch dir and PUT it once to `CACHE_URL`; print the outcome.

    No workdir is `skipped` `no_workdir` and an archive past `CACHE_MAX_BYTES` is `too_large`,
    both without a request. A non-2xx answer or a transport error is `upload_failed`. The
    archive file is removed in every case.
    """
    url = env.get("CACHE_URL", "")
    workdir_text = env.get("CACHE_DIR", "")
    if not workdir_text or not os.path.isdir(workdir_text):
        _emit("skipped", reason="no_workdir")
        return EXIT_OK
    try:
        url = _url(env)
        max_bytes = _max_bytes(env)
        verify = _verify_tls(env)
        exclude = _exclude(env)
    except _BadEnv as exc:
        _emit("failed", reason=f"upload_failed: {exc}")
        return EXIT_FAILED
    scratch = Path(env.get("CACHE_SCRATCH") or DEFAULT_SCRATCH)
    path = scratch / ARCHIVE_NAME
    try:
        scratch.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("wb") as out:
                packed = archive.pack(
                    Path(workdir_text),
                    out,
                    exclude=exclude,
                    max_bytes=max_bytes,
                    require_redacted=_require_redacted(env),
                )
        except archive.TooLarge:
            _emit("failed", reason="too_large")
            return EXIT_FAILED
        except Exception as exc:  # noqa: BLE001
            _emit("failed", reason=f"upload_failed: archive: {_error(exc, url)}")
            return EXIT_FAILED
        size = packed.compressed_bytes
        skipped_links = packed.skipped_links
        try:
            with (
                path.open("rb") as body,
                httpx.Client(transport=transport, timeout=TIMEOUT, verify=verify) as client,
            ):
                response = client.put(
                    url,
                    content=body,
                    headers={"Content-Length": str(size), "Content-Type": CONTENT_TYPE},
                )
        except Exception as exc:  # noqa: BLE001
            _emit(
                "failed", reason=f"upload_failed: {_error(exc, url)}", skipped_links=skipped_links
            )
            return EXIT_FAILED
        if not response.is_success:
            _emit(
                "failed",
                reason=f"upload_failed: HTTP {response.status_code}",
                skipped_links=skipped_links,
            )
            return EXIT_FAILED
        _emit("saved", nbytes=size, skipped_links=skipped_links)
        return EXIT_OK
    finally:
        path.unlink(missing_ok=True)
