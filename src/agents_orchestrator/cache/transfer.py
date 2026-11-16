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
import tempfile
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


SESSIONS_DIR = "/sessions"


def _require_redacted(workdir_text: str) -> bool:
    """The sessions target (the `/sessions` mount) always gates `config/` on the marker."""
    return os.path.realpath(workdir_text) == SESSIONS_DIR


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
                    require_redacted=_require_redacted(workdir_text),
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


# ---------------------------------------------------------------------------------------------
# Per-entry backup jobs (S-68): one Job carries a JSON list of entries; every entry has its own
# outcome, and a missing or unreadable restore is a state, never an error.

STATE_DIR_DEFAULT = "/backup-state"
RESTORE_TMP_PREFIX = ".restore-"
DIR_MODE = 0o755


def _emit_entries(entries: list[dict[str, Any]], **extra: Any) -> None:
    sys.stderr.flush()
    print(json.dumps({"entries": entries, **extra}), file=sys.stdout, flush=True)


def _load_list(env: Mapping[str, str], name: str) -> list[dict[str, Any]]:
    raw = env.get(name, "")
    if not raw:
        raise _BadEnv(f"{name} is not set")
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        raise _BadEnv(f"{name} is not JSON") from None
    if not isinstance(value, list) or not all(isinstance(v, dict) for v in value):
        raise _BadEnv(f"{name} is not a JSON list of objects")
    return value


def _entry_target(entry: Mapping[str, Any]) -> tuple[Path, Path]:
    """(root, target) of an entry; the path must be relative with no `..` segment."""
    root_dir = entry.get("root_dir")
    path = entry.get("path", ".")
    if not isinstance(root_dir, str) or not root_dir or not isinstance(path, str) or not path:
        raise _BadEnv("entry needs root_dir and path")
    parts = path.split("/")
    if path.startswith("/") or ".." in parts or (path != "." and "" in parts):
        raise _BadEnv("entry path must be relative with no '..' segment")
    root = Path(root_dir)
    return root, (root if path == "." else root / path)


def _entry_exclude(entry: Mapping[str, Any]) -> list[str]:
    value = entry.get("exclude") or []
    if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
        raise _BadEnv("entry exclude is not a list of strings")
    return value


def _report(root: Path, exclude: list[str]) -> dict[str, Any]:
    files = archive.manifest(root, exclude)
    return {
        "digest": archive.digest(files),
        "manifest": (
            [f.as_dict() for f in files] if len(files) <= archive.MANIFEST_MAX_FILES else None
        ),
    }


def _merge_plan(temp: Path, target: Path) -> tuple[list[Path], list[tuple[Path, Path]]]:
    """(dirs to create, (src, dest) moves) for merging `temp` into the existing `target`.

    Nothing is changed here. A symlink already in the target is never followed: a symlinked
    directory in the way, or a file-versus-directory conflict, raises before any change. A
    symlinked or file leaf is replaced as a leaf.
    """
    dirs: list[Path] = []
    moves: list[tuple[Path, Path]] = []
    if target.is_symlink():
        raise OSError(f"restore target {target.name!r} is a symlink")
    stack = [(temp, target)]
    while stack:
        src_dir, dest_dir = stack.pop()
        for src in sorted(src_dir.iterdir()):
            dest = dest_dir / src.name
            present = os.path.lexists(dest)
            if src.is_dir() and not src.is_symlink():
                if present and (dest.is_symlink() or not dest.is_dir()):
                    raise OSError(f"restore conflict at {dest.relative_to(target).as_posix()!r}")
                if not present:
                    dirs.append(dest)
                stack.append((src, dest))
            else:
                if present and dest.is_dir() and not dest.is_symlink():
                    raise OSError(f"restore conflict at {dest.relative_to(target).as_posix()!r}")
                moves.append((src, dest))
    return dirs, moves


def _merge_into(temp: Path, target: Path) -> None:
    """Move the extracted tree to `target`; existing files are overwritten, none deleted.

    An absent target gets the temp dir renamed into place with the normal directory mode (not
    mkdtemp's 0700). An existing one is merged member-wise after the whole plan checked out, so
    a symlink or conflict leaves it untouched; moves are renames within one filesystem.
    """
    if not os.path.lexists(target):
        target.parent.mkdir(parents=True, exist_ok=True)
        temp.chmod(DIR_MODE)
        temp.replace(target)
        return
    dirs, moves = _merge_plan(temp, target)
    for directory in dirs:  # parents sort before children
        directory.mkdir(exist_ok=True)
    for src, dest in moves:
        os.replace(src, dest)
    shutil.rmtree(temp, ignore_errors=True)


def _restore_one(
    entry: Mapping[str, Any],
    client: httpx.Client,
    state_dir: Path | None,
) -> dict[str, Any]:
    index = entry.get("index")
    result: dict[str, Any] = {"index": index}
    url = str(entry.get("url") or "")
    version = entry.get("version")
    if state_dir is not None and (not isinstance(index, int) or isinstance(index, bool)):
        return {**result, "state": "failed", "reason": "entry index must be an integer"}
    temp: Path | None = None
    reader: _BodyReader | None = None
    try:
        root, target = _entry_target(entry)
        exclude = _entry_exclude(entry)
        if not url:
            raise _BadEnv("entry has no url")
        with client.stream("GET", url) as response:
            if response.status_code == 404:
                return {**result, "state": "missing"}
            if response.status_code != 200:
                return {**result, "state": "failed", "reason": f"HTTP {response.status_code}"}
            # A sibling temp dir (inside the root for the root itself): the target is touched
            # only after the whole archive extracted.
            parent = root if target == root else target.parent
            parent.mkdir(parents=True, exist_ok=True)
            temp = Path(tempfile.mkdtemp(prefix=RESTORE_TMP_PREFIX, dir=parent))
            reader = _BodyReader(response.iter_bytes())
            archive.unpack(cast(BinaryIO, reader), temp)
            while reader.read(io.DEFAULT_BUFFER_SIZE):
                pass
            size = reader.delivered
        _merge_into(temp, target)
        temp = None
        report = _report(target, exclude)
        result = {**result, "state": "restored", "size": size, **report}
        if version is not None:
            result["version"] = version
        if state_dir is not None:
            try:
                _write_state(state_dir, entry, version, report["manifest"])
            except OSError as exc:  # the files are restored: a state error is only a warning
                result["warning"] = f"backup-state not written: {_error(exc, url)}"
        return result
    except archive.ExtractError as exc:
        return {**result, "state": "unreadable", "reason": _error(exc, url)}
    except Exception as exc:  # noqa: BLE001 - every failure becomes this entry's outcome
        return {**result, "state": "failed", "reason": _error(exc, url)}
    finally:
        if temp is not None:
            shutil.rmtree(temp, ignore_errors=True)


def _write_state(state_dir: Path, entry: Mapping[str, Any], version: Any, manifest: Any) -> None:
    """`backup-state/<index>.json`: what the restore put on disk, for the dirty check later."""
    state_dir.mkdir(parents=True, exist_ok=True)
    body = {"name": entry.get("name"), "version": version, "manifest": manifest}
    target = state_dir / f"{entry['index']}.json"
    tmp = target.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(body))
    tmp.replace(target)


def _state_dir(env: Mapping[str, str]) -> Path | None:
    raw = env.get("CACHE_STATE_DIR", STATE_DIR_DEFAULT)
    return Path(raw) if raw and Path(raw).is_dir() else None


def restore_entries(env: Mapping[str, str], *, transport: httpx.BaseTransport | None = None) -> int:
    """Restore every entry of `CACHE_ENTRIES`; print `{"entries": [...]}`; exit 1 on a failure.

    Each entry ends `restored`, `missing` (404, nothing touched), `unreadable` (extraction failed
    in a temp dir that is removed, nothing touched) or `failed`. Missing and unreadable are
    warnings for the caller: the exit code stays 0.
    """
    try:
        entries = _load_list(env, "CACHE_ENTRIES")
        verify = _verify_tls(env)
    except _BadEnv as exc:
        _emit_entries([], reason=f"bad_env: {exc}")
        return EXIT_FAILED
    state_dir = _state_dir(env)
    with httpx.Client(transport=transport, timeout=TIMEOUT, verify=verify) as client:
        results = [_restore_one(e, client, state_dir) for e in entries]
    _emit_entries(results)
    return EXIT_FAILED if any(r["state"] == "failed" for r in results) else EXIT_OK


def _save_one(
    entry: Mapping[str, Any],
    client: httpx.Client,
    scratch: Path,
    max_bytes: int,
) -> dict[str, Any]:
    index = entry.get("index")
    result: dict[str, Any] = {"index": index}
    url = str(entry.get("url") or "")
    path = scratch / f"{index}-{ARCHIVE_NAME}"
    try:
        _, target = _entry_target(entry)
        exclude = _entry_exclude(entry)
        if not url:
            raise _BadEnv("entry has no url")
        if not target.is_dir():
            return {**result, "state": "skipped", "reason": "no_dir"}
        if entry.get("root") == "claude" and archive.holds_secret(target, exclude):
            return {**result, "state": "refused_secret"}
        scratch.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("wb") as out:
                packed = archive.pack(target, out, exclude=exclude, max_bytes=max_bytes)
        except archive.TooLarge:
            return {**result, "state": "failed", "reason": "too_large"}
        report = _report(target, exclude)
        with path.open("rb") as body:
            response = client.put(
                url,
                content=body,
                headers={
                    "Content-Length": str(packed.compressed_bytes),
                    "Content-Type": CONTENT_TYPE,
                },
            )
        if not response.is_success:
            return {**result, "state": "failed", "reason": f"HTTP {response.status_code}"}
        return {**result, "state": "uploaded", "size": packed.compressed_bytes, **report}
    except Exception as exc:  # noqa: BLE001
        return {**result, "state": "failed", "reason": _error(exc, url)}
    finally:
        path.unlink(missing_ok=True)


def save_entries(env: Mapping[str, str], *, transport: httpx.BaseTransport | None = None) -> int:
    """Save every entry of `CACHE_ENTRIES` to its own URL; print `{"entries": [...]}`.

    A `claude` root entry holding unredacted session config or a credentials file is
    `refused_secret` and sends nothing. Each upload goes to a fresh immutable object.
    """
    try:
        entries = _load_list(env, "CACHE_ENTRIES")
        verify = _verify_tls(env)
        max_bytes = _max_bytes(env)
    except _BadEnv as exc:
        _emit_entries([], reason=f"bad_env: {exc}")
        return EXIT_FAILED
    scratch = Path(env.get("CACHE_SCRATCH") or DEFAULT_SCRATCH)
    with httpx.Client(transport=transport, timeout=TIMEOUT, verify=verify) as client:
        results = [_save_one(e, client, scratch, max_bytes) for e in entries]
    _emit_entries(results)
    return EXIT_FAILED if any(r["state"] == "failed" for r in results) else EXIT_OK


def resolve(env: Mapping[str, str]) -> int:
    """Digest the paths a name template asks for (`CACHE_RESOLVE`, PVC mounted read-only).

    Input `[{root_dir, path, length, exclude}]`; output `{"digests": {"<path>:<length>": hex}}`
    where hex is the first `length` characters of `archive.dir_sha256` (`none` stays `none`).
    No URL, no write.
    """
    try:
        entries = _load_list(env, "CACHE_RESOLVE")
        digests: dict[str, str] = {}
        for entry in entries:
            root, _ = _entry_target(entry)
            path = entry["path"]
            length = entry.get("length", 16)
            if not isinstance(length, int) or isinstance(length, bool) or length < 1:
                raise _BadEnv("entry length is a positive integer")
            full = archive.dir_sha256(root, path, _entry_exclude(entry))
            digests[f"{path}:{length}"] = full if full == archive.NO_PATH_DIGEST else full[:length]
    except (_BadEnv, KeyError) as exc:
        sys.stderr.flush()
        print(json.dumps({"digests": {}, "reason": f"bad_env: {exc}"}), flush=True)
        return EXIT_FAILED
    sys.stderr.flush()
    print(json.dumps({"digests": digests}), flush=True)
    return EXIT_OK
