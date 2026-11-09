"""Pack a session workdir into a capped tar.zst stream and unpack one safely (design D5, D6).

Save never follows symlinks, skips sockets, FIFOs and device files, and skips every symlink
whose target is absolute or resolves outside the workdir (a `.venv` interpreter link), since
restore would refuse it. Restore streams through
`tarfile`'s `data` filter (no absolute paths, no `..`, no links out of the workdir, no device
nodes, setuid bits cleared), refuses sparse members and anything past a decompressed byte budget,
and never deletes or empties what is already in the workdir.
"""

from __future__ import annotations

import fnmatch
import io
import logging
import os
import stat
import tarfile
from collections.abc import Buffer
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO

import zstandard

from agents_orchestrator.spec import CACHE_ALWAYS_EXCLUDE

# The single-PUT limit of S3 and RGW, and so the largest compressed archive (gate Q2).
MAX_ARCHIVE_BYTES = 5 * 1024**3
# Decompressed budget of one restore: a decompression bomb stops here, not at a full volume.
UNPACK_MAX_BYTES = 10 * MAX_ARCHIVE_BYTES
COMPRESSION_LEVEL = 3

_log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Packed:
    """Outcome of one pack: compressed bytes written, symlinks left out for pointing outside."""

    compressed_bytes: int
    skipped_links: int


class TooLarge(Exception):
    """The compressed archive passed its cap; the caller keeps the previous archive."""


class ExtractError(Exception):
    """A corrupt stream, a refused member, or a restore past its decompressed budget."""


def _require_workdir(workdir: Path) -> None:
    """Refuse an empty or root workdir before anything touches the filesystem."""
    text = os.fspath(workdir)
    if not text or os.path.normpath(text) == "." or not os.path.abspath(text).strip("/"):
        raise ValueError(f"refusing workdir {text!r}: empty or the filesystem root")


class _CappedWriter(io.RawIOBase):
    """Forwards compressed output to `out`, raising TooLarge before it would pass the cap."""

    def __init__(self, out: BinaryIO, max_bytes: int) -> None:
        super().__init__()
        self._out = out
        self._max = max_bytes
        self.written = 0

    def writable(self) -> bool:
        return True

    def write(self, data: Buffer, /) -> int:
        size = memoryview(data).nbytes
        if self.written + size > self._max:
            raise TooLarge(f"compressed archive passes {self._max} bytes")
        self._out.write(data)
        self.written += size
        return size

    def flush(self) -> None:
        # The caller owns `out` and may close it before this wrapper is finalized.
        if not self._out.closed:
            self._out.flush()


def _excluded(rel: str, patterns: list[str]) -> bool:
    return any(fnmatch.fnmatchcase(rel, pattern) for pattern in patterns)


def _link_escapes(path: str, real_root: str) -> bool:
    """True when the symlink at `path` is absolute or resolves outside `real_root`."""
    target = os.readlink(path)
    if os.path.isabs(target):
        return True
    resolved = os.path.realpath(os.path.join(os.path.dirname(path), target))
    return os.path.commonpath([resolved, real_root]) != real_root


def _add(tar: tarfile.TarFile, path: str, rel: str, real_root: str) -> bool:
    """Add one entry by lstat: directories, regular files and symlinks that stay inside.

    Returns True when the entry is a symlink skipped for pointing outside the workdir.
    """
    try:
        mode = os.lstat(path).st_mode
        if stat.S_ISLNK(mode) and _link_escapes(path, real_root):
            return True
        if not (stat.S_ISDIR(mode) or stat.S_ISREG(mode) or stat.S_ISLNK(mode)):
            return False
        info = tar.gettarinfo(path, arcname=rel)
        if info.isreg():
            with open(path, "rb") as fh:
                tar.addfile(info, fh)
        else:
            tar.addfile(info)
    except FileNotFoundError:
        pass  # removed while the workdir was walked
    return False


SESSION_FILE = "session.json"
SESSION_CONFIG = "config"
REDACTED_MARKER = ".redacted"


def _holds_unmarked_config(dirpath: str) -> bool:
    """True for a session dir (`session.json` and `config/`) with no `.redacted` file."""
    config = os.path.join(dirpath, SESSION_CONFIG)
    if not os.path.isdir(config) or os.path.islink(config):
        return False
    if not os.path.isfile(os.path.join(dirpath, SESSION_FILE)):
        return False
    marker = os.path.join(dirpath, REDACTED_MARKER)
    return not (os.path.isfile(marker) and not os.path.islink(marker))


def pack(
    workdir: Path,
    out: BinaryIO,
    *,
    exclude: list[str],
    max_bytes: int,
    require_redacted: bool = False,
) -> Packed:
    """Write `workdir` as a zstd-compressed tar to `out`; return the bytes and skipped links.

    Members are workdir-relative POSIX paths. `.agent-init` and each `exclude` glob (fnmatch on
    the relative path) are left out; an excluded directory prunes its subtree. A symlink whose
    target is absolute or resolves outside the workdir (directly or through other links) is
    skipped and counted, logged as one count line, never the targets. Raises TooLarge as soon
    as the compressed output would pass `max_bytes`.

    With `require_redacted`, the `config/` subtree of a wrapper session dir (a dir holding
    `session.json` and `config/`) is packed only when the dir also holds a `.redacted` marker
    file; otherwise it is left out and one fixed line naming the session id (the dir name) is
    logged.
    """
    _require_workdir(workdir)
    root = os.fspath(workdir)
    real_root = os.path.realpath(root)
    skipped = 0
    patterns = [*CACHE_ALWAYS_EXCLUDE, *exclude]
    capped = _CappedWriter(out, max_bytes)
    compressor = zstandard.ZstdCompressor(level=COMPRESSION_LEVEL)
    with (
        compressor.stream_writer(capped, closefd=False) as zst,
        tarfile.open(fileobj=zst, mode="w|", format=tarfile.PAX_FORMAT) as tar,
    ):
        for dirpath, dirnames, filenames in os.walk(root, followlinks=False):
            base = os.path.relpath(dirpath, root)
            kept = []
            hold_config = (
                require_redacted and SESSION_CONFIG in dirnames and _holds_unmarked_config(dirpath)
            )
            if hold_config:
                _log.warning(
                    "session config left out of the archive: no redacted marker (session %s)",
                    os.path.basename(dirpath),
                )
            for name in sorted(dirnames):
                rel = name if base == "." else f"{base}/{name}"
                if hold_config and name == SESSION_CONFIG:
                    continue
                if not _excluded(rel, patterns):
                    kept.append(name)
                    skipped += _add(tar, os.path.join(dirpath, name), rel, real_root)
            dirnames[:] = kept  # prunes excluded subtrees; symlinked dirs are never entered
            for name in sorted(filenames):
                rel = name if base == "." else f"{base}/{name}"
                if not _excluded(rel, patterns):
                    skipped += _add(tar, os.path.join(dirpath, name), rel, real_root)
    _log.info("workspace pack skipped %d symlinks pointing outside the workdir", skipped)
    return Packed(compressed_bytes=capped.written, skipped_links=skipped)


class _BudgetReader(io.RawIOBase):
    """Reads the decompressed tar stream, refusing it once it passes `max_bytes`."""

    def __init__(self, src: zstandard.ZstdDecompressionReader, max_bytes: int) -> None:
        super().__init__()
        self._src = src
        self._max = max_bytes
        self.read_bytes = 0

    def readable(self) -> bool:
        return True

    def readinto(self, buffer: Buffer, /) -> int:
        view = memoryview(buffer).cast("B")
        data = self._src.read(len(view))
        self.read_bytes += len(data)
        if self.read_bytes > self._max:
            raise ExtractError(f"archive expands past {self._max} bytes")
        view[: len(data)] = data
        return len(data)


def unpack(src: BinaryIO, workdir: Path, *, max_bytes: int = UNPACK_MAX_BYTES) -> int:
    """Extract a zstd-compressed tar from `src` into `workdir`; return the members extracted.

    `workdir` is created when missing and nothing already in it is deleted. A corrupt stream, a
    refused member (absolute path, `..`, a link escaping `workdir`, a device node or FIFO, a
    sparse file) or more than `max_bytes` of decompressed data raises ExtractError. Errors
    reading `src` itself propagate unchanged, so a failed download is never an extract failure.
    """
    _require_workdir(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    reader = _BudgetReader(
        zstandard.ZstdDecompressor().stream_reader(src, closefd=False), max_bytes
    )
    count = 0
    declared = 0
    try:
        with tarfile.open(fileobj=reader, mode="r|", errorlevel=2) as tar:
            for member in tar:
                if member.name.startswith("/"):
                    # The data filter would strip the slash; an absolute member is refused.
                    raise ExtractError(f"refusing absolute member {member.name!r}")
                if member.issparse():
                    raise ExtractError(f"refusing sparse member {member.name!r}")
                declared += member.size
                if declared > max_bytes:
                    raise ExtractError(f"archive expands past {max_bytes} bytes")
                tar.extract(member, workdir, filter="data")
                count += 1
    except (tarfile.TarError, zstandard.ZstdError) as exc:
        raise ExtractError(str(exc)) from exc
    return count
