"""Archive name grammar: the one validator for workspace cache names (restore, save, sweep, api).

A name is 1 to 4 `/`-joined segments, each matching `CACHE_KEY_RE` (so no empty, `.` or `..`
segment and no leading dot), at most 200 characters in all. The first segment is never `sessions`
(that is the transcripts namespace `<prefix>sessions/<slug>.tar.zst`). A restore name may end in
one `/`: the prefix form, resolved to the newest object under it. The orchestrator owns the prefix
and the `.tar.zst` suffix; a bad name is rejected, never rewritten.

Fixed-width `now` values (`20261031t120000z`, lowercase to fit the segment rule) make names sort
chronologically by plain string order, which the latest-under-prefix restore and the sweep rely on.
"""

from __future__ import annotations

import re

CACHE_KEY_RE = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
MAX_SEGMENTS = 4
MAX_NAME_LENGTH = 200
RESERVED_FIRST_SEGMENT = "sessions"
SUFFIX = ".tar.zst"


class ArchiveNameError(ValueError):
    """A name that breaks the archive name grammar (or a template that does not render to one).

    `line` is the template line when the failure came from the template.
    """

    def __init__(self, message: str, line: int | None = None) -> None:
        super().__init__(message)
        self.line = line


def validate_name(name: str, *, allow_prefix: bool) -> str:
    """Return `name` when it satisfies the grammar, else raise ArchiveNameError.

    `allow_prefix` accepts one trailing `/` (a restore name resolved to the newest object under
    it); a save name must name exactly one object.
    """
    if not isinstance(name, str) or not name:
        raise ArchiveNameError("archive name is empty")
    if len(name) > MAX_NAME_LENGTH:
        raise ArchiveNameError(f"archive name is longer than {MAX_NAME_LENGTH} characters")
    body = name
    if name.endswith("/"):
        if not allow_prefix:
            raise ArchiveNameError("archive name must not end in '/' (only a restore name may)")
        body = name[:-1]
    segments = body.split("/")
    if len(segments) > MAX_SEGMENTS:
        raise ArchiveNameError(f"archive name has more than {MAX_SEGMENTS} segments")
    for segment in segments:
        if not CACHE_KEY_RE.fullmatch(segment):
            raise ArchiveNameError(
                f"archive name segment {segment!r} must match {CACHE_KEY_RE.pattern}"
            )
    if segments[0] == RESERVED_FIRST_SEGMENT:
        raise ArchiveNameError(f"archive name must not start with '{RESERVED_FIRST_SEGMENT}/'")
    return name


def object_name_for(prefix: str, name: str) -> str:
    """The one object of a save (or exact restore) name: `<prefix><name>.tar.zst`."""
    validate_name(name, allow_prefix=False)
    return f"{prefix}{name}{SUFFIX}"


def list_prefix_for(prefix: str, name: str) -> str:
    """The listing prefix of a restore name in prefix form; the trailing `/` stays.

    Keeping it means `foo/` never matches `foo-bar/`.
    """
    validate_name(name, allow_prefix=True)
    if not name.endswith("/"):
        raise ArchiveNameError("archive name is not in prefix form (no trailing '/')")
    return f"{prefix}{name}"


def group_of(key: str, prefix: str = "") -> str:
    """The retention group of an object key: everything up to its last `/` below `prefix`.

    `proj/20261031t120000z.tar.zst` groups under `proj/`; a flat name (no `/` below the prefix)
    groups alone, as its own key.
    """
    rest = key[len(prefix) :] if key.startswith(prefix) else key
    head, sep, _ = rest.rpartition("/")
    return f"{prefix}{head}/" if sep else key
