"""Label rules shared in spirit with agents-taskboard (duplicated on purpose: no shared code).

A label is trimmed and lowercased, 1 to 40 characters of [a-z0-9-_.:]; a list is deduplicated
keeping first-seen order and holds at most 20 labels. The `project:` prefix is reserved for
labels the orchestrator derives itself, so a stored label never starts with it.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

LABEL_MAX_LEN = 40
LABELS_MAX = 20
RESERVED_PREFIX = "project:"
_LABEL = re.compile(r"[a-z0-9\-_.:]{1,40}")


def normalize_label(raw: str, *, allow_reserved: bool = False) -> str:
    label = raw.strip().lower()
    if not _LABEL.fullmatch(label):
        raise ValueError(
            f"invalid label {raw!r}: use 1 to {LABEL_MAX_LEN} characters of a-z, 0-9, - _ . :"
        )
    if label.startswith(RESERVED_PREFIX) and not allow_reserved:
        raise ValueError(
            f"invalid label {raw!r}: the {RESERVED_PREFIX!r} prefix is reserved and cannot be set"
        )
    return label


def normalize_labels(raw: Iterable[str], *, allow_reserved: bool = False) -> list[str]:
    """Normalised labels, deduplicated in first-seen order; ValueError on an invalid label, a
    reserved `project:` label (unless `allow_reserved`, used by list filters) or more than 20
    distinct labels."""
    labels = list(dict.fromkeys(normalize_label(r, allow_reserved=allow_reserved) for r in raw))
    if len(labels) > LABELS_MAX:
        raise ValueError(f"at most {LABELS_MAX} labels are allowed")
    return labels
