"""Label rules shared in spirit with agents-taskboard (duplicated on purpose: no shared code).

A label is trimmed and lowercased, 1 to 40 characters of [a-z0-9-_.:]; a list is deduplicated
keeping first-seen order and holds at most 20 labels.
"""

from __future__ import annotations

import re
from collections.abc import Iterable

LABEL_MAX_LEN = 40
LABELS_MAX = 20
_LABEL = re.compile(r"[a-z0-9\-_.:]{1,40}")


def normalize_label(raw: str) -> str:
    label = raw.strip().lower()
    if not _LABEL.fullmatch(label):
        raise ValueError(
            f"invalid label {raw!r}: use 1 to {LABEL_MAX_LEN} characters of a-z, 0-9, - _ . :"
        )
    return label


def normalize_labels(raw: Iterable[str]) -> list[str]:
    """Normalised labels, deduplicated in first-seen order; ValueError on an invalid label or
    more than 20 distinct labels."""
    labels = list(dict.fromkeys(normalize_label(r) for r in raw))
    if len(labels) > LABELS_MAX:
        raise ValueError(f"at most {LABELS_MAX} labels are allowed")
    return labels
