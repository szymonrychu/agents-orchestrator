"""Orchestrator-side redaction of free text that came from a pod (logs, wrapper errors).

The wrapper redacts its own failure messages; init script logs and anything else read from a pod
arrive unredacted. `redact_text` strips URL userinfo and token-like values before the text is
stored or shown. It is a best-effort filter for known shapes, not a guarantee.
"""

from __future__ import annotations

import re

MASK = "***"

_USERINFO = re.compile(r"(?P<scheme>[A-Za-z][A-Za-z0-9+.\-]*://)[^\s/@?#]+@")
_BEARER = re.compile(r"(?i)\b(?P<kind>bearer|basic)\s+[A-Za-z0-9._~+/=\-]{8,}")
_PREFIXED = re.compile(
    r"\b(?:gh[pousr]_[A-Za-z0-9]{20,}|github_pat_[A-Za-z0-9_]{20,}|glpat-[A-Za-z0-9_\-]{16,}"
    r"|sk-[A-Za-z0-9_\-]{16,}|xox[abprs]-[A-Za-z0-9\-]{10,}|AKIA[0-9A-Z]{16}"
    r"|eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,})"
)
_ASSIGNED = re.compile(
    r"""(?ix)
    (?P<key>[A-Za-z0-9_.\-]*(?:token|secret|passw(?:or)?d|passwd|api[_-]?key|credential|authorization)
            [A-Za-z0-9_.\-]*)
    (?P<sep>["']?\s*[:=]\s*["']?)
    (?P<value>[^\s"',;&]+)
    """
)
# Short, common names are only secrets inside a URL query string, so they need a ? or & before them
# (a bare `code=1` or `key: x` in a log line stays readable).
_QUERY = re.compile(
    r"(?i)(?P<lead>[?&])(?P<key>[A-Za-z0-9_.\-]*(?:sig|signature|key|code))=(?P<value>[^\s&#\"']+)"
)


def redact_text(text: str | None) -> str | None:
    """`text` with URL userinfo, bearer values, token shapes and `key=value` secrets masked."""
    if not text:
        return text
    out = _USERINFO.sub(lambda m: f"{m['scheme']}{MASK}@", text)
    out = _BEARER.sub(lambda m: f"{m['kind']} {MASK}", out)
    out = _PREFIXED.sub(MASK, out)
    out = _QUERY.sub(lambda m: f"{m['lead']}{m['key']}={MASK}", out)
    return _ASSIGNED.sub(lambda m: f"{m['key']}{m['sep']}{MASK}", out)
