"""Whether a message text is a Claude Code slash command (design D2, amended).

Mirrors claude's own parser (claude 2.1.304: `t.trim()`, a leading "/", the name ends at the
first `search(/\\s/)`, the args are `.trim()`'d), and the claude-code-wrapper turn guard applies
the same rule, character for character:

- Whitespace (W) is exactly JS `\\s`, the same set as JS `trim`: U+0009-000D, U+0020, U+00A0,
  U+1680, U+2000-200A, U+2028, U+2029, U+202F, U+205F, U+3000 and U+FEFF. Python's `isspace`
  differs (it adds U+001C-001F and U+0085 and lacks U+FEFF), so it is never used here.
- Ignorable (D) is every other character of Unicode category Cc or Cf (ESC, NUL, DEL, C1,
  zero-width and bidi characters): the PTY paste deletes some of them and claude never shows
  the rest, so they are skipped in the leading run and dropped from the name.

After the run marker is removed, leading W and D characters are skipped, the text must start
with "/", the name runs to the first W character with D characters dropped, and the args are
the rest stripped of W only, newlines kept. An empty name is a command (claude answers
"Commands are in the form /command [args]" and the wrapper refuses it invalid_command); a name
matching the command regex is a command with no length cap; any other name (a path such as
"/etc/hosts") is prose that claude sends to the model.
"""

import re
import unicodedata

_WHITESPACE = "".join(
    chr(code)
    for code in (
        *range(0x09, 0x0E),
        0x20,
        0xA0,
        0x1680,
        *range(0x2000, 0x200B),
        0x2028,
        0x2029,
        0x202F,
        0x205F,
        0x3000,
        0xFEFF,
    )
)
_W = frozenset(_WHITESPACE)
_CLASS = f"[{_WHITESPACE}]"
# A trailing agents-common run marker, as built by agents_common.runs.marker
# (f"<!-- agents-{run_kind}:{run_id} -->") and posted by runs.py:_post_once as
# f"{text}\n\n{marker}". The orchestrator does not depend on agents-common.
_RUN_MARKER = re.compile(rf"\n\n<!-- agents-[^{_WHITESPACE}:]+:[^{_WHITESPACE}]+ -->{_CLASS}*\Z")
NAME_PATTERN = r"[a-zA-Z0-9_][a-zA-Z0-9:_-]*"
_NAME = re.compile(NAME_PATTERN)


def _ignorable(char: str) -> bool:
    """A Cc or Cf character that is not JS whitespace."""
    return char not in _W and unicodedata.category(char) in ("Cc", "Cf")


def parse(text: str) -> tuple[str, str] | None:
    """Return (name, args) when text is a slash command, else None; name is "" when empty."""
    body = _RUN_MARKER.sub("", text)
    start = 0
    while start < len(body) and (body[start] in _W or _ignorable(body[start])):
        start += 1
    if body[start : start + 1] != "/":
        return None
    end = start + 1
    while end < len(body) and body[end] not in _W:
        end += 1
    name = "".join(char for char in body[start + 1 : end] if not _ignorable(char))
    if name and not _NAME.fullmatch(name):
        return None
    first, last = end, len(body)
    while first < last and body[first] in _W:
        first += 1
    while last > first and body[last - 1] in _W:
        last -= 1
    return name, body[first:last]
