"""The slash rule (design D2): the same case table as the wrapper's turn guard tests
(claude-code-wrapper tests/test_session_turns.py, test_slash_rule_matches_claude_parser and
test_slash_rule_leaves_paths_and_prose), case for case, so the two sides cannot drift."""

import sys
import unicodedata

import pytest
from pydantic import TypeAdapter, ValidationError

from agents_orchestrator import slash
from agents_orchestrator.slash import parse
from agents_orchestrator.spec import AgentCommand

LONG_NAME = "a" * 100

COMMANDS = [
    ("/reload-plugins", ("reload-plugins", "")),
    ("  /compact keep the plan", ("compact", "keep the plan")),
    ("/workbench:start-development x", ("workbench:start-development", "x")),
    ("/model sonnet", ("model", "sonnet")),
    ("/_x", ("_x", "")),
    ("/tmp", ("tmp", "")),
    (f"/{LONG_NAME}", (LONG_NAME, "")),
    ("/x a\nb", ("x", "a\nb")),
    ("/x\ta", ("x", "a")),
    ("/x\na", ("x", "a")),
    ("/exit\u00a0x", ("exit", "x")),
    ("/clear\x0bx", ("clear", "x")),
    ("/login\rx", ("login", "x")),
    ("\ufeff/login", ("login", "")),
    ("/exit\u3000", ("exit", "")),
    ("\x1b/exit", ("exit", "")),
    ("/ex\x1bit", ("exit", "")),
    ("/ex\u200bit", ("exit", "")),
    ("\x1c/exit", ("exit", "")),
    ("/exit\x1cx", ("exitx", "")),
]

EMPTY_NAME = [
    ("/", ("", "")),
    ("/ is root", ("", "is root")),
]

PROSE = [
    "please compact the session",
    "/workdir/foo is broken",
    "/etc/hosts",
    "//x",
    "/notes.txt",
    "see /compact",
    "x\u00a0/exit",
    "/etc/hosts\u00a0x",
    "",
]

MARKED = [
    ("/review-pr 12\n\n<!-- agents-webhook:42 -->", ("review-pr", "12")),
    ("/compact\n\n<!-- agents-schedule:7 -->", ("compact", "")),
    (
        "/x <!-- agents-webhook:42 --> keep",
        ("x", "<!-- agents-webhook:42 --> keep"),
    ),
    (
        "/x <!-- agents-webhook:42 -->\n\nmore",
        ("x", "<!-- agents-webhook:42 -->\n\nmore"),
    ),
]


@pytest.mark.parametrize(("text", "expected"), COMMANDS)
def test_parse_slash_commands(text: str, expected: tuple[str, str]) -> None:
    assert parse(text) == expected


@pytest.mark.parametrize(("text", "expected"), EMPTY_NAME)
def test_parse_empty_name_is_command(text: str, expected: tuple[str, str]) -> None:
    assert parse(text) == expected


@pytest.mark.parametrize("text", PROSE)
def test_parse_leaves_prose(text: str) -> None:
    assert parse(text) is None


@pytest.mark.parametrize(("text", "expected"), MARKED)
def test_parse_strips_trailing_run_marker(text: str, expected: tuple[str, str]) -> None:
    assert parse(text) == expected


def test_parse_marker_on_prose_stays_prose() -> None:
    assert parse("/etc/hosts\n\n<!-- agents-webhook:42 -->") is None
    assert parse("hello\n\n<!-- agents-matrix:abc-1 -->") is None


# Exactly JS \s (the same set as JS trim), checked under node v24.
JS_WHITESPACE = (
    [chr(c) for c in range(0x09, 0x0E)]
    + [" ", "\u00a0", "\u1680"]
    + [chr(c) for c in range(0x2000, 0x200B)]
    + ["\u2028", "\u2029", "\u202f", "\u205f", "\u3000", "\ufeff"]
)
# Python's isspace adds these (and lacks U+FEFF); claude does not split a name at them.
NOT_JS_WHITESPACE = [chr(c) for c in range(0x1C, 0x20)] + ["\u0085"]


@pytest.mark.parametrize("char", JS_WHITESPACE, ids=lambda c: f"U+{ord(c):04X}")
def test_parse_whitespace_set_is_js_whitespace(char: str) -> None:
    assert parse(f"/a{char}b") == ("a", "b")
    assert parse(f"/a{char}{char}b{char}{char}") == ("a", "b")
    assert parse(f"{char}/a") == ("a", "")
    assert parse(f"/{char}b") == ("", "b")


@pytest.mark.parametrize("char", NOT_JS_WHITESPACE, ids=lambda c: f"U+{ord(c):04X}")
def test_parse_non_js_whitespace_does_not_split_or_strip(char: str) -> None:
    assert parse(f"/a{char}b") == ("ab", "")
    assert parse(f"{char}/a") == ("a", "")
    assert parse(f"/a {char}b{char}") == ("a", f"{char}b{char}")


def test_no_whitespace_or_invisible_char_hides_a_slash_command() -> None:
    hidden = []
    for code in range(sys.maxunicode + 1):
        char = chr(code)
        if unicodedata.category(char) not in {"Zs", "Zl", "Zp", "Cc", "Cf"}:
            continue
        if parse(f"{char}/exit") is None or parse(f"/exit{char}x") is None:
            hidden.append(f"U+{code:04X}")
    assert hidden == []


def test_agent_command_pattern_shares_slash_name() -> None:
    assert slash._NAME.pattern == slash.NAME_PATTERN
    adapter = TypeAdapter(AgentCommand)
    for ok in ("*", "compact", "plugin:cmd", "_x"):
        assert adapter.validate_python(ok) == ok
    for bad in ("/x", "a.b", "-x"):
        with pytest.raises(ValidationError):
            adapter.validate_python(bad)
    metadata = AgentCommand.__metadata__[0].metadata[0]
    assert metadata.pattern == rf"^(\*|{slash.NAME_PATTERN})$"
