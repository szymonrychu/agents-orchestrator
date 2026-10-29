"""Create-time configuration fields: wrapper settings, leader lease and autoclose."""

from __future__ import annotations

import copy
from typing import Any

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_spec

WRAPPER: dict[str, Any] = {
    "hooks": {
        "onCreate": [
            {
                "type": "command",
                "run": "echo hi",
                "env": {"FOO_1": "bar"},
                "timeout_seconds": 5,
                "blocking": True,
                "on_error": "deny",
            }
        ],
        "onAgentToolCall": [
            {
                "type": "webhook",
                "url": "https://example.com/hook",
                "secret": "s3cret",
                "headers": {"X-A": "b"},
                "retries": 2,
                "matcher": "Bash|Edit",
            }
        ],
        "onTick": [{"type": "command", "run": "date", "interval_seconds": 30, "when": "busy"}],
    },
    "auto_compact": {
        "idle_seconds": 120,
        "instructions": "keep the plan",
        "min_context_tokens": 5000,
    },
    "system_append": "Be brief.",
    "permissions": {"allow": ["Bash(ls:*)"], "deny": ["WebFetch"]},
    "mcp_servers": {
        "files": {"type": "stdio", "command": "mcp-files", "args": ["--ro"], "env": {"A": "b"}},
        "remote": {"type": "http", "url": "https://mcp.example.com", "headers": {"X": "y"}},
        "stream": {"type": "sse", "url": "https://sse.example.com", "headers": {}},
    },
    "plugins": {
        "marketplaces": [{"name": "mine", "source": "github:me/plugins"}],
        "install": [
            {"plugin": "a@mine", "scope": "project", "required": False},
            {"plugin": "b@claude-plugins-official", "scope": "user", "required": True},
        ],
    },
    "turn_stall_seconds": 600,
    "max_restarts": 4,
    "restart_continue_prompt": "continue",
    "turn_max_seconds": 3600,
    "agent_commands": ["compact", "context", "mcp", "reload-plugins", "reload-skills"],
}


def wrapper(**changes: Any) -> dict[str, Any]:
    out = copy.deepcopy(WRAPPER)
    out.update(changes)
    return out


def refused(raw: dict[str, Any]) -> str:
    with pytest.raises(ValidationError) as exc:
        SessionSpec.model_validate(raw)
    return str(exc.value)


def test_wrapper_block_round_trips() -> None:
    spec = SessionSpec.model_validate({"wrapper": WRAPPER})
    assert spec.wrapper is not None
    assert spec.wrapper.model_dump(exclude_none=True) == WRAPPER
    assert spec.model_dump(exclude_none=True)["wrapper"] == WRAPPER


@pytest.mark.parametrize(
    ("hooks", "path"),
    [
        ({"onBogus": [{"type": "command", "run": "x"}]}, "hooks.onBogus"),
        ({"onTurn": [{"type": "command", "run": "x", "matcher": "a"}]}, "hooks.onTurn.0.matcher"),
        ({"onTick": [{"type": "command", "run": "x"}]}, "hooks.onTick.0.interval_seconds"),
        (
            {"onTurn": [{"type": "command", "run": "x", "interval_seconds": 5}]},
            "hooks.onTurn.0.interval_seconds",
        ),
        ({"onTurn": [{"type": "command", "run": "x", "when": "busy"}]}, "hooks.onTurn.0.when"),
        (
            {"onTick": [{"type": "command", "run": "x", "interval_seconds": 5, "when": "x"}]},
            "when",
        ),
        ({"onTurn": [{"type": "webhook", "url": "ftp://x.example"}]}, "hooks.onTurn.0.webhook.url"),
        ({"onTurn": [{"type": "webhook", "url": "http://"}]}, "hooks.onTurn.0.webhook.url"),
        ({"onTurn": [{"type": "command", "run": "x"}] * 21}, "hooks.onTurn"),
        (
            {"onAgentToolCall": [{"type": "command", "run": "x", "matcher": "("}]},
            "hooks.onAgentToolCall.0.command.matcher",
        ),
        ({"onTurn": [{"type": "command", "run": "x", "env": {"bad": "1"}}]}, "env"),
        ({"onTurn": [{"type": "command", "run": ""}]}, "run"),
        ({"onTurn": [{"type": "command", "run": "x" * 16385}]}, "run"),
    ],
)
def test_hook_rules_refused(hooks: dict[str, Any], path: str) -> None:
    assert path in refused({"wrapper": {"hooks": hooks}})


@pytest.mark.parametrize(
    ("block", "path"),
    [
        ({"auto_compact": {"instructions": "a\nb"}}, "wrapper.auto_compact.instructions"),
        ({"auto_compact": {"instructions": "x" * 2001}}, "wrapper.auto_compact.instructions"),
        (
            {"mcp_servers": {"any-mcp": {"type": "http", "url": "http://x"}}},
            "wrapper.mcp_servers",
        ),
        (
            {"mcp_servers": {"ccw": {"type": "http", "url": "http://x"}}},
            "wrapper.mcp_servers.ccw: the name is reserved",
        ),
        (
            {"mcp_servers": {"bad name": {"type": "http", "url": "http://x"}}},
            "wrapper.mcp_servers",
        ),
        (
            {"plugins": {"install": [{"plugin": "a@nowhere"}]}},
            "wrapper.plugins.install.0.plugin",
        ),
        ({"plugins": {"install": [{"plugin": "nomarket"}]}}, "wrapper.plugins.install.0.plugin"),
        ({"agent_commands": ["compact", "rm -rf"]}, "wrapper.agent_commands"),
        ({"agent_commands": ["notes.txt"]}, "wrapper.agent_commands"),
        ({"system_append": "x" * 16385}, "wrapper.system_append"),
        ({"max_restarts": 11}, "wrapper.max_restarts"),
        ({"turn_stall_seconds": -1}, "wrapper.turn_stall_seconds"),
        ({"turn_max_seconds": -1}, "wrapper.turn_max_seconds"),
        ({"unknown": 1}, "wrapper.unknown"),
    ],
)
def test_wrapper_value_rules_refused(block: dict[str, Any], path: str) -> None:
    assert path in refused({"wrapper": block})


@pytest.mark.parametrize(
    "names", [["rm"], ["_x", "my-plugin:do_it", "Review2"], ["*"], ["compact", "*"]]
)
def test_agent_commands_accept_names_and_wildcard(names: list[str]) -> None:
    spec = SessionSpec.model_validate({"wrapper": {"agent_commands": names}})
    assert spec.wrapper is not None
    assert spec.wrapper.agent_commands == names


def _settings(monkeypatch: pytest.MonkeyPatch, **env: str) -> Settings:
    monkeypatch.setenv("ORCH_DATABASE_URL", "postgresql://x/y")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return Settings()  # type: ignore[call-arg]


def test_lease_and_autoclose_bounds(monkeypatch: pytest.MonkeyPatch) -> None:
    for ok in (60, 604800):
        assert SessionSpec(replicas=1, max_replicas=2, leader_lease_seconds=ok)
    for bad in (59, 604801):
        assert "leader_lease_seconds" in refused({"max_replicas": 2, "leader_lease_seconds": bad})
    message = "leader_lease_seconds requires a hive session (max_replicas > 1)"
    assert message in refused({"leader_lease_seconds": 60})
    assert message in refused({"replicas": 1, "max_replicas": 1, "leader_lease_seconds": 60})

    for bad in (0, 2592001):
        assert "autoclose_after_seconds" in refused({"autoclose_after_seconds": bad})

    settings = _settings(monkeypatch)
    assert settings.autoclose_min_seconds == 300
    validate_spec(SessionSpec(autoclose_after_seconds=300), settings)
    with pytest.raises(SpecInvalid) as exc:
        validate_spec(SessionSpec(autoclose_after_seconds=299), settings)
    assert exc.value.errors == ["autoclose_after_seconds 299 is below the minimum 300"]

    lowered = _settings(monkeypatch, ORCH_AUTOCLOSE_MIN_SECONDS="30")
    validate_spec(SessionSpec(autoclose_after_seconds=30), lowered)
    with pytest.raises(SettingsError):
        _settings(monkeypatch, ORCH_AUTOCLOSE_MIN_SECONDS="0")


def test_unset_fields_stay_sparse() -> None:
    dumped = SessionSpec().model_dump(exclude_none=True)
    for key in ("wrapper", "leader_lease_seconds", "autoclose_after_seconds"):
        assert key not in dumped
    only = SessionSpec.model_validate({"wrapper": {"max_restarts": 2}})
    assert only.model_dump(exclude_none=True)["wrapper"] == {"max_restarts": 2}


def test_reuse_running_agent_sparse_bool() -> None:
    assert SessionSpec(reuse_running_agent=True).reuse_running_agent is True
    assert SessionSpec(reuse_running_agent=False).reuse_running_agent is False
    assert SessionSpec().reuse_running_agent is None
    assert "reuse_running_agent" in refused({"reuse_running_agent": "yes"})
    assert "reuse_running_agent" in refused({"reuse_running_agent": 2})
    assert "reuse_running_agent" not in SessionSpec().model_dump(exclude_none=True)
    assert "reuse_running_agent" not in SessionSpec().model_dump(exclude_unset=True)
    kept = SessionSpec(reuse_running_agent=False).model_dump(exclude_none=True)
    assert kept["reuse_running_agent"] is False
