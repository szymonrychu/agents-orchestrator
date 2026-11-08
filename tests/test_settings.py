import ipaddress
import re
import secrets
from pathlib import Path

import pytest

from agents_orchestrator.settings import DiscardHook, Settings, SettingsError

README = Path(__file__).resolve().parent.parent / "README.md"
DB = "postgresql://u:p@h/db"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


def test_defaults():
    s = Settings()
    assert s.database_url == DB
    assert s.master_key is None
    assert s.namespace == "agents"
    assert s.agent_image == ""
    assert s.check_image == ""
    assert s.default_model == "claude-opus-5-5"
    assert s.default_effort == ""
    assert s.models[0] == "claude-opus-5-5"
    assert s.default_replicas == 1
    assert s.default_auto_compact_pct == 95
    assert s.pvc_storage_class == "rook-ceph-rwx"
    assert s.pvc_size == "20Gi"
    assert s.claude_secret_name == "agents-orchestrator-claude"
    assert s.image_pull_secret == "regcred"
    assert s.any_mcp_config_file == ""
    assert s.wrapper_port == 8080
    assert s.cluster_domain == "cluster.local"
    assert s.agent_termination_grace_seconds == 90
    assert s.http_host == "0.0.0.0"
    assert s.http_port == 8000
    assert s.dispatch_poll_seconds == 2
    assert s.reconcile_poll_seconds == 10
    assert s.ingest_refresh_seconds == 5
    assert s.shutdown_grace_seconds == 20
    assert s.log_level == "INFO"
    assert s.discard_hook_timeout_seconds == 120
    assert s.any_mcp_repo == "https://github.com/szymonrychu/any-mcp"
    assert s.any_mcp_ref == "v0.5.0"
    assert s.any_mcp_plugins_repo == "https://github.com/szymonrychu/any-mcp-tatara-plugins"
    assert s.any_mcp_plugins_ref == "v0.14.0"
    assert s.on_start_timeout_seconds == 600
    assert s.discard_hook() is None


def test_static_dir_retired(monkeypatch):
    monkeypatch.setenv("ORCH_STATIC_DIR", "/app/static")
    s = Settings()
    assert "static_dir" not in Settings.model_fields
    assert not hasattr(s, "static_dir")


def test_check_image_defaults_to_agent_image(monkeypatch):
    monkeypatch.setenv("ORCH_AGENT_IMAGE", "img:1")
    s = Settings()
    assert s.check_image == "img:1"
    assert s.require_agent_image() == "img:1"
    monkeypatch.setenv("ORCH_CHECK_IMAGE", "chk:2")
    assert Settings().check_image == "chk:2"


def test_require_agent_image_missing():
    with pytest.raises(SettingsError, match="ORCH_AGENT_IMAGE"):
        Settings().require_agent_image()


def test_models_comma_separated(monkeypatch):
    monkeypatch.setenv("ORCH_MODELS", "a, b,,c")
    assert Settings().models == ["a", "b", "c"]


def test_database_url_required(monkeypatch):
    monkeypatch.delenv("ORCH_DATABASE_URL")
    with pytest.raises(SettingsError, match="ORCH_DATABASE_URL"):
        Settings()


def test_error_names_every_bad_variable(monkeypatch):
    monkeypatch.delenv("ORCH_DATABASE_URL")
    monkeypatch.setenv("ORCH_HTTP_PORT", "abc")
    with pytest.raises(SettingsError) as e:
        Settings()
    assert "ORCH_DATABASE_URL" in str(e.value)
    assert "ORCH_HTTP_PORT" in str(e.value)


def test_master_key_from_file(monkeypatch, tmp_path):
    f = tmp_path / "key"
    f.write_text("f" * 40 + "\n")
    monkeypatch.setenv("ORCH_MASTER_KEY", "e" * 40)
    monkeypatch.setenv("ORCH_MASTER_KEY_FILE", str(f))
    assert Settings().require_master_key() == "f" * 40


def test_master_key_from_env_and_required(monkeypatch):
    with pytest.raises(SettingsError, match="ORCH_MASTER_KEY"):
        Settings().require_master_key()
    monkeypatch.setenv("ORCH_MASTER_KEY", "e" * 32)
    assert Settings().require_master_key() == "e" * 32


def test_master_key_too_short_rejected(monkeypatch, tmp_path):
    monkeypatch.setenv("ORCH_MASTER_KEY", "short")
    with pytest.raises(SettingsError, match="ORCH_MASTER_KEY"):
        Settings()
    monkeypatch.delenv("ORCH_MASTER_KEY")
    f = tmp_path / "key"
    f.write_text("  " + "x" * 31 + "  \n")
    monkeypatch.setenv("ORCH_MASTER_KEY_FILE", str(f))
    with pytest.raises(SettingsError, match="ORCH_MASTER_KEY"):
        Settings()


def test_single_namespace_cap_and_timing_defaults():
    s = Settings()
    assert s.namespace == "agents"
    assert not hasattr(s, "sessions_namespace")
    assert "sessions_namespace" not in Settings.model_fields
    assert s.allowed_cidrs == ["192.168.1.0/24"]
    assert s.agent_images == []
    assert s.max_replicas == 16
    assert s.max_pvc_size == "100Gi"
    assert s.max_sessions == 20
    assert s.wrapper_send_timeout_seconds == 15
    assert s.dispatching_stale_seconds == 60
    assert s.dispatching_heartbeat_seconds == 5
    assert s.lock_heartbeat_seconds == 5
    assert s.lock_tcp_user_timeout_seconds == 15
    assert s.retry_backoff_base_seconds == 2
    assert s.retry_backoff_max_seconds == 60
    assert s.ambiguous_settle_seconds == 60
    assert s.ambiguous_deadline_seconds == 600


def test_allowed_cidrs_parsed_and_bad_entry_rejected(monkeypatch):
    monkeypatch.setenv("ORCH_ALLOWED_CIDRS", "10.0.0.0/8, 192.168.1.0/24")
    assert Settings().allowed_networks() == [
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("192.168.1.0/24"),
    ]
    monkeypatch.setenv("ORCH_ALLOWED_CIDRS", "10.0.0.0/8,nonsense")
    with pytest.raises(SettingsError, match="ORCH_ALLOWED_CIDRS"):
        Settings()


def test_image_allowlist_includes_agent_image(monkeypatch):
    monkeypatch.setenv("ORCH_AGENT_IMAGE", "base:1")
    monkeypatch.setenv("ORCH_AGENT_IMAGES", "x:1, y:2")
    assert Settings().image_allowlist() == frozenset({"base:1", "x:1", "y:2"})


def test_send_timeout_must_be_well_below_stale_cutoff(monkeypatch):
    monkeypatch.setenv("ORCH_WRAPPER_SEND_TIMEOUT_SECONDS", "31")
    monkeypatch.setenv("ORCH_AMBIGUOUS_SETTLE_SECONDS", "100")
    with pytest.raises(SettingsError, match="ORCH_WRAPPER_SEND_TIMEOUT_SECONDS"):
        Settings()
    monkeypatch.setenv("ORCH_WRAPPER_SEND_TIMEOUT_SECONDS", "30")
    Settings()


def test_retry_backoff_and_ambiguity_windows(monkeypatch):
    s = Settings()
    assert [s.retry_delay_seconds(n) for n in (1, 2, 3, 4)] == [2, 4, 8, 16]
    assert s.retry_delay_seconds(10) == 60
    monkeypatch.setenv("ORCH_AMBIGUOUS_SETTLE_SECONDS", "29")
    with pytest.raises(SettingsError, match="ORCH_AMBIGUOUS_SETTLE_SECONDS"):
        Settings()
    monkeypatch.setenv("ORCH_AMBIGUOUS_SETTLE_SECONDS", "60")
    monkeypatch.setenv("ORCH_AMBIGUOUS_DEADLINE_SECONDS", "60")
    with pytest.raises(SettingsError, match="ORCH_AMBIGUOUS_DEADLINE_SECONDS"):
        Settings()
    monkeypatch.setenv("ORCH_AMBIGUOUS_DEADLINE_SECONDS", "600")
    monkeypatch.setenv("ORCH_RETRY_BACKOFF_BASE_SECONDS", "0")
    with pytest.raises(SettingsError, match="ORCH_RETRY_BACKOFF_BASE_SECONDS"):
        Settings()
    monkeypatch.setenv("ORCH_RETRY_BACKOFF_BASE_SECONDS", "100")
    with pytest.raises(SettingsError, match="ORCH_RETRY_BACKOFF_MAX_SECONDS"):
        Settings()


def test_discard_hook_command_or_url(monkeypatch):
    monkeypatch.setenv("ORCH_DISCARD_HOOK_COMMAND", "/bin/export")
    assert Settings().discard_hook() == DiscardHook(kind="command", target="/bin/export")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_URL", "https://h/x")
    with pytest.raises(SettingsError, match="ORCH_DISCARD_HOOK_URL"):
        Settings()
    monkeypatch.delenv("ORCH_DISCARD_HOOK_COMMAND")
    assert Settings().discard_hook() == DiscardHook(kind="url", target="https://h/x")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_URL", "ftp://h/x")
    with pytest.raises(SettingsError, match="ORCH_DISCARD_HOOK_URL"):
        Settings()


def test_discard_hook_token_needs_url(monkeypatch, caplog):
    monkeypatch.setenv("ORCH_DISCARD_HOOK_TOKEN", "t0k3n")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_DISCARD_HOOK_TOKEN" in exc.value.problems
    assert "t0k3n" not in str(exc.value)
    monkeypatch.setenv("ORCH_DISCARD_HOOK_URL", "https://history.example/hook")
    s = Settings()
    assert s.discard_hook_token == "t0k3n"
    assert s.discard_hook() == DiscardHook(
        "url", "https://history.example/hook", "settings", token="t0k3n"
    )
    for bad in ("t0k3n\n", "t0 k3n", "t0k3n\x07", "t0k\u00e9n", "t0k3n\u00a0", "t0k3n\x7f"):
        monkeypatch.setenv("ORCH_DISCARD_HOOK_TOKEN", bad)
        with caplog.at_level("DEBUG"), pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_DISCARD_HOOK_TOKEN" in exc.value.problems, repr(bad)
        shown = str(exc.value) + repr(exc.value) + repr(exc.value.problems) + caplog.text
        shown = shown.replace("ORCH_DISCARD_HOOK_TOKEN", "")
        for part in ("t0", "k3", "3n", "k\u00e9", "\u00e9n", "\u00a0", "\x7f", "\x07"):
            assert part not in shown, (repr(bad), part)
    monkeypatch.delenv("ORCH_DISCARD_HOOK_TOKEN")
    assert Settings().discard_hook() == DiscardHook("url", "https://history.example/hook")


def test_discard_hook_token_refused_with_command(monkeypatch):
    monkeypatch.setenv("ORCH_DISCARD_HOOK_COMMAND", "/bin/export")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_TOKEN", "t0k3n")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_DISCARD_HOOK_TOKEN" in exc.value.problems
    assert "ORCH_DISCARD_HOOK_COMMAND" in exc.value.problems["ORCH_DISCARD_HOOK_TOKEN"]
    assert "t0k3n" not in str(exc.value)


def test_discard_hook_token_hidden_in_repr(monkeypatch):
    monkeypatch.setenv("ORCH_DISCARD_HOOK_URL", "https://history.example/hook")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_TOKEN", "t0k3n")
    settings = Settings()
    hook = settings.discard_hook()
    assert hook is not None and hook.token == "t0k3n"
    assert "t0k3n" not in repr(settings) and "t0k3n" not in str(settings)
    assert "t0k3n" not in repr(hook) and "t0k3n" not in str(hook)


def test_any_mcp_ref_must_be_release_tag(monkeypatch):
    monkeypatch.setenv("ORCH_ANY_MCP_REF", "v1.2.3")
    assert Settings().any_mcp_ref == "v1.2.3"
    monkeypatch.setenv("ORCH_ANY_MCP_REF", "main")
    with pytest.raises(SettingsError, match="ORCH_ANY_MCP_REF"):
        Settings()


def test_any_mcp_plugins_ref_must_be_release_tag(monkeypatch):
    monkeypatch.setenv("ORCH_ANY_MCP_PLUGINS_REF", "v1.2.3")
    assert Settings().any_mcp_plugins_ref == "v1.2.3"
    for bad in ("main", "1.2.3", "v1.2", "2ca3d44"):
        monkeypatch.setenv("ORCH_ANY_MCP_PLUGINS_REF", bad)
        with pytest.raises(SettingsError, match="ORCH_ANY_MCP_PLUGINS_REF"):
            Settings()


def test_attachment_caps_defaults_and_env(monkeypatch):
    s = Settings()
    assert (s.attachments_max_bytes, s.attachments_max_total_bytes, s.attachments_max_count) == (
        6291456,
        8388608,
        8,
    )
    monkeypatch.setenv("ORCH_ATTACHMENTS_MAX_COUNT", "0")
    assert Settings().attachments_max_count == 0
    monkeypatch.setenv("ORCH_ATTACHMENTS_MAX_BYTES", "0")
    with pytest.raises(SettingsError, match="ORCH_ATTACHMENTS_MAX_BYTES"):
        Settings()


def test_env_names_documented_in_readme():
    text = README.read_text()
    assert "## Configuration" in text
    for name in Settings.model_fields:
        assert f"`ORCH_{name.upper()}`" in text, name
    assert re.search(r"`ORCH_MASTER_KEY_FILE`", text)


def test_cross_field_errors_all_reported(monkeypatch):
    monkeypatch.setenv("ORCH_MASTER_KEY", "short")
    monkeypatch.setenv("ORCH_WRAPPER_SEND_TIMEOUT_SECONDS", "100")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_URL", "ftp://x")
    monkeypatch.setenv("ORCH_ANY_MCP_REF", "main")
    monkeypatch.setenv("ORCH_ALLOWED_CIDRS", "nope,10.0.0.0/8,bad2")
    with pytest.raises(SettingsError) as e:
        Settings()
    for var in (
        "ORCH_MASTER_KEY",
        "ORCH_WRAPPER_SEND_TIMEOUT_SECONDS",
        "ORCH_DISCARD_HOOK_URL",
        "ORCH_ANY_MCP_REF",
        "ORCH_ALLOWED_CIDRS",
    ):
        assert var in e.value.problems
    assert "nope" in e.value.problems["ORCH_ALLOWED_CIDRS"]
    assert "bad2" in e.value.problems["ORCH_ALLOWED_CIDRS"]


def test_error_names_every_bad_variable_across_validators(monkeypatch):
    monkeypatch.delenv("ORCH_DATABASE_URL")
    monkeypatch.setenv("ORCH_HTTP_PORT", "abc")
    monkeypatch.setenv("ORCH_ANY_MCP_REF", "main")
    monkeypatch.setenv("ORCH_ALLOWED_CIDRS", "nope")
    with pytest.raises(SettingsError) as e:
        Settings()
    for var in ("DATABASE_URL", "HTTP_PORT", "ANY_MCP_REF", "ALLOWED_CIDRS"):
        assert f"ORCH_{var}" in str(e.value)


def test_form_defaults_and_overrides(monkeypatch):
    s = Settings()
    assert s.models == [
        "claude-opus-5-5",
        "claude-sonnet-5-5",
        "claude-haiku-5-5",
        "claude-fable-5-1",
    ]
    assert s.default_model == "claude-opus-5-5"
    assert s.efforts == ["low", "medium", "high", "xhigh", "max"]
    assert s.default_effort == ""
    assert (s.default_cpu_request, s.default_cpu_limit) == ("500m", "4")
    assert (s.default_memory_request, s.default_memory_limit) == ("1Gi", "8Gi")

    monkeypatch.setenv("ORCH_MODELS", "a,b")
    monkeypatch.setenv("ORCH_DEFAULT_MODEL", "b")
    monkeypatch.setenv("ORCH_EFFORTS", "low,high")
    monkeypatch.setenv("ORCH_DEFAULT_EFFORT", "high")
    monkeypatch.setenv("ORCH_DEFAULT_CPU_REQUEST", "250m")
    monkeypatch.setenv("ORCH_DEFAULT_CPU_LIMIT", "2")
    monkeypatch.setenv("ORCH_DEFAULT_MEMORY_REQUEST", "512Mi")
    monkeypatch.setenv("ORCH_DEFAULT_MEMORY_LIMIT", "4Gi")
    s = Settings()
    assert (s.models, s.default_model, s.efforts, s.default_effort) == (
        ["a", "b"],
        "b",
        ["low", "high"],
        "high",
    )
    assert (s.default_cpu_request, s.default_cpu_limit) == ("250m", "2")
    assert (s.default_memory_request, s.default_memory_limit) == ("512Mi", "4Gi")


def test_form_defaults_validated(monkeypatch):
    monkeypatch.setenv("ORCH_DEFAULT_CPU_REQUEST", "lots")
    monkeypatch.setenv("ORCH_DEFAULT_MEMORY_LIMIT", "0")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_DEFAULT_CPU_REQUEST" in exc.value.problems
    assert "ORCH_DEFAULT_MEMORY_LIMIT" in exc.value.problems


def test_default_model_and_effort_must_be_listed(monkeypatch):
    monkeypatch.setenv("ORCH_MODELS", "a,b")
    assert Settings().default_model == "a"
    monkeypatch.setenv("ORCH_DEFAULT_MODEL", "zzz")
    monkeypatch.setenv("ORCH_DEFAULT_EFFORT", "turbo")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_DEFAULT_MODEL" in exc.value.problems
    assert "ORCH_DEFAULT_EFFORT" in exc.value.problems


def test_default_resources_request_above_limit_rejected(monkeypatch):
    monkeypatch.setenv("ORCH_DEFAULT_CPU_REQUEST", "8")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_DEFAULT_CPU_REQUEST" in exc.value.problems


def test_chat_settings(monkeypatch):
    s = Settings()
    assert (s.chat_url, s.chat_token) == ("", "")
    token = "chat-" + secrets.token_hex(16)
    monkeypatch.setenv("ORCH_CHAT_URL", "http://connector-api-agents:8001")
    monkeypatch.setenv("ORCH_CHAT_TOKEN", token)
    s = Settings()
    assert (s.chat_url, s.chat_token) == ("http://connector-api-agents:8001", token)
    assert token not in repr(s)
    assert token not in str(s)
    monkeypatch.setenv("ORCH_CHAT_URL", "https://chat.example/prefix")
    assert Settings().chat_url == "https://chat.example/prefix"
    for bad in ("ftp://chat", "chat:8001", "http://", "http://chat/?q=1", "http://chat/#f"):
        monkeypatch.setenv("ORCH_CHAT_URL", bad)
        with pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_CHAT_URL" in exc.value.problems, bad
        assert token not in str(exc.value)
    monkeypatch.delenv("ORCH_CHAT_TOKEN")
    monkeypatch.setenv("ORCH_CHAT_URL", "http://chat:8001")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_CHAT_URL" in str(exc.value) and "ORCH_CHAT_TOKEN" in str(exc.value)
    monkeypatch.delenv("ORCH_CHAT_URL")
    monkeypatch.setenv("ORCH_CHAT_TOKEN", token)
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_CHAT_URL" in str(exc.value) and "ORCH_CHAT_TOKEN" in str(exc.value)
    assert token not in str(exc.value)


def test_taskboard_settings(monkeypatch):
    s = Settings()
    assert (s.taskboard_url, s.taskboard_token, s.taskboard_available) == ("", "", False)
    token = "tb-" + secrets.token_hex(16)
    monkeypatch.setenv("ORCH_TASKBOARD_URL", "http://agents-taskboard-agents:8001")
    monkeypatch.setenv("ORCH_TASKBOARD_TOKEN", token)
    s = Settings()
    assert (s.taskboard_url, s.taskboard_token) == ("http://agents-taskboard-agents:8001", token)
    assert s.taskboard_available
    assert token not in repr(s)
    for bad in ("ftp://tb", "tb:8001", "http://", "http://tb/?q=1", "http://tb/#f"):
        monkeypatch.setenv("ORCH_TASKBOARD_URL", bad)
        with pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_TASKBOARD_URL" in exc.value.problems, bad
    monkeypatch.delenv("ORCH_TASKBOARD_TOKEN")
    monkeypatch.setenv("ORCH_TASKBOARD_URL", "http://tb:8001")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_TASKBOARD_URL" in str(exc.value) and "ORCH_TASKBOARD_TOKEN" in str(exc.value)
    monkeypatch.delenv("ORCH_TASKBOARD_URL")
    monkeypatch.setenv("ORCH_TASKBOARD_TOKEN", token)
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "ORCH_TASKBOARD_URL" in str(exc.value) and "ORCH_TASKBOARD_TOKEN" in str(exc.value)
    assert token not in str(exc.value)


def test_usage_settings(monkeypatch):
    s = Settings()
    assert (s.usage_url, s.usage_token) == ("", "")
    assert s.usage_available is False
    assert s.usage_quota_default == 100
    assert s.usage_cache_seconds == 30
    assert s.usage_timeout_seconds == 3
    token = "usage-" + secrets.token_hex(16)
    monkeypatch.setenv("ORCH_USAGE_URL", "http://agents-usage-api:8000")
    monkeypatch.setenv("ORCH_USAGE_TOKEN", token)
    s = Settings()
    assert (s.usage_url, s.usage_token) == ("http://agents-usage-api:8000", token)
    assert s.usage_available is True
    assert token not in repr(s)
    assert token not in str(s)
    for bad in ("ftp://usage", "usage:8000", "http://", "http://usage/?q=1", "http://usage/#f"):
        monkeypatch.setenv("ORCH_USAGE_URL", bad)
        with pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_USAGE_URL" in exc.value.problems, bad
        assert token not in str(exc.value)


def test_usage_url_and_token_come_as_a_pair(monkeypatch):
    token = "usage-" + secrets.token_hex(16)
    monkeypatch.setenv("ORCH_USAGE_URL", "http://usage:8000")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "set ORCH_USAGE_URL and ORCH_USAGE_TOKEN together, or neither" in str(exc.value)
    monkeypatch.delenv("ORCH_USAGE_URL")
    monkeypatch.setenv("ORCH_USAGE_TOKEN", token)
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert "set ORCH_USAGE_URL and ORCH_USAGE_TOKEN together, or neither" in str(exc.value)
    assert token not in str(exc.value)


def test_history_pair(monkeypatch):
    pair = "set ORCH_HISTORY_URL and ORCH_HISTORY_TOKEN together, or neither"
    s = Settings()
    assert (s.history_url, s.history_token) == ("", "")
    assert s.admin_upstream("history") is None
    token = "history-" + secrets.token_hex(16)
    monkeypatch.setenv("ORCH_HISTORY_URL", "http://agents-history:8001")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert pair in str(exc.value)
    monkeypatch.delenv("ORCH_HISTORY_URL")
    monkeypatch.setenv("ORCH_HISTORY_TOKEN", token)
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert pair in str(exc.value)
    assert token not in str(exc.value)
    monkeypatch.setenv("ORCH_HISTORY_URL", "http://agents-history:8001")
    s = Settings()
    assert s.admin_upstream("history") == ("http://agents-history:8001", token)
    assert token not in repr(s)
    for bad in ("ftp://history", "history:8001", "http://", "http://history/?q=1", "http://h/#f"):
        monkeypatch.setenv("ORCH_HISTORY_URL", bad)
        with pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_HISTORY_URL" in exc.value.problems, bad
        assert token not in str(exc.value)


def test_agent_usage_settings(monkeypatch):
    s = Settings()
    assert (s.agent_usage_url, s.agent_usage_secret) == ("", "")
    assert s.agent_usage_enabled is False
    monkeypatch.setenv("ORCH_AGENT_USAGE_URL", "http://agents-usage:8002")
    monkeypatch.setenv("ORCH_AGENT_USAGE_SECRET", "agents-usage-ingest")
    s = Settings()
    assert (s.agent_usage_url, s.agent_usage_secret) == (
        "http://agents-usage:8002",
        "agents-usage-ingest",
    )
    assert s.agent_usage_enabled is True


def test_agent_usage_url_and_secret_come_as_a_pair(monkeypatch):
    pair = "set ORCH_AGENT_USAGE_URL and ORCH_AGENT_USAGE_SECRET together, or neither"
    monkeypatch.setenv("ORCH_AGENT_USAGE_URL", "http://agents-usage:8002")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert pair in str(exc.value)
    monkeypatch.delenv("ORCH_AGENT_USAGE_URL")
    monkeypatch.setenv("ORCH_AGENT_USAGE_SECRET", "agents-usage-ingest")
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert pair in str(exc.value)


def test_agent_usage_bad_url_refused(monkeypatch):
    monkeypatch.setenv("ORCH_AGENT_USAGE_SECRET", "agents-usage-ingest")
    # the wrapper's usage.url check also refuses a trailing slash and a missing host
    for bad in (
        "ftp://usage",
        "usage:8002",
        "http://",
        "http://:8002",
        "http://usage/?q=1",
        "http://usage/#f",
        "http://usage:8002/",
        "http://usage:8002/base/",
    ):
        monkeypatch.setenv("ORCH_AGENT_USAGE_URL", bad)
        with pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_AGENT_USAGE_URL" in exc.value.problems, bad


def test_agent_usage_bad_secret_name_refused(monkeypatch):
    monkeypatch.setenv("ORCH_AGENT_USAGE_URL", "http://agents-usage:8002")
    for bad in ("Agents_Usage", "-lead", "trail-", "a b", "a..b", "x" * 254):
        monkeypatch.setenv("ORCH_AGENT_USAGE_SECRET", bad)
        with pytest.raises(SettingsError) as exc:
            Settings()
        assert "ORCH_AGENT_USAGE_SECRET" in exc.value.problems, bad
    monkeypatch.setenv("ORCH_AGENT_USAGE_SECRET", "agents.usage-ingest")
    assert Settings().agent_usage_secret == "agents.usage-ingest"


@pytest.mark.parametrize(
    ("name", "bad"),
    [
        ("ORCH_USAGE_QUOTA_DEFAULT", "0"),
        ("ORCH_USAGE_QUOTA_DEFAULT", "101"),
        ("ORCH_USAGE_CACHE_SECONDS", "0"),
        ("ORCH_USAGE_CACHE_SECONDS", "601"),
        ("ORCH_USAGE_TIMEOUT_SECONDS", "0"),
        ("ORCH_USAGE_TIMEOUT_SECONDS", "31"),
    ],
)
def test_usage_numeric_bounds(monkeypatch, name, bad):
    monkeypatch.setenv(name, bad)
    with pytest.raises(SettingsError) as exc:
        Settings()
    assert name in exc.value.problems


def test_usage_numeric_overrides(monkeypatch):
    monkeypatch.setenv("ORCH_USAGE_QUOTA_DEFAULT", "90")
    monkeypatch.setenv("ORCH_USAGE_CACHE_SECONDS", "60")
    monkeypatch.setenv("ORCH_USAGE_TIMEOUT_SECONDS", "1.5")
    s = Settings()
    assert (s.usage_quota_default, s.usage_cache_seconds, s.usage_timeout_seconds) == (90, 60, 1.5)


def test_priority_send_timeout_covers_the_wrapper_interrupt_budget():
    from agents_orchestrator.settings import PRIORITY_HOLD_SLACK_SECONDS

    s = Settings(database_url="postgresql://x", master_key="k" * 32)
    assert s.wrapper_interrupt_settle_seconds == 20
    # two interrupts (each up to one settle wait) and the flushed-queue wait in between
    assert s.priority_send_timeout_seconds == 15 + 3 * 20 + PRIORITY_HOLD_SLACK_SECONDS
    s = Settings(
        database_url="postgresql://x",
        master_key="k" * 32,
        wrapper_send_timeout_seconds=5,
        wrapper_interrupt_settle_seconds=2,
    )
    assert s.priority_send_timeout_seconds == 5 + 3 * 2 + PRIORITY_HOLD_SLACK_SECONDS
    with pytest.raises(SettingsError):
        Settings(
            database_url="postgresql://x", master_key="k" * 32, wrapper_interrupt_settle_seconds=-1
        )


def test_close_grace_default_and_bounds(monkeypatch):
    assert Settings().close_grace_seconds == 30
    monkeypatch.setenv("ORCH_CLOSE_GRACE_SECONDS", "10")
    assert Settings().close_grace_seconds == 10
    monkeypatch.setenv("ORCH_CLOSE_GRACE_SECONDS", "3600")
    assert Settings().close_grace_seconds == 3600
    for bad in ("-1", "0", "9", "3601"):
        monkeypatch.setenv("ORCH_CLOSE_GRACE_SECONDS", bad)
        with pytest.raises(SettingsError, match="ORCH_CLOSE_GRACE_SECONDS"):
            Settings()


def test_inactivity_defaults_off():
    s = Settings()
    assert (s.inactivity_reminder_seconds, s.inactivity_close_seconds) == (0, 0)
    assert s.inactivity_handoff_seconds == 300


def test_inactivity_reminder_must_be_below_close(monkeypatch):
    def load(reminder: str, close: str) -> Settings:
        monkeypatch.setenv("ORCH_INACTIVITY_REMINDER_SECONDS", reminder)
        monkeypatch.setenv("ORCH_INACTIVITY_CLOSE_SECONDS", close)
        return Settings()

    for bad in (("3600", "3600"), ("7200", "3600")):
        with pytest.raises(SettingsError, match="ORCH_INACTIVITY_REMINDER_SECONDS"):
            load(*bad)
    for ok in (("3600", "14400"), ("3600", "0"), ("0", "600"), ("0", "0")):
        s = load(*ok)
        assert (s.inactivity_reminder_seconds, s.inactivity_close_seconds) == tuple(map(int, ok))


def test_inactivity_bounds(monkeypatch):
    for var in ("REMINDER", "CLOSE"):
        name = f"ORCH_INACTIVITY_{var}_SECONDS"
        for bad in ("59", "2592001", "-1"):
            monkeypatch.setenv(name, bad)
            with pytest.raises(SettingsError, match=name):
                Settings()
        for ok in ("0", "60", "2592000"):
            monkeypatch.setenv(name, ok)
            assert getattr(Settings(), name.removeprefix("ORCH_").lower()) == int(ok)
        monkeypatch.delenv(name)
    name = "ORCH_INACTIVITY_HANDOFF_SECONDS"
    for bad in ("-1", "3601"):
        monkeypatch.setenv(name, bad)
        with pytest.raises(SettingsError, match=name):
            Settings()
    for ok in ("0", "3600"):
        monkeypatch.setenv(name, ok)
        assert Settings().inactivity_handoff_seconds == int(ok)


def test_session_reply_window(monkeypatch):
    assert Settings().session_reply_window_seconds == 86400
    monkeypatch.setenv("ORCH_SESSION_REPLY_WINDOW_SECONDS", "0")
    assert Settings().session_reply_window_seconds == 0
    monkeypatch.setenv("ORCH_SESSION_REPLY_WINDOW_SECONDS", "-1")
    with pytest.raises(SettingsError, match="ORCH_SESSION_REPLY_WINDOW_SECONDS"):
        Settings()


def test_proposal_settings_defaults_and_bounds(monkeypatch):
    s = Settings()
    assert (s.proposal_ttl_seconds, s.proposals_open_max, s.proposal_sweep_seconds) == (
        604800,
        20,
        60,
    )
    cases = {
        "ORCH_PROPOSAL_TTL_SECONDS": ("3600", ("3599", "60", "0")),
        "ORCH_PROPOSALS_OPEN_MAX": ("1", ("0", "-1")),
        "ORCH_PROPOSAL_SWEEP_SECONDS": ("10", ("9", "1", "0")),
    }
    for name, (ok, bads) in cases.items():
        for bad in bads:
            monkeypatch.setenv(name, bad)
            with pytest.raises(SettingsError, match=name):
                Settings()
        monkeypatch.setenv(name, ok)
        Settings()


def test_handover_transcript_defaults_and_bounds(monkeypatch):
    s = Settings()
    assert (s.handover_transcript_bytes, s.handover_output_chars) == (65536, 500)
    cases = {
        "ORCH_HANDOVER_TRANSCRIPT_BYTES": (("8192", "1048576"), ("8191", "1048577", "-1")),
        "ORCH_HANDOVER_OUTPUT_CHARS": (("100", "4000"), ("99", "4001", "0")),
    }
    for name, (oks, bads) in cases.items():
        for bad in bads:
            monkeypatch.setenv(name, bad)
            with pytest.raises(SettingsError, match=name):
                Settings()
        for ok in oks:
            monkeypatch.setenv(name, ok)
            assert getattr(Settings(), name.removeprefix("ORCH_").lower()) == int(ok)
        monkeypatch.delenv(name)


def test_rollout_cap_must_exceed_window(monkeypatch):
    s = Settings()
    assert (s.rollout_handover_seconds, s.rollout_max_defer_seconds) == (1800, 14400)
    cases = {
        "ORCH_ROLLOUT_HANDOVER_SECONDS": (("0", "1800"), ("-1", "86401")),
        "ORCH_ROLLOUT_MAX_DEFER_SECONDS": (("600", "86400"), ("599", "86401")),
    }
    for name, (oks, bads) in cases.items():
        for bad in bads:
            monkeypatch.setenv(name, bad)
            with pytest.raises(SettingsError, match=name):
                Settings()
        for ok in oks:
            monkeypatch.setenv(name, ok)
            assert getattr(Settings(), name.removeprefix("ORCH_").lower()) == int(ok)
        monkeypatch.delenv(name)
        monkeypatch.setenv("ORCH_ROLLOUT_HANDOVER_SECONDS", "0")  # the cap's floor is 600
    # the cap must exceed the window: a deferral never ends before the forced handover is due
    monkeypatch.setenv("ORCH_ROLLOUT_HANDOVER_SECONDS", "3600")
    for bad in ("600", "3600"):
        monkeypatch.setenv("ORCH_ROLLOUT_MAX_DEFER_SECONDS", bad)
        with pytest.raises(SettingsError, match="ORCH_ROLLOUT_MAX_DEFER_SECONDS"):
            Settings()
    monkeypatch.setenv("ORCH_ROLLOUT_MAX_DEFER_SECONDS", "3601")
    assert Settings().rollout_max_defer_seconds == 3601
