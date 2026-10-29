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
    assert s.any_mcp_plugins_ref == "v0.5.1"
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
