import ipaddress

import pytest

from agents_orchestrator.settings import DiscardHook, Settings, SettingsError

pytestmark = pytest.mark.critical

DB = "postgresql://u:p@h/db"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    import os

    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


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


def test_discard_hook_token_hidden_in_repr(monkeypatch):
    monkeypatch.setenv("ORCH_DISCARD_HOOK_URL", "https://history.example/hook")
    monkeypatch.setenv("ORCH_DISCARD_HOOK_TOKEN", "t0k3n")
    settings = Settings()
    hook = settings.discard_hook()
    assert hook is not None and hook.token == "t0k3n"
    assert "t0k3n" not in repr(settings) and "t0k3n" not in str(settings)
    assert "t0k3n" not in repr(hook) and "t0k3n" not in str(hook)


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


def test_allowed_cidrs_parsed_and_bad_entry_rejected(monkeypatch):
    monkeypatch.setenv("ORCH_ALLOWED_CIDRS", "10.0.0.0/8, 192.168.1.0/24")
    assert Settings().allowed_networks() == [
        ipaddress.ip_network("10.0.0.0/8"),
        ipaddress.ip_network("192.168.1.0/24"),
    ]
    monkeypatch.setenv("ORCH_ALLOWED_CIDRS", "10.0.0.0/8,nonsense")
    with pytest.raises(SettingsError, match="ORCH_ALLOWED_CIDRS"):
        Settings()
