"""Workspace cache configuration: settings fields and the spec field (design D6, D7, D8)."""

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.spec import (
    CACHE_ALWAYS_EXCLUDE,
    CACHE_EXCLUDE_MAX,
    INIT_DIR,
    SessionSpec,
)

DB = "postgresql://x/y"


def _settings(monkeypatch=None, **kw):
    kw.setdefault("database_url", DB)
    kw.setdefault("agent_image", "img:1")
    return Settings(**kw)


def test_cache_settings_defaults_and_env(monkeypatch):
    s = _settings()
    assert s.cache_s3_endpoint == ""
    assert s.cache_s3_region == "us-east-1"
    assert s.cache_s3_bucket == ""
    assert s.cache_s3_prefix == "workspace-cache/"
    assert s.cache_s3_path_style is False
    assert s.cache_s3_verify_tls is True
    assert s.cache_s3_access_key_id == ""
    assert s.cache_s3_secret_access_key == ""
    assert s.cache_image == ""
    assert s.cache_max_bytes == 5368709120
    assert s.cache_timeout_seconds == 1800
    assert s.cache_available is False
    for name, value in {
        "ORCH_CACHE_S3_ENDPOINT": "http://rgw:80",
        "ORCH_CACHE_S3_REGION": "eu-1",
        "ORCH_CACHE_S3_BUCKET": "b",
        "ORCH_CACHE_S3_PREFIX": "p/",
        "ORCH_CACHE_S3_PATH_STYLE": "true",
        "ORCH_CACHE_S3_VERIFY_TLS": "false",
        "ORCH_CACHE_S3_ACCESS_KEY_ID": "ak",
        "ORCH_CACHE_S3_SECRET_ACCESS_KEY": "sk",
        "ORCH_CACHE_IMAGE": "cache:1",
        "ORCH_CACHE_MAX_BYTES": "1000",
        "ORCH_CACHE_TIMEOUT_SECONDS": "60",
    }.items():
        monkeypatch.setenv(name, value)
    s = _settings()
    assert s.cache_s3_endpoint == "http://rgw:80"
    assert s.cache_s3_region == "eu-1"
    assert s.cache_s3_bucket == "b"
    assert s.cache_s3_prefix == "p/"
    assert s.cache_s3_path_style is True
    assert s.cache_s3_verify_tls is False
    assert s.cache_s3_access_key_id == "ak"
    assert s.cache_s3_secret_access_key == "sk"
    assert s.cache_image == "cache:1"
    assert s.cache_max_bytes == 1000
    assert s.cache_timeout_seconds == 60
    assert s.cache_available is True


@pytest.mark.parametrize(
    ("kw", "var"),
    [
        ({"cache_max_bytes": 5368709121}, "ORCH_CACHE_MAX_BYTES"),
        ({"cache_max_bytes": 0}, "ORCH_CACHE_MAX_BYTES"),
        ({"cache_s3_endpoint": "ftp://x"}, "ORCH_CACHE_S3_ENDPOINT"),
        ({"cache_s3_endpoint": "rgw:80"}, "ORCH_CACHE_S3_ENDPOINT"),
        ({"cache_timeout_seconds": 59}, "ORCH_CACHE_TIMEOUT_SECONDS"),
        ({"cache_timeout_seconds": 86401}, "ORCH_CACHE_TIMEOUT_SECONDS"),
    ],
)
def test_cache_settings_refused(kw, var):
    with pytest.raises(SettingsError) as e:
        _settings(**kw)
    assert var in e.value.problems
    _settings(
        cache_max_bytes=5368709120, cache_timeout_seconds=86400, cache_s3_endpoint="https://s3"
    )


def test_require_cache_names_missing():
    _settings().require_cache()
    with pytest.raises(SettingsError) as e:
        _settings(cache_s3_bucket="b").require_cache()
    assert set(e.value.problems) == {
        "ORCH_CACHE_S3_ACCESS_KEY_ID",
        "ORCH_CACHE_S3_SECRET_ACCESS_KEY",
        "ORCH_CACHE_IMAGE",
    }
    with pytest.raises(SettingsError) as e:
        _settings(
            cache_s3_bucket="b", cache_s3_access_key_id="a", cache_s3_secret_access_key="s"
        ).require_cache()
    assert set(e.value.problems) == {"ORCH_CACHE_IMAGE"}
    _settings(
        cache_s3_bucket="b",
        cache_s3_access_key_id="a",
        cache_s3_secret_access_key="s",
        cache_image="i",
    ).require_cache()


def test_secret_key_not_in_repr():
    s = _settings(cache_s3_secret_access_key="TOPSECRETVALUE")
    assert "TOPSECRETVALUE" not in repr(s)
    assert "TOPSECRETVALUE" not in str(s)


def test_backup_entries_are_gone_from_the_spec():
    # entry backups became volumes (S-77): the field no longer exists
    assert CACHE_EXCLUDE_MAX == 50
    assert CACHE_ALWAYS_EXCLUDE == (INIT_DIR,)
    entry = {"root": "workspace", "path": ".", "mode": "both", "name": "k"}
    with pytest.raises(ValidationError) as e:
        SessionSpec.model_validate({"backup": [entry]})
    assert e.value.errors()[0]["type"] == "extra_forbidden"
    assert "backup" not in SessionSpec().model_dump(mode="json", exclude_unset=True)


def test_sweep_settings_defaults():
    s = _settings()
    assert s.cache_retention_days == 30
    assert s.cache_keep_newest == 3
    assert s.cache_sweep_interval_seconds == 6 * 3600
    assert s.cache_sweep_grace_seconds == s.cache_timeout_seconds + 600
    assert s.cache_sweep_enabled is True
    assert s.cache_sweep_dry_run is True  # deleting needs an explicit opt-in
    assert s.cache_s3_api_access_key_id == ""
    assert s.cache_s3_api_secret_access_key == ""


def test_sweep_grace_follows_the_cache_timeout_unless_set():
    assert _settings(cache_timeout_seconds=100).cache_sweep_grace_seconds == 700
    assert _settings(cache_sweep_grace_seconds=5).cache_sweep_grace_seconds == 5
    assert _settings(cache_sweep_grace_seconds=0).cache_sweep_grace_seconds == 0


def test_sweep_settings_from_env(monkeypatch):
    for name, value in {
        "ORCH_CACHE_RETENTION_DAYS": "7",
        "ORCH_CACHE_KEEP_NEWEST": "2",
        "ORCH_CACHE_SWEEP_INTERVAL_SECONDS": "120",
        "ORCH_CACHE_SWEEP_GRACE_SECONDS": "30",
        "ORCH_CACHE_SWEEP_ENABLED": "false",
        "ORCH_CACHE_SWEEP_DRY_RUN": "false",
        "ORCH_CACHE_S3_API_ACCESS_KEY_ID": "api-id",
        "ORCH_CACHE_S3_API_SECRET_ACCESS_KEY": "api-secret",
    }.items():
        monkeypatch.setenv(name, value)
    s = _settings()
    assert (s.cache_retention_days, s.cache_keep_newest) == (7, 2)
    assert (s.cache_sweep_interval_seconds, s.cache_sweep_grace_seconds) == (120, 30)
    assert s.cache_sweep_enabled is False
    assert s.cache_sweep_dry_run is False
    assert s.cache_s3_api_access_key_id == "api-id"


@pytest.mark.parametrize(
    ("kw", "var"),
    [
        ({"cache_retention_days": 0}, "ORCH_CACHE_RETENTION_DAYS"),
        ({"cache_keep_newest": 0}, "ORCH_CACHE_KEEP_NEWEST"),
        ({"cache_sweep_interval_seconds": 59}, "ORCH_CACHE_SWEEP_INTERVAL_SECONDS"),
        ({"cache_sweep_grace_seconds": -1}, "ORCH_CACHE_SWEEP_GRACE_SECONDS"),
        ({"cache_retention_days": 3651}, "ORCH_CACHE_RETENTION_DAYS"),
        ({"cache_keep_newest": 1001}, "ORCH_CACHE_KEEP_NEWEST"),
        ({"cache_sweep_interval_seconds": 2592001}, "ORCH_CACHE_SWEEP_INTERVAL_SECONDS"),
        ({"cache_sweep_grace_seconds": 604801}, "ORCH_CACHE_SWEEP_GRACE_SECONDS"),
        ({"cache_s3_api_access_key_id": "only-id"}, "ORCH_CACHE_S3_API_SECRET_ACCESS_KEY"),
        ({"cache_s3_api_secret_access_key": "only-secret"}, "ORCH_CACHE_S3_API_SECRET_ACCESS_KEY"),
    ],
)
def test_sweep_settings_refused(kw, var):
    with pytest.raises(SettingsError) as e:
        _settings(**kw)
    assert var in e.value.problems


def test_api_secret_not_in_repr():
    s = _settings(
        cache_s3_api_access_key_id="id", cache_s3_api_secret_access_key="TOPSECRETAPIVALUE"
    )
    assert "TOPSECRETAPIVALUE" not in repr(s)


def test_s3_role_helpers():
    bare = _settings()
    assert not bare.reconciler_s3_configured and not bare.api_s3_configured
    on = _settings(cache_s3_bucket="b", cache_s3_access_key_id="a", cache_s3_secret_access_key="s")
    assert on.reconciler_s3_configured is True
    assert on.api_s3_configured is False  # the api needs its own pair
    both = _settings(
        cache_s3_bucket="b",
        cache_s3_access_key_id="a",
        cache_s3_secret_access_key="s",
        cache_s3_api_access_key_id="ai",
        cache_s3_api_secret_access_key="as",
    )
    assert both.api_s3_configured is True
    assert both.api_s3_credentials() == ("ai", "as")
    assert both.reconciler_s3_credentials() == ("a", "s")
    assert not _settings(
        cache_s3_api_access_key_id="ai", cache_s3_api_secret_access_key="as"
    ).api_s3_configured  # no bucket
