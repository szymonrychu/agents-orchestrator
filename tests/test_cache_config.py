"""Workspace cache configuration: settings fields and the spec field (design D6, D7, D8)."""

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.spec import (
    CACHE_ALWAYS_EXCLUDE,
    CACHE_EXCLUDE_MAX,
    INIT_DIR,
    SessionSpec,
    SpecInvalid,
    WorkspaceCache,
    validate_spec,
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


def test_workspace_cache_validation():
    assert CACHE_EXCLUDE_MAX == 50
    assert CACHE_ALWAYS_EXCLUDE == (INIT_DIR,)
    assert WorkspaceCache().restore_key is None
    assert WorkspaceCache().save_key is None
    assert WorkspaceCache().exclude == []
    WorkspaceCache(restore_key="a", exclude=["node_modules", "build/*.o"])
    WorkspaceCache(restore_key="a" + "b" * 62, save_key="p/q")
    WorkspaceCache(restore_key="", save_key="")  # fresh start, restore-only
    WorkspaceCache(restore_key="proj/", save_key="proj/{{ now }}")  # templates render later
    for bad in ("Bad Key", "-a", ".a", "a//b", "a" * 64, "sessions/x", "a/b/c/d/e", "a" * 257):
        with pytest.raises(ValidationError):
            WorkspaceCache(restore_key=bad)
        with pytest.raises(ValidationError):
            WorkspaceCache(save_key=bad)
    with pytest.raises(ValidationError):
        WorkspaceCache(save_key="proj/")  # only a restore name may end in '/'
    with pytest.raises(ValidationError):
        WorkspaceCache(key="a")  # the single key is gone
    for bad in ([""], ["/abs"], ["a/../b"], [".."], ["x" * 257], ["x"] * 51):
        with pytest.raises(ValidationError):
            WorkspaceCache(exclude=bad)
    WorkspaceCache(exclude=["x" * 256] * 50)
    with pytest.raises(ValidationError):
        WorkspaceCache(bogus=1)
    with pytest.raises(ValidationError):
        SessionSpec.model_validate({"cache": {"restore_key": "Bad Key"}})


def test_cache_sparse_dump():
    assert "cache" not in SessionSpec().model_dump(mode="json", exclude_none=True)
    assert "cache" not in SessionSpec().model_dump(mode="json", exclude_unset=True)
    spec = SessionSpec.model_validate({"cache": {}})
    assert spec.cache == WorkspaceCache()
    assert spec.model_dump(mode="json", exclude_none=True)["cache"] == {"exclude": []}


def test_validate_spec_cache_unavailable():
    off = _settings()
    on = _settings(cache_s3_bucket="b")
    spec = SessionSpec(cache=WorkspaceCache(restore_key="k", save_key="k"))
    validate_spec(SessionSpec(), off)
    with pytest.raises(SpecInvalid) as e:
        validate_spec(spec, off)
    assert "workspace cache is not configured (ORCH_CACHE_S3_BUCKET is unset)" in e.value.errors
    validate_spec(spec, on)
    validate_spec(
        spec, off, stored=SessionSpec(cache=WorkspaceCache(restore_key="k", save_key="k"))
    )
    with pytest.raises(SpecInvalid):
        validate_spec(
            spec,
            off,
            stored=SessionSpec(cache=WorkspaceCache(restore_key="other", save_key="other")),
        )
    with pytest.raises(SpecInvalid):
        validate_spec(spec, off, stored=SessionSpec())


@pytest.mark.parametrize(
    ("restore", "save", "fragment"),
    [
        ("{{ nope }}", "ok", "cache.restore_key"),
        ("ok", "{% if %}", "cache.save_key"),
        ("{{ user }}", "ok", "cache.restore_key"),  # renders to the sample user: fine
        ("{{ session_slug }}/../x", "ok", "cache.restore_key"),
        ("ok", "{{ date }}", "cache.save_key"),  # 2026-01-01 is a valid segment
        ("ok", "UPPER-{{ now }}", "cache.save_key"),
        ("ok", "{{ 'sessions' }}/{{ now }}", "cache.save_key"),
        ("ok", "{{ preset_name }}/", "cache.save_key"),  # a save name is not a prefix
    ],
)
def test_validate_spec_renders_key_templates(restore, save, fragment):
    spec = SessionSpec(cache=WorkspaceCache(restore_key=restore, save_key=save))
    valid = {("{{ user }}", "ok"), ("ok", "{{ date }}")}
    if (restore, save) in valid:
        validate_spec(spec, _settings(cache_s3_bucket="b"))
        return
    with pytest.raises(SpecInvalid) as e:
        validate_spec(spec, _settings(cache_s3_bucket="b"))
    assert any(m.startswith(fragment) for m in e.value.errors), e.value.errors


def test_validate_spec_accepts_versioned_templates():
    spec = SessionSpec(
        cache=WorkspaceCache(
            restore_key="{{ preset_name }}/", save_key="{{ preset_name }}/{{ now }}"
        )
    )
    validate_spec(spec, _settings(cache_s3_bucket="b"))


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
