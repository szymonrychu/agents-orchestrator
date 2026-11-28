"""Workspace cache configuration: settings fields and the spec field (design D6, D7, D8)."""

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings
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


def test_api_secret_not_in_repr():
    s = _settings(
        cache_s3_api_access_key_id="id", cache_s3_api_secret_access_key="TOPSECRETAPIVALUE"
    )
    assert "TOPSECRETAPIVALUE" not in repr(s)
