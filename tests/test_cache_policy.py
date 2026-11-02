"""SweepPolicy: the env seed and its dict form (no database)."""

from agents_orchestrator.cache.policy import SweepPolicy
from agents_orchestrator.settings import Settings


def _settings(**kw):
    kw.setdefault("database_url", "postgresql://x/y")
    kw.setdefault("agent_image", "img:1")
    return Settings(**kw)


def test_from_settings_is_the_env_seed():
    s = _settings(
        cache_retention_days=7,
        cache_keep_newest=2,
        cache_sweep_interval_seconds=120,
        cache_sweep_enabled=False,
        cache_sweep_dry_run=False,
    )
    assert SweepPolicy.from_settings(s) == SweepPolicy(
        False, False, 7, 2, 120, s.cache_timeout_seconds + 600
    )


def test_from_settings_uses_an_explicit_grace():
    s = _settings(cache_sweep_grace_seconds=5)
    assert SweepPolicy.from_settings(s).grace_seconds == 5


def test_to_dict_names():
    policy = SweepPolicy(True, True, 30, 3, 21600, 2400)
    assert policy.to_dict() == {
        "enabled": True,
        "dry_run": True,
        "retention_days": 30,
        "keep_newest": 3,
        "interval_seconds": 21600,
        "grace_seconds": 2400,
    }
