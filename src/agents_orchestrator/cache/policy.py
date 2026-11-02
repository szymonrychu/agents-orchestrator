"""The archive sweep policy: the six knobs the sweeper and the settings API share."""

from __future__ import annotations

from dataclasses import dataclass

from agents_orchestrator.settings import Settings


@dataclass(frozen=True)
class SweepPolicy:
    enabled: bool
    dry_run: bool
    retention_days: int
    keep_newest: int
    interval_seconds: int
    grace_seconds: int

    @classmethod
    def from_settings(cls, settings: Settings) -> SweepPolicy:
        """The ORCH_CACHE_* values: what applies until a policy is saved in the database."""
        grace = settings.cache_sweep_grace_seconds
        assert grace is not None  # Settings._resolve fills the default
        return cls(
            enabled=settings.cache_sweep_enabled,
            dry_run=settings.cache_sweep_dry_run,
            retention_days=settings.cache_retention_days,
            keep_newest=settings.cache_keep_newest,
            interval_seconds=settings.cache_sweep_interval_seconds,
            grace_seconds=grace,
        )

    def to_dict(self) -> dict[str, bool | int]:
        return {
            "enabled": self.enabled,
            "dry_run": self.dry_run,
            "retention_days": self.retention_days,
            "keep_newest": self.keep_newest,
            "interval_seconds": self.interval_seconds,
            "grace_seconds": self.grace_seconds,
        }
