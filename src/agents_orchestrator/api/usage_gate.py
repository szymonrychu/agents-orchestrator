"""The hard usage gate: where the account's usage stands (`UsageSource`) and whether a start
or scale-up must be refused (`breach`).

The usage service (agents-usage) answers `GET /usage/v1/current`. Missing, stale or reset data
never gates: an unreachable service, a timeout, a bad answer, an absent or stale window all
mean "allow", with one warning per cache period.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx
from pydantic import BaseModel, ConfigDict, StrictBool, StrictFloat, StrictInt, ValidationError

from agents_orchestrator.spec import UsageQuota

log = logging.getLogger(__name__)

#: The only windows that gate; every other window is shown by the usage page and never gates.
GATED_METRICS = ("five_hour", "seven_day")

CURRENT_PATH = "/usage/v1/current"


@dataclass(frozen=True)
class UsageWindow:
    percent: float
    resets_at: datetime
    stale: bool


@dataclass(frozen=True)
class Breach:
    metric: str
    percent: float
    limit: int
    resets_at: datetime


def breach(
    windows: dict[str, UsageWindow] | None,
    quota: UsageQuota | None,
    default_pct: int,
    now: datetime,
) -> Breach | None:
    """The breach to report, or None when a start may proceed.

    A metric gates when its window exists, is not stale, has not reset yet and
    `percent >= limit` (`>=`, so the default 100 gates at the real limit). The limit is the
    quota's value for that metric, else `default_pct`. With several breaches the one with the
    latest `resets_at` is reported: that is when a start can succeed.
    """
    if not windows:
        return None
    found: list[Breach] = []
    for metric in GATED_METRICS:
        window = windows.get(metric)
        if window is None or window.stale or window.resets_at <= now:
            continue
        limit = getattr(quota, f"{metric}_pct", None) if quota is not None else None
        limit = default_pct if limit is None else limit
        if window.percent >= limit:
            found.append(Breach(metric, window.percent, limit, window.resets_at))
    return max(found, key=lambda b: b.resets_at, default=None)


class _Window(BaseModel):
    model_config = ConfigDict(extra="ignore")

    percent: StrictFloat | StrictInt
    resets_at: datetime
    stale: StrictBool


class _Current(BaseModel):
    model_config = ConfigDict(extra="ignore")

    windows: dict[str, _Window]


def _parse(body: bytes) -> dict[str, UsageWindow]:
    current = _Current.model_validate_json(body)
    return {
        metric: UsageWindow(
            float(w.percent),
            w.resets_at if w.resets_at.tzinfo else w.resets_at.replace(tzinfo=UTC),
            w.stale,
        )
        for metric, w in current.windows.items()
    }


class UsageSource:
    """Reads the usage windows from agents-usage, caching the answer, and a failure just as
    long, for `cache_seconds`. One refresh at a time: a burst at expiry sends one request."""

    def __init__(
        self,
        url: str,
        token: str,
        cache_seconds: float,
        timeout: float,
        transport: httpx.AsyncBaseTransport | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._url = url.rstrip("/") + CURRENT_PATH
        self._timeout = timeout
        self._cache_seconds = cache_seconds
        self._clock = clock
        self._client = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {token}"}, timeout=timeout, transport=transport
        )
        self._lock = asyncio.Lock()
        self._fetched_at: float | None = None
        self._windows: dict[str, UsageWindow] | None = None

    def _fresh(self) -> bool:
        return (
            self._fetched_at is not None and self._clock() - self._fetched_at < self._cache_seconds
        )

    async def current(self) -> dict[str, UsageWindow] | None:
        """The windows by metric, or None when the usage service has no usable answer."""
        if self._fresh():
            return self._windows
        async with self._lock:
            if not self._fresh():
                self._windows = await self._fetch()
                self._fetched_at = self._clock()
            return self._windows

    async def _fetch(self) -> dict[str, UsageWindow] | None:
        try:
            async with asyncio.timeout(self._timeout):
                response = await self._client.get(self._url)
            response.raise_for_status()
            return _parse(response.content)
        except (httpx.HTTPError, TimeoutError, ValidationError) as exc:
            # Never the exception text: an httpx message can carry the request URL.
            log.warning(
                "usage unavailable, the gate allows for %ss: %s",
                self._cache_seconds,
                type(exc).__name__,
            )
            return None

    async def aclose(self) -> None:
        await self._client.aclose()
