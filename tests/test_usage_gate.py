"""The usage gate's pure breach rule and the cached usage source (no database)."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

import httpx
import pytest

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.usage_gate import (
    GATED_METRICS,
    Breach,
    UsageSource,
    UsageWindow,
    breach,
    check,
    enforce,
    refusal,
)
from agents_orchestrator.spec import UsageQuota

NOW = datetime(2026, 11, 1, 12, tzinfo=UTC)


def W(p, stale=False, hours=24):
    return UsageWindow(p, NOW + timedelta(hours=hours), stale)


def test_gated_metrics():
    assert GATED_METRICS == ("five_hour", "seven_day")


def test_default_100_gates_at_100():
    assert breach({"seven_day": W(100.0)}, None, 100, NOW) == Breach(
        "seven_day", 100.0, 100, NOW + timedelta(hours=24)
    )


def test_over_100_gates():
    assert breach({"seven_day": W(103.0)}, None, 100, NOW) == Breach(
        "seven_day", 103.0, 100, NOW + timedelta(hours=24)
    )


def test_below_quota_passes():
    assert breach({"seven_day": W(79.99)}, UsageQuota(seven_day_pct=80), 100, NOW) is None


def test_at_quota_gates():
    got = breach({"seven_day": W(80.0)}, UsageQuota(seven_day_pct=80), 100, NOW)
    assert got == Breach("seven_day", 80.0, 80, NOW + timedelta(hours=24))


def test_default_applies_without_quota_for_that_metric():
    quota = UsageQuota(five_hour_pct=50)
    assert breach({"seven_day": W(99.0)}, quota, 100, NOW) is None
    assert breach({"seven_day": W(99.0)}, quota, 90, NOW).limit == 90


def test_quota_per_metric():
    quota = UsageQuota(five_hour_pct=50)
    windows = {"five_hour": W(60.0, hours=3), "seven_day": W(99.0)}
    got = breach(windows, quota, 100, NOW)
    assert got == Breach("five_hour", 60.0, 50, NOW + timedelta(hours=3))


def test_stale_never_gates():
    assert (
        breach({"seven_day": W(99.0, stale=True)}, UsageQuota(seven_day_pct=10), 100, NOW) is None
    )


def test_reset_window_never_gates():
    assert breach({"seven_day": W(99.0, hours=0)}, UsageQuota(seven_day_pct=10), 100, NOW) is None
    assert breach({"seven_day": W(99.0, hours=-1)}, UsageQuota(seven_day_pct=10), 100, NOW) is None


def test_absent_and_unavailable():
    quota = UsageQuota(seven_day_pct=10)
    assert breach({}, quota, 100, NOW) is None
    assert breach(None, quota, 100, NOW) is None


def test_ungated_metric_ignored():
    assert breach({"seven_day_opus": W(100.0)}, None, 100, NOW) is None


def test_latest_reset_reported():
    windows = {"five_hour": W(100.0, hours=2), "seven_day": W(100.0, hours=48)}
    got = breach(windows, None, 100, NOW)
    assert got.metric == "seven_day"
    assert got.resets_at == NOW + timedelta(hours=48)
    windows = {"five_hour": W(100.0, hours=60), "seven_day": W(100.0, hours=48)}
    assert breach(windows, None, 100, NOW).metric == "five_hour"


# --- UsageSource ---------------------------------------------------------------------------

RESETS = "2026-11-02T12:00:00+00:00"
OBSERVED = "2026-11-01T11:58:00+00:00"
BODY = {
    "windows": {
        "seven_day": {"percent": 22.0, "resets_at": RESETS, "observed_at": OBSERVED, "stale": False}
    },
    "last_poll": {"at": OBSERVED, "ok": True, "error": None},
}


class Clock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now


def source(handler, clock=None, timeout=3.0):
    return UsageSource(
        "http://usage:8000",
        "t",
        cache_seconds=30,
        timeout=timeout,
        transport=httpx.MockTransport(handler),
        clock=clock or Clock(),
    )


class Counting:
    def __init__(self, response):
        self.response = response
        self.requests: list[httpx.Request] = []

    def __call__(self, request):
        self.requests.append(request)
        return self.response(request) if callable(self.response) else self.response


async def test_source_parses_and_sends_bearer():
    def handler(request):
        assert request.headers["Authorization"] == "Bearer t"
        assert request.url.path == "/usage/v1/current"
        return httpx.Response(200, json=BODY)

    src = source(handler)
    assert await src.current() == {
        "seven_day": UsageWindow(22.0, datetime(2026, 11, 2, 12, tzinfo=UTC), False)
    }
    await src.aclose()


async def test_source_keeps_the_base_url_prefix():
    seen = []

    def handler(request):
        seen.append(str(request.url))
        return httpx.Response(200, json=BODY)

    src = UsageSource(
        "http://usage:8000/prefix/",
        "t",
        cache_seconds=30,
        timeout=3,
        transport=httpx.MockTransport(handler),
    )
    assert await src.current() is not None
    assert seen == ["http://usage:8000/prefix/usage/v1/current"]
    await src.aclose()


async def test_source_empty_windows_is_available_but_empty():
    src = source(Counting(httpx.Response(200, json={"windows": {}, "last_poll": None})))
    assert await src.current() == {}


async def test_source_caches_30s():
    clock = Clock()
    handler = Counting(httpx.Response(200, json=BODY))
    src = source(handler, clock)
    await src.current()
    clock.now = 29
    await src.current()
    assert len(handler.requests) == 1
    clock.now = 31
    await src.current()
    assert len(handler.requests) == 2


async def test_source_failure_cached(caplog):
    clock = Clock()
    handler = Counting(httpx.Response(500, text="boom"))
    src = source(handler, clock)
    with caplog.at_level(logging.WARNING, logger="agents_orchestrator.api.usage_gate"):
        assert await src.current() is None
        clock.now = 10
        assert await src.current() is None
    assert len(handler.requests) == 1
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "usage unavailable" in warnings[0].getMessage()
    clock.now = 31
    assert await src.current() is None
    assert len(handler.requests) == 2


async def test_source_recovers_after_cached_failure():
    clock = Clock()
    answers = [httpx.Response(503), httpx.Response(200, json=BODY)]
    src = source(Counting(lambda request: answers.pop(0)), clock)
    assert await src.current() is None
    clock.now = 31
    assert (await src.current())["seven_day"].percent == 22.0


async def test_source_timeout_is_unavailable():
    async def handler(request):
        await asyncio.sleep(5)
        return httpx.Response(200, json=BODY)

    src = source(handler, timeout=0.05)
    started = asyncio.get_running_loop().time()
    assert await src.current() is None
    assert asyncio.get_running_loop().time() - started < 2


async def test_source_transport_error_is_unavailable():
    def handler(request):
        raise httpx.ConnectError("refused", request=request)

    assert await source(handler).current() is None


async def test_source_read_timeout_is_unavailable():
    def handler(request):
        raise httpx.ReadTimeout("slow", request=request)

    assert await source(handler).current() is None


@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(200, text="not json"),
        httpx.Response(200, json={"windows": {"x": {"percent": "a"}}}),
        httpx.Response(200, json={"windows": []}),
        httpx.Response(200, json=[]),
        httpx.Response(200, json={"last_poll": None}),
        httpx.Response(
            200,
            json={"windows": {"seven_day": {"percent": 1, "resets_at": "soon", "stale": False}}},
        ),
        httpx.Response(
            200,
            json={"windows": {"seven_day": {"percent": True, "resets_at": RESETS, "stale": False}}},
        ),
    ],
)
async def test_source_bad_body_is_unavailable(response):
    assert await source(lambda request: response).current() is None


async def test_source_single_flight_at_expiry():
    clock = Clock()

    async def handler(request):
        await asyncio.sleep(0.05)
        return httpx.Response(200, json=BODY)

    seen = Counting(handler)
    src = source(seen, clock)
    results = await asyncio.gather(*(src.current() for _ in range(10)))
    assert len(seen.requests) == 1
    assert all(r == results[0] and r is not None for r in results)
    clock.now = 31
    await asyncio.gather(*(src.current() for _ in range(10)))
    assert len(seen.requests) == 2


async def test_source_never_logs_the_token(caplog):
    src = UsageSource(
        "http://usage:8000",
        "secret-token-value",
        cache_seconds=30,
        timeout=3,
        transport=httpx.MockTransport(lambda request: httpx.Response(500)),
    )
    with caplog.at_level(logging.DEBUG):
        await src.current()
    assert "secret-token-value" not in caplog.text
    assert "secret-token-value" not in repr(src)


def test_refusal_body():
    resets = NOW + timedelta(hours=2, seconds=0.5)
    err = refusal(Breach("seven_day", 85.0, 80, resets), NOW)
    assert (err.status, err.code, err.retry_after) == (409, "usage_quota", 7201)
    assert err.message == (
        f"seven_day usage 85% is at or above the quota 80% until {resets.isoformat()}"
    )
    assert err.extra == {
        "usage": {
            "metric": "seven_day",
            "percent": 85.0,
            "limit": 80,
            "resets_at": resets.isoformat(),
        }
    }


def test_refusal_retry_after_at_least_one():
    assert refusal(Breach("five_hour", 100.0, 100, NOW), NOW).retry_after == 1
    assert refusal(Breach("five_hour", 100.0, 100, NOW - timedelta(hours=1)), NOW).retry_after == 1


def test_check_raises_the_refusal_or_passes():
    quota = UsageQuota(seven_day_pct=80)
    with pytest.raises(ApiError) as caught:
        check({"seven_day": W(85.0)}, quota, 100, NOW)
    assert caught.value.code == "usage_quota"
    assert caught.value.extra["usage"]["limit"] == 80
    check({"seven_day": W(79.0)}, quota, 100, NOW)
    check(None, quota, 100, NOW)


def _current(percent):
    resets = (datetime.now(UTC) + timedelta(days=1)).isoformat()
    window = {"percent": percent, "resets_at": resets, "stale": False}
    return httpx.Response(200, json={"windows": {"seven_day": window}})


async def test_enforce_fetches_and_raises():
    src = source(lambda request: _current(90.0))
    with pytest.raises(ApiError) as caught:
        await enforce(src, UsageQuota(seven_day_pct=80), 100)
    assert caught.value.code == "usage_quota"
    await enforce(src, None, 100)
    await src.aclose()


async def test_enforce_without_source_allows():
    await enforce(None, UsageQuota(seven_day_pct=1), 1)


async def test_enforce_unavailable_allows():
    await enforce(source(lambda request: httpx.Response(503)), UsageQuota(seven_day_pct=1), 1)
