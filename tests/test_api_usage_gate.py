"""The hard usage gate on every session create path and every scale-up (409 `usage_quota`).

Running sessions, restarts and scale-down are never gated; missing, stale or unavailable usage
allows. The usage service is an httpx.MockTransport behind the app's own `UsageSource`.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta

import httpx
import pytest_asyncio
from factories import attach_preset, insert_session
from test_agent_scale import hive as scale_hive
from test_agent_scale import replicas_of
from test_agent_session_messaging import alone
from test_agent_session_messaging import hive as admin_hive
from test_api_agent import IP1, headers
from test_api_agent import client_for as agent_client
from test_api_agent import make_settings as agent_settings
from test_api_sessions import assert_error, client_for, make_settings
from test_service_port import DEFAULT_ID, TOKENS
from test_service_port import client as service_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings

URL = "http://agents-usage:8000"
C = "/api/v1/sessions"
P = "/api/v1/presets"
RESETS = datetime.now(UTC).replace(microsecond=0) + timedelta(days=2)


def windows(seven_day: float | None = None, five_hour: float | None = None, stale=False) -> dict:
    out = {}
    for metric, percent in (("seven_day", seven_day), ("five_hour", five_hour)):
        if percent is not None:
            out[metric] = {
                "percent": percent,
                "resets_at": RESETS.isoformat(),
                "observed_at": datetime.now(UTC).isoformat(),
                "stale": stale,
                "source": "statusline",
            }
    return {"windows": out, "last_sample": None, "last_probe": None}


class Upstream:
    """The usage service: answers `body`, or `status`, and counts the calls."""

    def __init__(self, body: dict | None = None, status: int = 200, delay: float = 0):
        self.body = body or windows()
        self.status = status
        self.delay = delay
        self.calls = 0

    async def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        assert request.url.path == "/usage/v1/current"
        if self.delay:
            await asyncio.sleep(self.delay)
        if self.status != 200:
            return httpx.Response(self.status, text="boom")
        return httpx.Response(200, json=self.body)


_APPS: list = []


@pytest_asyncio.fixture(autouse=True)
async def close_usage_sources():
    """ASGITransport does not run the app lifespan, so close each app's usage client here."""
    yield
    apps, _APPS[:] = list(_APPS), []
    for app in apps:
        if app.state.usage is not None:
            await app.state.usage.aclose()


def gated(settings: Settings, pool, upstream: Upstream, configured: bool = True, **kw):
    if configured:
        settings = settings.model_copy(update={"usage_url": URL, "usage_token": "t"})
    app = create_app(settings, pool, usage_transport=httpx.MockTransport(upstream), **kw)
    _APPS.append(app)
    return app


async def session_count(pool) -> int:
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM sessions")
        return (await cur.fetchone())[0]


async def make_preset(http, name, spec) -> dict:
    r = await http.post(P, json={"name": name, "spec": spec})
    assert r.status_code == 201, r.text
    return r.json()


def assert_refused(r, metric: str, percent: float, limit: int) -> None:
    assert_error(r, 409, "usage_quota")
    err = r.json()["error"]
    assert err["usage"] == {
        "metric": metric,
        "percent": percent,
        "limit": limit,
        "resets_at": RESETS.isoformat(),
    }
    assert isinstance(err["retry_after"], int) and err["retry_after"] >= 1
    assert r.headers["Retry-After"] == str(err["retry_after"])
    assert metric in err["message"] and f"{limit}%" in err["message"]


async def test_preset_over_quota_refused(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=85.0)))
    async with client_for(app) as http:
        preset = await make_preset(http, "capped", {"usage_quota": {"seven_day_pct": 80}})
        before = await session_count(pool)
        r = await http.post(C, json={"preset_id": preset["id"], "slug": "over"})
    assert_refused(r, "seven_day", 85.0, 80)
    assert await session_count(pool) == before
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT 1 FROM sessions WHERE slug = 'over'")
        assert await cur.fetchone() is None


async def test_preset_under_quota_created(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=85.0)))
    async with client_for(app) as http:
        preset = await make_preset(http, "roomy", {"usage_quota": {"seven_day_pct": 90}})
        r = await http.post(C, json={"preset_id": preset["id"], "slug": "under"})
    assert r.status_code == 201, r.text
    assert r.json()["spec"]["usage_quota"]["seven_day_pct"] == 90
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT spec->'usage_quota'->>'seven_day_pct' FROM sessions WHERE slug = 'under'"
        )
        assert (await cur.fetchone())[0] == "90"


async def test_default_quota_applies(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=100.0)))
    async with client_for(app) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "full"})
    assert_refused(r, "seven_day", 100.0, 100)
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=99.9)))
    async with client_for(app) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "nearly"})
    assert r.status_code == 201, r.text


async def test_raw_spec_quota(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(five_hour=60.0)))
    async with client_for(app) as http:
        r = await http.post(C, json={"slug": "raw", "spec": {"usage_quota": {"five_hour_pct": 50}}})
    assert_refused(r, "five_hour", 60.0, 50)
    assert await session_count(pool) == 0


async def test_unavailable_allows(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(status=500))
    async with client_for(app) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "down"})
    assert r.status_code == 201, r.text
    slow = Upstream(windows(seven_day=100.0), delay=30)
    app = gated(make_settings(dsn), pool, slow)
    started = asyncio.get_running_loop().time()
    async with client_for(app) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "slow"})
    assert r.status_code == 201, r.text
    assert asyncio.get_running_loop().time() - started < 5
    assert slow.calls == 1


async def test_stale_allows(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=99.0, stale=True)))
    async with client_for(app) as http:
        r = await http.post(
            C, json={"slug": "stale", "spec": {"usage_quota": {"seven_day_pct": 10}}}
        )
    assert r.status_code == 201, r.text


async def test_unconfigured_allows(pool, dsn):
    upstream = Upstream(windows(seven_day=100.0))
    app = gated(make_settings(dsn), pool, upstream, configured=False)
    assert app.state.usage is None
    async with client_for(app) as http:
        r = await http.post(C, json={"preset_id": DEFAULT_ID, "slug": "off"})
    assert r.status_code == 201, r.text
    assert upstream.calls == 0


async def test_service_port_gated(pool, dsn):
    settings = make_settings(dsn, service_tokens=TOKENS)
    app = gated(settings, pool, Upstream(windows(seven_day=100.0)))
    async with service_client(app, settings.service_port) as c:
        r = await c.post(C, json={"preset_id": DEFAULT_ID, "slug": "wh-1"})
    assert_refused(r, "seven_day", 100.0, 100)
    assert await session_count(pool) == 0


async def test_admin_route_gated(pool, dsn):
    boss = await alone(pool, "adm-boss", superleader=True)
    app = gated(agent_settings(dsn), pool, Upstream(windows(seven_day=85.0)))
    async with agent_client(app, boss.leader_ip) as http:
        r = await http.post(
            f"{ADMIN_PREFIX}/presets",
            json={"name": "child", "spec": {"usage_quota": {"seven_day_pct": 80}}},
            headers=headers(boss.id),
        )
        assert r.status_code == 201, r.text
        r = await http.post(
            f"{ADMIN_PREFIX}/sessions",
            json={"preset_id": r.json()["id"], "slug": "child-one"},
            headers=headers(boss.id),
        )
    assert_refused(r, "seven_day", 85.0, 80)
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT 1 FROM sessions WHERE slug = 'child-one'")
        assert await cur.fetchone() is None


async def test_scale_up_refused(pool, dsn):
    spec = {"replicas": 1, "max_replicas": 3, "usage_quota": {"seven_day_pct": 80}}
    c = await insert_session(pool, spec=spec)
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=85.0)))
    async with client_for(app) as http:
        r = await http.post(f"{C}/{c['id']}/scale", json={"replicas": 2})
        assert_refused(r, "seven_day", 85.0, 80)
        # the same count is not a scale-up
        r = await http.post(f"{C}/{c['id']}/scale", json={"replicas": 1})
        assert r.status_code == 202, r.text
    assert await replicas_of(pool, c["id"]) == 1


async def test_scale_down_never_gated(pool, dsn):
    c = await insert_session(pool, spec={"replicas": 3, "max_replicas": 3})
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=100.0, five_hour=100.0)))
    async with client_for(app) as http:
        r = await http.post(f"{C}/{c['id']}/scale", json={"replicas": 2})
    assert r.status_code == 202, r.text
    assert await replicas_of(pool, c["id"]) == 2


async def test_scale_missing_session_404(pool, dsn):
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=100.0)))
    async with client_for(app) as http:
        r = await http.post(f"{C}/{uuid.uuid4()}/scale", json={"replicas": 2})
    assert_error(r, 404, "not_found")


async def test_agent_scale_up_refused(pool, dsn):
    c, _, _ = await scale_hive(pool, replicas=1, max_replicas=3)
    app = gated(agent_settings(dsn), pool, Upstream(windows(seven_day=100.0)))
    async with agent_client(app) as http:
        r = await http.post("/agent/v1/scale", json={"replicas": 2}, headers=headers(c["id"]))
    assert_refused(r, "seven_day", 100.0, 100)
    assert await replicas_of(pool, c["id"]) == 1


async def test_agent_scale_non_leader_over_quota_is_not_leader(pool, dsn):
    c, _, _ = await scale_hive(pool, replicas=1, max_replicas=3)
    app = gated(agent_settings(dsn), pool, Upstream(windows(seven_day=100.0)))
    async with agent_client(app, IP1) as http:
        r = await http.post("/agent/v1/scale", json={"replicas": 2}, headers=headers(c["id"]))
    assert_error(r, 403, "not_leader")
    assert "usage" not in r.json()["error"] and "Retry-After" not in r.headers
    assert await replicas_of(pool, c["id"]) == 1


async def test_agent_scale_up_under_quota_applies(pool, dsn):
    c, _, _ = await scale_hive(pool, replicas=1, max_replicas=3)
    app = gated(agent_settings(dsn), pool, Upstream(windows(seven_day=50.0)))
    async with agent_client(app) as http:
        r = await http.post("/agent/v1/scale", json={"replicas": 2}, headers=headers(c["id"]))
    assert r.status_code == 202, r.text
    assert r.json()["replicas"] == 2 and r.json()["previous_replicas"] == 1
    assert await replicas_of(pool, c["id"]) == 2


async def test_agent_scale_down_never_gated(pool, dsn):
    c, a0, _ = await scale_hive(pool, replicas=2, max_replicas=3)
    app = gated(agent_settings(dsn), pool, Upstream(windows(seven_day=100.0)))
    async with agent_client(app) as http:
        r = await http.post("/agent/v1/scale", json={"replicas": 1}, headers=headers(c["id"]))
    assert r.status_code == 202, r.text
    assert await replicas_of(pool, c["id"]) == 1


async def test_restart_not_gated(pool, dsn):
    boss = await admin_hive(pool, "rs-boss", superleader=True)
    other = await admin_hive(pool, "rs-other")
    await attach_preset(pool, other.id)
    app = gated(agent_settings(dsn), pool, Upstream(windows(seven_day=100.0, five_hour=100.0)))
    async with agent_client(app, boss.leader_ip) as http:
        r = await http.post(
            f"{ADMIN_PREFIX}/sessions/{other.slug}/restart", headers=headers(boss.id)
        )
    assert r.status_code == 202, r.text
    assert len(r.json()["command_ids"]) == 2


async def test_meta_usage():
    app = gated(make_settings(), None, Upstream())
    async with client_for(app) as http:
        meta = (await http.get("/api/v1/meta")).json()
    assert meta["usage"] == {"configured": True, "quota_default_pct": 100}
    app = gated(make_settings(usage_quota_default=90), None, Upstream(), configured=False)
    async with client_for(app) as http:
        meta = (await http.get("/api/v1/meta")).json()
    assert meta["usage"] == {"configured": False, "quota_default_pct": 90}
