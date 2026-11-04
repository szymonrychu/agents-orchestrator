"""GET /agent/v1/admin/usage: the account's usage windows for a superleader (read-only), from the
same `UsageSource` the usage gate reads. Non-superleaders are refused by the admin gate."""

from __future__ import annotations

from test_agent_session_messaging import alone
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_usage_gate import RESETS, Upstream, close_usage_sources, gated, windows  # noqa: F401

from agents_orchestrator.api.admin import ADMIN_PREFIX

USAGE = f"{ADMIN_PREFIX}/usage"


async def test_superleader_reads_both_windows(pool, dsn):
    boss = await alone(pool, "u-boss", superleader=True)
    app = gated(make_settings(dsn), pool, Upstream(windows(five_hour=42.5, seven_day=61)))
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(USAGE, headers=headers(boss.id))
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["available"] is True
    assert body["source"] == "agents-usage"
    for key, pct in (("five_hour", 42.5), ("seven_day", 61.0)):
        w = body[key]
        assert w["utilization_pct"] == pct
        assert w["resets_at"] == RESETS.isoformat().replace("+00:00", "Z")
        assert w["stale"] is False
        assert w["source"] == "statusline"
    assert body["updated_at"] == max(
        body["five_hour"]["observed_at"], body["seven_day"]["observed_at"]
    )


async def test_missing_window_is_null(pool, dsn):
    boss = await alone(pool, "u-one", superleader=True)
    app = gated(make_settings(dsn), pool, Upstream(windows(seven_day=10)))
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(USAGE, headers=headers(boss.id))
    assert r.status_code == 200, r.text
    assert r.json()["five_hour"] is None
    assert r.json()["seven_day"]["utilization_pct"] == 10.0


async def test_unavailable_service_answers_not_available(pool, dsn):
    boss = await alone(pool, "u-down", superleader=True)
    app = gated(make_settings(dsn), pool, Upstream(status=500))
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(USAGE, headers=headers(boss.id))
    assert r.status_code == 200, r.text
    assert r.json() == {
        "configured": True,
        "available": False,
        "source": "agents-usage",
        "updated_at": None,
        "five_hour": None,
        "seven_day": None,
    }


async def test_not_configured(pool, dsn):
    boss = await alone(pool, "u-off", superleader=True)
    app = gated(make_settings(dsn), pool, Upstream(), configured=False)
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(USAGE, headers=headers(boss.id))
    assert r.status_code == 200, r.text
    assert r.json()["configured"] is False
    assert r.json()["available"] is False


async def test_non_superleader_refused(pool, dsn):
    plain = await alone(pool, "u-plain")
    upstream = Upstream(windows(five_hour=1))
    app = gated(make_settings(dsn), pool, upstream)
    async with client_for(app, plain.leader_ip) as http:
        r = await http.get(USAGE, headers=headers(plain.id))
    assert_error(r, 403, "not_superleader")
    assert upstream.calls == 0
