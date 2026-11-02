"""Cache cleanup settings and run-now routes; preview and delete read the stored policy."""

from __future__ import annotations

import hashlib
import logging
from datetime import datetime, timedelta

import httpx
import pytest
from test_api_archives import NO_CREDS, NOW, PRE, A, client, obj, s3, settings_for  # noqa: F401
from test_api_sessions import assert_error

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import cache_sweeps

S = "/api/v1/cache/cleanup/settings"
RUN = "/api/v1/cache/cleanup/run"
PREVIEW = "/api/v1/cache/cleanup/preview"
GOOD = {
    "enabled": True,
    "dry_run": False,
    "retention_days": 14,
    "keep_newest": 2,
    "interval_seconds": 3600,
    "grace_seconds": 0,
}


async def test_get_without_a_row_is_the_environment_seed(pool, dsn):
    async with client(pool, dsn) as c:
        r = await c.get(S)
    body = r.json()
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    assert body["source"] == "environment"
    assert body["policy"] == body["defaults"]
    assert set(body["policy"]) == set(GOOD)
    assert body["policy"]["retention_days"] == 30 and body["policy"]["keep_newest"] == 3
    assert body["policy"]["interval_seconds"] == 21600
    assert body["policy"]["enabled"] is True and body["policy"]["dry_run"] is True
    assert body["policy"]["grace_seconds"] == settings_for(dsn).cache_timeout_seconds + 600
    assert body["updated_at"] is None and body["run_requested_at"] is None
    assert body["run_pending"] is False and body["last_sweep"] is None


async def test_put_round_trip(pool, dsn, caplog):
    with caplog.at_level(logging.INFO):
        async with client(pool, dsn) as c:
            before = (await c.get(S)).json()
            r = await c.put(S, json=GOOD)
            again = (await c.get(S)).json()
    body = r.json()
    assert r.status_code == 200 and r.headers["cache-control"] == "no-store"
    assert body["source"] == "database" and body["policy"] == GOOD
    assert body["defaults"] == before["defaults"] and body["updated_at"]
    assert again == body
    audit = [m for m in caplog.messages if m.startswith("audit: cache cleanup settings")]
    assert len(audit) == 1 and "old=env" in audit[0] and "'retention_days': 14" in audit[0]


async def test_put_audit_shows_the_stored_policy_as_old(pool, dsn, caplog):
    async with client(pool, dsn) as c:
        await c.put(S, json=GOOD)
        with caplog.at_level(logging.INFO):
            await c.put(S, json={**GOOD, "retention_days": 20})
    audit = [m for m in caplog.messages if m.startswith("audit: cache cleanup settings")]
    assert len(audit) == 1 and "old={" in audit[0] and "'retention_days': 14" in audit[0]


@pytest.mark.parametrize(
    "patch",
    [
        {"retention_days": 0},
        {"retention_days": 3651},
        {"keep_newest": 0},
        {"keep_newest": 1001},
        {"interval_seconds": 59},
        {"interval_seconds": 2592001},
        {"grace_seconds": -1},
        {"grace_seconds": 604801},
        {"extra": 1},
        {"enabled": None},
    ],
)
async def test_put_validates_and_stores_nothing(pool, dsn, patch):
    body = {**GOOD, **patch}
    body = {k: v for k, v in body.items() if v is not None}
    async with client(pool, dsn) as c:
        assert_error(await c.put(S, json=body), 422, "invalid_request")
        assert (await c.get(S)).json()["source"] == "environment"


async def test_put_accepts_the_bounds(pool, dsn):
    low = {**GOOD, "retention_days": 1, "keep_newest": 1, "interval_seconds": 60}
    high = {
        "enabled": False,
        "dry_run": True,
        "retention_days": 3650,
        "keep_newest": 1000,
        "interval_seconds": 2592000,
        "grace_seconds": 604800,
    }
    async with client(pool, dsn) as c:
        for body in (low, high):
            r = await c.put(S, json=body)
            assert r.status_code == 200 and r.json()["policy"] == body


async def test_run_is_202_and_pending_without_saving_the_policy(pool, dsn, caplog):
    with caplog.at_level(logging.INFO):
        async with client(pool, dsn) as c:
            r = await c.post(RUN)
    body = r.json()
    assert r.status_code == 202 and r.headers["cache-control"] == "no-store"
    assert body["run_pending"] is True and body["run_requested_at"]
    assert body["source"] == "environment"
    assert any(m.startswith("audit: cache cleanup run requested") for m in caplog.messages)


async def test_run_pending_clears_once_a_later_sweep_started(pool, dsn):
    async with client(pool, dsn) as c:
        requested = (await c.post(RUN)).json()["run_requested_at"]
        started = datetime.fromisoformat(requested) + timedelta(seconds=1)
        result = {"started_at": started.isoformat(), "deleted": 1, "dry_run": True}
        async with pool.connection() as conn:
            await cache_sweeps.record(conn, started + timedelta(seconds=1), result)
        body = (await c.get(S)).json()
    assert body["run_pending"] is False and body["last_sweep"] == result


async def test_preview_uses_the_stored_policy(pool, dsn, s3):  # noqa: F811
    stamps = [NOW - timedelta(days=d) for d in (1, 40, 50, 60)]
    s3.objects = [obj(f"proj/{n}", when) for n, when in zip("abcd", stamps, strict=True)]
    stored = {**GOOD, "keep_newest": 1, "retention_days": 30, "enabled": False}
    async with client(pool, dsn, cache_keep_newest=3) as c:
        assert (await c.put(S, json=stored)).status_code == 200
        body = (await c.get(PREVIEW)).json()
    assert [i["key"] for i in body["would_delete"]] == [
        f"{PRE}proj/b.tar.zst",
        f"{PRE}proj/c.tar.zst",
    ]
    assert (body["retention_days"], body["keep_newest"]) == (30, 1)
    assert body["grace_seconds"] == 0
    assert body["sweep_dry_run"] is False and body["sweep_enabled"] is False


async def test_delete_uses_the_stored_grace(pool, dsn, s3):  # noqa: F811
    fresh = f"{PRE}proj/new.tar.zst"
    s3.objects = [obj("proj/new", NOW - timedelta(seconds=5))]
    async with client(pool, dsn) as c:
        r = await c.request("DELETE", A, json={"keys": [fresh]})
        assert r.json()["refused"] == [{"key": fresh, "reason": "younger than the grace period"}]
        assert s3.deleted == []
        await c.put(S, json={**GOOD, "grace_seconds": 0})
        r = await c.request("DELETE", A, json={"keys": [fresh]})
    assert r.json()["deleted"] == [fresh] and s3.deleted == [[fresh]]


async def test_settings_routes_need_no_api_s3_pair(pool, dsn):
    async with client(pool, dsn, **NO_CREDS) as c:
        assert (await c.get(S)).status_code == 200
        assert (await c.put(S, json=GOOD)).status_code == 200
        assert (await c.post(RUN)).status_code == 202


async def test_service_tokens_cannot_reach_the_settings_routes(pool, dsn):
    token = "svc-example-token-value"
    digest = hashlib.sha256(token.encode()).hexdigest()
    app = create_app(settings_for(dsn, service_tokens=f"agents-webhook:{digest}"), pool)
    transport = httpx.ASGITransport(app=app, client=("203.0.113.9", 4444))
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://orch:8002",
        headers={"Authorization": f"Bearer {token}"},
    ) as c:
        for method, path in (("GET", S), ("PUT", S), ("POST", RUN)):
            r = await c.request(method, path)
            assert r.status_code == 403, (method, path, r.text)
