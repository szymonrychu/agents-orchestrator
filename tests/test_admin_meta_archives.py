"""GET /agent/v1/admin/meta, /archives, /archives/settings and /archives/cleanup/preview: the
superleader reads the deploy settings and the cache archives, with the same answers and errors as
the main port. The presigned download is not exposed (a credential-bearing URL)."""

from __future__ import annotations

import uuid

import pytest
from test_admin_audit_detail import admin_rows
from test_agent_session_messaging import alone
from test_api_agent import AGENT_PORT, KEY, client_for, headers
from test_api_archives import NO_CREDS, PRE, client, obj, s3, settings_for  # noqa: F401
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

A = f"{ADMIN_PREFIX}/archives"


def agent_settings(dsn, **kw):
    """The main-port archive settings plus what the agent port needs (key, port)."""
    kw.setdefault("master_key", KEY)
    kw.setdefault("agent_port", AGENT_PORT)
    return settings_for(dsn, **kw)


async def admin_get(pool, dsn, path, *, superleader=True, params=None, **kw):
    boss = await alone(pool, f"ma-{uuid.uuid4().hex[:8]}", superleader=superleader)
    app = create_app(agent_settings(dsn, **kw), pool)
    async with client_for(app, boss.leader_ip) as http:
        return await http.get(path, headers=headers(boss.id), params=params)


async def main_get(pool, dsn, path, params=None, **kw):
    async with client(pool, dsn, **kw) as c:
        return await c.get(path, params=params)


async def test_meta_equals_the_main_port(pool, dsn):
    admin = await admin_get(pool, dsn, f"{ADMIN_PREFIX}/meta")
    main = await main_get(pool, dsn, "/api/v1/meta")
    assert admin.status_code == 200, admin.text
    assert admin.json() == main.json()


async def test_meta_carries_no_credential(pool, dsn):
    r = await admin_get(pool, dsn, f"{ADMIN_PREFIX}/meta")
    for secret in ("reconciler-example-secret", "api-example-secret", "TESTIDAPIROLE"):
        assert secret not in r.text


async def test_archives_list_equals_the_main_port(pool, dsn, s3):  # noqa: F811
    s3.objects = [obj(f"proj/{i:02d}") for i in range(4)]
    params = {"limit": 3}
    admin = await admin_get(pool, dsn, A, params=params)
    main = await main_get(pool, dsn, "/api/v1/cache/archives", params)
    assert admin.status_code == 200, admin.text
    assert admin.json() == main.json()
    assert admin.json()["next_cursor"]
    nxt = {"limit": 3, "cursor": admin.json()["next_cursor"]}
    admin2 = await admin_get(pool, dsn, A, params=nxt)
    main2 = await main_get(pool, dsn, "/api/v1/cache/archives", nxt)
    assert admin2.json() == main2.json()


async def test_archives_list_rejects_a_bad_prefix(pool, dsn, s3):  # noqa: F811
    r = await admin_get(pool, dsn, A, params={"prefix": "../x"})
    assert_error(r, 400, "bad_request")


async def test_settings_and_preview_equal_the_main_port(pool, dsn, s3):  # noqa: F811
    s3.objects = [obj("proj/a"), obj("proj/b")]
    for admin_path, main_path in (
        (f"{A}/settings", "/api/v1/cache/cleanup/settings"),
        (f"{A}/cleanup/preview", "/api/v1/cache/cleanup/preview"),
    ):
        admin = await admin_get(pool, dsn, admin_path)
        main = await main_get(pool, dsn, main_path)
        assert admin.status_code == 200, admin.text
        assert admin.json() == main.json()


@pytest.mark.parametrize("path", [A, f"{A}/cleanup/preview"])
async def test_503_without_the_s3_pair(pool, dsn, s3, path):  # noqa: F811
    r = await admin_get(pool, dsn, path, **NO_CREDS)
    assert_error(r, 503, "unavailable")
    assert s3.creds == []


@pytest.mark.parametrize(
    "suffix", ["/meta", "/archives", "/archives/settings", "/archives/cleanup/preview"]
)
async def test_regular_leader_is_refused(pool, dsn, s3, suffix):  # noqa: F811
    r = await admin_get(pool, dsn, f"{ADMIN_PREFIX}{suffix}", superleader=False)
    assert r.status_code == 403, r.text


async def test_download_is_not_exposed(pool, dsn, s3):  # noqa: F811
    s3.objects = [obj("proj/a")]
    r = await admin_get(pool, dsn, f"{A}/download", params={"key": f"{PRE}proj/a.tar.zst"})
    assert r.status_code in (404, 405)
    assert "location" not in r.headers


POLICY = {
    "enabled": True,
    "dry_run": True,
    "retention_days": 30,
    "keep_newest": 3,
    "interval_seconds": 3600,
    "grace_seconds": 600,
}


async def admin_put(pool, dsn, body, *, superleader=True, **kw):
    boss = await alone(pool, f"mp-{uuid.uuid4().hex[:8]}", superleader=superleader)
    app = create_app(agent_settings(dsn, **kw), pool)
    async with client_for(app, boss.leader_ip) as http:
        return await http.put(f"{A}/settings", headers=headers(boss.id), json=body)


async def stored_policy(pool):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT count(*) FROM cache_sweep_settings")
        return (await cur.fetchone())[0]


async def test_admin_put_cleanup_settings_200(pool, dsn):
    r = await admin_put(pool, dsn, POLICY)
    assert r.status_code == 200, r.text
    assert r.headers["cache-control"] == "no-store"
    assert r.json()["policy"] == POLICY
    assert r.json()["source"] == "database"
    main = await main_get(pool, dsn, "/api/v1/cache/cleanup/settings")
    assert main.json()["policy"] == POLICY


async def test_admin_put_cleanup_settings_without_s3_pair(pool, dsn):
    r = await admin_put(pool, dsn, POLICY, **NO_CREDS)
    assert r.status_code == 200, r.text
    assert r.json()["policy"] == POLICY


async def test_admin_put_cleanup_settings_out_of_bounds_422(pool, dsn):
    r = await admin_put(pool, dsn, {**POLICY, "retention_days": 0})
    assert r.status_code == 422, r.text
    assert '"input"' not in r.text
    assert await stored_policy(pool) == 0


async def test_admin_put_cleanup_settings_partial_body_422(pool, dsn):
    r = await admin_put(pool, dsn, {"enabled": False})
    assert r.status_code == 422, r.text
    assert await stored_policy(pool) == 0


async def test_admin_put_cleanup_settings_audit_keys_only(pool, dsn):
    r = await admin_put(pool, dsn, POLICY)
    assert r.status_code == 200, r.text
    row = [x for x in await admin_rows(pool) if x["method"] == "PUT"][-1]
    assert row["fields"] == [
        "dry_run",
        "enabled",
        "grace_seconds",
        "interval_seconds",
        "keep_newest",
        "retention_days",
    ]
    assert set(row) == {"method", "path", "status", "session", "secrets", "fields"}


async def test_admin_put_cleanup_settings_regular_leader_403(pool, dsn):
    r = await admin_put(pool, dsn, POLICY, superleader=False)
    assert_error(r, 403, "not_superleader")
    assert await stored_policy(pool) == 0
