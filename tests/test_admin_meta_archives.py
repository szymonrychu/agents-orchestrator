"""GET /agent/v1/admin/meta, /archives, /archives/settings and /archives/cleanup/preview: the
superleader reads the deploy settings and the cache archives, with the same answers and errors as
the main port. The presigned download is not exposed (a credential-bearing URL)."""

from __future__ import annotations

import uuid

import pytest
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
