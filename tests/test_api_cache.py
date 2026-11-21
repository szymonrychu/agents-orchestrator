"""Cache availability in /meta (design D7, D8) and the removed backup entries."""

from __future__ import annotations

from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app

C = "/api/v1/sessions"


def app_client(pool, dsn, **kw):
    kw.setdefault("cache_s3_bucket", "b")
    return client_for(create_app(make_settings(dsn, **kw), pool))


def entry(name, **over):
    return {"root": "claude", "path": "skills", "mode": "both", "name": name, **over}


async def test_meta_cache(pool, dsn):
    async with app_client(pool, dsn, cache_max_bytes=1234) as http:
        r = await http.get("/api/v1/meta")
        assert r.json()["cache"] == {"available": True, "max_bytes": 1234}
    async with app_client(pool, dsn, cache_s3_bucket="") as http:
        assert (await http.get("/api/v1/meta")).json()["cache"]["available"] is False


async def test_old_cache_field_is_422(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "old", "spec": {"cache": {"restore_key": "a"}}})
        assert r.status_code == 422, r.text


async def test_backup_entries_are_422_on_a_session_create(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "old", "spec": {"backup": [entry("k")]}})
        assert_error(r, 422, "invalid_request")
