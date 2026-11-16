"""Backup entry names at session create and cache availability in /meta (design D7, D8)."""

from __future__ import annotations

from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app

C = "/api/v1/sessions"
P = "/api/v1/presets"


def app_client(pool, dsn, **kw):
    kw.setdefault("cache_s3_bucket", "b")
    return client_for(create_app(make_settings(dsn, **kw), pool))


def entry(name, **over):
    return {"root": "claude", "path": "skills", "mode": "both", "name": name, **over}


async def make_preset(http, name, spec):
    r = await http.post(P, json={"name": name, "spec": spec})
    assert r.status_code == 201, r.text
    return r.json()


async def test_meta_cache(pool, dsn):
    async with app_client(pool, dsn, cache_max_bytes=1234) as http:
        r = await http.get("/api/v1/meta")
        assert r.json()["cache"] == {"available": True, "max_bytes": 1234}
    async with app_client(pool, dsn, cache_s3_bucket="") as http:
        assert (await http.get("/api/v1/meta")).json()["cache"]["available"] is False


async def test_session_without_a_preset_may_carry_entries_and_keeps_them(pool, dsn):
    async with app_client(pool, dsn) as http:
        spec = {"backup": [entry("{{ project }}/skills")]}
        r = await http.post(C, json={"slug": "alpha", "spec": spec})
        assert r.status_code == 201, r.text
        assert r.json()["spec"]["backup"][0]["name"] == "{{ project }}/skills"


async def test_no_default_entries_from_a_preset(pool, dsn):
    """Decision 767: a preset without entries starts a session without entries."""
    async with app_client(pool, dsn) as http:
        preset = await make_preset(http, "plain", {})
        r = await http.post(C, json={"preset_id": preset["id"], "slug": "plain-1"})
        assert r.status_code == 201, r.text
        assert r.json()["spec"].get("backup") is None


async def test_preset_entries_round_trip_and_copy(pool, dsn):
    async with app_client(pool, dsn) as http:
        preset = await make_preset(http, "src", {"backup": [entry("k", exclude=["t"])]})
        got = (await http.get(f"{P}/{preset['id']}")).json()
        assert got["spec"]["backup"][0]["exclude"] == ["t"]
        cp = await http.post(f"{P}/{preset['id']}/copy", json={})
        assert cp.status_code == 201, cp.text
        assert cp.json()["spec"]["backup"][0]["name"] == "k"


async def test_old_cache_field_is_422(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "old", "spec": {"cache": {"restore_key": "a"}}})
        assert r.status_code == 422, r.text


async def test_bad_names_are_422(pool, dsn):
    async with app_client(pool, dsn) as http:
        for bad in ("Bad Key", "{{ nope }}", "{% if %}", "sessions/x", "a" * 257, "a/"):
            r = await http.post(C, json={"slug": "bad", "spec": {"backup": [entry(bad)]}})
            assert_error(r, 422, "invalid_request")
        ok = await http.post(C, json={"slug": "good", "spec": {"backup": [entry("p/{{ now }}")]}})
        assert ok.status_code == 201, ok.text


async def test_names_that_render_empty_for_this_session_are_422(pool, dsn):
    """The preview sample renders fine; this preset's name squeezes to nothing."""
    async with app_client(pool, dsn) as http:
        preset = await make_preset(http, "...", {"backup": [entry("{{ preset_name }}")]})
        r = await http.post(C, json={"preset_id": preset["id"], "slug": "sym"})
        assert_error(r, 422, "invalid_request")
        assert "backup[0].name" in r.json()["error"]["message"]
        r = await http.post(C, json={"preset_id": preset["id"]})
        assert_error(r, 422, "invalid_request")


async def test_template_error_reports_the_line(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "bad", "spec": {"backup": [entry("a\n{% if %}")]}})
        assert_error(r, 422, "invalid_request")
        assert "line 2" in r.json()["error"]["message"]
        assert "backup[0].name" in r.json()["error"]["message"]


async def test_session_name_renders_with_the_real_context(pool, dsn):
    async with app_client(pool, dsn) as http:
        spec = {"backup": [entry("{{ project }}/{{ session_slug }}/{{ now }}")]}
        r = await http.post(C, json={"slug": "alpha", "spec": spec})
        assert r.status_code == 201, r.text
