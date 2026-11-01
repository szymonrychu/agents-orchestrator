"""Cache key resolution at session create and cache availability in /meta (design D7, D8)."""

from __future__ import annotations

import re
import uuid

from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.cache.keys import validate_name
from agents_orchestrator.spec import default_cache_key

C = "/api/v1/sessions"
P = "/api/v1/presets"


def app_client(pool, dsn, **kw):
    kw.setdefault("cache_s3_bucket", "b")
    return client_for(create_app(make_settings(dsn, **kw), pool))


async def make_preset(http, name, spec):
    r = await http.post(P, json={"name": name, "spec": spec})
    assert r.status_code == 201, r.text
    return r.json()


def test_default_cache_key():
    a, b = uuid.uuid4(), uuid.uuid4()
    assert default_cache_key(a) == default_cache_key(a)
    assert default_cache_key(a) != default_cache_key(b)
    key = default_cache_key(a)
    assert re.fullmatch(r"preset-[0-9a-f]{16}", key)
    assert validate_name(key, allow_prefix=False) == key


async def test_create_from_preset_derives_key(pool, dsn):
    async with app_client(pool, dsn) as http:
        preset = await make_preset(http, "keyless", {"cache": {}})
        want = default_cache_key(uuid.UUID(preset["id"]))
        keys = []
        for _ in range(2):
            r = await http.post(C, json={"preset_id": preset["id"]})
            assert r.status_code == 201, r.text
            cache = r.json()["spec"]["cache"]
            keys.append((cache["restore_key"], cache["save_key"]))
        assert keys == [(want, want)] * 2
        got = (await http.get(f"{C}/{r.json()['id']}")).json()
        assert got["spec"]["cache"]["restore_key"] == got["spec"]["cache"]["save_key"] == want


async def test_create_from_preset_keeps_user_key(pool, dsn):
    async with app_client(pool, dsn) as http:
        preset = await make_preset(
            http, "keyed", {"cache": {"restore_key": "mine", "exclude": ["tmp"]}}
        )
        r = await http.post(C, json={"preset_id": preset["id"]})
        assert r.status_code == 201, r.text
        # an explicit restore_key leaves save_key unset: restore-only, no derived default
        assert r.json()["spec"]["cache"] == {
            "restore_key": "mine",
            "save_key": None,
            "exclude": ["tmp"],
        }


async def test_raw_spec_needs_key(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "raw", "spec": {"cache": {}}})
        assert_error(r, 422, "invalid_request")
        assert (
            "cache.restore_key or cache.save_key is required without a preset"
            in r.json()["error"]["message"]
        )
        ok = await http.post(C, json={"slug": "raw2", "spec": {"cache": {"restore_key": "k1"}}})
        assert ok.status_code == 201, ok.text
        ok = await http.post(
            C, json={"slug": "raw3", "spec": {"cache": {"restore_key": "", "save_key": ""}}}
        )
        assert ok.status_code == 201, ok.text


async def test_preset_cache_stored_sparse(pool, dsn):
    async with app_client(pool, dsn) as http:
        preset = await make_preset(http, "sparse", {"cache": {}})
        assert not preset["spec"]["cache"].get("restore_key")
        put = await http.put(f"{P}/{preset['id']}", json={"name": "sparse", "spec": {"cache": {}}})
        assert put.status_code == 200, put.text
        got = (await http.get(f"{P}/{preset['id']}")).json()
        assert not got["spec"]["cache"].get("restore_key")


async def test_meta_cache(pool, dsn):
    async with app_client(pool, dsn, cache_max_bytes=1234) as http:
        r = await http.get("/api/v1/meta")
        assert r.json()["cache"] == {"available": True, "max_bytes": 1234}
    async with app_client(pool, dsn, cache_s3_bucket="") as http:
        assert (await http.get("/api/v1/meta")).json()["cache"]["available"] is False


def test_default_cache_key_is_pinned():
    # Changing the salt or slice would orphan every existing cache in S3.
    assert default_cache_key(uuid.UUID(int=1)) == "preset-4660fe5f5bc805e5"


async def test_preset_prefix_reserved(pool, dsn):
    async with app_client(pool, dsn) as http:
        msg = "prefix preset- is reserved for preset-derived keys"
        victim = await make_preset(http, "victim", {"cache": {}})
        stolen = default_cache_key(uuid.UUID(victim["id"]))
        for key in (stolen, "preset-anything"):
            r = await http.post(
                C, json={"slug": "raw", "spec": {"cache": {"restore_key": key, "save_key": key}}}
            )
            assert_error(r, 422, "invalid_request")
            assert msg in r.json()["error"]["message"]
            r = await http.post(
                P, json={"name": "evil", "spec": {"cache": {"restore_key": key, "save_key": key}}}
            )
            assert_error(r, 422, "invalid_request")
            assert msg in r.json()["error"]["message"]
            r = await http.put(
                f"{P}/{victim['id']}",
                json={"name": "victim", "spec": {"cache": {"save_key": "preset-x"}}},
            )
            assert_error(r, 422, "invalid_request")
            assert msg in r.json()["error"]["message"]
        # a preset may carry its own derived key; other slugs keep working
        own = await http.put(
            f"{P}/{victim['id']}",
            json={"name": "victim", "spec": {"cache": {"restore_key": stolen, "save_key": stolen}}},
        )
        assert own.status_code == 200, own.text
        r = await http.post(C, json={"preset_id": victim["id"]})
        assert r.status_code == 201, r.text
        assert r.json()["spec"]["cache"]["restore_key"] == stolen
        ok = await http.post(
            C, json={"slug": "okslug", "spec": {"cache": {"restore_key": "presets-x"}}}
        )
        assert ok.status_code == 201, ok.text


async def test_copy_drops_source_derived_key(pool, dsn):
    async with app_client(pool, dsn) as http:
        src = await make_preset(http, "src", {"cache": {}})
        own = default_cache_key(uuid.UUID(src["id"]))
        put = await http.put(
            f"{P}/{src['id']}",
            json={"name": "src", "spec": {"cache": {"restore_key": own, "save_key": own}}},
        )
        assert put.status_code == 200, put.text
        cp = await http.post(f"{P}/{src['id']}/copy", json={"name": "src2"})
        assert cp.status_code == 201, cp.text
        assert not cp.json()["spec"]["cache"].get("restore_key")
        r = await http.post(C, json={"preset_id": cp.json()["id"]})
        assert r.status_code == 201, r.text
        assert r.json()["spec"]["cache"]["restore_key"] == default_cache_key(
            uuid.UUID(cp.json()["id"])
        )
        slug = await make_preset(
            http,
            "slugged",
            {"cache": {"restore_key": "mine", "save_key": "mine", "exclude": ["t"]}},
        )
        cp2 = await http.post(f"{P}/{slug['id']}/copy", json={"name": "slugged2"})
        assert cp2.json()["spec"]["cache"] == {
            "restore_key": "mine",
            "save_key": "mine",
            "exclude": ["t"],
        }


async def test_dynamic_first_segment_prefix_is_rejected(pool, dsn):
    async with app_client(pool, dsn) as http:
        spec = {"cache": {"save_key": "preset-{{ session_slug }}"}}
        r = await http.post(C, json={"slug": "dyn", "spec": spec})
        assert_error(r, 422, "invalid_request")
        assert "prefix preset- is reserved" in r.json()["error"]["message"]


async def test_bad_key_templates_are_422(pool, dsn):
    async with app_client(pool, dsn) as http:
        for bad in ("Bad Key", "{{ nope }}", "{% if %}", "sessions/x", "a" * 257):
            r = await http.post(C, json={"slug": "bad", "spec": {"cache": {"save_key": bad}}})
            assert_error(r, 422, "invalid_request")
        r = await http.post(C, json={"slug": "bad", "spec": {"cache": {"save_key": "a/"}}})
        assert_error(r, 422, "invalid_request")  # a save name may not be a prefix
        ok = await http.post(
            C,
            json={
                "slug": "good",
                "spec": {"cache": {"restore_key": "p/", "save_key": "p/{{ now }}"}},
            },
        )
        assert ok.status_code == 201, ok.text
