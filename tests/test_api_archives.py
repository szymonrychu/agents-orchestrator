"""Cache archives api: list, download, cleanup preview, manual delete (api-role credentials)."""

from __future__ import annotations

import hashlib
import logging
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, unquote, urlsplit

import httpx
import pytest
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api import archives as archives_mod
from agents_orchestrator.api.app import create_app
from agents_orchestrator.cache.presign import ArchiveObject, DeleteFailure
from agents_orchestrator.db import cache_sweeps, sessions
from agents_orchestrator.spec import SessionSpec

PRE = "workspace-cache/"
A = "/api/v1/cache/archives"
NOW = datetime.now(UTC)
OLD = NOW - timedelta(days=90)
FRESH = NOW - timedelta(seconds=5)


def obj(name, when=OLD, size=100):
    return ArchiveObject(f"{PRE}{name}.tar.zst", size, when)


class FakeS3:
    """Replaces the botocore calls the router makes; records the credentials it was given."""

    def __init__(self, objects):
        self.objects = list(objects)
        self.deleted: list[list[str]] = []
        self.fail: dict[str, DeleteFailure] = {}
        self.creds: list = []
        self.pages = 0

    def list_page(self, settings, prefix, *, token=None, limit=1000, credentials=None):
        self.creds.append(credentials)
        rows = sorted((o for o in self.objects if o.key.startswith(prefix)), key=lambda o: o.key)
        start = int(token) if token else 0
        page = rows[start : start + limit]
        nxt = str(start + limit) if start + limit < len(rows) else None
        self.pages += 1
        return page, nxt

    def list_archives(self, settings, prefix, *, credentials=None):
        self.creds.append(credentials)
        return [o for o in self.objects if o.key.startswith(prefix)]

    def delete_keys(self, settings, keys, *, credentials=None):
        self.creds.append(credentials)
        self.deleted.append(list(keys))
        return [self.fail[k] for k in keys if k in self.fail]


def settings_for(dsn, **kw):
    kw.setdefault("cache_s3_endpoint", "http://rgw.example:80")
    kw.setdefault("cache_s3_bucket", "bucket")
    kw.setdefault("cache_s3_path_style", True)
    kw.setdefault("cache_s3_access_key_id", "TESTIDRECONCILER")
    kw.setdefault("cache_s3_secret_access_key", "reconciler-example-secret")
    kw.setdefault("cache_image", "orch:1")
    kw.setdefault("cache_s3_api_access_key_id", "TESTIDAPIROLE")
    kw.setdefault("cache_s3_api_secret_access_key", "api-example-secret")
    return make_settings(dsn, **kw)


@pytest.fixture
def s3(monkeypatch):
    fake = FakeS3([])
    monkeypatch.setattr(archives_mod.s3, "list_page", fake.list_page)
    monkeypatch.setattr(archives_mod.s3, "list_archives", fake.list_archives)
    monkeypatch.setattr(archives_mod.s3, "delete_keys", fake.delete_keys)
    return fake


def client(pool, dsn, **kw):
    return client_for(create_app(settings_for(dsn, **kw), pool))


# ------------------------------------------------------------------ 503 without credentials

NO_CREDS = {"cache_s3_api_access_key_id": "", "cache_s3_api_secret_access_key": ""}


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", A, None),
        ("GET", f"{A}/download?key={PRE}x.tar.zst", None),
        ("GET", "/api/v1/cache/cleanup/preview", None),
        ("DELETE", A, {"keys": [f"{PRE}x.tar.zst"]}),
    ],
)
async def test_503_without_api_credentials(pool, dsn, s3, method, path, body):
    async with client(pool, dsn, **NO_CREDS) as c:
        r = await c.request(method, path, json=body)
    assert_error(r, 503, "unavailable")
    assert s3.creds == []


async def test_503_without_a_bucket(pool, dsn, s3):
    async with client(pool, dsn, cache_s3_bucket="") as c:
        assert_error(await c.get(A), 503, "unavailable")


async def test_api_credentials_are_the_api_pair_not_the_reconciler_pair(pool, dsn, s3):
    s3.objects = [obj("proj/a")]
    async with client(pool, dsn) as c:
        await c.get(A)
    assert s3.creds[0] == ("TESTIDAPIROLE", "api-example-secret")


# ------------------------------------------------------------------ list


async def test_list_pages_and_reports_references(pool, dsn, s3):
    s3.objects = [obj(f"proj/{i:02d}") for i in range(5)] + [obj("other/x")]
    async with pool.connection() as conn:
        spec = SessionSpec(cache={"restore_key": "proj/00", "save_key": ""})
        await sessions.create(conn, "alpha", spec)
    async with client(pool, dsn) as c:
        first = (await c.get(A, params={"limit": 3})).json()
        assert [i["name"] for i in first["items"]] == ["other/x", "proj/00", "proj/01"]
        assert first["next_cursor"]
        second = (await c.get(A, params={"limit": 3, "cursor": first["next_cursor"]})).json()
        assert [i["name"] for i in second["items"]] == ["proj/02", "proj/03", "proj/04"]
        assert second["next_cursor"] is None
    by_name = {i["name"]: i for i in first["items"]}
    assert by_name["proj/00"]["referenced_by"] == ["alpha"] and by_name["proj/00"]["protected"]
    assert by_name["proj/01"]["referenced_by"] == [] and not by_name["proj/01"]["protected"]
    item = by_name["proj/01"]
    assert item["key"] == f"{PRE}proj/01.tar.zst" and item["size"] == 100 and item["last_modified"]


async def test_list_prefix_filters_inside_the_cache_prefix(pool, dsn, s3):
    s3.objects = [obj("proj/a"), obj("other/b")]
    async with client(pool, dsn) as c:
        items = (await c.get(A, params={"prefix": "proj/"})).json()["items"]
    assert [i["name"] for i in items] == ["proj/a"]


@pytest.mark.parametrize("prefix", ["../x", "a/../b", "/abs", "UP", "a b", "x" * 201])
async def test_list_rejects_bad_prefixes(pool, dsn, s3, prefix):
    async with client(pool, dsn) as c:
        assert_error(await c.get(A, params={"prefix": prefix}), 400, "bad_request")


async def test_list_rejects_a_bad_cursor_and_limit(pool, dsn, s3):
    async with client(pool, dsn) as c:
        assert_error(await c.get(A, params={"cursor": "a b;c"}), 400, "bad_request")
        assert_error(await c.get(A, params={"limit": 0}), 422, "invalid_request")


async def test_list_hides_non_archives(pool, dsn, s3):
    s3.objects = [obj("proj/a"), ArchiveObject(f"{PRE}proj/readme.txt", 1, OLD)]
    async with client(pool, dsn) as c:
        items = (await c.get(A)).json()["items"]
    assert [i["name"] for i in items] == ["proj/a"]


# ------------------------------------------------------------------ download


async def test_download_redirects_to_a_short_lived_presigned_get(pool, dsn, s3):
    key = f"{PRE}proj/20261031t120000z.tar.zst"
    async with client(pool, dsn) as c:
        r = await c.get(f"{A}/download", params={"key": key}, follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["cache-control"] == "no-store"
    loc = urlsplit(r.headers["location"])
    q = {k: v[0] for k, v in parse_qs(loc.query).items()}
    assert unquote(loc.path).endswith(f"/bucket/{key}")
    assert q["X-Amz-Expires"] == "300"
    assert (
        q["response-content-disposition"] == 'attachment; filename="proj_20261031t120000z.tar.zst"'
    )
    assert "TESTIDAPIROLE" in q["X-Amz-Credential"]  # signed with the api pair
    assert "PUT" not in r.headers["location"]


@pytest.mark.parametrize(
    "key",
    [
        "other-prefix/x.tar.zst",
        f"{PRE}x.txt",
        f"{PRE}x.tar.zst.bak",
        f"{PRE}../escape.tar.zst",
        f"{PRE}a//b.tar.zst",
        f"{PRE}UPPER.tar.zst",
        f"{PRE}sessions.tar.zst",
        f"{PRE}.tar.zst",
        f"{PRE}a\nb.tar.zst",
        PRE,
        "",
    ],
)
async def test_download_rejects_keys_outside_the_grammar(pool, dsn, s3, key):
    async with client(pool, dsn) as c:
        r = await c.get(f"{A}/download", params={"key": key}, follow_redirects=False)
    assert_error(r, 400, "bad_request")


async def test_download_allows_a_transcripts_object(pool, dsn, s3):
    async with client(pool, dsn) as c:
        r = await c.get(
            f"{A}/download", params={"key": f"{PRE}sessions/alpha.tar.zst"}, follow_redirects=False
        )
    assert r.status_code == 302


# ------------------------------------------------------------------ cleanup preview


async def test_preview_matches_the_planner_and_includes_the_last_sweep(pool, dsn, s3):
    stamps = [NOW - timedelta(days=d) for d in (1, 40, 50, 60)]
    s3.objects = [obj(f"proj/{n}", when) for n, when in zip("abcd", stamps, strict=True)]
    async with pool.connection() as conn:
        await cache_sweeps.record(conn, NOW, {"deleted": 4, "dry_run": False})
    async with client(pool, dsn, cache_keep_newest=1) as c:
        r = await c.get("/api/v1/cache/cleanup/preview")
    body = r.json()
    assert r.headers["cache-control"] == "no-store"
    assert [i["key"] for i in body["would_delete"]] == [
        f"{PRE}proj/b.tar.zst",
        f"{PRE}proj/c.tar.zst",
    ]
    assert body["count"] == 2 and body["total_bytes"] == 200
    assert body["would_delete"][0]["reason"]
    assert (body["retention_days"], body["keep_newest"]) == (30, 1)
    assert body["sweep_dry_run"] is True and body["sweep_enabled"] is True
    assert body["last_sweep"] == {"deleted": 4, "dry_run": False}


async def test_preview_never_lists_a_protected_key(pool, dsn, s3):
    s3.objects = [obj("proj/z", NOW - timedelta(days=1)), obj("proj/b"), obj("proj/a")]
    async with pool.connection() as conn:
        spec = SessionSpec(cache={"restore_key": "proj/b", "save_key": ""})
        await sessions.create(conn, "alpha", spec)
    async with client(pool, dsn, cache_keep_newest=1) as c:
        body = (await c.get("/api/v1/cache/cleanup/preview")).json()
    assert [i["key"] for i in body["would_delete"]] == [f"{PRE}proj/a.tar.zst"]
    assert body["last_sweep"] is None


# ------------------------------------------------------------------ manual delete


async def test_delete_removes_unreferenced_old_archives(pool, dsn, s3, caplog):
    s3.objects = [obj("proj/a"), obj("proj/b")]
    with caplog.at_level(logging.INFO):
        async with client(pool, dsn) as c:
            r = await c.request("DELETE", A, json={"keys": [f"{PRE}proj/a.tar.zst"]})
    assert r.json() == {"deleted": [f"{PRE}proj/a.tar.zst"], "refused": [], "errors": []}
    assert s3.deleted == [[f"{PRE}proj/a.tar.zst"]]
    assert s3.creds[-1] == ("TESTIDAPIROLE", "api-example-secret")
    audit = [m for m in caplog.messages if m.startswith("audit: cache archives delete")]
    assert len(audit) == 1 and f"{PRE}proj/a.tar.zst" in audit[0]


async def test_delete_refuses_referenced_young_missing_and_invalid_keys(pool, dsn, s3):
    s3.objects = [obj("proj/live"), obj("proj/young", FRESH), obj("proj/ok")]
    async with pool.connection() as conn:
        spec = SessionSpec(cache={"restore_key": "proj/live", "save_key": ""})
        await sessions.create(conn, "alpha", spec)
    keys = [
        f"{PRE}proj/live.tar.zst",
        f"{PRE}proj/young.tar.zst",
        f"{PRE}proj/gone.tar.zst",
        "elsewhere/x.tar.zst",
        f"{PRE}sessions/alpha.tar.zst",
        f"{PRE}proj/ok.tar.zst",
    ]
    s3.objects.append(obj("sessions/alpha"))
    async with client(pool, dsn) as c:
        r = await c.request("DELETE", A, json={"keys": keys})
    body = r.json()
    assert body["deleted"] == [f"{PRE}proj/ok.tar.zst"]
    reasons = {x["key"]: x["reason"] for x in body["refused"]}
    assert "referenced by session alpha" in reasons[keys[0]]
    assert "grace" in reasons[keys[1]]
    assert reasons[keys[2]] == "not found"
    assert "prefix" in reasons[keys[3]]
    assert "referenced by session alpha" in reasons[keys[4]]  # the session's transcripts
    assert s3.deleted == [[f"{PRE}proj/ok.tar.zst"]]


async def test_delete_rechecks_the_protected_set_server_side(pool, dsn, s3, monkeypatch):
    """A key the caller saw as free may have become referenced: the check runs at delete time."""
    s3.objects = [obj("proj/a")]
    async with client(pool, dsn) as c:
        first = await c.request("DELETE", A, json={"keys": [f"{PRE}proj/a.tar.zst"]})
        assert first.json()["deleted"] == [f"{PRE}proj/a.tar.zst"]
        async with pool.connection() as conn:
            spec = SessionSpec(cache={"restore_key": "proj/a", "save_key": ""})
            await sessions.create(conn, "newcomer", spec)
        s3.deleted.clear()
        again = await c.request("DELETE", A, json={"keys": [f"{PRE}proj/a.tar.zst"]})
    assert again.json()["deleted"] == [] and again.json()["refused"][0]["key"].endswith("a.tar.zst")
    assert s3.deleted == []


async def test_delete_surfaces_s3_errors(pool, dsn, s3):
    s3.objects = [obj("proj/a"), obj("proj/b")]
    s3.fail[f"{PRE}proj/b.tar.zst"] = DeleteFailure(f"{PRE}proj/b.tar.zst", "AccessDenied", "no")
    async with client(pool, dsn) as c:
        r = await c.request(
            "DELETE", A, json={"keys": [f"{PRE}proj/a.tar.zst", f"{PRE}proj/b.tar.zst"]}
        )
    body = r.json()
    assert body["deleted"] == [f"{PRE}proj/a.tar.zst"]
    assert body["errors"] == [
        {"key": f"{PRE}proj/b.tar.zst", "code": "AccessDenied", "message": "no"}
    ]


async def test_delete_body_is_validated(pool, dsn, s3):
    async with client(pool, dsn) as c:
        assert_error(await c.request("DELETE", A, json={"keys": []}), 422, "invalid_request")
        assert_error(
            await c.request("DELETE", A, json={"keys": ["a"], "x": 1}), 422, "invalid_request"
        )
        assert_error(
            await c.request("DELETE", A, json={"keys": ["a"] * 1001}), 422, "invalid_request"
        )


async def test_delete_dedupes_keys(pool, dsn, s3):
    s3.objects = [obj("proj/a")]
    key = f"{PRE}proj/a.tar.zst"
    async with client(pool, dsn) as c:
        r = await c.request("DELETE", A, json={"keys": [key, key]})
    assert r.json()["deleted"] == [key] and s3.deleted == [[key]]


# ------------------------------------------------------------------ the service port


async def test_service_tokens_cannot_reach_the_archive_routes(pool, dsn, s3):
    token = "svc-example-token-value"
    digest = hashlib.sha256(token.encode()).hexdigest()
    app = create_app(settings_for(dsn, service_tokens=f"agents-webhook:{digest}"), pool)
    transport = httpx.ASGITransport(app=app, client=("203.0.113.9", 4444))
    async with httpx.AsyncClient(
        transport=transport,
        base_url="http://orch:8002",
        headers={"Authorization": f"Bearer {token}"},
    ) as c:
        for method, path in (
            ("GET", A),
            ("GET", f"{A}/download?key={PRE}x.tar.zst"),
            ("GET", "/api/v1/cache/cleanup/preview"),
            ("DELETE", A),
        ):
            r = await c.request(method, path)
            assert r.status_code == 403, (method, path, r.text)
    assert s3.creds == []
