"""Backups api: names, versions, manifest, download, copy, delete (api-role credentials)."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api import backups as backups_mod
from agents_orchestrator.api.app import create_app
from agents_orchestrator.cache.presign import DeleteFailure
from agents_orchestrator.db import backups, sessions
from agents_orchestrator.spec import SessionSpec

PRE = "workspace-cache/"
A = "/api/v1/backups"
NOW = datetime.now(UTC)
OLD = NOW - timedelta(days=90)


class FakeS3:
    """Replaces the botocore calls the router makes; records the credentials it was given."""

    def __init__(self):
        self.deleted: list[list[str]] = []
        self.copied: list[tuple[str, str]] = []
        self.fail: dict[str, DeleteFailure] = {}
        self.creds: list = []
        self.copy_error: Exception | None = None

    def delete_keys(self, settings, keys, *, credentials=None):
        self.creds.append(credentials)
        self.deleted.append(list(keys))
        return [self.fail[k] for k in keys if k in self.fail]

    def copy_object(self, settings, src, dst, *, client=None, credentials=None):
        self.creds.append(credentials)
        if self.copy_error:
            raise self.copy_error
        self.copied.append((src, dst))

    def list_archives(self, settings, prefix, *, credentials=None):
        self.creds.append(credentials)
        return []


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
    fake = FakeS3()
    monkeypatch.setattr(backups_mod.s3, "delete_keys", fake.delete_keys)
    monkeypatch.setattr(backups_mod.s3, "copy_object", fake.copy_object)
    monkeypatch.setattr(backups_mod.s3, "list_archives", fake.list_archives)
    return fake


def client(pool, dsn, **kw):
    return client_for(create_app(settings_for(dsn, **kw), pool))


NO_CREDS = {"cache_s3_api_access_key_id": "", "cache_s3_api_secret_access_key": ""}


async def add_version(pool, name, *, age=OLD, digest=None, size=100, manifest=None, **kw):
    """Record one version of `name` (the later call becomes current) aged `age`."""
    key = f"{PRE}b/{name}/{uuid.uuid4().hex[:8]}.tar.zst"
    async with pool.connection() as conn, conn.transaction():
        _, v = await backups.record_save(
            conn,
            name=name,
            object_key=key,
            size=size,
            digest=digest or uuid.uuid4().hex,
            manifest=manifest,
            base_version=None,
            origin=kw.get("origin", "save"),
            session_slug=kw.get("slug", "writer"),
            preset_id=None,
            project="p",
            created_by="actor",
            orphan=False,
            volume_writer=name.startswith(backups.RESERVED_PREFIX),
        )
        await conn.execute("UPDATE backup_versions SET created_at = %s WHERE id = %s", (age, v.id))
    return v


# ------------------------------------------------------------------ 503 without credentials


@pytest.mark.parametrize(
    ("method", "path", "body"),
    [
        ("GET", A, None),
        ("GET", f"{A}/proj/a/versions", None),
        ("GET", f"{A}/proj/a/versions/{uuid.uuid4()}/manifest", None),
        ("GET", f"{A}/proj/a/versions/{uuid.uuid4()}/download", None),
        ("GET", f"{A}/cleanup/preview", None),
        ("POST", f"{A}/copy", {"source": "a", "target": "b"}),
        ("DELETE", f"{A}/proj/a", None),
        ("DELETE", f"{A}/proj/a/versions/{uuid.uuid4()}", None),
    ],
)
async def test_503_without_api_credentials(pool, dsn, s3, method, path, body):
    async with client(pool, dsn, **NO_CREDS) as c:
        r = await c.request(method, path, json=body)
    assert_error(r, 503, "unavailable")
    assert s3.creds == []


async def test_old_archive_routes_404(pool, dsn, s3):
    async with client(pool, dsn) as c:
        for method, path in (
            ("GET", "/api/v1/cache/archives"),
            ("GET", "/api/v1/cache/archives/download"),
            ("DELETE", "/api/v1/cache/archives"),
            ("GET", "/api/v1/cache/cleanup/settings"),
            ("GET", "/api/v1/cache/cleanup/preview"),
            ("POST", "/api/v1/cache/cleanup/run"),
        ):
            r = await c.request(method, path)
            assert r.status_code == 404, (method, path, r.status_code)


# ------------------------------------------------------------------ list and versions


async def reference(pool, slug, version) -> None:
    """A session whose recorded status names `version` as its restore source."""
    async with pool.connection() as conn:
        s = await sessions.create(conn, slug, SessionSpec())
        await conn.execute(
            "UPDATE sessions SET status = jsonb_build_object('backup', jsonb_build_object("
            "'entries', jsonb_build_array(jsonb_build_object('version', %s::text)))) WHERE id = %s",
            (str(version.id), s.id),
        )


async def test_list_by_name(pool, dsn, s3):
    for i in range(5):
        await add_version(pool, f"proj/{i:02d}")
    await add_version(pool, "other/x")
    second = await add_version(pool, "proj/00")
    await reference(pool, "alpha", second)
    async with client(pool, dsn) as c:
        first = (await c.get(A, params={"limit": 3})).json()
        assert [i["name"] for i in first["items"]] == ["other/x", "proj/00", "proj/01"]
        assert first["next_cursor"]
        nxt = (await c.get(A, params={"limit": 3, "cursor": first["next_cursor"]})).json()
        assert [i["name"] for i in nxt["items"]] == ["proj/02", "proj/03", "proj/04"]
        assert nxt["next_cursor"] is None
        found = (await c.get(A, params={"q": "other"})).json()["items"]
        assert [i["name"] for i in found] == ["other/x"]
    by_name = {i["name"]: i for i in first["items"]}
    top = by_name["proj/00"]
    assert top["version_count"] == 2 and top["current"]["id"] == str(second.id)
    assert top["referenced_by"] == ["alpha"] and top["protected"]
    assert set(top["current"]) == {"id", "size", "saved_at", "session_slug", "origin"}
    assert by_name["proj/01"]["referenced_by"] == [] and not by_name["proj/01"]["protected"]


async def test_list_rejects_a_bad_cursor_and_limit(pool, dsn, s3):
    async with client(pool, dsn) as c:
        assert_error(await c.get(A, params={"cursor": "a b;c"}), 400, "bad_request")
        assert_error(await c.get(A, params={"limit": 0}), 422, "invalid_request")


async def test_versions_newest_first(pool, dsn, s3):
    old = await add_version(pool, "proj/a", age=NOW - timedelta(days=3))
    mid = await add_version(pool, "proj/a", age=NOW - timedelta(days=2))
    new = await add_version(pool, "proj/a", age=NOW - timedelta(days=1))
    async with client(pool, dsn) as c:
        r = await c.get(f"{A}/proj/a/versions", params={"limit": 2})
        body = r.json()
        assert [v["id"] for v in body["items"]] == [str(new.id), str(mid.id)]
        assert [v["current"] for v in body["items"]] == [True, False]
        rest = (await c.get(f"{A}/proj/a/versions", params={"cursor": body["next_cursor"]})).json()
        assert [v["id"] for v in rest["items"]] == [str(old.id)]
        assert_error(await c.get(f"{A}/proj/missing/versions"), 404, "backup_not_found")
        assert_error(await c.get(f"{A}/Bad Name/versions"), 400, "bad_request")


async def test_manifest_route(pool, dsn, s3):
    files = [{"path": "a.txt", "sha256": "ab", "size": 1, "mode": 420}]
    v = await add_version(pool, "proj/a", manifest=files)
    big = await add_version(pool, "proj/b")
    async with client(pool, dsn) as c:
        r = await c.get(f"{A}/proj/a/versions/{v.id}/manifest")
        assert r.json() == {"name": "proj/a", "version": str(v.id), "files": files}
        assert (await c.get(f"{A}/proj/b/versions/{big.id}/manifest")).json()["files"] is None
        r = await c.get(f"{A}/proj/a/versions/{uuid.uuid4()}/manifest")
        assert_error(r, 404, "version_not_found")


# ------------------------------------------------------------------ download


async def test_download_redirect(pool, dsn, s3):
    v = await add_version(pool, "proj/a")
    async with client(pool, dsn) as c:
        r = await c.get(f"{A}/proj/a/versions/{v.id}/download", follow_redirects=False)
        assert r.status_code == 302
        assert r.headers["cache-control"] == "no-store"
        loc = urlsplit(r.headers["location"])
        q = {k: x[0] for k, x in parse_qs(loc.query).items()}
        assert unquote(loc.path).endswith(f"/bucket/{v.object_key}")
        assert q["X-Amz-Expires"] == "300"
        assert q["response-content-disposition"] == (
            f'attachment; filename="proj-a-{str(v.id)[:8]}.tar.zst"'
        )
        assert "TESTIDAPIROLE" in q["X-Amz-Credential"]
        missing = await c.get(f"{A}/proj/a/versions/{uuid.uuid4()}/download")
        assert_error(missing, 404, "version_not_found")


# ------------------------------------------------------------------ copy


async def test_copy_creates_new_current_version(pool, dsn, s3):
    src = await add_version(pool, "proj/a", digest="d-a")
    async with client(pool, dsn) as c:
        r = await c.post(f"{A}/copy", json={"source": "proj/a", "target": "proj/b"})
    assert r.status_code == 201, r.text
    body = r.json()
    assert body["name"] == "proj/b" and body["origin"] == "copy" and body["current"] is True
    assert body["digest"] == "d-a" and body["size"] == src.size
    ((copied_src, copied_dst),) = s3.copied
    assert copied_src == src.object_key
    assert copied_dst.startswith(f"{PRE}b/proj/b/") and copied_dst.endswith(".tar.zst")
    assert s3.creds[0] == ("TESTIDAPIROLE", "api-example-secret")
    async with pool.connection() as conn:
        cur = await backups.current(conn, "proj/b")
        assert str(cur.id) == body["id"] and cur.object_key == copied_dst
        assert (await backups.current(conn, "proj/a")).id == src.id  # the source is untouched


async def test_copy_older_version_back(pool, dsn, s3):
    old = await add_version(pool, "proj/a", digest="d1", age=NOW - timedelta(days=2))
    new = await add_version(pool, "proj/a", digest="d2", age=NOW - timedelta(days=1))
    async with client(pool, dsn) as c:
        r = await c.post(
            f"{A}/copy",
            json={"source": "proj/a", "source_version": str(old.id), "target": "proj/a"},
        )
    assert r.status_code == 201, r.text
    async with pool.connection() as conn:
        cur = await backups.current(conn, "proj/a")
        page = await backups.versions(conn, "proj/a", limit=10, cursor=None)
    assert str(cur.id) == r.json()["id"] and cur.digest == "d1" and cur.origin == "copy"
    assert len(page.items) == 3 and new.id in {v.id for v in page.items}  # nothing lost
    assert {v.id: v.state for v in page.items}[new.id] == "superseded"


async def test_copy_same_content_records_nothing(pool, dsn, s3):
    cur = await add_version(pool, "proj/a", digest="d1")
    async with client(pool, dsn) as c:
        r = await c.post(f"{A}/copy", json={"source": "proj/a", "target": "proj/a"})
    assert r.status_code == 201 and r.json()["id"] == str(cur.id)
    assert s3.deleted == [[s3.copied[0][1]]]  # the fresh object is dropped


async def test_copy_missing_source_404(pool, dsn, s3):
    v = await add_version(pool, "proj/a")
    async with client(pool, dsn) as c:
        r = await c.post(f"{A}/copy", json={"source": "proj/none", "target": "proj/b"})
        assert_error(r, 404, "backup_not_found")
        r = await c.post(
            f"{A}/copy",
            json={"source": "proj/a", "source_version": str(uuid.uuid4()), "target": "proj/b"},
        )
        assert_error(r, 404, "version_not_found")
        r = await c.post(f"{A}/copy", json={"source": "proj/a", "target": "Bad Name"})
        assert_error(r, 400, "bad_request")
    assert s3.copied == []
    async with pool.connection() as conn:
        assert await backups.current(conn, "proj/b") is None
        assert (await backups.current(conn, "proj/a")).id == v.id


async def test_copy_storage_failure_is_502(pool, dsn, s3):
    from botocore.exceptions import ClientError

    await add_version(pool, "proj/a")
    s3.copy_error = ClientError({"Error": {"Code": "InternalError"}}, "CopyObject")
    async with client(pool, dsn) as c:
        r = await c.post(f"{A}/copy", json={"source": "proj/a", "target": "proj/b"})
    assert_error(r, 502, "storage_error")
    async with pool.connection() as conn:
        assert await backups.current(conn, "proj/b") is None


# ------------------------------------------------------------------ delete


async def test_delete_current_refused(pool, dsn, s3):
    await add_version(pool, "proj/a")
    cur = await add_version(pool, "proj/a")
    async with client(pool, dsn) as c:
        r = await c.delete(f"{A}/proj/a/versions/{cur.id}")
    assert_error(r, 409, "current_version")
    assert s3.deleted == []


async def test_delete_old_version_removes_row_then_object(pool, dsn, s3):
    old = await add_version(pool, "proj/a")
    await add_version(pool, "proj/a")
    async with client(pool, dsn) as c:
        r = await c.delete(f"{A}/proj/a/versions/{old.id}")
        assert r.status_code == 204
        assert_error(await c.delete(f"{A}/proj/a/versions/{old.id}"), 404, "version_not_found")
    assert s3.deleted == [[old.object_key]]
    async with pool.connection() as conn:
        assert await backups.get_version(conn, "proj/a", old.id) is None


async def test_delete_only_version_deletes_the_name(pool, dsn, s3):
    only = await add_version(pool, "proj/a")
    async with client(pool, dsn) as c:
        assert (await c.delete(f"{A}/proj/a/versions/{only.id}")).status_code == 204
        assert (await c.get(A)).json()["items"] == []


async def test_delete_refuses_a_referenced_version(pool, dsn, s3):
    old = await add_version(pool, "proj/a")
    await add_version(pool, "proj/a")
    async with pool.connection() as conn:
        s = await sessions.create(conn, "alpha", SessionSpec())
        await conn.execute(
            "UPDATE sessions SET status = jsonb_build_object('backup', jsonb_build_object("
            "'entries', jsonb_build_array(jsonb_build_object('version', %s::text)))) WHERE id = %s",
            (str(old.id), s.id),
        )
    async with client(pool, dsn) as c:
        r = await c.delete(f"{A}/proj/a/versions/{old.id}")
    assert_error(r, 409, "referenced")
    assert "alpha" in r.text and s3.deleted == []


async def test_delete_refuses_a_version_inside_the_grace_period(pool, dsn, s3):
    young = await add_version(pool, "proj/a", age=NOW - timedelta(seconds=5))
    await add_version(pool, "proj/a", age=NOW)
    async with client(pool, dsn) as c:
        assert_error(await c.delete(f"{A}/proj/a/versions/{young.id}"), 409, "too_young")
        await c.put(
            f"{A}/cleanup/settings",
            json={
                "enabled": True,
                "dry_run": True,
                "retention_days": 30,
                "keep_newest": 3,
                "interval_seconds": 3600,
                "grace_seconds": 0,
            },
        )
        assert (await c.delete(f"{A}/proj/a/versions/{young.id}")).status_code == 204


async def test_delete_name_removes_every_version(pool, dsn, s3):
    one = await add_version(pool, "proj/a")
    two = await add_version(pool, "proj/a")
    await add_version(pool, "proj/b")
    async with client(pool, dsn) as c:
        assert (await c.delete(f"{A}/proj/a")).status_code == 204
        assert_error(await c.delete(f"{A}/proj/a"), 404, "backup_not_found")
        assert [i["name"] for i in (await c.get(A)).json()["items"]] == ["proj/b"]
    assert sorted(s3.deleted[0]) == sorted([one.object_key, two.object_key])


async def test_delete_name_refused_while_a_session_references_a_version(pool, dsn, s3):
    v = await add_version(pool, "proj/a")
    await reference(pool, "alpha", v)
    async with client(pool, dsn) as c:
        assert_error(await c.delete(f"{A}/proj/a"), 409, "referenced")
