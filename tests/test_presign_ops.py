"""List, delete and download-presign operations on the cache bucket (botocore Stubber)."""

from __future__ import annotations

from datetime import UTC, datetime
from urllib.parse import parse_qs, unquote, urlsplit

import pytest
from botocore.stub import Stubber

from agents_orchestrator.cache import presign as p
from agents_orchestrator.cache.presign import (
    ArchiveObject,
    DeleteFailure,
    S3Credentials,
    delete_keys,
    latest_under,
    list_archives,
    list_page,
    presign_get,
)
from agents_orchestrator.settings import Settings

PREFIX = "workspace-cache/"
WHEN = datetime(2026, 10, 31, 12, tzinfo=UTC)


def _settings(**kw) -> Settings:
    kw.setdefault("database_url", "postgresql://x/y")
    kw.setdefault("agent_image", "img:1")
    kw.setdefault("cache_s3_endpoint", "http://rgw:80")
    kw.setdefault("cache_s3_bucket", "b")
    kw.setdefault("cache_s3_path_style", True)
    kw.setdefault("cache_s3_access_key_id", "AKIATESTCACHE")
    kw.setdefault("cache_s3_secret_access_key", "reconciler-example-secret")
    return Settings(**kw)


@pytest.fixture
def stub(monkeypatch):
    """The cached client, wrapped in a Stubber; records the credentials each call asked for."""
    calls: list[tuple[str, str]] = []
    real = p._client.__wrapped__

    holder: dict[tuple[str, str], object] = {}

    def fake(endpoint, region, path_style, verify, key_id, secret):
        calls.append((key_id, secret))
        client = holder.get((key_id, secret)) or real(
            endpoint, region, path_style, verify, key_id, secret
        )
        holder[(key_id, secret)] = client
        return client

    monkeypatch.setattr(p, "_client", fake)
    first = p._client(
        "http://rgw:80", "us-east-1", True, True, "AKIATESTCACHE", "reconciler-example-secret"
    )
    stubber = Stubber(first)
    stubber.activate()
    stubber.calls = calls
    stubber.holder = holder
    yield stubber
    stubber.assert_no_pending_responses()


def _obj(key, size=10, when=WHEN):
    return {"Key": key, "Size": size, "LastModified": when, "ETag": '"x"'}


def _page(keys, token=None, truncated=False):
    body = {"IsTruncated": truncated, "Contents": [_obj(k) for k in keys], "KeyCount": len(keys)}
    if truncated:
        body["NextContinuationToken"] = token
    return body


def _expect_list(stub, prefix, response, token=None):
    params = {"Bucket": "b", "Prefix": prefix, "MaxKeys": 1000}
    if token:
        params["ContinuationToken"] = token
    stub.add_response("list_objects_v2", response, params)


def test_list_archives_paginates_at_1000(stub):
    _expect_list(stub, PREFIX, _page([f"{PREFIX}a.tar.zst"], "t1", True))
    _expect_list(stub, PREFIX, _page([f"{PREFIX}b.tar.zst"]), token="t1")
    got = list_archives(_settings(), PREFIX)
    assert got == [
        ArchiveObject(f"{PREFIX}a.tar.zst", 10, WHEN),
        ArchiveObject(f"{PREFIX}b.tar.zst", 10, WHEN),
    ]


def test_list_archives_absent_contents_is_empty(stub):
    _expect_list(stub, PREFIX, {"IsTruncated": False, "KeyCount": 0})
    assert list_archives(_settings(), PREFIX) == []


def test_latest_under_picks_the_max_key(stub):
    pre = f"{PREFIX}proj/"
    keys = [
        f"{pre}20260101t000000z.tar.zst",
        f"{pre}20261031t120000z.tar.zst",
        f"{pre}20260601t000000z.tar.zst",
    ]
    _expect_list(stub, pre, _page(keys))
    got = latest_under(_settings(), pre)
    assert got is not None and got.key == keys[1]


def test_latest_under_ignores_nested_and_foreign_objects(stub):
    pre = f"{PREFIX}foo/"
    _expect_list(
        stub,
        pre,
        _page([f"{pre}old.tar.zst", f"{pre}bar/zzz.tar.zst", f"{pre}note.txt", f"{pre}"]),
    )
    got = latest_under(_settings(), pre)
    assert got is not None and got.key == f"{pre}old.tar.zst"


def test_latest_under_empty_is_none(stub):
    _expect_list(stub, f"{PREFIX}foo/", {"IsTruncated": False, "KeyCount": 0})
    assert latest_under(_settings(), f"{PREFIX}foo/") is None


def test_latest_under_requires_a_trailing_slash(stub):
    with pytest.raises(ValueError):
        latest_under(_settings(), f"{PREFIX}foo")


def test_foo_prefix_is_listed_with_its_slash_only(stub):
    # the prefix handed to S3 keeps the '/', so `foo/` cannot match `foo-bar/`
    _expect_list(stub, f"{PREFIX}foo/", {"IsTruncated": False, "KeyCount": 0})
    list_archives(_settings(), f"{PREFIX}foo/")


def _delete_params(keys):
    return {
        "Bucket": "b",
        "Delete": {"Objects": [{"Key": k} for k in keys], "Quiet": True},
    }


def test_delete_keys_batches_at_1000_and_is_quiet(stub):
    keys = [f"{PREFIX}k{i}.tar.zst" for i in range(2001)]
    for chunk in (keys[:1000], keys[1000:2000], keys[2000:]):
        stub.add_response("delete_objects", {}, _delete_params(chunk))
    assert delete_keys(_settings(), keys) == []


def test_delete_keys_returns_per_key_errors(stub):
    keys = [f"{PREFIX}a.tar.zst", f"{PREFIX}b.tar.zst"]
    stub.add_response(
        "delete_objects",
        {"Errors": [{"Key": keys[1], "Code": "AccessDenied", "Message": "nope"}]},
        _delete_params(keys),
    )
    assert delete_keys(_settings(), keys) == [DeleteFailure(keys[1], "AccessDenied", "nope")]


def test_delete_keys_empty_makes_no_call(stub):
    assert delete_keys(_settings(), []) == []


def test_delete_keys_continues_past_a_failing_batch(stub):
    keys = [f"{PREFIX}k{i}.tar.zst" for i in range(1001)]
    stub.add_client_error(
        "delete_objects", "InternalError", expected_params=_delete_params(keys[:1000])
    )
    stub.add_response("delete_objects", {}, _delete_params(keys[1000:]))
    got = delete_keys(_settings(), keys)
    assert [f.key for f in got] == keys[:1000]
    assert {f.code for f in got} == {"InternalError"}


def test_presign_get_signs_the_disposition():
    url = presign_get(_settings(), f"{PREFIX}proj/x.tar.zst", disposition="x.tar.zst")
    q = {k: v[0] for k, v in parse_qs(urlsplit(url).query).items()}
    assert q["response-content-disposition"] == 'attachment; filename="x.tar.zst"'
    assert q["X-Amz-Expires"] == "300"
    assert unquote(urlsplit(url).path).endswith("/b/workspace-cache/proj/x.tar.zst")


def test_presign_get_without_disposition_has_none():
    url = presign_get(_settings(), f"{PREFIX}x.tar.zst")
    assert "response-content-disposition" not in url


@pytest.mark.parametrize("name", ["a\r\nb", "a\x00b", "a\x7fb", 'a"b', "a\\b", ""])
def test_presign_get_rejects_bad_filenames(name):
    with pytest.raises(ValueError):
        presign_get(_settings(), f"{PREFIX}x.tar.zst", disposition=name)


def test_presign_get_rejects_keys_outside_the_prefix_or_suffix():
    for bad in ("other/x.tar.zst", f"{PREFIX}x.txt", f"{PREFIX}"):
        with pytest.raises(ValueError):
            presign_get(_settings(), bad)


def test_explicit_credentials_pick_another_client(stub):
    creds = S3Credentials("AKIAAPIROLE", "api-example-secret")
    _expect_list(stub, PREFIX, {"IsTruncated": False, "KeyCount": 0})
    # the reconciler client is the stubbed one; the api client is a different, unstubbed one
    list_archives(_settings(), PREFIX)
    assert stub.calls[-1] == ("AKIATESTCACHE", "reconciler-example-secret")
    url = presign_get(_settings(), f"{PREFIX}x.tar.zst", credentials=creds)
    assert "AKIAAPIROLE" in url
    assert stub.calls[-1] == ("AKIAAPIROLE", "api-example-secret")


def test_client_config_uses_when_required_checksums_timeouts_and_retries():
    client = p._client(
        "http://rgw:80", "us-east-1", True, True, "AKIACFGCHECK", "cfg-example-secret"
    )
    cfg = client.meta.config
    assert cfg.request_checksum_calculation == "when_required"
    assert cfg.response_checksum_validation == "when_required"
    assert cfg.connect_timeout and cfg.read_timeout
    assert cfg.retries["total_max_attempts"] >= 3


def test_list_page_returns_the_token_of_the_next_page(stub):
    stub.add_response(
        "list_objects_v2",
        _page([f"{PREFIX}a.tar.zst"], "t9", True),
        {"Bucket": "b", "Prefix": PREFIX, "MaxKeys": 5},
    )
    stub.add_response(
        "list_objects_v2",
        _page([f"{PREFIX}b.tar.zst"]),
        {"Bucket": "b", "Prefix": PREFIX, "MaxKeys": 5, "ContinuationToken": "t9"},
    )
    items, token = list_page(_settings(), PREFIX, limit=5)
    assert [o.key for o in items] == [f"{PREFIX}a.tar.zst"] and token == "t9"
    items, token = list_page(_settings(), PREFIX, limit=5, token="t9")
    assert [o.key for o in items] == [f"{PREFIX}b.tar.zst"] and token is None
