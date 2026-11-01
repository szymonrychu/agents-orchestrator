"""Presigned cache object URLs, signed in the reconciler (design D2, D8).

The reconciler alone holds the S3 credentials; each cache Job gets one SigV4 URL for one method
(GET to restore, PUT to save) on exactly one object, expiring after the Job's own deadline
(`cache_timeout_seconds`) plus 600 seconds. Presigning is local: no network call is made.

There is one object per target: `<prefix><name>.tar.zst` for a workspace cache name (the workdir)
and `<prefix>sessions/<slug>.tar.zst` for a session's Claude transcripts (D7). A cache name never
starts with `sessions` (`cache.keys.validate_name`), so the two never name the same object.

Besides presigning, this module lists and deletes archives (the sweep and the api, which run
with their own credentials: `S3Credentials`) and presigns a download GET with a Content-Disposition
filename. They all share the one cached `_client`.

A PUT URL also signs `Content-Type` (the one shared `transfer.CONTENT_TYPE` the CLI sends):
Ceph RGW treats a PUT that carries an unsigned Content-Type as anonymous and answers 403. GET signs
only the host. The secret is read from the settings (a `repr=False` field)
and handed straight to botocore; it is never logged and is not part of any URL.
"""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Literal, NamedTuple

from botocore.config import Config
from botocore.session import get_session

from agents_orchestrator.cache.transfer import CONTENT_TYPE
from agents_orchestrator.settings import Settings

EXPIRY_MARGIN_SECONDS = 600
SUFFIX = ".tar.zst"
LIST_PAGE_SIZE = 1000
DELETE_BATCH = 1000
DOWNLOAD_TTL_SECONDS = 300
_CONNECT_TIMEOUT_SECONDS = 5
_READ_TIMEOUT_SECONDS = 60
_DISPOSITION_FORBIDDEN = frozenset('"\\')
_OPERATIONS = {"GET": "get_object", "PUT": "put_object"}
CacheTarget = Literal["workdir", "sessions"]


def object_name(settings: Settings, key: str) -> str:
    """The one object per cache key, overwritten on every save."""
    return f"{settings.cache_s3_prefix}{key}.tar.zst"


def sessions_object_name(settings: Settings, slug: str) -> str:
    """The one transcripts object per session slug, overwritten on every save."""
    return f"{settings.cache_s3_prefix}sessions/{slug}.tar.zst"


def target_object_name(settings: Settings, target: CacheTarget, name: str) -> str:
    """The object of `target`: `name` is the cache key (workdir) or the session slug (sessions)."""
    if target == "workdir":
        return object_name(settings, name)
    if target == "sessions":
        return sessions_object_name(settings, name)
    raise ValueError(f"unknown cache target {target!r}")


class S3Credentials(NamedTuple):
    """An access key pair; the default is the reconciler's (`cache_s3_access_key_id`)."""

    access_key_id: str
    secret_access_key: str


@dataclass(frozen=True)
class ArchiveObject:
    key: str
    size: int
    last_modified: datetime


@dataclass(frozen=True)
class DeleteFailure:
    """One key S3 refused to delete in an otherwise successful DeleteObjects call."""

    key: str
    code: str
    message: str


@lru_cache(maxsize=8)
def _client(
    endpoint: str,
    region: str,
    path_style: bool,
    verify_tls: bool,
    access_key_id: str,
    secret_access_key: str,
) -> Any:
    return get_session().create_client(
        "s3",
        endpoint_url=endpoint or None,
        region_name=region,
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        verify=verify_tls,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path" if path_style else "virtual"},
            # RGW predating the default CRC32 trailer must not see one on DeleteObjects.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
            connect_timeout=_CONNECT_TIMEOUT_SECONDS,
            read_timeout=_READ_TIMEOUT_SECONDS,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


def _client_for(settings: Settings, credentials: S3Credentials | None = None) -> Any:
    key_id, secret = credentials or (
        settings.cache_s3_access_key_id,
        settings.cache_s3_secret_access_key,
    )
    return _client(
        settings.cache_s3_endpoint,
        settings.cache_s3_region,
        settings.cache_s3_path_style,
        settings.cache_s3_verify_tls,
        key_id,
        secret,
    )


def presign(
    settings: Settings,
    method: Literal["GET", "PUT"],
    name: str,
    target: CacheTarget = "workdir",
) -> str:
    """One SigV4 URL for `method` on the object of `target` and `name`, valid for the Job's life.

    `name` is the workspace cache name for the workdir and the session slug for the sessions.
    """
    return presign_object(settings, method, target_object_name(settings, target, name))


def presign_object(settings: Settings, method: Literal["GET", "PUT"], key: str) -> str:
    """One SigV4 URL for `method` on the full object `key`, valid for the Job's life."""
    operation = _OPERATIONS.get(method)
    if operation is None:
        raise ValueError(f"cache URLs are GET or PUT, not {method!r}")
    params = {"Bucket": settings.cache_s3_bucket, "Key": key}
    if method == "PUT":
        params["ContentType"] = CONTENT_TYPE
    url: str = _client_for(settings).generate_presigned_url(
        operation,
        Params=params,
        ExpiresIn=settings.cache_timeout_seconds + EXPIRY_MARGIN_SECONDS,
        HttpMethod=method,
    )
    return url


def list_archives(
    settings: Settings, prefix: str, *, credentials: S3Credentials | None = None
) -> list[ArchiveObject]:
    """Every object under the full key `prefix`, in listing order (pages of 1000)."""
    client = _client_for(settings, credentials)
    pages = client.get_paginator("list_objects_v2").paginate(
        Bucket=settings.cache_s3_bucket,
        Prefix=prefix,
        PaginationConfig={"PageSize": LIST_PAGE_SIZE},
    )
    return [
        ArchiveObject(o["Key"], int(o["Size"]), o["LastModified"])
        for page in pages
        for o in page.get("Contents", ())
    ]


def list_page(
    settings: Settings,
    prefix: str,
    *,
    token: str | None = None,
    limit: int = LIST_PAGE_SIZE,
    credentials: S3Credentials | None = None,
) -> tuple[list[ArchiveObject], str | None]:
    """One page of at most `limit` objects under `prefix` and the token of the next page, if any."""
    args: dict[str, Any] = {
        "Bucket": settings.cache_s3_bucket,
        "Prefix": prefix,
        "MaxKeys": min(max(limit, 1), LIST_PAGE_SIZE),
    }
    if token:
        args["ContinuationToken"] = token
    page = _client_for(settings, credentials).list_objects_v2(**args)
    found = [
        ArchiveObject(o["Key"], int(o["Size"]), o["LastModified"]) for o in page.get("Contents", ())
    ]
    return found, (page.get("NextContinuationToken") if page.get("IsTruncated") else None)


def latest_under(
    settings: Settings, prefix: str, *, credentials: S3Credentials | None = None
) -> ArchiveObject | None:
    """The newest archive directly under `prefix` (a full key prefix ending in `/`), or None.

    Newest is the greatest key: names carry a fixed-width `now`, so they sort chronologically.
    The trailing `/` keeps `foo/` from matching `foo-bar/`; deeper objects (`foo/bar/x`) belong to
    another group and non-archives are ignored.
    """
    if not prefix.endswith("/"):
        raise ValueError(f"a listing prefix ends in '/', got {prefix!r}")
    found = [
        o
        for o in list_archives(settings, prefix, credentials=credentials)
        if o.key.endswith(SUFFIX) and "/" not in o.key[len(prefix) :] and len(o.key) > len(prefix)
    ]
    return max(found, key=lambda o: o.key, default=None)


def _batches(keys: list[str]) -> Iterator[list[str]]:
    for start in range(0, len(keys), DELETE_BATCH):
        yield keys[start : start + DELETE_BATCH]


def delete_keys(
    settings: Settings, keys: list[str], *, credentials: S3Credentials | None = None
) -> list[DeleteFailure]:
    """Delete `keys` in batches of 1000 (Quiet) and return every per-key failure.

    S3 answers 200 with an `Errors` list for keys it refused; those are returned, not raised. A
    batch whose whole call fails (network, 5xx after retries) marks each of its keys failed with
    the client error's code, and the next batch still runs.
    """
    from botocore.exceptions import BotoCoreError, ClientError

    client = _client_for(settings, credentials)
    failures: list[DeleteFailure] = []
    for batch in _batches(list(keys)):
        try:
            result = client.delete_objects(
                Bucket=settings.cache_s3_bucket,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
        except (ClientError, BotoCoreError) as exc:
            code = (
                exc.response.get("Error", {}).get("Code", "")
                if isinstance(exc, ClientError)
                else ""
            )
            failures += [DeleteFailure(k, code or type(exc).__name__, str(exc)) for k in batch]
            continue
        failures += [
            DeleteFailure(e.get("Key", ""), e.get("Code", ""), e.get("Message", ""))
            for e in result.get("Errors", ())
        ]
    return failures


def presign_get(
    settings: Settings,
    key: str,
    *,
    disposition: str | None = None,
    expires: int = DOWNLOAD_TTL_SECONDS,
    credentials: S3Credentials | None = None,
) -> str:
    """A short-lived GET URL for the full object `key`, optionally forcing a download filename.

    `key` must be an archive inside the cache prefix. `disposition` is the bare filename: it is
    signed as `attachment; filename="<name>"` and must hold no control character, quote or
    backslash (RGW rejects control characters).
    """
    if not key.startswith(settings.cache_s3_prefix) or not key.endswith(SUFFIX):
        raise ValueError(f"{key!r} is not an archive under {settings.cache_s3_prefix!r}")
    if len(key) <= len(settings.cache_s3_prefix) + len(SUFFIX):
        raise ValueError("empty archive name")
    params: dict[str, Any] = {"Bucket": settings.cache_s3_bucket, "Key": key}
    if disposition is not None:
        bad = not disposition or any(
            ord(c) < 0x20 or ord(c) == 0x7F or c in _DISPOSITION_FORBIDDEN for c in disposition
        )
        if bad:
            raise ValueError(
                "download filename must be non-empty with no control character or quote"
            )
        params["ResponseContentDisposition"] = f'attachment; filename="{disposition}"'
    url: str = _client_for(settings, credentials).generate_presigned_url(
        "get_object", Params=params, ExpiresIn=expires, HttpMethod="GET"
    )
    return url
