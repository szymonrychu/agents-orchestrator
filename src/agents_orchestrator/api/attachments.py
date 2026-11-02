"""Validation and decoding of message attachments (the api enforces the caps, not the db)."""

from __future__ import annotations

import base64
import binascii
import json
import re

from agents_orchestrator.api import schemas
from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.db import queue
from agents_orchestrator.settings import Settings

ALLOWED_MIMES: frozenset[str] = frozenset(
    {"image/png", "image/jpeg", "application/json", "text/plain"}
)

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
_PNG = b"\x89PNG\r\n\x1a\n"
_JPEG = b"\xff\xd8\xff"


def _bad(message: str) -> ApiError:
    return ApiError(422, "invalid_attachment", message)


def _check_content(name: str, mime: str, data: bytes) -> None:
    ok = True
    if mime == "image/png":
        ok = data.startswith(_PNG)
    elif mime == "image/jpeg":
        ok = data.startswith(_JPEG)
    elif mime == "application/json":
        try:
            json.loads(data)
        except ValueError:
            ok = False
    elif mime == "text/plain":
        try:
            data.decode("utf-8")
        except UnicodeDecodeError:
            ok = False
    if not ok:
        raise _bad(f"attachment {name!r} does not hold {mime} content")


def decode(
    items: list[schemas.MessageAttachment], settings: Settings
) -> list[queue.StoredAttachment]:
    """Validate and decode the attachments of one request; sizes are checked before decoding."""
    if len(items) > settings.attachments_max_count:
        raise ApiError(
            413, "too_large", f"at most {settings.attachments_max_count} attachments per message"
        )
    max_b64 = 4 * -(-settings.attachments_max_bytes // 3)
    out: list[queue.StoredAttachment] = []
    seen: set[str] = set()
    total = 0
    for item in items:
        if len(item.data_b64) > max_b64:
            raise ApiError(
                413,
                "too_large",
                f"attachment exceeds {settings.attachments_max_bytes} bytes",
            )
        if not _NAME.fullmatch(item.name):
            raise _bad("attachment name must match ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")
        if item.name in seen:
            raise _bad(f"duplicate attachment name {item.name!r}")
        seen.add(item.name)
        if item.mime not in ALLOWED_MIMES:
            raise _bad(f"attachment mime {item.mime!r} is not allowed")
        try:
            data = base64.b64decode(item.data_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise _bad(f"attachment {item.name!r} is not valid base64") from exc
        if not data:
            raise _bad(f"attachment {item.name!r} is empty")
        if len(data) > settings.attachments_max_bytes:
            raise ApiError(
                413,
                "too_large",
                f"attachment exceeds {settings.attachments_max_bytes} bytes",
            )
        total += len(data)
        if total > settings.attachments_max_total_bytes:
            raise ApiError(
                413,
                "too_large",
                f"attachments exceed {settings.attachments_max_total_bytes} bytes in total",
            )
        _check_content(item.name, item.mime, data)
        out.append(queue.StoredAttachment(item.name, item.mime, data))
    return out
