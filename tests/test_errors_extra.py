"""`ApiError.extra` fields merged into the error object, and the gate no create path can skip."""

from __future__ import annotations

import inspect
import json

import httpx

from agents_orchestrator.api import errors, sessions
from agents_orchestrator.api.errors import ApiError, error_response

USAGE = {"metric": "seven_day", "percent": 85.0, "limit": 80, "resets_at": "2026-11-02T12:00:00Z"}


def test_extra_merged():
    r = error_response(409, "usage_quota", "m", retry_after=5, extra={"usage": USAGE})
    assert r.status_code == 409
    assert r.headers["Retry-After"] == "5"
    assert json.loads(r.body) == {
        "error": {"code": "usage_quota", "message": "m", "retry_after": 5, "usage": USAGE}
    }


def test_no_extra_keeps_the_shape():
    r = error_response(409, "conflict", "m")
    assert json.loads(r.body) == {"error": {"code": "conflict", "message": "m"}}
    assert "Retry-After" not in r.headers


async def test_api_error_handler_sends_extra():
    from fastapi import FastAPI

    app = FastAPI()
    errors.install(app)

    @app.get("/boom")
    async def boom():
        raise ApiError(409, "usage_quota", "m", retry_after=7, extra={"usage": USAGE})

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t") as c:
        r = await c.get("/boom")
    assert r.status_code == 409
    assert r.headers["Retry-After"] == "7"
    assert r.json() == {
        "error": {"code": "usage_quota", "message": "m", "retry_after": 7, "usage": USAGE}
    }


def test_create_requires_usage_keyword():
    p = inspect.signature(sessions.create).parameters["usage"]
    assert p.kind is inspect.Parameter.KEYWORD_ONLY and p.default is inspect.Parameter.empty
