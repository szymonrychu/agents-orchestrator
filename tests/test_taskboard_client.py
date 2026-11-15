"""The taskboard open-decisions client: filtering, identity headers and the never-raises rule."""

from __future__ import annotations

import json
import logging
import uuid

import httpx
import pytest

from agents_orchestrator.settings import Settings
from agents_orchestrator.taskboard_client import OpenDecision, open_decisions, post_notice

SESSION = uuid.uuid4()
TOKEN = "CANARY-taskboard-token"


def settings(**kw) -> Settings:
    values = {
        "database_url": "postgresql://unused/db",
        "allowed_cidrs": "10.1.2.0/24",
        "master_key": "k" * 32,
        "taskboard_url": "http://taskboard",
        "taskboard_token": TOKEN,
    }
    return Settings(**{**values, **kw})


def decision(id_, *, status="open", asking=None, question="q?") -> dict:
    return {
        "id": id_,
        "status": status,
        "asking_session": str(asking or SESSION),
        "question": question,
    }


def transport(handler) -> httpx.MockTransport:
    return httpx.MockTransport(handler)


async def test_filters_to_open_decisions_of_the_session():
    body = {
        "items": [
            {
                "key": "T-1",
                "decisions": [
                    decision(1, question="ship it?"),
                    decision(2, status="answered"),
                    decision(3, asking=uuid.uuid4()),
                ],
            },
            {"key": "T-2", "decisions": []},
            {"key": "T-3", "decisions": [decision(4, question="which?")]},
        ],
        "next_cursor": None,
    }
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return httpx.Response(200, json=body)

    result = await open_decisions(
        settings(), SESSION, "hive-a", "proj", transport=transport(handler)
    )
    assert result == [OpenDecision("T-1", "1", "ship it?"), OpenDecision("T-3", "4", "which?")]
    [request] = seen
    assert request.url.path == "/taskboard/api/v1/tasks"
    assert request.url.params["session"] == str(SESSION)
    assert request.url.params["waiting"] == "true"
    assert request.headers["authorization"] == f"Bearer {TOKEN}"
    assert request.headers["x-orch-session"] == str(SESSION)
    assert request.headers["x-orch-slug"] == "hive-a"
    assert request.headers["x-orch-projects"] == "proj"


async def test_none_when_unconfigured():
    cfg = settings(taskboard_url="", taskboard_token="")
    assert await open_decisions(cfg, SESSION, "s", "p") is None


def raising(exc):
    def handler(request):
        raise exc(request=request) if exc is not ValueError else exc("x")

    return handler


@pytest.mark.parametrize(
    "handler",
    [
        lambda r: httpx.Response(500, json={}),
        raising(httpx.ReadTimeout),
        raising(httpx.ConnectError),
        lambda r: httpx.Response(200, content=b"<html>not json"),
        lambda r: httpx.Response(200, json={"unexpected": True}),
    ],
    ids=["500", "timeout", "connect", "not-json", "bad-shape"],
)
async def test_none_on_any_failure(handler, caplog):
    result = await open_decisions(settings(), SESSION, "s", "p", transport=transport(handler))
    assert result is None
    assert TOKEN not in caplog.text


def _notice_settings() -> Settings:
    return settings(taskboard_token="CANARY-tb-0001", credential_notice_project="agents")


async def test_post_notice_request_shape():
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return httpx.Response(201, json={})

    ok = await post_notice(
        _notice_settings(),
        "credential-warn-2027-10-23",
        "t",
        "b",
        transport=transport(handler),
    )
    assert ok is True
    assert len(seen) == 1
    req = seen[0]
    assert req.method == "POST"
    assert req.url.path == "/taskboard/api/v1/tasks"
    assert req.headers["X-Orch-Session"] == "orchestrator-credential-watch"
    assert req.headers["X-Orch-Slug"] == "orchestrator"
    assert req.headers["X-Orch-Projects"] == "agents"
    assert req.headers["Authorization"] == "Bearer CANARY-tb-0001"
    assert json.loads(req.content) == {
        "title": "t",
        "body": "b",
        "kind": "task",
        "project": "agents",
        "labels": ["credential"],
        "client_key": "credential-warn-2027-10-23",
    }


@pytest.mark.parametrize("status,expected", [(201, True), (200, True), (500, False)])
async def test_post_notice_status(status, expected, caplog):
    caplog.set_level(logging.DEBUG)
    ok = await post_notice(
        _notice_settings(),
        "k",
        "t",
        "BODY-TEXT-XYZ",
        transport=transport(lambda r: httpx.Response(status, json={})),
    )
    assert ok is expected
    if status == 500:
        assert "status 500" in caplog.text
    assert "CANARY-tb-0001" not in caplog.text
    assert "BODY-TEXT-XYZ" not in caplog.text


async def test_post_notice_connect_error_is_false(caplog):
    caplog.set_level(logging.DEBUG)

    def handler(request):
        raise httpx.ConnectError("boom CANARY-tb-0001")

    ok = await post_notice(
        _notice_settings(), "k", "t", "BODY-TEXT-XYZ", transport=transport(handler)
    )
    assert ok is False
    assert "ConnectError" in caplog.text
    assert "CANARY-tb-0001" not in caplog.text
