"""wrapper.managed_settings: spec validation, the client PUT and the bootstrapper overlay."""

from __future__ import annotations

import json

import httpx
import pytest
from pydantic import ValidationError
from test_wrapper_session_bootstrap import Env, env, fake  # noqa: F401

from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.db import sessions
from agents_orchestrator.spec import MANAGED_SETTINGS_MAX, SessionSpec, WrapperSettings
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import WrapperError

OVERLAY = {"env": {"FOO": "bar"}, "enabledPlugins": {"x@y": True}}


def test_wrapper_settings_managed_settings_accepts_object():
    w = WrapperSettings(managed_settings=OVERLAY)
    assert w.managed_settings == OVERLAY
    assert SessionSpec(wrapper={"managed_settings": OVERLAY}).wrapper.managed_settings == OVERLAY
    assert WrapperSettings().managed_settings is None
    assert WrapperSettings().model_dump(mode="json", exclude_none=True) == {}


def test_managed_settings_too_large_rejected():
    def size(x: dict) -> int:
        return len(json.dumps({"overlay": x}, separators=(",", ":")))

    base = {"k": ""}
    fill = MANAGED_SETTINGS_MAX - size(base)
    ok = {"k": "a" * fill}
    assert size(ok) == MANAGED_SETTINGS_MAX
    WrapperSettings(managed_settings=ok)
    with pytest.raises(ValidationError, match="too large"):
        WrapperSettings(managed_settings={"k": "a" * (fill + 1)})


@pytest.mark.parametrize(
    "name", ["API_TOKEN", "MY_KEY", "client_secret", "DB_PASSWORD", "OAUTH_AUTH", "CREDENTIALS"]
)
def test_managed_settings_rejects_secret_env(name):
    with pytest.raises(ValidationError) as ei:
        WrapperSettings(managed_settings={"env": {name: "CANARY-orch-mset"}})
    with pytest.raises(ValidationError) as nested:
        SessionSpec(wrapper={"managed_settings": {"env": {name: "CANARY-orch-mset"}}})
    # the API reports `loc: msg` only, never the input
    api_text = "; ".join(f"{e['loc']}: {e['msg']}" for e in nested.value.errors())
    assert "CANARY-orch-mset" not in api_text and name not in api_text
    text = str(ei.value)
    assert "wrapper.managed_settings.env may not carry credentials" in text
    assert "CANARY-orch-mset" not in text
    assert name not in text.replace("wrapper.managed_settings.env", "")


async def test_client_put_managed_settings():
    seen = {}

    def h(r: httpx.Request) -> httpx.Response:
        seen.update(method=r.method, path=r.url.path, body=json.loads(r.content), auth=r.headers)
        return httpx.Response(200, json={"applied": True})

    http = httpx.AsyncClient(transport=httpx.MockTransport(h), base_url="http://w:8080")
    c = WrapperClient("http://w:8080", "tok", http=http)
    assert await c.put_managed_settings(OVERLAY) == {"applied": True}
    assert seen["method"] == "PUT" and seen["path"] == "/v1/managed-settings"
    assert seen["body"] == {"overlay": OVERLAY}
    assert seen["auth"]["authorization"] == "Bearer tok"


class Recording(WrapperClient):
    calls: list[str]
    put_error: WrapperError | None = None

    def __init__(self, *a, **kw) -> None:
        super().__init__(*a, **kw)
        self.calls = []

    async def put_managed_settings(self, overlay):
        self.calls.append("put_managed_settings")
        if self.put_error is not None:
            raise self.put_error
        return {}

    async def list_sessions(self, *a, **kw):
        self.calls.append("list_sessions")
        return await super().list_sessions(*a, **kw)

    async def create_session(self, *a, **kw):
        self.calls.append("create_session")
        return await super().create_session(*a, **kw)


async def _rec(env, fake, err=None) -> Recording:  # noqa: F811
    rec = Recording(fake.base_url, fake.token)
    rec.put_error = err
    env.client_override = rec
    return rec


async def test_bootstrap_puts_overlay_before_create(env, fake):  # noqa: F811
    await env.session("alpha", wrapper={"managed_settings": OVERLAY})
    rec = await _rec(env, fake)
    await env.rec.reconcile_once()
    assert rec.calls == ["put_managed_settings", "list_sessions", "create_session"]
    assert len(fake.sessions) == 1


async def test_bootstrap_without_overlay_does_not_put(env, fake):  # noqa: F811
    await env.session("alpha")
    rec = await _rec(env, fake)
    await env.rec.reconcile_once()
    assert rec.calls == ["list_sessions", "create_session"]


async def test_bootstrap_overlay_refused_records_failure(env, fake):  # noqa: F811
    c = await env.session("alpha", wrapper={"managed_settings": OVERLAY})
    rec = await _rec(env, fake, WrapperError(422, "locked_key", "locked"))
    await env.rec.reconcile_once()
    assert "create_session" not in rec.calls and not fake.sessions
    async with env.pool.connection() as conn:
        s = await sessions.get(conn, c.id)
        v = await sessions_api.one_view(conn, s, env.settings)
    (failure,) = v.summary.bootstrap_failures
    assert failure.step == "managed_settings"
    assert failure.message == "managed settings refused: locked_key"


async def test_bootstrap_overlay_5xx_is_deferred_not_recorded(env, fake):  # noqa: F811
    c = await env.session("alpha", wrapper={"managed_settings": OVERLAY})
    rec = await _rec(env, fake, WrapperError(503, "unavailable", "x"))
    await env.rec.reconcile_once()
    assert "create_session" not in rec.calls
    async with env.pool.connection() as conn:
        s = await sessions.get(conn, c.id)
        v = await sessions_api.one_view(conn, s, env.settings)
    assert v.summary.bootstrap_failures == []
