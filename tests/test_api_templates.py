"""POST /templates/preview: sandboxed render with a fixed sample context per surface."""

from __future__ import annotations

import pytest_asyncio
from test_api_sessions import client_for, make_settings

from agents_orchestrator.api import templates
from agents_orchestrator.api.app import create_app

URL = "/api/v1/templates/preview"


@pytest_asyncio.fixture
async def http(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        yield c


async def post(http, template, surface="web_chat_start", **extra):
    return await http.post(URL, json={"template": template, "surface": surface, **extra})


async def test_preview_renders_with_sample_context(http):
    r = await post(http, "hi {{ session_slug }} {{ user }}")
    assert r.status_code == 200
    body = r.json()
    sample = templates.SAMPLES["web_chat_start"]
    assert body["error"] is None
    assert body["rendered"] == f"hi {sample['session_slug']} {sample['user']}"
    assert body["variables"] == ["session_slug", "user"]


async def test_undeclared_variables_listed_sorted(http):
    r = await post(http, "{{ session_slug }} {{ zeta }} {{ alpha }}")
    assert r.json()["variables"] == ["alpha", "session_slug", "zeta"]
    assert r.json()["error"]["message"]


async def test_syntax_error_reports_line(http):
    r = await post(http, "ok\n{% if %}\n")
    body = r.json()
    assert r.status_code == 200
    assert body["rendered"] is None
    assert body["error"]["line"] == 2
    assert body["variables"] == []


PLAN_KEYS = {
    "web_chat_start": {"session_slug", "preset_name", "user", "now"},
    "preset_text": {"session_slug", "preset_name", "env", "now"},
    "init_script": {"session_slug", "preset_name", "env"},
    "archive_name": {"session_slug", "preset_name", "now", "date", "user"},
}


def test_samples_have_exactly_the_plan_keys():
    assert {k: set(v) for k, v in templates.SAMPLES.items()} == PLAN_KEYS


async def test_env_and_now_render_per_surface(http):
    r = await post(
        http,
        "{% for k, v in env.items() %}{{ k }}={{ v }};{% endfor %}{{ now }}",
        surface="preset_text",
    )
    assert r.json()["error"] is None
    assert r.json()["rendered"].endswith("2026-01-01T00:00:00Z")
    r = await post(http, "{{ now }}", surface="init_script")
    assert r.json()["error"] is not None


async def test_unknown_surface_is_422(http):
    r = await post(http, "x", surface="nope")
    assert r.status_code == 422


async def test_every_surface_has_a_sample(http):
    for surface in PLAN_KEYS:
        r = await post(http, "plain", surface=surface)
        assert r.json() == {"rendered": "plain", "error": None, "variables": []}


async def test_sample_context_is_deterministic(http):
    a = await post(http, "{{ session_slug }} {{ now }}", surface="web_chat_start")
    b = await post(http, "{{ session_slug }} {{ now }}", surface="web_chat_start")
    assert a.json() == b.json()
    assert a.json()["rendered"]


async def test_context_overrides_sample(http):
    r = await post(http, "{{ session_slug }}", context={"session_slug": "mine"})
    assert r.json()["rendered"] == "mine"


async def test_oversized_template_is_an_error(http):
    r = await post(http, "x" * (16 * 1024 + 1))
    body = r.json()
    assert r.status_code == 200
    assert body["rendered"] is None
    assert "limit" in body["error"]["message"]


async def test_archive_name_surface_previews_the_name(http):
    r = await post(http, "{{ preset_name }}/{{ now }}", surface="archive_name")
    assert r.json() == {
        "rendered": "default/20260101t000000z",
        "error": None,
        "variables": ["now", "preset_name"],
    }
