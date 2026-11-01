"""README API coverage and the committed OpenAPI document (D2, D11)."""

from __future__ import annotations

import re
from pathlib import Path

from fastapi.routing import APIRoute

from agents_orchestrator.api import openapi
from agents_orchestrator.api.app import create_app
from agents_orchestrator.settings import Settings

ROOT = Path(__file__).resolve().parent.parent
README = (ROOT / "README.md").read_text(encoding="utf-8")
STREAMS = (ROOT / "src" / "agents_orchestrator" / "api" / "streams.py").read_text(encoding="utf-8")


def section(title: str) -> str:
    """The body of the `## title` section (up to the next H2)."""
    match = re.search(rf"^## {re.escape(title)}\s*$", README, re.MULTILINE)
    assert match, f"README has no '## {title}' section"
    rest = README[match.end() :]
    nxt = re.search(r"^## ", rest, re.MULTILINE)
    return rest[: nxt.start()] if nxt else rest


def _walk(router, prefix: str = ""):
    """(path, methods) of every route; FastAPI includes routers lazily, so descend into them."""
    for route in router.routes:
        if isinstance(route, APIRoute):
            yield prefix + route.path, route.methods
        elif hasattr(route, "original_router"):
            yield from _walk(route.original_router, prefix + route.include_context.prefix)


def app_routes() -> set[str]:
    app = create_app(Settings(database_url="postgresql://unused/db"), pool=None)
    out = set()
    for path, methods in _walk(app):
        if path.startswith("/api/v1"):
            out.update(f"{m} {path}" for m in methods - {"HEAD"})
    return out


def readme_routes() -> set[str]:
    found = re.findall(
        r"^### `(GET|POST|PATCH|PUT|DELETE) (/api/v1\S*)`\s*$", section("API"), re.MULTILINE
    )
    return {f"{m} {p}" for m, p in found}


def test_every_route_documented() -> None:
    expected = app_routes()
    assert len(expected) >= 16
    missing = expected - readme_routes()
    assert not missing, f"undocumented routes: {sorted(missing)}"


def test_no_stale_route_in_readme() -> None:
    stale = readme_routes() - app_routes()
    assert not stale, f"README documents routes the app lacks: {sorted(stale)}"


def test_openapi_json_matches_app() -> None:
    committed = (ROOT / "openapi.json").read_text(encoding="utf-8")
    assert committed == openapi.export(), (
        "run: uv run python -m agents_orchestrator.api.openapi > openapi.json"
    )
    assert committed.endswith("\n")


def test_sse_event_types_documented() -> None:
    emitted = set(re.findall(r'event="([a-z_]+)"', STREAMS))
    assert {
        "snapshot",
        "session_status",
        "agent_status",
        "message_status",
        "session_removed",
        "leader_changed",
    } <= emitted
    events = section("API").split("### Events", 1)
    assert len(events) == 2, "README ## API has no '### Events' subsection"
    rows = set(re.findall(r"^\|\s*`([a-z_]+)`\s*\|", events[1].split("\n### ")[0], re.MULTILINE))
    missing = emitted - rows
    assert not missing, f"undocumented SSE event names: {sorted(missing)}"


def test_readme_documents_allowlist_and_limits() -> None:
    api = section("API")
    for needle in (
        "forbidden",
        "session_limit",
        "ORCH_ALLOWED_CIDRS",
        "caps",
        "max_replicas",
        "additive",
    ):
        assert needle in api, needle


def test_architecture_documents_single_writer() -> None:
    arch = section("Architecture").lower()
    for needle in (
        "single-writer",
        "one writer per table",
        "session_changed",
        "agent_free",
        "polling",
    ):
        assert needle in arch, needle
    assert section("Data model").strip()


def test_readme_documents_leaving_agent_detection() -> None:
    scaling = section("API").split("### Scaling", 1)[1].split("\n### ")[0]
    for needle in ("spec.replicas", "ordinal", "status.drain", "leaving", "waiting", "since"):
        assert needle in scaling, needle


SERVICE_ROUTES = (
    "GET /api/v1/meta",
    "GET /api/v1/presets",
    "GET /api/v1/presets/{preset_id}",
    "GET /api/v1/sessions",
    "POST /api/v1/sessions",
    "GET /api/v1/sessions/{session_id}",
    "GET /api/v1/sessions/{session_id}/messages",
    "POST /api/v1/sessions/{session_id}/messages",
    "GET /api/v1/messages/{message_id}",
    "GET /api/v1/stream",
)


def test_service_port_documented() -> None:
    from agents_orchestrator.api.service_auth import ALLOWED_ROUTES

    assert len(SERVICE_ROUTES) == len(ALLOWED_ROUTES)
    config = section("Configuration")
    for name in ("ORCH_SERVICE_PORT", "ORCH_SERVICE_TOKENS", "ORCH_SERVICE_TOKENS_FILE"):
        assert name in config, f"{name} missing from Configuration"
    match = re.search(r"^### Service tokens\s*$", section("Security"), re.MULTILINE)
    assert match, "README ## Security has no '### Service tokens' subsection"
    body = section("Security")[match.end() :]
    body = body.split("\n### ")[0]
    for route in SERVICE_ROUTES:
        assert f"`{route}`" in body, f"service route not documented: {route}"
    for needle in ("openssl rand -hex 32", "sha256sum", "Bearer", "401", "403"):
        assert needle in body, f"Service tokens section lacks {needle!r}"
    assert "serviceApi" in section("Helm chart") and "-services" in section("Helm chart")


def test_readme_documents_workspace_cache() -> None:
    text = (ROOT / "README.md").read_text(encoding="utf-8")
    assert "\n## Workspace cache\n" in text
    section = text.split("\n## Workspace cache\n", 1)[1].split("\n## ", 1)[0]
    names = [f"ORCH_{n.upper()}" for n in Settings.model_fields if n.startswith("cache_")]
    assert names
    for name in names:
        assert f"`{name}`" in section, name
    for word in ("spec.cache", "status.cache", "skipped_links", "last writer wins", "presigned"):
        assert word in section, word
