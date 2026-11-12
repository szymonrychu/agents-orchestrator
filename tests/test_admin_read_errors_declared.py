"""Every superleader admin route (reads of T-120 slice 2, writes of slice 3) declares in OpenAPI
the error statuses it really returns (403 `not_superleader`, 404 `not_found`, 400 `bad_request`,
502/503 upstream), like the archives reads. 422 is never pinned: FastAPI adds it to every route
with parameters."""

from __future__ import annotations

import pytest
from test_api_archives import settings_for

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions/{{slug}}"
DECLARED: dict[tuple[str, str], set[int]] = {
    ("GET", f"{ADMIN_PREFIX}/meta"): {403},
    ("GET", f"{ADMIN_PREFIX}/sessions"): {403},
    ("GET", f"{S}/activity"): {403, 404},
    ("GET", f"{S}/events"): {400, 403, 404},
    ("GET", f"{S}/messages"): {403, 404},
    ("GET", f"{S}/locks"): {403, 404},
    ("GET", f"{S}/commands"): {403, 404},
    ("GET", f"{ADMIN_PREFIX}/vault"): {403, 502, 503},
    ("GET", f"{ADMIN_PREFIX}/usage/history"): {403, 503},
    ("GET", f"{ADMIN_PREFIX}/usage/sessions"): {403, 503},
}


@pytest.mark.parametrize("method,path", sorted(DECLARED))
def test_admin_route_declares_its_error_statuses(method, path, dsn, pool):
    op = create_app(settings_for(dsn), pool).openapi()["paths"][path][method.lower()]
    codes = {int(c) for c in op["responses"]} - {200, 201, 202, 204, 422}
    assert codes == DECLARED[(method, path)]
