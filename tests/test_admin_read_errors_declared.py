"""Every superleader read added in T-120 slice 2 declares in OpenAPI the error statuses it really
returns (403 `not_superleader`, 404 `not_found`, 400 `bad_request`, 503 upstream), like the
archives reads."""

from __future__ import annotations

import pytest
from test_api_archives import settings_for

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app

S = f"{ADMIN_PREFIX}/sessions/{{slug}}"
DECLARED = {
    f"{ADMIN_PREFIX}/meta": {403},
    f"{ADMIN_PREFIX}/sessions": {403},
    f"{S}/activity": {403, 404},
    f"{S}/events": {400, 403, 404},
    f"{S}/messages": {403, 404},
    f"{S}/locks": {403, 404},
    f"{S}/commands": {403, 404},
    f"{ADMIN_PREFIX}/usage/history": {403, 503},
    f"{ADMIN_PREFIX}/usage/sessions": {403, 503},
}


@pytest.mark.parametrize("path", sorted(DECLARED))
def test_new_read_declares_its_error_statuses(path, dsn, pool):
    op = create_app(settings_for(dsn), pool).openapi()["paths"][path]["get"]
    codes = {int(c) for c in op["responses"]} - {200, 422}
    assert codes == DECLARED[path]
