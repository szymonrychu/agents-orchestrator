"""E2E1: the relay table is bound to the upstream APIs, both ways.

The fixtures are the committed OpenAPI documents of the upstream services on their main branch
(agents-webhook, agents-schedule, agents-matrix-connector). Refresh them when an upstream admin
route changes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agents_orchestrator.api.admin_relay import SERVICES

FIXTURES = Path(__file__).parent / "fixtures" / "upstream"
SPECS = {
    "webhook": "agents-webhook.openapi.json",
    "schedule": "agents-schedule.openapi.json",
    "connector": "agents-matrix-connector.openapi-agent.json",
}
HTTP = {"get", "put", "post", "patch", "delete"}


def upstream_routes(service: str) -> set[tuple[str, str]]:
    """(METHOD, path below the upstream prefix) of the admin surface in the upstream spec."""
    prefix = SERVICES[service][1]
    paths = json.loads((FIXTURES / SPECS[service]).read_text())["paths"]
    return {
        (method.upper(), path[len(prefix) :])
        for path, item in paths.items()
        if path.startswith(prefix + "/")
        for method in item
        if method in HTTP
    }


@pytest.mark.parametrize("service", sorted(SERVICES))
def test_every_relay_entry_exists_upstream(service):
    relay = {(m, p) for m, p, _ in SERVICES[service][3]}
    assert relay - upstream_routes(service) == set()


@pytest.mark.parametrize("service", sorted(SERVICES))
def test_every_upstream_admin_route_is_relayed(service):
    relay = {(m, p) for m, p, _ in SERVICES[service][3]}
    assert upstream_routes(service) - relay == set()
