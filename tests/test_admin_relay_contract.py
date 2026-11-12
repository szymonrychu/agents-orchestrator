"""E2E1: the relay table is bound to the upstream APIs, both ways.

The fixtures are the committed OpenAPI documents of the upstream services on their main branch
(agents-webhook, agents-schedule, agents-matrix-connector). Refresh them when an upstream admin
route changes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agents_orchestrator.api.admin_relay import GUARDS, HISTORY_DEFAULT_LIMIT, SERVICES

FIXTURES = Path(__file__).parent / "fixtures" / "upstream"
SPECS = {
    "webhook": "agents-webhook.openapi.json",
    "schedule": "agents-schedule.openapi.json",
    "connector": "agents-matrix-connector.openapi-agent.json",
    "history": "agents-history.openapi.json",
    "usage": "agents-usage.openapi.json",
}
# usage routes of the usage gate and the statusLine ingest, not part of the admin surface
NOT_RELAYED = {"/usage/v1/current", "/usage/v1/samples"}
HTTP = {"get", "put", "post", "patch", "delete"}


def upstream_routes(service: str) -> set[tuple[str, str]]:
    """(METHOD, path below the upstream prefix) of the admin surface in the upstream spec."""
    prefix = SERVICES[service][1]
    paths = json.loads((FIXTURES / SPECS[service]).read_text())["paths"]
    return {
        (method.upper(), path[len(prefix) :])
        for path, item in paths.items()
        if path.startswith(prefix + "/") and path not in NOT_RELAYED
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


def test_every_relay_route_has_a_guard():
    routes = {(s, m, t) for s, (_, _, _, rs) in SERVICES.items() for m, t, _ in rs}
    assert set(GUARDS) == routes


def _spec(service: str) -> dict:
    return json.loads((FIXTURES / SPECS[service]).read_text())


def _resolve(doc: dict, schema: dict) -> dict:
    if "$ref" in schema:
        node = doc
        for part in schema["$ref"].removeprefix("#/").split("/"):
            node = node[part]
        return _resolve(doc, node)
    return schema


def _op(service: str, method: str, template: str) -> dict:
    return _spec(service)["paths"][SERVICES[service][1] + template][method.lower()]


def _variants(doc: dict, schema: dict) -> list[dict]:
    schema = _resolve(doc, schema)
    options = schema.get("anyOf") or schema.get("oneOf")
    return [v for o in options for v in _variants(doc, o)] if options else [schema]


def test_guard_fields_exist_upstream():
    for (service, method, template), guard in GUARDS.items():
        doc = _spec(service)
        op = _op(service, method, template)
        if guard.body_preset:
            schema = op["requestBody"]["content"]["application/json"]["schema"]
            assert any("preset_id" in v.get("properties", {}) for v in _variants(doc, schema)), (
                service,
                method,
                template,
            )
        if guard.rule:
            schemas = doc["components"]["schemas"]
            assert {"list", "kind", "value"} <= set(schemas["RuleIn"]["properties"])
            assert {"allow", "deny"} <= set(schemas["RulesView"]["properties"])
            assert "id" in schemas["RuleView"]["properties"]
            assert {"room_id", "preset_id"} <= set(schemas["RoomView"]["properties"])
        if guard.cursor:
            params = {p["name"]: p for p in op["parameters"]}
            assert params["limit"]["schema"]["default"] == HISTORY_DEFAULT_LIMIT, template
            assert "before_id" in params, template
        if guard.stored:
            list_path = {"endpoint": "/endpoints", "schedule": "/schedules"}
            lister = _op(service, "GET", list_path[guard.stored])
            ok = next(v for k, v in lister["responses"].items() if k.startswith("2"))
            item = _resolve(doc, ok["content"]["application/json"]["schema"]["items"])
            assert "preset_id" in item["properties"]
