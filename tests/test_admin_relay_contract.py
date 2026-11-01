"""E2E1: the relay table is bound to the upstream APIs, both ways.

The fixtures are the committed OpenAPI documents of the upstream services on their main branch
(agents-webhook, agents-schedule, agents-matrix-connector). Refresh them when an upstream admin
route changes.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from agents_orchestrator.api.admin_relay import GUARDS, SERVICES

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
        field = guard.list_filter or guard.answer
        if field:
            ok = next(v for k, v in op["responses"].items() if k.startswith("2"))
            schema = ok["content"]["application/json"]["schema"]
            item = _resolve(doc, schema.get("items", schema))
            assert field in item["properties"], (service, method, template)
        if guard.list_filter == "schedule_id" or guard.answer == "schedule_id":
            assert (
                "preset_id"
                in _spec("schedule")["components"]["schemas"]["ScheduleView"]["properties"]
            )
        if guard.stored:
            list_path = {"endpoint": "/endpoints", "schedule": "/schedules", "room": "/rooms"}
            lister = _op(service, "GET", list_path[guard.stored])
            ok = next(v for k, v in lister["responses"].items() if k.startswith("2"))
            item = _resolve(doc, ok["content"]["application/json"]["schema"]["items"])
            assert "preset_id" in item["properties"]
