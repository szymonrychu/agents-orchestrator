"""T-120 slice 2, task 2: the relay reads every object and refuses writes on unmanageable ones.

Decision 388: a GET is forwarded unfiltered (null-preset objects included); a write whose body
or stored object sits on a preset the superleader may not manage is 403 `not_manageable` and
never reaches the upstream. agents-usage is relayed read only.
"""

from __future__ import annotations

import pytest
import test_admin_relay
from test_admin_relay import (
    _body_for,
    _sent,
    ident,
    ids,
    make_app,
    relay_url,
)
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX

rig = test_admin_relay.rig  # the shared relay fixture


async def test_list_endpoints_includes_unmanageable_preset(rig):
    r = await rig.call("GET", relay_url("webhook", "/endpoints"))
    assert r.status_code == 200
    assert ids(r) == {"7", "8", "10"}  # visible, hidden and privileged


async def test_lists_of_events_schedules_runs_include_hidden_and_null(rig):
    r = await rig.call("GET", relay_url("webhook", "/events"))
    assert ids(r) == {"7", "8", "9"}
    r = await rig.call("GET", relay_url("schedule", "/schedules"))
    assert ids(r) == {str(ident("schedule", n)) for n in (7, 8, 10)}
    r = await rig.call("GET", relay_url("schedule", "/runs"))
    assert ids(r) == {str(ident("schedule", n)) for n in (7, 8, 9)}


async def test_get_schedule_of_unmanageable_preset_200(rig):
    for template in ("/schedules/{schedule_id}", "/schedules/{schedule_id}/runs"):
        r = await rig.call("GET", relay_url("schedule", template, 8))
        assert r.status_code == 200, r.text


async def test_history_records_of_unmanageable_200(rig):
    for n in (8, 9):
        r = await rig.call("GET", relay_url("history", "/sessions/{session_id}/records", n))
        assert r.status_code == 200, r.text


async def test_webhook_event_of_null_preset_200(rig):
    r = await rig.call("GET", relay_url("webhook", "/events/{id}", 9))
    assert r.status_code == 200 and r.json()["id"] == 9
    r = await rig.call("GET", relay_url("schedule", "/runs/{run_id}", 9))
    assert r.status_code == 200


WRITES_ON_STORED = [
    ("webhook", "PATCH", "/endpoints/{id}"),
    ("webhook", "DELETE", "/endpoints/{id}"),
    ("webhook", "POST", "/endpoints/{id}/clone"),
    ("webhook", "PUT", "/endpoints/{id}/filters"),
    ("webhook", "POST", "/endpoints/{id}/rotate"),
    ("schedule", "PUT", "/schedules/{schedule_id}"),
    ("schedule", "DELETE", "/schedules/{schedule_id}"),
    ("schedule", "POST", "/schedules/{schedule_id}/run"),
]


@pytest.mark.parametrize(("service", "method", "template"), WRITES_ON_STORED)
async def test_write_of_unmanageable_stored_object_403_not_forwarded(
    rig, service, method, template
):
    body = _body_for(rig, service, method, template, rig.vis)
    r = await rig.call(method, relay_url(service, template, 8), json=body)
    assert_error(r, 403, "not_manageable")
    assert not _sent(rig, service, method, template, 8)
    assert all(c.method != method for c in rig.fakes[service].calls)


async def test_patch_endpoint_of_unmanageable_403_not_forwarded(rig):
    body = _body_for(rig, "webhook", "PATCH", "/endpoints/{id}", rig.vis)
    r = await rig.call("PATCH", relay_url("webhook", "/endpoints/{id}", 8), json=body)
    assert_error(r, 403, "not_manageable")
    assert all(c.method != "PATCH" for c in rig.fakes["webhook"].calls)


@pytest.mark.parametrize(
    ("service", "method", "template"),
    [
        ("webhook", "POST", "/endpoints"),
        ("webhook", "POST", "/endpoints/{id}/clone"),
        ("schedule", "POST", "/schedules"),
        ("schedule", "PUT", "/schedules/{schedule_id}"),
    ],
)
async def test_create_with_unmanageable_body_preset_403(rig, service, method, template):
    body = _body_for(rig, service, method, template, rig.hid)
    r = await rig.call(method, relay_url(service, template), json=body)
    assert_error(r, 403, "not_manageable")
    assert not _sent(rig, service, method, template)


async def test_unknown_stored_object_stays_404(rig):
    body = _body_for(rig, "webhook", "PATCH", "/endpoints/{id}", rig.vis)
    r = await rig.call("PATCH", relay_url("webhook", "/endpoints/{id}", 404), json=body)
    assert_error(r, 404, "not_found")


async def test_history_cursor_header_unchanged_on_a_full_page(rig):
    r = await rig.call("GET", relay_url("history", "/sessions"), params={"limit": 3})
    assert r.headers["x-next-before-id"] == "7"
    assert len(r.json()) == 3


# ------------------------------------------------------------------------------------- usage


async def test_usage_history_relayed(rig):
    r = await rig.call(
        "GET",
        ADMIN_PREFIX + "/usage/history",
        params={"from": "2026-11-01T00:00:00Z", "to": "2026-11-02T00:00:00Z",
                "metric": "five_hour", "max_points": 50},
    )  # fmt: skip
    assert r.status_code == 200, r.text
    (call,) = rig.fakes["usage"].calls
    assert call.path == "/usage/v1/history"
    assert call.headers["authorization"] == f"Bearer {rig.tokens['usage']}"
    for want in ("from=", "to=", "metric=five_hour", "max_points=50"):
        assert want in call.query


async def test_usage_sessions_relayed(rig):
    r = await rig.call("GET", ADMIN_PREFIX + "/usage/sessions", params={"limit": 5})
    assert r.status_code == 200, r.text
    (call,) = rig.fakes["usage"].calls
    assert call.path == "/usage/v1/sessions" and call.query == "limit=5"
    assert call.headers["authorization"] == f"Bearer {rig.tokens['usage']}"


@pytest.mark.parametrize("path", ["/usage/history", "/usage/sessions"])
@pytest.mark.parametrize("method", ["POST", "PUT", "PATCH", "DELETE"])
async def test_usage_other_methods_405(rig, path, method):
    r = await rig.call(method, ADMIN_PREFIX + path)
    assert r.status_code == 405
    assert rig.fakes["usage"].calls == []


async def test_usage_unconfigured_503(pool, dsn):
    boss = await alone(pool, "usage-boss", superleader=True)
    app, *_ = make_app(dsn, pool, usage_url="", usage_token="")
    async with client_for(app, boss.leader_ip) as http:
        r = await http.get(ADMIN_PREFIX + "/usage/history", headers=headers(boss.id))
    assert_error(r, 503, "upstream_unconfigured")
