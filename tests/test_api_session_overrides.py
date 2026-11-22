"""POST /sessions from a preset with `overrides` (agents-common `create_session`, S-83)."""

from __future__ import annotations

from test_api_sessions import assert_error, client_for, make_settings
from test_start_prompt_first import _preset, _rows

from agents_orchestrator.api.app import create_app

C = "/api/v1/sessions"


async def test_create_from_preset_with_overrides(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        pid = await _preset(
            http, "ov", {"replicas": 3, "max_replicas": 3, "start_prompt": "preset prompt"}
        )
        given = await http.post(
            C,
            json={
                "preset_id": pid,
                "slug": "given",
                "overrides": {"replicas": 1, "start_prompt": "from the schedule"},
            },
        )
        empty = await http.post(
            C,
            json={"preset_id": pid, "slug": "empty", "overrides": {"start_prompt": ""}},
        )
    assert given.status_code == 201, given.text
    assert given.json()["spec"]["replicas"] == 1
    assert [m["text"] for m in await _rows(pool, given.json()["id"])] == ["from the schedule"]
    assert empty.status_code == 201, empty.text
    assert empty.json()["spec"]["replicas"] == 3
    assert await _rows(pool, empty.json()["id"]) == []


async def test_overrides_need_preset(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        r = await http.post(C, json={"slug": "raw", "spec": {}, "overrides": {"start_prompt": "x"}})
    assert_error(r, 422, "overrides_need_preset")


async def test_overrides_replicas_above_max_refused(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as http:
        pid = await _preset(http, "ov-max", {"replicas": 1, "max_replicas": 2})
        r = await http.post(C, json={"preset_id": pid, "slug": "big", "overrides": {"replicas": 3}})
    assert_error(r, 422, "overrides_replicas_above_max")
