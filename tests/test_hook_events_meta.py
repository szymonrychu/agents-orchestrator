"""`/meta` hook events: one orchestrator list, equal to a snapshot of the wrapper's list."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from test_api_sessions import client_for, make_settings

from agents_orchestrator import spec
from agents_orchestrator.api.app import create_app

SNAPSHOT = Path(__file__).parent / "fixtures" / "wrapper_hook_events.json"


def test_orchestrator_hook_events_equal_the_wrapper_snapshot():
    # Regenerate the snapshot from claude_code_wrapper.config.hook_events_meta() when the wrapper
    # changes its events; there is no cross-service import.
    assert [asdict(s) for s in spec.HOOK_EVENT_SPECS] == json.loads(SNAPSHOT.read_text())


def test_derived_names_follow_the_specs():
    assert tuple(s.name for s in spec.HOOK_EVENT_SPECS) == spec.HOOK_EVENTS
    assert spec.TOOL_EVENTS == ("onAgentToolCall", "onToolResult")


async def test_meta_hook_events_shape_and_rules(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        events = (await c.get("/api/v1/meta")).json()["hook_events"]
    assert [e["name"] for e in events] == list(spec.HOOK_EVENTS)
    for e in events:
        assert set(e) == {
            "name",
            "can_veto",
            "supports_matcher",
            "supports_when",
            "supports_interval",
            "interval_required",
        }
    assert {e["name"] for e in events if e["supports_matcher"]} == {
        "onAgentToolCall",
        "onToolResult",
    }
    assert {e["name"] for e in events if e["supports_interval"]} == {"onTick"}
    assert {e["name"] for e in events if e["supports_when"]} == {"onTick"}
    assert {e["name"] for e in events if e["interval_required"]} == {"onTick"}
