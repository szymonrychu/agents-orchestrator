"""`/meta` lists the permission modes the preset form and the session menu offer."""

from __future__ import annotations

from test_api_sessions import client_for, make_settings

from agents_orchestrator.api.app import create_app


async def test_meta_permission_modes(pool, dsn):
    app = create_app(make_settings(dsn), pool)
    async with client_for(app) as c:
        meta = (await c.get("/api/v1/meta")).json()
    assert meta["permission_modes"] == [
        {"value": "bypassPermissions", "label": "Allow all"},
        {"value": "auto", "label": "Auto"},
    ]


async def test_meta_reports_inactivity(pool, dsn):
    settings = make_settings(
        dsn,
        inactivity_reminder_seconds=3600,
        inactivity_close_seconds=14400,
        inactivity_handoff_seconds=300,
    )
    async with client_for(create_app(settings, pool)) as c:
        meta = (await c.get("/api/v1/meta")).json()
    assert meta["inactivity"] == {
        "reminder_seconds": 3600,
        "close_seconds": 14400,
        "handoff_seconds": 300,
    }
