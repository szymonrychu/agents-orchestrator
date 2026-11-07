"""Session configuration through the public API: wrapper block, lease, autoclose, view fields."""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest
from factories import insert_agent, insert_session
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app

C = "/api/v1/sessions"
P = "/api/v1/presets"

WRAPPER = {
    "hooks": {
        "onTurnComplete": [{"type": "command", "run": "echo done", "timeout_seconds": 5}],
        "onTick": [{"type": "webhook", "url": "https://example.com/h", "interval_seconds": 60}],
    },
    "auto_compact": {"idle_seconds": 120, "instructions": "keep the plan"},
    "system_append": "Be brief.",
    "permissions": {"allow": ["Bash(ls)"], "deny": ["WebFetch"]},
    "turn_stall_seconds": 600,
    "max_restarts": 2,
    "agent_commands": ["compact", "context"],
}
CONFIG = {
    "replicas": 2,
    "max_replicas": 3,
    "wrapper": WRAPPER,
    "autoclose_after_seconds": 3600,
}


def app_client(pool, dsn, **kw):
    return client_for(create_app(make_settings(dsn, **kw), pool))


def unset(value):
    """Drop the null keys a full model dump adds, so a stored spec compares with the sent one."""
    if isinstance(value, dict):
        return {k: unset(v) for k, v in value.items() if v is not None}
    if isinstance(value, list):
        return [unset(v) for v in value]
    return value


def assert_config(spec: dict) -> None:
    assert unset(spec["wrapper"]) == WRAPPER
    assert spec["autoclose_after_seconds"] == 3600


async def test_config_fields_round_trip_through_presets_and_sessions(pool, dsn):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "raw", "spec": CONFIG})
        assert r.status_code == 201, r.text
        assert_config(r.json()["spec"])
        assert_config((await http.get(f"{C}/{r.json()['id']}")).json()["spec"])

        r = await http.post(P, json={"name": "cfg", "spec": CONFIG})
        assert r.status_code == 201, r.text
        preset = r.json()
        assert_config(preset["spec"])
        r = await http.put(f"{P}/{preset['id']}", json={"name": "cfg", "spec": CONFIG})
        assert r.status_code == 200, r.text
        assert_config(r.json()["spec"])

        r = await http.post(C, json={"preset_id": preset["id"], "slug": "from-preset"})
        assert r.status_code == 201, r.text
        assert_config(r.json()["spec"])
        assert_config((await http.get(f"{C}/{r.json()['id']}")).json()["spec"])


BAD = [
    (
        {"wrapper": {"hooks": {"onNope": [{"type": "command", "run": "x"}]}}},
        "wrapper.hooks.onNope",
    ),
    ({"max_replicas": 3, "leader_lease_seconds": 600}, "leader_lease_seconds"),
    ({"autoclose_after_seconds": 60}, "autoclose_after_seconds"),
]


@pytest.mark.parametrize(("spec", "field"), BAD)
async def test_config_fields_refused_with_named_errors(pool, dsn, spec, field):
    async with app_client(pool, dsn) as http:
        r = await http.post(C, json={"slug": "bad", "spec": spec})
        assert_error(r, 422, "invalid_request")
        assert field in r.json()["error"]["message"]
        r = await http.post(P, json={"name": "bad", "spec": spec})
        assert_error(r, 422, "invalid_request")
        assert field in r.json()["error"]["message"]
        ok = await http.post(P, json={"name": "ok", "spec": {}})
        r = await http.put(f"{P}/{ok.json()['id']}", json={"name": "ok", "spec": spec})
        assert_error(r, 422, "invalid_request")
        assert field in r.json()["error"]["message"]


async def test_session_view_leader_and_autoclose_fields(pool, dsn):
    plain = await insert_session(pool, spec={"replicas": 1})
    timed = await insert_session(
        pool, spec={"replicas": 2, "max_replicas": 2, "autoclose_after_seconds": 3600}
    )
    a0 = await insert_agent(pool, timed["id"], ordinal=0, ready=True)
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO session_leaders (session_id, agent_id, epoch, since, reason)"
            " VALUES (%s, %s, 4, now() - interval '1 hour', 'compaction')",
            (timed["id"], a0["id"]),
        )
        cur = await conn.execute(
            "SELECT since FROM session_leaders WHERE session_id = %s", (timed["id"],)
        )
        since = (await cur.fetchone())[0]
    async with app_client(pool, dsn) as http:
        one = (await http.get(f"{C}/{plain['id']}")).json()
        assert (one["closes_at"], one["leader_since"], one["leader_reason"]) == (None, None, None)
        got = (await http.get(f"{C}/{timed['id']}")).json()
        listed = {v["id"]: v for v in (await http.get(C)).json()}[str(timed["id"])]
    for view in (got, listed):
        assert datetime.fromisoformat(view["closes_at"]) == timed["created_at"] + timedelta(
            seconds=3600
        )
        assert datetime.fromisoformat(view["leader_since"]) == since
        assert view["leader_reason"] == "compaction"
