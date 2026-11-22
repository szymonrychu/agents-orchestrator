"""Agent routes of volumes (S-77 step 9): a session lists the volumes it mounts and saves one on
demand only when its attachment has backup_restore (Q12)."""

from __future__ import annotations

import pytest
from factories import insert_agent, insert_session
from fakes.fake_kube import FakeKube
from test_api_agent import client_for, headers, make_settings, set_ip
from test_api_sessions import assert_error

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import volumes

V = "/agent/v1/volumes"


async def volume(pool, name, state="ready"):
    async with pool.connection() as conn:
        v = await volumes.create(
            conn,
            name=name,
            projects=["default"],
            size="1Gi",
            storage_class="sc",
            created_by="u",
            actor_kind="user",
            actor="u",
        )
        await conn.execute("UPDATE volumes SET state = %s WHERE id = %s", (state, v.id))
    return v


async def session_with(pool, ip, atts):
    row = await insert_session(
        pool, spec={"replicas": 1, "volumes": [{**a, "volume": str(a["volume"])} for a in atts]}
    )
    agent = await insert_agent(pool, row["id"], ordinal=0, ready=True)
    await set_ip(pool, agent["id"], ip)
    return row


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool, kube=FakeKube(namespace="agents"))


async def call(app, row, ip, method, path):
    async with client_for(app, ip) as http:
        return await http.request(method, path, headers=headers(row["id"]))


async def test_status_lists_only_the_callers_attachments(app, pool):
    mine, other = await volume(pool, "mine"), await volume(pool, "other")
    row = await session_with(
        pool,
        "10.9.1.1",
        [{"volume": mine.id, "mode": "rw", "mount_path": "/m", "backup_restore": True}],
    )
    await session_with(pool, "10.9.1.2", [{"volume": other.id, "mode": "ro"}])
    r = await call(app, row, "10.9.1.1", "GET", V)
    assert r.status_code == 200, r.text
    assert r.json() == [
        {
            "name": "mine",
            "id": str(mine.id),
            "mount_path": "/m",
            "mode": "rw",
            "backup_restore": True,
            "state": "ready",
            "current_version": None,
            "last_save_at": None,
        }
    ]


@pytest.mark.parametrize(
    ("flag", "state", "name", "status", "code"),
    [
        (True, "ready", "data", 202, None),
        (False, "ready", "data", 403, "backup_not_enabled"),
        (True, "saving", "data", 409, "volume_busy"),
        (True, "ready", "elsewhere", 404, "volume_not_found"),
        (True, "ready", "nothing", 404, "volume_not_found"),
    ],
)
async def test_save_needs_an_attached_volume_with_backup_on_and_a_ready_state(
    app, pool, flag, state, name, status, code
):
    mine, _ = await volume(pool, "data", state), await volume(pool, "elsewhere")
    row = await session_with(
        pool, "10.9.1.3", [{"volume": mine.id, "mode": "rw", "backup_restore": flag}]
    )
    r = await call(app, row, "10.9.1.3", "POST", f"{V}/{name}/save")
    if code:
        assert_error(r, status, code)
    else:
        assert r.status_code == status, r.text
        assert r.json()["state"] == "saving" and r.json()["name"] == "data"
        async with pool.connection() as conn:
            events = (await volumes.events(conn, mine.id, limit=10)).items
        started = next(e for e in events if e.kind == "save_started")
        assert started.actor_kind == "agent" and started.detail.get("trigger") == "agent"
