"""The credential notice sweep: one taskboard notice per threshold, retried until posted."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
from factories import insert_agent, insert_session, set_agent_status

from agents_orchestrator.api.credential_sweep import sweep_once
from agents_orchestrator.db import status
from agents_orchestrator.settings import Settings

E = datetime(2027, 10, 23, tzinfo=UTC)


def make(dsn: str = "postgresql://unused/db", **kw) -> Settings:
    values = {
        "database_url": dsn,
        "allowed_cidrs": "10.1.2.0/24",
        "master_key": "k" * 32,
        "taskboard_url": "http://taskboard",
        "taskboard_token": "CANARY-tb-0001",
        "credential_notice_project": "agents",
        "claude_credential_expires_at": E,
    }
    return Settings(**{**values, **kw})


def recorder(code: int = 201):
    seen: list[httpx.Request] = []

    def handler(request):
        seen.append(request)
        return httpx.Response(code, json={})

    return seen, httpx.MockTransport(handler)


async def test_sweep_posts_current_phase_once(pool):
    seen, tr = recorder()
    s = make()
    posted: set[str] = set()
    now = E - timedelta(days=5)
    assert await sweep_once(pool, s, now, posted, transport=tr) == ["credential-warn-2027-10-23"]
    assert await sweep_once(pool, s, now, posted, transport=tr) == []
    assert len(seen) == 1


async def test_sweep_jump_to_expired_posts_only_expired(pool):
    seen, tr = recorder()
    got = await sweep_once(pool, make(), E + timedelta(hours=1), set(), transport=tr)
    assert got == ["credential-expired-2027-10-23"]
    assert len(seen) == 1


async def test_sweep_posts_auth_failed_when_agent_failed(pool):
    c = await insert_session(pool, slug="cred-sweep")
    a = await insert_agent(pool, c["id"], ready=True)
    await set_agent_status(pool, a["id"], state="idle")
    async with pool.connection() as conn:
        await status.set_auth_failed(conn, a["id"], "revoked")
    seen, tr = recorder()
    got = await sweep_once(pool, make(), E - timedelta(days=30), set(), transport=tr)
    assert got == ["credential-auth_failed-2027-10-23"]


async def test_sweep_off_without_project(pool):
    seen, tr = recorder()
    got = await sweep_once(
        pool, make(credential_notice_project=""), E + timedelta(hours=1), set(), transport=tr
    )
    assert got == []
    assert seen == []


async def test_sweep_off_without_taskboard(pool):
    seen, tr = recorder()
    s = make(taskboard_url="", taskboard_token="")
    assert await sweep_once(pool, s, E + timedelta(hours=1), set(), transport=tr) == []
    assert seen == []


async def test_sweep_retries_after_failure(pool):
    now = E - timedelta(days=5)
    posted: set[str] = set()
    _, bad = recorder(500)
    assert await sweep_once(pool, make(), now, posted, transport=bad) == []
    assert posted == set()
    _, good = recorder(201)
    assert await sweep_once(pool, make(), now, posted, transport=good) == [
        "credential-warn-2027-10-23"
    ]


async def test_sweep_new_date_new_key(pool):
    seen, tr = recorder()
    e2 = E + timedelta(days=365)
    posted = {"credential-warn-2027-10-23"}
    got = await sweep_once(
        pool, make(claude_credential_expires_at=e2), e2 - timedelta(days=5), posted, transport=tr
    )
    assert got == ["credential-warn-2028-10-22"]
