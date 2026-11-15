"""T-225 canary: the screen tail of an `auth_failed` event and the taskboard token never leave the
places allowed for them. The tail is stored only in the wrapper event row (redacted upstream by
the wrapper) and is in no other table, response body, notice request or log record. The taskboard
token travels in a request header only."""

from __future__ import annotations

import base64
import logging
from datetime import UTC, datetime, timedelta

import httpx
from test_api_sessions import client_for, make_settings
from test_ingester import fetch, harness, status_row  # noqa: F401

from agents_orchestrator.api.app import create_app
from agents_orchestrator.api.credential_sweep import sweep_once

TAIL = "CANARY-orch-tail-0001"
TB_CANARY = "CANARY-orch-tb-0001"
E = datetime(2027, 10, 23, tzinfo=UTC)


async def table_texts(pool) -> list[tuple[str, str]]:
    """Every row of every table as text."""
    out: list[tuple[str, str]] = []
    async with pool.connection() as conn:
        cur = await conn.execute(
            "SELECT table_name FROM information_schema.tables "
            "WHERE table_schema = 'public' AND table_type = 'BASE TABLE'"
        )
        for (table,) in await cur.fetchall():
            cur = await conn.execute(f'SELECT to_jsonb(t)::text FROM "{table}" t')
            out.extend((table, row[0]) for row in await cur.fetchall())
    return out


async def test_credential_canary_never_leaks(harness, pool, dsn, caplog) -> None:  # noqa: F811
    caplog.set_level(logging.DEBUG)
    h = await harness()
    await h.start()
    await h.stored_through(h.last_fake_seq())
    h.fake.emit("auth_failed", {"reason": "expired", "tail": f"OAuth token revoked {TAIL}"})
    await h.stored_through(h.last_fake_seq())
    assert (await status_row(pool, h.agent_id))["auth_state"] == "failed"

    settings = make_settings(
        dsn,
        taskboard_url="http://taskboard",
        taskboard_token=TB_CANARY,
        credential_notice_project="agents",
        claude_credential_expires_at=E,
    )
    requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(201, json={})

    posted: set[str] = set()
    now = E - timedelta(days=5)
    done = await sweep_once(pool, settings, now, posted, transport=httpx.MockTransport(handler))
    assert sorted(done) == ["credential-auth_failed-2027-10-23", "credential-warn-2027-10-23"]
    assert len(requests) == 2

    bodies: list[str] = []
    app = create_app(settings, pool)
    async with client_for(app) as http:
        for path in (
            f"/api/v1/sessions/{h.session_id}/agents",
            f"/api/v1/sessions/{h.session_id}",
            "/api/v1/meta",
        ):
            r = await http.get(path)
            assert r.status_code == 200, (path, r.text)
            bodies.append(r.text)

    notices = [r.content.decode() for r in requests]
    forbidden = [TAIL, TB_CANARY]
    forbidden += [base64.b64encode(x.encode()).decode().rstrip("=") for x in forbidden]
    rows = await table_texts(pool)
    for needle in forbidden:
        assert not [b for b in bodies if needle in b], f"{needle} in a response"
        assert not [n for n in notices if needle in n], f"{needle} in a notice body"
        assert needle not in caplog.text, f"{needle} in a log record"
    # the stored wrapper event is the one allowed place of the tail; a copy elsewhere fails
    assert not [t for t, row in rows if TB_CANARY in row]
    assert [t for t, row in rows if TAIL in row and t != "events"] == []
    assert [t for t, row in rows if TAIL in row] == ["events"]
    # the token is a header only
    assert all(r.headers["authorization"] == f"Bearer {TB_CANARY}" for r in requests)
