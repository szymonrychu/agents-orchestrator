"""The admin audit row carries non-value `detail` only when a route noted it, and the shared
own-session refusal of the superleader writes."""

from __future__ import annotations

from types import SimpleNamespace

import pytest
from psycopg.rows import dict_row
from test_agent_session_messaging import alone

from agents_orchestrator.api import admin
from agents_orchestrator.db import sessions as sessions_repo


async def _request(pool):
    boss = await alone(pool, "ad-boss", superleader=True)
    async with pool.connection() as conn:
        session = await sessions_repo.get(conn, boss.id)
    state = SimpleNamespace(
        admin_caller=(SimpleNamespace(id=boss.leader_id), session),
        admin_secrets=[],
        admin_vault=[],
    )
    return SimpleNamespace(
        state=state,
        url=SimpleNamespace(path="/agent/v1/admin/x"),
        method="POST",
        app=SimpleNamespace(state=SimpleNamespace(pool=pool)),
    )


async def admin_rows(pool) -> list[dict]:
    async with pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        await cur.execute("SELECT data FROM events WHERE type = 'admin_call' ORDER BY id")
        return [r["data"] for r in await cur.fetchall()]


async def test_audit_row_carries_detail_when_noted(pool):
    req = await _request(pool)
    admin.note_detail(req, path="src/a.py", previous="p-0", to=None)
    await admin._audit(req, 200)
    (row,) = await admin_rows(pool)
    assert row["detail"] == {"path": "src/a.py", "previous": "p-0", "to": None}


async def test_audit_row_without_detail_has_no_detail_key(pool):
    req = await _request(pool)
    await admin._audit(req, 200)
    (row,) = await admin_rows(pool)
    assert "detail" not in row


def test_own_session_error():
    e = admin.own_session("scale")
    assert (e.status, e.code) == (422, "own_session")


@pytest.mark.parametrize("value", [["a"], {"k": "v"}, b"x", (1, 2), object()])
async def test_note_detail_refuses_non_scalar(pool, value):
    req = await _request(pool)
    with pytest.raises(TypeError, match="JSON scalar"):
        admin.note_detail(req, path=value)
    assert not hasattr(req.state, "admin_detail")
