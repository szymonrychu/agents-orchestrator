"""claim_self_restart: atomic cooldown claim on sessions.last_self_restart_at (D13)."""

from __future__ import annotations

import asyncio
import uuid

import pytest
from factories import insert_session
from test_api_sessions import make_settings

from agents_orchestrator.db import sessions as repo


async def claim(pool, session_id, cooldown=600):
    async with pool.connection() as conn:
        return await repo.claim_self_restart(conn, session_id, cooldown)


async def test_first_claim_wins_and_second_inside_window_is_refused(pool):
    cl = await insert_session(pool)
    first = await claim(pool, cl["id"])
    assert first.claimed is True
    assert first.retry_after == 0
    second = await claim(pool, cl["id"])
    assert second.claimed is False
    assert 0 < second.retry_after <= 600


async def test_claim_after_the_window_succeeds(pool):
    cl = await insert_session(pool)
    assert (await claim(pool, cl["id"])).claimed
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET last_self_restart_at = now() - interval '601 seconds'"
            " WHERE id = %s",
            (cl["id"],),
        )
    assert (await claim(pool, cl["id"])).claimed


async def test_retry_after_counts_down_from_the_last_claim(pool):
    cl = await insert_session(pool)
    assert (await claim(pool, cl["id"])).claimed
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE sessions SET last_self_restart_at = now() - interval '590 seconds'"
            " WHERE id = %s",
            (cl["id"],),
        )
    refused = await claim(pool, cl["id"])
    assert not refused.claimed
    assert 1 <= refused.retry_after <= 10


async def test_concurrent_claims_exactly_one_wins(pool):
    cl = await insert_session(pool)
    results = await asyncio.gather(*(claim(pool, cl["id"]) for _ in range(8)))
    assert sum(r.claimed for r in results) == 1


async def test_cooldown_is_per_session(pool):
    a = await insert_session(pool)
    b = await insert_session(pool)
    assert (await claim(pool, a["id"])).claimed
    assert (await claim(pool, b["id"])).claimed


async def test_unknown_session_is_not_found(pool):
    with pytest.raises(repo.SessionNotFound):
        await claim(pool, uuid.uuid4())


def test_cooldown_setting_defaults_to_600_and_is_configurable():
    assert make_settings().self_restart_cooldown_seconds == 600
    assert make_settings(self_restart_cooldown_seconds=30).self_restart_cooldown_seconds == 30
    with pytest.raises(ValueError):
        make_settings(self_restart_cooldown_seconds=0)
