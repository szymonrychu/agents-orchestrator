"""Claims are held at and after the credential end; messages stay queued."""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from test_dispatcher import make_env

HELD = "claims held"


async def test_dispatch_held_after_expiry(pool, dsn, caplog):
    e = await make_env(pool, dsn)
    try:
        await e.agent()
        m = await e.msg(text="hold me")
        at = datetime.now(UTC) - timedelta(minutes=1)
        d = e.dispatcher(claude_credential_expires_at=at)
        with caplog.at_level(logging.INFO):
            await d.drain()
            await d.drain()
        await d.aclose()
        assert (await e.message(m["id"]))["status"] == "queued"
        assert e.fake.received_turns == 0
        assert sum(HELD in r.getMessage() for r in caplog.records) == 1
    finally:
        await e.fake.stop()


async def test_dispatch_continues_in_refuse(pool, dsn):
    e = await make_env(pool, dsn)
    try:
        await e.agent()
        m = await e.msg(text="go on")
        at = datetime.now(UTC) + timedelta(hours=1)
        d = e.dispatcher(claude_credential_expires_at=at)
        await d.drain()
        await d.aclose()
        assert (await e.message(m["id"]))["status"] == "sent"
    finally:
        await e.fake.stop()
