"""A failed agent bootstrap is visible: persisted failure, redacted log tail, start_state on the
session summary and the sessions list, a clear create-timeout message (T-79)."""

from __future__ import annotations

import pytest
import pytest_asyncio
from fakes.fake_wrapper import FakeWrapper
from test_agent_session_messaging import _app, _list, alone
from test_wrapper_session_bootstrap import Env

from agents_orchestrator.api import sessions as sessions_api
from agents_orchestrator.db import sessions
from agents_orchestrator.redact import redact_text
from agents_orchestrator.wrapper.client import WrapperClient
from agents_orchestrator.wrapper.models import WrapperUnreachable

SECRET_URL = "https://deploy:hunter2secret@git.example/org/repo.git"
SECRET_TOKEN = "ghp_" + "a1B2c3D4e5" * 3
LOG = f"mise install\nfetching {SECRET_URL}\nGITHUB_TOKEN={SECRET_TOKEN}\nERROR: build failed"


@pytest_asyncio.fixture
async def fake():
    async with FakeWrapper() as fw:
        yield fw


@pytest_asyncio.fixture
async def env(pool, dsn, fake) -> Env:
    return Env(pool, dsn, fake)


async def view(env: Env, session_id):
    async with env.pool.connection() as conn:
        c = await sessions.get(conn, session_id)
        return await sessions_api.one_view(conn, c, env.settings)


async def fail_first_session(env: Env, fake: FakeWrapper, c) -> None:
    sid = await env.seed_session(c.id, "ready")
    fake.fail_session("mise_install", LOG, session_id=sid)
    fake.set_create_error(500, "bootstrap_failed")


def test_redact_text_strips_userinfo_and_tokens():
    out = redact_text(LOG)
    assert "hunter2secret" not in out
    assert SECRET_TOKEN not in out
    assert "https://***@git.example/org/repo.git" in out
    assert "ERROR: build failed" in out
    assert redact_text(None) is None
    assert redact_text("plain text") == "plain text"


async def test_failed_snapshot_shows_redacted_log_tail(env, fake):
    c = await env.session("alpha")
    await fail_first_session(env, fake, c)
    await env.rec.reconcile_once()

    v = await view(env, c.id)
    assert v.summary.start_state == "bootstrap_failed"
    (failure,) = v.summary.bootstrap_failures
    assert failure.pod == "agent-alpha-0"
    assert failure.step == "mise_install"
    assert "hunter2secret" not in failure.log_tail
    assert SECRET_TOKEN not in failure.log_tail
    assert "ERROR: build failed" in failure.log_tail
    assert "hunter2secret" not in failure.message


async def test_failure_clears_after_recovery(env, fake):
    c = await env.session("alpha")
    await fail_first_session(env, fake, c)
    await env.rec.reconcile_once()
    assert (await view(env, c.id)).summary.start_state == "bootstrap_failed"

    fake.set_create_error(None)
    await env.rec.reconcile_once()
    v = await view(env, c.id)
    assert v.summary.bootstrap_failures == []
    assert v.summary.start_state == "ready"


async def test_create_timeout_names_timeout_and_step(env, fake):
    c = await env.session("alpha")
    fake.delay_create(1.5)
    env.client_override = WrapperClient(fake.base_url, fake.token, create_timeout=0.3)
    await env.rec.reconcile_once()

    message = (await env.status(c.id))["message"]
    assert "create timed out after 0.3s, wrapper still at step mise_install" in message
    assert "ReadTimeout:" not in message
    v = await view(env, c.id)
    assert v.summary.start_state == "bootstrap_failed"
    assert v.summary.bootstrap_failures[0].step == "create"
    assert "wrapper still at step mise_install" in v.summary.bootstrap_failures[0].message


async def test_empty_exception_text_falls_back_to_type_and_context(fake):
    client = WrapperClient(fake.base_url, "x", http=None)
    fake.delay_create(1.0)
    short = WrapperClient(fake.base_url, fake.token, create_timeout=0.2)
    with pytest.raises(WrapperUnreachable) as err:
        await short.create_session("n", "w")
    assert str(err.value).strip() not in ("", "ReadTimeout:")
    assert "POST /sessions" in str(err.value)
    await client.aclose()
    await short.aclose()


async def test_sessions_list_has_start_state_and_reason(pool, dsn):
    boss = await alone(pool, "s-boss", superleader=True)
    ok = await alone(pool, "s-ok")
    bad = await alone(pool, "s-bad")
    booting = await alone(pool, "s-booting")
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET start_failure = %s::jsonb WHERE id = %s",
            ('{"step": "mise_install", "message": "mise exploded\\nsecond line"}', bad.leader_id),
        )
        await conn.execute("UPDATE agents SET ready = false WHERE id = %s", (booting.leader_id,))
    r = await _list(_app(pool, dsn), boss)
    assert r.status_code == 200, r.text
    by_slug = {s["slug"]: s for s in r.json()["sessions"]}
    assert by_slug[ok.slug]["start_state"] == "ready"
    assert by_slug[ok.slug]["start_reason"] is None
    assert by_slug[bad.slug]["start_state"] == "bootstrap_failed"
    assert by_slug[bad.slug]["start_reason"] == "mise_install: mise exploded"
    assert by_slug[booting.slug]["start_state"] == "booting"
    assert by_slug[booting.slug]["start_reason"]
