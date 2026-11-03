"""T-26: a pre-discard hook that keeps failing never holds a discard forever.

The reconciler gives up on an owed hook `ORCH_DISCARD_HOOK_GIVE_UP_SECONDS` after its first
failure, and a forced discard (`?force=true`) gives up at once; either way the discard completes
and the workspace PVC is kept, detached from the session, as the backup.
"""

from __future__ import annotations

import logging

import httpx
import pytest
from test_discard_hook import Env, populate, retry_now, url_hooks

from agents_orchestrator.db import sessions
from agents_orchestrator.k8s.labels import MANAGED_BY_LABEL, SESSION_LABEL
from agents_orchestrator.reconciler.loop import RETAINED_LABEL
from agents_orchestrator.settings import Settings, SettingsError

PVC = "agent-alpha-workspace"


def refused(calls: list[str]):
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        raise httpx.ConnectError("All connection attempts failed")

    return httpx.MockTransport(handler)


async def past_give_up(env: Env, session_id, seconds: int) -> None:
    """Move the first failure `seconds` into the past, and the backoff gate with it."""
    await env.sql(
        "UPDATE sessions SET status = jsonb_set(status, '{discard_hook_first_failed_at}',"
        " to_jsonb((now() - make_interval(secs => %s))::text)) WHERE id = %s",
        (seconds, session_id),
    )
    await retry_now(env, session_id)


async def held_session(env: Env, calls: list[str]) -> sessions.Session:
    """A converged session whose settings URL hook refuses every connection, discarded once."""
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    await populate(env, c)
    url_hooks(env)
    env.rec.hook_transport = refused(calls)
    await env.discard(c.id)
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert got is not None and got.desired == "deleting"
    assert "ConnectError" in got.status["discard_hook_failed"]
    assert got.status["discard_hook_first_failed_at"]
    assert env.kube.has("StatefulSet", "agent-alpha")
    return got


def assert_pvc_retained(env: Env, slug: str, session_id, reason: str) -> None:
    assert env.kube.has("PersistentVolumeClaim", PVC)
    meta = env.kube.get("PersistentVolumeClaim", PVC)["metadata"]
    labels = meta.get("labels") or {}
    assert SESSION_LABEL not in labels and MANAGED_BY_LABEL not in labels
    assert labels[RETAINED_LABEL] == slug
    ann = meta["annotations"]
    assert ann[f"{RETAINED_LABEL}-session-id"] == str(session_id)
    assert ann[f"{RETAINED_LABEL}-reason"] == f"discard hook abandoned ({reason}): settings"


async def test_failing_hook_is_abandoned_after_the_bound_and_discard_completes(
    pool, dsn, tmp_path, caplog
):
    env = Env(pool, dsn, tmp_path, discard_hook_give_up_seconds=600)
    calls: list[str] = []
    c = await held_session(env, calls)

    # inside the bound it keeps retrying and holds everything
    await past_give_up(env, c.id, 300)
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert got is not None and got.status["discard_hook_failures"] == 2
    assert "discard_hook_abandoned" not in got.status
    assert env.kube.has("StatefulSet", "agent-alpha")

    # past the bound: one last attempt, then the hook is abandoned and recorded
    real_remaining = env.rec._remaining

    async def lingering(slug):
        return ["Pod/agent-alpha-0"]

    env.rec._remaining = lingering
    await past_give_up(env, c.id, 601)
    with caplog.at_level(logging.WARNING, logger="agents_orchestrator.reconciler.loop"):
        await env.rec.reconcile_once()
    assert len(calls) == 3
    got = await env.get(c.id)
    assert got is not None
    abandoned = got.status["discard_hook_abandoned"]
    assert set(abandoned) == {"settings"}
    assert abandoned["settings"]["reason"] == "retry_limit"
    assert "ConnectError" in abandoned["settings"]["error"]
    assert abandoned["settings"]["at"]
    for key in ("discard_hook_next_at", "discard_hook_first_failed_at", "discard_hook_errors"):
        assert key not in got.status
    assert "discard_hook_delivered" not in got.status
    assert any("abandoned" in r.getMessage() and "alpha" in r.getMessage() for r in caplog.records)
    assert not env.kube.has("StatefulSet", "agent-alpha")
    assert_pvc_retained(env, "alpha", c.id, "retry_limit")

    env.rec._remaining = real_remaining
    await env.rec.reconcile_once()
    assert len(calls) == 3  # an abandoned hook is never run again
    assert await env.get(c.id) is None
    assert_pvc_retained(env, "alpha", c.id, "retry_limit")  # the backup outlives the row


async def test_force_discard_of_a_session_stuck_in_deleting_completes(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    calls: list[str] = []
    c = await held_session(env, calls)
    await env.rec.reconcile_once()  # still backing off: stuck
    assert await env.get(c.id) is not None and len(calls) == 1

    async with env.pool.connection() as conn:
        forced = await sessions.request_delete(conn, c.id, force=True)
    assert forced.discard_forced_at is not None
    await env.rec.reconcile_once()

    assert len(calls) == 1  # no further attempt: the hook is abandoned at once
    assert await env.get(c.id) is None
    assert not env.kube.has("StatefulSet", "agent-alpha")
    assert_pvc_retained(env, "alpha", c.id, "forced")


async def test_force_on_an_active_session_flips_it_and_abandons_the_hook_unrun(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    calls: list[str] = []
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    url_hooks(env)
    env.rec.hook_transport = refused(calls)
    async with env.pool.connection() as conn:
        forced = await sessions.request_delete(conn, c.id, force=True)
    assert forced.desired == "deleting" and forced.generation == c.generation + 1
    await env.rec.reconcile_once()
    assert calls == []
    assert await env.get(c.id) is None
    assert_pvc_retained(env, "alpha", c.id, "forced")


async def test_force_without_an_owed_hook_deletes_the_pvc(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    assert env.settings.discard_hook() is None
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, c.id, force=True)
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None
    assert env.kube.names("PersistentVolumeClaim") == []


async def test_hook_success_after_failures_is_the_normal_path(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path, discard_hook_give_up_seconds=600)
    calls: list[str] = []
    c = await held_session(env, calls)
    recorder = url_hooks(env)  # the receiver recovers inside the bound

    real_remaining = env.rec._remaining

    async def lingering(slug):
        return ["Pod/agent-alpha-0"]

    env.rec._remaining = lingering
    await past_give_up(env, c.id, 300)
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert len(recorder.requests) == 1
    assert set(got.status["discard_hook_delivered"]) == {"settings"}
    for key in (
        "discard_hook_abandoned",
        "discard_hook_failed",
        "discard_hook_failures",
        "discard_hook_next_at",
        "discard_hook_first_failed_at",
        "discard_hook_errors",
    ):
        assert key not in got.status
    env.rec._remaining = real_remaining
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None
    assert await env.history_rows(c.id) == 0
    assert env.kube.names("PersistentVolumeClaim") == []  # delivered: nothing retained


def test_give_up_setting_default_env_and_bounds(monkeypatch):
    base = {"database_url": "postgresql://x/y", "agent_image": "img:1"}
    assert Settings(**base).discard_hook_give_up_seconds == 1800
    monkeypatch.setenv("ORCH_DISCARD_HOOK_GIVE_UP_SECONDS", "900")
    assert Settings(**base).discard_hook_give_up_seconds == 900
    for bad in (0, -1, 7 * 86400 + 1):
        with pytest.raises(SettingsError):
            Settings(**base, discard_hook_give_up_seconds=bad)
