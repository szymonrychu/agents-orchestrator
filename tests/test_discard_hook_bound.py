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
from agents_orchestrator.reconciler import loop as loop_mod
from agents_orchestrator.reconciler.loop import RETAINED_LABEL
from agents_orchestrator.settings import Settings, SettingsError

PVC = "agent-alpha-workspace"


def refused(calls: list[str]):
    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(str(request.url))
        raise httpx.ConnectError("All connection attempts failed")

    return httpx.MockTransport(handler)


async def past_give_up(env: Env, session_id, seconds: int, hook: str = "settings") -> None:
    """Move `hook`'s first failure `seconds` into the past, and the backoff gate with it."""
    await env.sql(
        "UPDATE sessions SET status = jsonb_set(status, %s::text[],"
        " to_jsonb((now() - make_interval(secs => %s))::text)) WHERE id = %s",
        (["discard_hook_first_failed_at", hook], seconds, session_id),
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
    assert set(got.status["discard_hook_first_failed_at"]) == {"settings"}
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
    assert abandoned["settings"]["reason"] == "timeout"
    assert "ConnectError" in abandoned["settings"]["error"]
    assert abandoned["settings"]["at"]
    for key in ("discard_hook_next_at", "discard_hook_first_failed_at", "discard_hook_errors"):
        assert key not in got.status
    assert "discard_hook_delivered" not in got.status
    assert any("abandoned" in r.getMessage() and "alpha" in r.getMessage() for r in caplog.records)
    assert not env.kube.has("StatefulSet", "agent-alpha")
    assert_pvc_retained(env, "alpha", c.id, "timeout")

    env.rec._remaining = real_remaining
    await env.rec.reconcile_once()
    assert len(calls) == 3  # an abandoned hook is never run again
    assert await env.get(c.id) is None
    assert_pvc_retained(env, "alpha", c.id, "timeout")  # the backup outlives the row


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


async def test_each_hook_has_its_own_give_up_window(pool, dsn, tmp_path):
    """Hook A (settings) fails for most of the window, then recovers; hook B (the session's own
    URL) then fails for the first time. B gets a full window of its own, never A's leftover."""
    env = Env(pool, dsn, tmp_path, discard_hook_give_up_seconds=1800)
    c = await env.session("alpha", discard_hook_url="https://spec.example/hook")
    await env.rec.reconcile_once()
    url_hooks(env)
    calls: list[str] = []
    settings_up = False

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.url.host)
        if request.url.host == "settings.example":
            if settings_up:
                return httpx.Response(204)
            env.rec._hook_spent = 10**6  # A spends the pass budget: B stays owed, unrun
            raise httpx.ConnectError("settings down")
        raise httpx.ConnectError("spec down")

    env.rec.hook_transport = httpx.MockTransport(handler)
    real_remaining = env.rec._remaining

    async def lingering(slug):
        return ["Pod/agent-alpha-0"]

    await env.discard(c.id)
    await env.rec.reconcile_once()
    await past_give_up(env, c.id, 1700)
    await env.rec.reconcile_once()
    assert calls == ["settings.example", "settings.example"]
    got = await env.get(c.id)
    assert set(got.status["discard_hook_first_failed_at"]) == {"settings"}

    # A recovers 1801 s after its first failure; B fails for the first time in the same pass
    settings_up = True
    await past_give_up(env, c.id, 1801)
    await env.rec.reconcile_once()
    assert calls[2:] == ["settings.example", "spec.example"]
    got = await env.get(c.id)
    assert got is not None and got.desired == "deleting"
    assert "discard_hook_abandoned" not in got.status
    assert set(got.status["discard_hook_delivered"]) == {"settings"}
    assert set(got.status["discard_hook_first_failed_at"]) == {"session"}
    assert set(got.status["discard_hook_errors"]) == {"session"}
    assert got.status["discard_hook_failed"] == "session: ConnectError: spec down"
    assert env.kube.has("StatefulSet", "agent-alpha")

    # B inside its own window keeps retrying
    await past_give_up(env, c.id, 1700, hook="session")
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert calls[4:] == ["spec.example"]
    assert "discard_hook_abandoned" not in got.status
    assert env.kube.has("StatefulSet", "agent-alpha")

    # past B's own window it is abandoned, A stays delivered
    env.rec._remaining = lingering
    await past_give_up(env, c.id, 1801, hook="session")
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert set(got.status["discard_hook_abandoned"]) == {"session"}
    assert got.status["discard_hook_abandoned"]["session"]["reason"] == "timeout"
    assert set(got.status["discard_hook_delivered"]) == {"settings"}
    assert not env.kube.has("StatefulSet", "agent-alpha")
    env.rec._remaining = real_remaining
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None


async def test_abandon_shows_in_status_message_and_notifies(pool, dsn, tmp_path, monkeypatch):
    env = Env(pool, dsn, tmp_path, discard_hook_give_up_seconds=600)
    calls: list[str] = []
    c = await held_session(env, calls)
    writes: list[dict] = []
    notified: list[tuple[str, dict]] = []
    real_write, real_notify = env.rec._write_status, loop_mod.notify

    async def spy_write(session, fields, *args, **kwargs):
        writes.append(dict(fields))
        await real_write(session, fields, *args, **kwargs)

    async def spy_notify(conn, channel, payload):
        notified.append((channel, payload))
        await real_notify(conn, channel, payload)

    env.rec._write_status = spy_write
    monkeypatch.setattr(loop_mod, "notify", spy_notify)

    async def lingering(slug):
        return ["Pod/agent-alpha-0"]

    env.rec._remaining = lingering
    await past_give_up(env, c.id, 601)
    await env.rec.reconcile_once()

    note = "discarding without the pre-discard hook settings (timeout)"
    abandon = next(w for w in writes if "discard_hook_abandoned" in w)
    entry = abandon["discard_hook_abandoned"]["settings"]
    assert entry["reason"] == "timeout" and "ConnectError" in entry["error"]
    assert abandon["phase"] == "deleting" and abandon["message"] == note
    assert ("status", {"session_id": str(c.id)}) in notified
    got = await env.get(c.id)
    assert got.status["message"] == f"waiting for Pod/agent-alpha-0; {note}"
    assert got.status["discard_hook_abandoned"]["settings"]["reason"] == "timeout"

    # a forced discard says so too
    d = await env.session("bravo")
    await env.rec.reconcile_once()
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, d.id, force=True)
    await env.rec.reconcile_once()
    got = await env.get(d.id)
    assert got.status["discard_hook_abandoned"]["settings"]["reason"] == "forced"
    assert got.status["message"].endswith(
        "discarding without the pre-discard hook settings (forced)"
    )


async def test_new_session_reusing_a_retained_slug_gets_its_own_workspace(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    calls: list[str] = []
    old = await held_session(env, calls)
    async with env.pool.connection() as conn:
        await sessions.request_delete(conn, old.id, force=True)
    await env.rec.reconcile_once()
    assert await env.get(old.id) is None
    assert_pvc_retained(env, "alpha", old.id, "forced")

    new = await env.session("alpha")  # the same slug, free again in the database
    await env.rec.reconcile_once()
    got = await env.get(new.id)
    suffixed = f"agent-alpha-{new.id.hex[:8]}-workspace"
    assert got.status["workspace_pvc"] == suffixed
    assert got.status["phase"] != "error"
    assert sorted(env.kube.names("PersistentVolumeClaim")) == sorted([PVC, suffixed])
    sts = env.kube.get("StatefulSet", "agent-alpha")
    claims = [
        v["persistentVolumeClaim"]["claimName"]
        for v in sts["spec"]["template"]["spec"]["volumes"]
        if "persistentVolumeClaim" in v
    ]
    assert claims == [suffixed]
    assert_pvc_retained(env, "alpha", old.id, "forced")  # the backup is untouched

    # the pinned name sticks once the operator deletes the old backup
    await env.kube.delete("PersistentVolumeClaim", "agents", PVC)
    await env.rec.reconcile_once()
    assert (await env.get(new.id)).status["workspace_pvc"] == suffixed

    # a normal discard of the new session deletes its own PVC only
    env.settings.discard_hook_url = ""
    await env.discard(new.id)
    await env.rec.reconcile_once()
    assert await env.get(new.id) is None
    assert env.kube.names("PersistentVolumeClaim") == []


async def test_a_fresh_slug_pins_the_deterministic_workspace_name(pool, dsn, tmp_path):
    env = Env(pool, dsn, tmp_path)
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    assert (await env.get(c.id)).status["workspace_pvc"] == PVC
    assert env.kube.names("PersistentVolumeClaim") == [PVC]


def test_give_up_setting_default_env_and_bounds(monkeypatch):
    base = {"database_url": "postgresql://x/y", "agent_image": "img:1"}
    assert Settings(**base).discard_hook_give_up_seconds == 1800
    monkeypatch.setenv("ORCH_DISCARD_HOOK_GIVE_UP_SECONDS", "900")
    assert Settings(**base).discard_hook_give_up_seconds == 900
    for bad in (0, -1, 7 * 86400 + 1):
        with pytest.raises(SettingsError):
            Settings(**base, discard_hook_give_up_seconds=bad)
