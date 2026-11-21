"""Live delivery of new backup versions and the on-demand save run (S-68, T-519)."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timedelta

import pytest
import pytest_asyncio
from psycopg import sql
from test_cache_restore import line, res, seed_backup
from test_cache_runner import SKIP_TX, RunnerEnv, entry
from test_cache_save import uploaded

from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import LIVE, RESTORE_POST, RESTORE_PRE, SAVE_NOW, job_name
from agents_orchestrator.reconciler.live import deliverable
from agents_orchestrator.spec import BackupEntry
from agents_orchestrator.wrapper.models import WrapperUnreachable

pytestmark = pytest.mark.asyncio

NAME = "proj/skills"
DONE = {"state": "done", "job": None, "plan": [], "reason": None}


class StubWrapper:
    """Records the commands each agent is sent (the reconciler's wrapper factory)."""

    calls: list[tuple[str, str, str]] = []

    def __init__(self, url: str, token: str) -> None:
        self.url = url

    async def command(self, session_id: str, name: str) -> dict:
        StubWrapper.calls.append((self.url, session_id, name))
        return {"state": "queued"}

    async def aclose(self) -> None:
        pass


async def new_version(pool, name: str = NAME, digest: str = "b" * 64):
    async with pool.connection() as conn, conn.transaction():
        _, version = await backups_repo.record_save(
            conn,
            name=name,
            object_key=f"workspace-cache/b/{name}/{digest[:8]}-other-aaaa.tar.zst",
            size=2,
            digest=digest,
            manifest=None,
            base_version=None,
            origin="save",
            session_slug="other",
            preset_id=None,
            project=None,
            created_by="other",
            orphan=False,
        )
    return version


class LiveEnv(RunnerEnv):
    def __init__(self, pool, dsn) -> None:
        super().__init__(pool, dsn)
        self.kube.pods_ready = True
        StubWrapper.calls = []
        self.rec.live.wrapper_factory = StubWrapper

    async def running(self, slug, entries, name=NAME):
        """A ready session that restored the current version of `name` into every entry."""
        await seed_backup(self.pool, name)
        c = await self.make(entries, slug=slug)
        async with self.pool.connection() as conn:
            version = (await backups_repo.current(conn, name)).id
            records = [
                {
                    "index": i,
                    "root": e["root"],
                    "path": e["path"],
                    "version": str(version),
                    "base_version": None,
                    "restore": {"state": "restored", "name": name, "version": str(version)},
                    "save": None,
                }
                for i, e in enumerate(entries)
            ]
            backup = {
                "transcripts": {"restore": SKIP_TX},
                "entries": records,
                "runs": {RESTORE_PRE: DONE, RESTORE_POST: DONE},
            }
            status = {**c.status, "phase": "ready", "backup": backup}
            c = await sessions.set_status(conn, c.id, status, c.observed_generation)
        return c, version

    async def request_save(self, c, index=0):
        async with self.pool.connection() as conn:
            await conn.execute(
                "UPDATE sessions SET status = jsonb_set(status, '{backup_save}', %s::jsonb)"
                " WHERE id = %s",
                (json.dumps({"id": str(uuid.uuid4()), "entry_index": index}), c.id),
            )

    async def live_state(self, c, index=0) -> dict | None:
        return (await self.entry(c.id, index)).get("live_delivery")

    def sent_entries(self, slug, key=LIVE) -> list[dict]:
        job = self.kube.get("Job", job_name(slug, key))
        return json.loads(job["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"])


@pytest_asyncio.fixture
async def env(pool, dsn) -> LiveEnv:
    return LiveEnv(pool, dsn)


LIVE_SKILLS = entry("claude", "skills", "both", NAME, live=True)


async def test_live_delivery_only_to_subscribed_sessions(env):
    on, v1 = await env.running("alpha", [LIVE_SKILLS])
    await env.running("beta", [entry("claude", "skills", "both", NAME)])  # live is off
    await env.tick()
    assert env.job_creates() == []  # nothing new yet: both hold the current version
    v2 = await new_version(env.pool)
    await env.tick()
    assert env.job_creates() == [job_name("alpha", LIVE)]
    (sent,) = env.sent_entries("alpha")
    assert sent["version"] == str(v2.id) and sent["skip_dirty"] is True
    assert sent["root"] == "claude" and sent["path"] == "skills" and sent["name"] == NAME
    assert "/b/proj/skills/" in sent["url"] and "X-Amz-Signature" in sent["url"]
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("restored", 0, size=2)))
    await env.tick()
    assert (await env.entry(on.id))["version"] == str(v2.id)
    assert (await env.live_state(on))["state"] == "delivered"
    assert env.job_creates() == [job_name("alpha", LIVE)]


async def test_reload_skills_sent_once_per_agent(env):
    c, _ = await env.running("alpha", [LIVE_SKILLS], name=NAME)
    await env.tick()
    async with env.pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_session_id = 'w-' || ordinal, url = 'http://a' || ordinal"
            " WHERE session_id = %s",
            (c.id,),
        )
    await new_version(env.pool)
    await env.tick()
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("restored", 0, size=2)))
    await env.tick(3)
    agents = [a for a in await env.agents(c.id) if a.wrapper_session_id]
    assert agents
    assert sorted((u, s) for u, s, n in StubWrapper.calls) == sorted(
        (a.url, a.wrapper_session_id) for a in agents
    )
    assert {n for _, _, n in StubWrapper.calls} == {"reload-skills"}
    await env.tick(2)
    assert len(StubWrapper.calls) == len(agents)  # once per agent, never again


@pytest.mark.parametrize(("path", "reloads"), [("rules", False), ("skills", True), (".", True)])
async def test_no_reload_for_rules_only_entry(env, path, reloads):
    c, _ = await env.running("alpha", [entry("claude", path, "both", NAME, live=True)])
    await env.tick()
    async with env.pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_session_id = 'w-' || ordinal, url = 'http://a' || ordinal"
            " WHERE session_id = %s",
            (c.id,),
        )
    await new_version(env.pool)
    await env.tick()
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("restored", 0, size=2)))
    await env.tick(3)
    assert (await env.entry(c.id))["restore"]["state"] == "restored"
    assert bool(StubWrapper.calls) is reloads


async def test_missed_notify_healed_next_tick(env):
    """No NOTIFY reaches the reconciler here: the per-tick comparison delivers anyway."""
    c, v1 = await env.running("alpha", [LIVE_SKILLS])
    await env.tick()
    v2 = await new_version(env.pool)  # the notify of this save is never heard
    await env.tick()
    assert env.job_creates() == [job_name("alpha", LIVE)]
    (sent,) = env.sent_entries("alpha")
    assert sent["version"] == str(v2.id)


async def test_record_save_notifies_backups_current(pool, dsn):
    import psycopg

    async with await psycopg.AsyncConnection.connect(dsn, autocommit=True) as listener:
        await listener.execute(sql.SQL("LISTEN backups_current"))
        await new_version(pool, "n/x")
        got = []
        async for n in listener.notifies(timeout=5, stop_after=1):
            got.append(json.loads(n.payload))
    assert got == [{"name": "n/x"}]


async def test_dirty_dir_skipped_with_warning(env):
    c, v1 = await env.running("alpha", [LIVE_SKILLS])
    await env.tick()
    v2 = await new_version(env.pool)
    await env.tick()
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("skipped_dirty", 0)))
    await env.tick()
    record = await env.entry(c.id)
    assert record["version"] == str(v1)  # the session keeps its own base
    assert record["live_delivery"]["state"] == "skipped_dirty" and record["live_delivery"][
        "version"
    ] == str(v2.id)
    warnings = [e["data"] for e in await env.events(c.id) if e["type"] == cache_mod.WARNING_EVENT]
    assert warnings == [
        {
            "index": 0,
            "name": NAME,
            "state": "skipped_dirty",
            "direction": "restore",
            "message": cache_mod.DIRTY_MESSAGE,
        }
    ]
    assert "orchestrator_backup_save" in cache_mod.DIRTY_MESSAGE
    await env.tick(3)
    assert env.job_creates() == [job_name("alpha", LIVE)]  # not retried for the same version
    assert StubWrapper.calls == []  # nothing was delivered: no reload
    v3 = await new_version(env.pool, digest="c" * 64)  # a later version is tried again
    await env.tick()
    assert env.job_creates() == [job_name("alpha", LIVE)] * 2
    assert env.sent_entries("alpha")[0]["version"] == str(v3.id)


async def test_saver_not_redelivered(env):
    saver, _ = await env.running("alpha", [LIVE_SKILLS])
    other, _ = await env.running("beta", [LIVE_SKILLS])
    await env.tick()
    await env.request_save(saver)
    await env.tick()
    save = job_name("alpha", SAVE_NOW)
    (sent,) = env.sent_entries("alpha", SAVE_NOW)
    assert sent["index"] == 0 and sent["name"] == NAME and "version" not in sent
    spec = env.kube.get("Job", save)["spec"]["template"]["spec"]
    mounts = {m["mountPath"]: m for m in spec["containers"][0]["volumeMounts"]}
    assert mounts["/claude"]["readOnly"] is True  # the PVC is read-only for the save Job
    assert mounts["/backup-state"].get("readOnly") is not True
    env.kube.finish_job(save, log=uploaded(5, "f" * 64))
    await env.tick()
    async with env.pool.connection() as conn:
        now = await backups_repo.current(conn, NAME)
    assert now.digest == "f" * 64 and now.created_by == "alpha"
    assert (await env.entry(saver.id))["version"] == str(now.id)
    assert "backup_save" not in (await env.status(saver.id))
    await env.tick(2)
    assert job_name("alpha", LIVE) not in env.job_creates()  # the saver keeps its own version
    assert job_name("beta", LIVE) in env.job_creates()  # the other session gets it
    assert env.sent_entries("beta")[0]["version"] == str(now.id)


async def test_save_request_for_a_pending_restore_is_closed_with_a_reason(env):
    c, _ = await env.running("alpha", [LIVE_SKILLS])
    async with env.pool.connection() as conn:
        st = (await sessions.get(conn, c.id)).status
        st["backup"]["entries"][0]["restore"] = {"state": "running"}
        await sessions.set_status(conn, c.id, st, c.observed_generation)
    await env.tick()
    await env.request_save(c)
    await env.tick()
    assert job_name("alpha", SAVE_NOW) not in env.job_creates()
    assert "backup_save" not in await env.status(c.id)
    run = await env.run(c.id, SAVE_NOW)
    assert run["state"] == "skipped" and run["reason"] == "restore_pending"


async def set_delivery_age(env, c, seconds):
    """Move the recorded live delivery `seconds` into the past (the retry backoff is time based)."""
    async with env.pool.connection() as conn:
        cur = await sessions.get(conn, c.id)
        backup = cur.status["backup"]
        record = backup["entries"][0]["live_delivery"]
        past = datetime.fromisoformat(record["at"]) - timedelta(seconds=seconds)
        record["at"] = past.isoformat()
        await sessions.set_status(conn, c.id, cur.status, cur.observed_generation)


async def test_failed_delivery_warns_and_is_retried_bounded(env):
    c, _ = await env.running("alpha", [LIVE_SKILLS])
    await env.tick()
    v2 = await new_version(env.pool)
    await env.tick()
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("failed", 0, reason="s3 hiccup")))
    await env.tick()
    record = await env.live_state(c)
    assert record["state"] == "failed" and record["version"] == str(v2.id)
    warnings = [e["data"] for e in await env.events(c.id) if e["type"] == cache_mod.WARNING_EVENT]
    assert [(w["state"], w["direction"]) for w in warnings] == [("failed", "restore")]
    assert "s3 hiccup" in warnings[0]["message"]
    await env.tick(3)
    assert env.job_creates() == [job_name("alpha", LIVE)]  # backoff: no tight loop
    for attempt in range(2, cache_mod.LIVE_ATTEMPTS + 1):
        await set_delivery_age(env, c, cache_mod.LIVE_RETRY_SECONDS + 1)
        await env.tick()
        assert env.job_creates() == [job_name("alpha", LIVE)] * attempt
        env.kube.finish_job(job_name("alpha", LIVE), log=line(res("failed", 0, reason="again")))
        await env.tick()
    await set_delivery_age(env, c, cache_mod.LIVE_RETRY_SECONDS + 1)
    await env.tick(3)
    assert len(env.job_creates()) == cache_mod.LIVE_ATTEMPTS  # bounded


async def test_pending_reload_survives_a_second_delivery(env):
    c, _ = await env.running("alpha", [LIVE_SKILLS])
    await env.tick()
    async with env.pool.connection() as conn:
        await conn.execute(
            "UPDATE agents SET wrapper_session_id = 'w-' || ordinal, url = 'http://a' || ordinal"
            " WHERE session_id = %s",
            (c.id,),
        )
    await new_version(env.pool)
    await env.tick()
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("restored", 0, size=2)))

    class Down(StubWrapper):
        async def command(self, session_id, name):
            raise WrapperUnreachable("down")

    env.rec.live.wrapper_factory = Down
    await env.tick()
    run = cache_mod.get_run(await env.status(c.id), LIVE)
    assert run["reload_pending"] is True
    await new_version(env.pool, digest="d" * 64)  # a second delivery starts first
    await env.tick()
    run = cache_mod.get_run(await env.status(c.id), LIVE)
    assert run["state"] == "running" and run["reload_pending"] is True  # coalesced, not lost
    env.rec.live.wrapper_factory = StubWrapper
    env.kube.finish_job(job_name("alpha", LIVE), log=line(res("restored", 0, size=2)))
    await env.tick(3)
    assert {n for _, _, n in StubWrapper.calls} == {"reload-skills"}


async def test_status_write_keeps_a_save_request_set_concurrently(env):
    """The lost-update fix: the write re-reads the row under lock, so a request the api set after
    this step read its session survives the reconciler's status write."""
    c, _ = await env.running("alpha", [LIVE_SKILLS])
    async with env.pool.connection() as conn:
        stale = await sessions.get(conn, c.id)
    await env.request_save(c)  # the api sets backup_save after `stale` was read
    await env.rec._write_status(stale, {"message": "x"}, stale.observed_generation)
    status = await env.status(c.id)
    assert status["message"] == "x" and "backup_save" in status


@pytest.mark.parametrize(
    ("root", "delivered"), [("workspace", True), ("claude", True), ("volume", False)]
)
async def test_a_volume_root_is_not_live_delivered(root, delivered):
    e = BackupEntry.model_construct(root=root, path=".", mode="restore", name="n", live=True)
    assert deliverable(e) is delivered
