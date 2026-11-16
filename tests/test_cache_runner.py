"""Backup entries run per phase around the init Job and are saved at discard (S-68, T-518)."""

from __future__ import annotations

import json

import pytest
import pytest_asyncio
from psycopg.rows import dict_row
from test_cache_restore import Env, line, res, seed_backup
from test_cache_save import SaveEnv, uploaded

from agents_orchestrator.db import backups as backups_repo
from agents_orchestrator.db import sessions
from agents_orchestrator.reconciler import cache as cache_mod
from agents_orchestrator.reconciler.cache import (
    RESOLVE_PRE,
    RESTORE_POST,
    RESTORE_PRE,
    SAVE,
    TX_RESTORE,
    TX_SAVE,
    job_name,
)
from agents_orchestrator.reconciler.init import init_job_name
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import tokens_for

pytestmark = pytest.mark.asyncio

SKIP_TX = {"state": "skipped", "reason": "not_configured"}


def entry(root="claude", path=".", mode="restore", name="n", **kw):
    return {"root": root, "path": path, "mode": mode, "name": name, **kw}


class RunnerEnv(SaveEnv):
    async def make(self, entries, slug="alpha", *, seed=(), tx=True, **spec):
        """A session with `entries`; `seed` names get a current version; transcripts skipped."""
        for name in seed:
            await seed_backup(self.pool, name)
        async with self.pool.connection() as conn:
            c = await sessions.create(conn, slug, SessionSpec(backup=entries, **spec))
            status = c.status
            if tx:
                status = {**status, "backup": {"transcripts": {"restore": SKIP_TX}}}
            return await sessions.set_status(conn, c.id, status, c.observed_generation)

    def job_creates(self) -> list[str]:
        return [c.name for c in self.kube.calls if (c.verb, c.kind) == ("create", "Job")]

    async def events(self, session_id) -> list[dict]:
        async with self.pool.connection() as conn, conn.cursor(row_factory=dict_row) as cur:
            await cur.execute(
                "SELECT type, data FROM events WHERE session_id = %s ORDER BY id", (session_id,)
            )
            return await cur.fetchall()


@pytest_asyncio.fixture
async def env(pool, dsn) -> RunnerEnv:
    return RunnerEnv(pool, dsn)


async def test_phase_order_before_and_after_init(env):
    pre = entry("claude", ".", "restore", "pre-name")
    post = entry("workspace", "gen", "restore", "post-name", after_init=True)
    c = await env.make([pre, post], seed=["pre-name", "post-name"], init_scripts=["echo a"])
    await env.tick()
    assert env.job_creates() == [job_name("alpha", RESTORE_PRE)]
    pre_job = env.kube.get("Job", job_name("alpha", RESTORE_PRE))
    (sent,) = json.loads(pre_job["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"])
    assert sent["index"] == 0 and sent["name"] == "pre-name" and sent["root"] == "claude"

    env.kube.finish_job(job_name("alpha", RESTORE_PRE), log=line(res("restored", 0, size=1)))
    await env.tick(2)
    assert env.job_creates()[-1] == init_job_name("alpha")
    assert job_name("alpha", RESTORE_POST) not in env.job_creates()  # init still runs

    env.kube.finish_job(init_job_name("alpha"), log="ok")
    await env.tick(2)
    assert env.job_creates() == [
        job_name("alpha", RESTORE_PRE),
        init_job_name("alpha"),
        job_name("alpha", RESTORE_POST),
    ]
    post_job = env.kube.get("Job", job_name("alpha", RESTORE_POST))
    (sent,) = json.loads(post_job["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"])
    assert sent["index"] == 1 and sent["root"] == "workspace" and sent["path"] == "gen"
    env.kube.finish_job(job_name("alpha", RESTORE_POST), log=line(res("restored", 1)))
    await env.tick(2)
    assert (await env.entry(c.id, 1))["restore"]["state"] == "restored"
    assert (await env.run(c.id, RESTORE_POST))["state"] == "done"


async def test_missing_restore_warns_and_session_starts(pool, dsn):
    from fakes.fake_wrapper import FakeWrapper

    async with FakeWrapper() as fake:
        wrapped = Env(pool, dsn, fake)
        wrapped.kube.pods_ready = True
        c = await RunnerEnv.make(wrapped, [entry(name="proj/absent")])
        fake.token = tokens_for(wrapped.settings, c.id).auth
        await wrapped.tick(3)
        record = (await wrapped.entry(c.id))["restore"]
        assert record["state"] == "missing" and record["name"] == "proj/absent"
        assert record["version"] is None
        assert not wrapped.kube.has("Job", job_name("alpha", RESTORE_PRE))
        assert (await wrapped.run(c.id, RESTORE_PRE))["state"] == "done"
        status = await wrapped.status(c.id)
        assert status["phase"] == "ready" and len(fake.sessions) == 1  # the session started
        warnings = [
            ev["data"]
            for ev in await RunnerEnv.events(wrapped, c.id)
            if ev["type"] == cache_mod.WARNING_EVENT
        ]
        assert warnings == [
            {"index": 0, "name": "proj/absent", "state": "missing", "direction": "restore"}
        ]


async def test_unreadable_restore_warns_and_the_run_is_done(env):
    c = await env.make([entry(name="x")], seed=["x"])
    await env.tick()
    env.kube.finish_job(
        job_name("alpha", RESTORE_PRE), log=line("unreadable", reason="ExtractError")
    )
    await env.tick(2)
    assert (await env.run(c.id, RESTORE_PRE))["state"] == "done"
    states = [ev["data"]["state"] for ev in await env.events(c.id)]
    assert states == ["unreadable"]


async def test_resolve_job_only_when_sha256_used(env):
    lock = entry("workspace", "deps", "restore", "deps-{{ sha256('package-lock.json') }}")
    c = await env.make([lock])
    await env.tick()
    resolve = job_name("alpha", RESOLVE_PRE)
    assert env.job_creates() == [resolve]  # the restore waits for the digests
    await seed_backup(env.pool, "deps-0123456789abcdef")
    env.kube.finish_job(
        resolve, log=json.dumps({"digests": {"package-lock.json:16": "0123456789abcdef"}})
    )
    await env.tick(2)
    assert env.job_creates() == [resolve, job_name("alpha", RESTORE_PRE)]
    record = (await env.entry(c.id))["restore"]
    assert record["name"] == "deps-0123456789abcdef" and record["state"] == "running"

    # a plain name never creates a resolve Job
    d = await env.make([entry(name="plain")], slug="beta", seed=["plain"])
    await env.tick()
    assert job_name("beta", RESOLVE_PRE) not in env.job_creates()
    assert job_name("beta", RESTORE_PRE) in env.job_creates()
    assert d


async def test_failed_resolve_fails_the_restore_entries_but_not_the_session(env):
    c = await env.make([entry("workspace", "d", "restore", "x-{{ sha256('f') }}")])
    await env.tick()
    env.kube.finish_job(job_name("alpha", RESOLVE_PRE), exit_code=1, log="killed")
    await env.tick(2)
    assert (await env.entry(c.id))["restore"]["reason"] == "resolve_failed"
    assert (await env.run(c.id, RESTORE_PRE))["state"] == "failed"


async def test_discard_saves_each_backup_entry(env):
    skills = entry("claude", "skills", "both", "skills/{{ project }}")
    src = entry("workspace", "src", "backup", "src/{{ session_slug }}")
    c = await env.make([skills, src], seed=["skills/default"])
    await env.tick()
    env.kube.finish_job(job_name("alpha", RESTORE_PRE), log=line(res("restored", 0, size=1)))
    await env.tick(2)
    await env.delete(c)
    await env.tick()
    save = job_name("alpha", SAVE)
    sent = json.loads(
        env.kube.get("Job", save)["spec"]["template"]["spec"]["containers"][0]["env"][0]["value"]
    )
    assert [(e["index"], e["name"]) for e in sent] == [(0, "skills/default"), (1, "src/alpha")]
    assert all("X-Amz-Signature" in e["url"] and "/b/" in e["url"] for e in sent)
    assert env.job_creates().count(save) == 1  # one Job for every entry
    env.kube.finish_job(
        save,
        log=line(
            res("uploaded", 0, size=3, digest="1" * 64, manifest=None),
            res("uploaded", 1, size=4, digest="2" * 64, manifest=None),
        ),
    )
    await env.tick()
    assert await env.gone(c)
    async with env.pool.connection() as conn:
        skills_now = await backups_repo.current(conn, "skills/default")
        src_now = await backups_repo.current(conn, "src/alpha")
    assert skills_now.digest == "1" * 64 and skills_now.created_by == "alpha"
    assert skills_now.base_version is not None  # based on the version it restored
    assert src_now.digest == "2" * 64 and src_now.base_version is None


async def test_failed_restore_saves_orphan(env):
    c = await env.make([entry(mode="both", name="x")], seed=["x"])
    await env.tick()
    env.kube.finish_job(job_name("alpha", RESTORE_PRE), exit_code=1, log=line("failed", reason="h"))
    await env.tick(2)
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(job_name("alpha", SAVE), log=uploaded(5, "9" * 64))
    await env.tick()
    async with env.pool.connection() as conn:
        versions = (await backups_repo.versions(conn, "x", limit=10, cursor=None)).items
        current = await backups_repo.current(conn, "x")
    assert sorted(v.state for v in versions) == ["current", "orphan"]
    assert current.digest == "d" * 64  # the pointer did not move


async def test_unchanged_save_deletes_object(env, monkeypatch):
    deleted: list[list[str]] = []

    def fake_delete(settings, keys, **kw):
        deleted.append(list(keys))
        return []

    monkeypatch.setattr(cache_mod, "delete_keys", fake_delete)
    c = await env.make([entry(mode="both", name="x")], seed=["x"])
    await env.tick()
    env.kube.finish_job(job_name("alpha", RESTORE_PRE), log=line(res("restored", 0, size=1)))
    await env.tick(2)
    await env.delete(c)
    await env.tick()
    key = (await env.run(c.id, SAVE))["plan"][0]["key"]
    env.kube.finish_job(job_name("alpha", SAVE), log=uploaded(5, "d" * 64))  # the seed digest
    await env.tick()
    assert deleted == [[key]]
    async with env.pool.connection() as conn:
        assert len((await backups_repo.versions(conn, "x", limit=10, cursor=None)).items) == 1
    assert env.saves[-1]["state"] == "done"


async def test_refused_secret_is_a_warning_not_a_failure(env):
    warned: list[dict] = []
    real = env.rec.cache._warn

    async def spy(session, warnings):
        warned.extend(warnings)
        await real(session, warnings)

    env.rec.cache._warn = spy
    c = await env.make([entry(mode="backup", name="x")])
    await env.tick(2)
    await env.delete(c)
    await env.tick()
    env.kube.finish_job(job_name("alpha", SAVE), log=line(res("refused_secret", 0)))
    await env.tick()
    assert env.saves[-1]["state"] == "done"
    assert warned == [{"index": 0, "name": "x", "state": "refused_secret", "direction": "save"}]
    assert await env.gone(c)


async def test_status_backup_shape(env):
    c = await env.make([entry(mode="both", name="x")], seed=["x"])
    await env.tick()
    env.kube.finish_job(job_name("alpha", RESTORE_PRE), log=line(res("restored", 0, size=1)))
    await env.tick(2)
    backup = await env.backup(c.id)
    assert set(backup) == {"entries", "runs", "transcripts"}
    (rec,) = backup["entries"]
    assert set(rec) == {"index", "root", "path", "version", "base_version", "restore", "save"}
    assert (rec["index"], rec["root"], rec["path"]) == (0, "claude", ".")
    assert set(rec["restore"]) == {"state", "name", "version", "at"}
    assert rec["restore"]["state"] == "restored" and rec["restore"]["name"] == "x"
    assert rec["version"] == rec["restore"]["version"] and rec["save"] is None
    async with env.pool.connection() as conn:
        assert {str(v) for v in await backups_repo.referenced_versions(conn)} == {rec["version"]}


async def test_no_workspace_entry_skips_transcripts(env):
    c = await env.make([entry(mode="both", name="x")], seed=["x"], tx=False)
    await env.tick(2)
    assert job_name("alpha", TX_RESTORE) not in env.job_creates()
    assert await env.run(c.id, TX_RESTORE) is None
    await env.delete(c)
    await env.tick(3)
    assert job_name("alpha", TX_SAVE) not in env.job_creates()
    assert await env.gone(c)
