"""The session reconciler and shared volumes: claim before any pod, gating, release on discard.

Real test Postgres, FakeKube; the volume reconciler runs where a test needs its Jobs.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest
from fakes.fake_kube import FakeKube

from agents_orchestrator.db import backups, sessions, volumes
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.volumes import VolumeReconciler
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

KEY = "k" * 32
IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"
PVC = "PersistentVolumeClaim"


@pytest.fixture
def kube():
    return FakeKube(namespace="agents")


@pytest.fixture
def rec(pool, kube, dsn):
    return Reconciler(pool, kube, Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE))


@pytest.fixture
def vrec(pool, kube, dsn):
    settings = Settings(
        database_url=dsn,
        master_key=KEY,
        agent_image=IMAGE,
        namespace="agents",
        cache_s3_endpoint="http://rgw.example:80",
        cache_s3_bucket="bucket",
        cache_s3_access_key_id="TESTIDEXAMPLEACCESSKEY",
        cache_s3_secret_access_key="s3cr3t-example-secret-access-key",
        cache_image="img:cache",
    )
    return VolumeReconciler(pool, kube, settings)


async def _volume(pool, kube, vrec) -> volumes.Volume:
    async with pool.connection() as conn, conn.transaction():
        v = await volumes.create(
            conn,
            name="data",
            projects=["p"],
            size="10Gi",
            storage_class="rook-ceph-rwx",
            created_by="alice",
            actor_kind="user",
            actor="alice",
        )
    await vrec.tick()
    kube.set_pvc(v.pvc_name, "Bound", "10Gi")
    await vrec.tick()
    (owner,) = kube.names("Job")
    kube.finish_job(owner)
    await vrec.tick()
    async with pool.connection() as conn:
        got = await volumes.get(conn, v.id)
    assert got.state == "ready"
    return got


async def _version(pool, vol) -> backups.Version:
    async with pool.connection() as conn, conn.transaction():
        _, v = await backups.record_save(
            conn,
            name=f"volume-{vol.name}",
            object_key=f"pre/b/volume-{vol.name}/{uuid.uuid4().hex}.tar.zst",
            size=5,
            digest="d" * 64,
            manifest=None,
            base_version=None,
            origin="save",
            session_slug="writer",
            preset_id=None,
            project="p",
            created_by="writer",
            orphan=False,
            volume_writer=True,
        )
    return v


async def _session(pool, slug, vol, *, mode="rw", backup_restore=False, **spec):
    att = {"volume": str(vol.id), "mode": mode, "backup_restore": backup_restore}
    async with pool.connection() as conn:
        return await sessions.create(conn, slug, SessionSpec(volumes=[att], **spec))


async def _delete(pool, session_id) -> None:
    async with pool.connection() as conn:
        await sessions.request_delete(conn, session_id)


async def _kinds(pool, vid) -> list[str]:
    async with pool.connection() as conn:
        page = await volumes.events(conn, vid, limit=200)
    return [e.kind for e in reversed(page.items)]


async def _claimants(pool, vid) -> set[uuid.UUID]:
    async with pool.connection() as conn:
        return {c.session_id for c in await volumes.unreleased_claims(conn, vid)}


def _pods(kube, slug) -> list[str]:
    return [n for n in kube.names("Pod") if n.startswith(f"agent-{slug}-")]


def _pod_template(kube, kind, slug) -> dict:
    if kind == "StatefulSet":
        return kube.get("StatefulSet", f"agent-{slug}")["spec"]["template"]["spec"]
    (job,) = [o for (k, _ns, _n), o in kube.objects.items() if k == "Job"]
    return job["spec"]["template"]["spec"]


@pytest.mark.parametrize("kind", ["StatefulSet", "Job"])
@pytest.mark.parametrize("mode", ["ro", "rw"])
async def test_attached_volume_is_mounted_by_the_pod_and_the_init_job(
    pool, kube, rec, vrec, kind, mode
):
    vol = await _volume(pool, kube, vrec)
    await _session(pool, "alpha", vol, mode=mode, init_scripts=["echo hi"])
    await rec.reconcile_once()
    pod = _pod_template(kube, kind, "alpha")
    (pv,) = [
        v
        for v in pod["volumes"]
        if v.get("persistentVolumeClaim", {}).get("claimName") == "vol-data"
    ]
    mounts = [m for c in pod["containers"] for m in c.get("volumeMounts", [])]
    (mount,) = [m for m in mounts if m["name"] == pv["name"]]
    assert (mount["mountPath"], mount["readOnly"]) == ("/volumes/data", mode == "ro")
    assert pv["persistentVolumeClaim"]["readOnly"] == (mode == "ro")


async def test_session_waits_at_zero_pods_while_its_volume_restores(pool, kube, rec, vrec):
    vol = await _volume(pool, kube, vrec)
    ver = await _version(pool, vol)
    async with pool.connection() as conn, conn.transaction():
        await volumes.begin_explicit_restore(conn, vol.id, version=ver.id, actor="alice")
    s = await _session(pool, "alpha", vol, init_scripts=["echo hi"])
    for _ in range(2):
        await rec.reconcile_once()
        assert _pods(kube, "alpha") == []
        assert kube.names("Job") == []  # nor the init Job, which mounts the volume too
    async with pool.connection() as conn:
        got = await sessions.get(conn, s.id)
    assert got.status["phase"] == "pending"
    assert "waiting for volume data" in got.status["message"]
    async with pool.connection() as conn:
        await volumes.finish_restore(conn, vol.id, version=ver.id)
    await rec.reconcile_once()
    assert "agent-alpha-0" in _pods(kube, "alpha")
    async with pool.connection() as conn:
        got = await sessions.get(conn, s.id)
    assert got.status["volume_restores_waited"] == ["data"]


@pytest.mark.parametrize(
    ("refused", "message"),
    [
        ("deleting", "volume data is being deleted"),
        ("owner_exhausted", "volume data failed: its root could not be handed to the agent user"),
    ],
)
async def test_refused_volume_holds_a_new_session_degraded(pool, kube, rec, vrec, refused, message):
    vol = await _volume(pool, kube, vrec)
    async with pool.connection() as conn, conn.transaction():
        if refused == "deleting":
            await volumes.mark_deleting(conn, vol.id)
        else:  # the owner Job failed past its retries
            await conn.execute(
                "UPDATE volumes SET state = 'failed', state_reason = 'owner_failed',"
                " owner_attempts = %s WHERE id = %s",
                (volumes.OWNER_ATTEMPTS_MAX, vol.id),
            )
    s = await _session(pool, "alpha", vol)
    await rec.reconcile_once()
    async with pool.connection() as conn:
        got = await sessions.get(conn, s.id)
    assert _pods(kube, "alpha") == []
    assert got.status["phase"] == "degraded"
    assert message in got.status["message"]


@pytest.mark.parametrize(
    ("volume_deleting", "init_scripts", "phase"),
    [(False, ["echo hi"], "pending"), (True, [], "degraded")],
)
async def test_only_a_refused_volume_holds_the_session_degraded(
    pool, kube, rec, vrec, volume_deleting, init_scripts, phase
):
    vol = await _volume(pool, kube, vrec)
    if volume_deleting:
        async with pool.connection() as conn, conn.transaction():
            await volumes.mark_deleting(conn, vol.id)
    s = await _session(pool, "alpha", vol, init_scripts=init_scripts)
    await rec.reconcile_once()
    async with pool.connection() as conn:
        got = await sessions.get(conn, s.id)
    assert got.status["phase"] == phase


async def test_running_session_keeps_its_pods_while_its_volume_resizes(pool, kube, rec, vrec):
    vol = await _volume(pool, kube, vrec)
    await _session(pool, "alpha", vol)
    await rec.reconcile_once()
    async with pool.connection() as conn, conn.transaction():
        await volumes.update(conn, vol.id, size="20Gi", actor_kind="user", actor="alice")
    await rec.reconcile_once()
    assert _pods(kube, "alpha") == ["agent-alpha-0"]


async def test_two_sessions_starting_at_once_restore_the_volume_once(pool, kube, dsn, vrec):
    vol = await _volume(pool, kube, vrec)
    await _version(pool, vol)
    await _session(pool, "alpha", vol, mode="rw", backup_restore=True)
    await _session(pool, "beta", vol, mode="ro", backup_restore=True)
    settings = Settings(database_url=dsn, master_key=KEY, agent_image=IMAGE)
    recs = [Reconciler(pool, kube, settings) for _ in range(2)]
    await asyncio.gather(*(r.reconcile_once() for r in recs))
    assert (await _kinds(pool, vol.id)).count("restore_started") == 1
    for _ in range(2):
        await vrec.tick()
        await recs[0].reconcile_once()
        assert _pods(kube, "alpha") == _pods(kube, "beta") == []
    jobs = [o for (k, _ns, _n), o in kube.objects.items() if k == "Job"]
    assert len(jobs) == 1


@pytest.mark.parametrize(
    ("backup_restore", "other_claimant", "saves"),
    [(True, False, 1), (True, True, 0), (False, False, 0)],
)
async def test_discard_releases_the_claim_and_saves_only_after_the_last_backup_mounter(
    pool, kube, rec, vrec, backup_restore, other_claimant, saves
):
    vol = await _volume(pool, kube, vrec)
    s = await _session(pool, "alpha", vol, backup_restore=backup_restore)
    if other_claimant:
        other = await _session(pool, "beta", vol)
    await rec.reconcile_once()
    await _delete(pool, s.id)
    await rec.reconcile_once()
    async with pool.connection() as conn:
        assert await sessions.get(conn, s.id) is None
    assert await _claimants(pool, vol.id) == ({other.id} if other_claimant else set())
    assert (await _kinds(pool, vol.id)).count("save_started") == saves


@pytest.mark.parametrize(
    ("first_mode", "first_backup", "saves"),
    [("rw", True, 1), ("ro", True, 0), ("rw", False, 0)],
)
async def test_last_release_saves_when_a_backup_on_writer_mounted_since_the_last_save(
    pool, kube, rec, vrec, first_mode, first_backup, saves
):
    vol = await _volume(pool, kube, vrec)
    first = await _session(pool, "alpha", vol, mode=first_mode, backup_restore=first_backup)
    last = await _session(pool, "beta", vol, mode="ro")
    await rec.reconcile_once()
    await _delete(pool, first.id)
    await rec.reconcile_once()
    assert (await _kinds(pool, vol.id)).count("save_started") == 0
    await _delete(pool, last.id)
    await rec.reconcile_once()
    assert await _claimants(pool, vol.id) == set()
    assert (await _kinds(pool, vol.id)).count("save_started") == saves


async def test_discard_leaves_the_volume_pvc_untouched(pool, kube, rec, vrec):
    vol = await _volume(pool, kube, vrec)
    s = await _session(pool, "alpha", vol, backup_restore=True)
    await rec.reconcile_once()
    await _delete(pool, s.id)
    await rec.reconcile_once()
    async with pool.connection() as conn:
        assert await sessions.get(conn, s.id) is None
    assert kube.has(PVC, "vol-data")
    assert (PVC, "agents", "vol-data") not in kube.deletes


async def test_deleting_session_with_live_pods_keeps_its_claim_and_blocks_a_restore(
    pool, kube, rec, vrec
):
    vol = await _volume(pool, kube, vrec)
    old = await _session(pool, "alpha", vol)
    await rec.reconcile_once()
    kube.fail("StatefulSet", "agent-alpha", verbs=["delete"])  # its pods keep running
    await _delete(pool, old.id)
    await rec.reconcile_once()
    assert _pods(kube, "alpha") == ["agent-alpha-0"]
    await _version(pool, vol)
    new = await _session(pool, "beta", vol, backup_restore=True)
    await rec.reconcile_once()
    assert await _claimants(pool, vol.id) == {old.id, new.id}
    kinds = await _kinds(pool, vol.id)
    assert "restore_started" not in kinds and "restore_skipped_mounted" in kinds
    assert _pods(kube, "beta") == ["agent-beta-0"]
