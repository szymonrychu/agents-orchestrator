"""The volume reconciler against the real test Postgres and the fake kube."""

import uuid

import pytest
from factories import insert_session
from fakes.fake_kube import FakeKube

from agents_orchestrator.db import volumes
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.k8s.labels import VOLUME_FINALIZER, volume_pod_label
from agents_orchestrator.reconciler.volumes import VolumeReconciler
from agents_orchestrator.settings import Settings

KEY = "x" * 32
PVC = "PersistentVolumeClaim"


@pytest.fixture
def kube():
    return FakeKube()


@pytest.fixture
def rec(pool, kube, dsn):
    settings = Settings(database_url=dsn, master_key=KEY, agent_image="img:1")
    return VolumeReconciler(pool, kube, settings)


async def _make(pool, *, name="data", size="10Gi"):
    async with pool.connection() as conn, conn.transaction():
        return await volumes.create(
            conn,
            name=name,
            projects=["p"],
            size=size,
            storage_class="rook-ceph-rwx",
            created_by="alice",
            actor_kind="user",
            actor="alice",
        )


async def _get(pool, vid):
    async with pool.connection() as conn:
        return await volumes.get(conn, vid)


async def _deleted_at(pool, vid):
    async with pool.connection() as conn:
        cur = await conn.execute("SELECT deleted_at FROM volumes WHERE id=%s", (vid,))
        return (await cur.fetchone())[0]


async def _kinds(pool, vid):
    async with pool.connection() as conn:
        page = await volumes.events(conn, vid, limit=200)
    return [(e.kind, e.actor_kind, e.actor) for e in reversed(page.items)]


async def _claim(pool, vid, session_id):
    async with pool.connection() as conn, conn.transaction():
        await volumes.claim(
            conn,
            volume_id=vid,
            session_id=session_id,
            preset_id=None,
            mode="rw",
            backup_restore=False,
            session_slug="s-x",
            current_version=None,
        )


async def _state(pool, vid):
    return (await _get(pool, vid)).state


async def _ready(pool, kube, rec, name="data"):
    v = await _make(pool, name=name)
    await rec.tick()
    kube.set_pvc(v.pvc_name, "Bound", "10Gi")
    await rec.tick()
    assert await _state(pool, v.id) == "ready"
    return v


async def test_provisioning_applies_pvc_and_turns_ready_when_bound(pool, kube, rec):
    v = await _make(pool)
    await rec.tick()
    pvc = kube.get(PVC, "vol-data")
    assert pvc["spec"]["accessModes"] == ["ReadWriteMany"]
    assert pvc["metadata"]["finalizers"] == [VOLUME_FINALIZER]
    assert await _state(pool, v.id) == "provisioning"
    kube.set_pvc("vol-data", "Bound", "10Gi")
    await rec.tick()
    assert await _state(pool, v.id) == "ready"


@pytest.mark.parametrize(
    ("setup", "reason"),
    [("apply_error", "pvc_apply_failed"), ("lost", "pvc_lost")],
)
async def test_provisioning_failures_set_a_fixed_reason(pool, kube, rec, setup, reason):
    v = await _make(pool)
    if setup == "apply_error":
        kube.fail(PVC, "vol-data", KubeError("boom"), verbs=["patch"])
        await rec.tick()
    else:
        await rec.tick()
        kube.set_pvc("vol-data", "Lost")
        await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.state_reason) == ("failed", reason)


async def test_resizing_patches_request_and_turns_ready_at_capacity(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    async with pool.connection() as conn, conn.transaction():
        await volumes.update(conn, v.id, size="20Gi", actor_kind="user", actor="alice")
    assert await _state(pool, v.id) == "resizing"
    await rec.tick()
    assert kube.get(PVC, "vol-data")["spec"]["resources"]["requests"]["storage"] == "20Gi"
    assert await _state(pool, v.id) == "resizing"
    kube.set_pvc("vol-data", "Bound", "20Gi")
    await rec.tick()
    assert await _state(pool, v.id) == "ready"
    kinds = [k for k, _, _ in await _kinds(pool, v.id)]
    assert kinds.count("resized") == 1


async def test_delete_removes_finalizer_then_pvc_and_sets_deleted_at(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    async with pool.connection() as conn, conn.transaction():
        await volumes.mark_deleting(conn, v.id)
    await rec.tick()
    assert not kube.has(PVC, "vol-data")
    assert kube.patches == [(PVC, "vol-data", {"metadata": {"finalizers": None}})]
    assert kube.deleted(PVC) == ["vol-data"]
    await rec.tick()
    assert await _deleted_at(pool, v.id) is not None
    assert ("deleted", "system", "reconciler") in await _kinds(pool, v.id)


async def test_delete_with_a_claim_keeps_the_pvc_and_records_refusal(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    async with pool.connection() as conn, conn.transaction():
        await volumes.mark_deleting(conn, v.id)
    async with pool.connection() as conn:  # a claim the guard did not see (a racing writer)
        await conn.execute(
            "INSERT INTO volume_mounts (volume_id, session_id, mode, backup_restore)"
            " VALUES (%s, %s, 'rw', false)",
            (v.id, uuid.uuid4()),
        )
    await rec.tick()
    assert kube.has(PVC, "vol-data")
    assert await _state(pool, v.id) == "ready"
    assert "delete_refused" in [k for k, _, _ in await _kinds(pool, v.id)]


async def test_external_delete_leaves_pvc_terminating_while_finalizer_held(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    await kube.delete(PVC, "agents", "vol-data")
    await rec.tick()
    pvc = kube.get(PVC, "vol-data")
    assert pvc["metadata"]["deletionTimestamp"]
    assert pvc["metadata"]["finalizers"] == [VOLUME_FINALIZER]
    assert await _state(pool, v.id) == "ready"


async def test_ready_volume_with_missing_pvc_fails(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    del kube.objects[(PVC, "agents", "vol-data")]
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.state_reason) == ("failed", "pvc_missing")


async def test_orphan_claim_released_by_system_only_when_no_pod_remains(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    gone, alive = uuid.uuid4(), await insert_session(pool)
    await _claim(pool, v.id, gone)
    await _claim(pool, v.id, alive["id"])
    kube.seed(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {
                "name": "old-0",
                "namespace": "agents",
                "labels": {volume_pod_label(v.id): "rw"},
            },
            "status": {"phase": "Running"},
        }
    )
    await rec.tick()
    async with pool.connection() as conn:
        assert {c.session_id for c in await volumes.unreleased_claims(conn, v.id)} == {
            gone,
            alive["id"],
        }
    await kube.delete_pod("agents", "old-0")
    await rec.tick()
    async with pool.connection() as conn:
        assert [c.session_id for c in await volumes.unreleased_claims(conn, v.id)] == [alive["id"]]
    assert ("released", "system", "reconciler") in await _kinds(pool, v.id)
