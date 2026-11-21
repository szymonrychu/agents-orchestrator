"""The volume reconciler against the real test Postgres and the fake kube."""

import json
import uuid

import pytest
from factories import insert_session
from fakes.fake_kube import FakeKube

from agents_orchestrator.db import backups, volumes
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.k8s.labels import VOLUME_FINALIZER, volume_pod_label
from agents_orchestrator.reconciler import volumes as volumes_mod
from agents_orchestrator.reconciler.volumes import VolumeReconciler
from agents_orchestrator.settings import Settings

KEY = "x" * 32
PVC = "PersistentVolumeClaim"


@pytest.fixture
def kube():
    return FakeKube()


@pytest.fixture
def rec(pool, kube, dsn):
    settings = Settings(
        database_url=dsn,
        master_key=KEY,
        agent_image="img:1",
        cache_s3_endpoint="http://rgw.example:80",
        cache_s3_bucket="bucket",
        cache_s3_access_key_id="TESTIDEXAMPLEACCESSKEY",
        cache_s3_secret_access_key="s3cr3t-example-secret-access-key",
        cache_image="img:cache",
    )
    return VolumeReconciler(pool, kube, settings)


async def _make(pool, *, name="data", size="10Gi", **kw):
    async with pool.connection() as conn, conn.transaction():
        return await volumes.create(
            conn,
            **kw,
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


# ------------------------------------------------------------------ restore and save Jobs

DIGEST = "d" * 64


async def _version(pool, vol, digest=DIGEST, *, slug="writer"):
    async with pool.connection() as conn, conn.transaction():
        _, v = await backups.record_save(
            conn,
            name=f"volume-{vol.name}",
            object_key=f"pre/b/volume-{vol.name}/{digest[:8]}-{uuid.uuid4().hex[:4]}.tar.zst",
            size=5,
            digest=digest,
            manifest=None,
            base_version=None,
            origin="save",
            session_slug=slug,
            preset_id=None,
            project="p",
            created_by=slug,
            orphan=False,
            volume_writer=True,
        )
    return v


async def _restore(pool, vol, version):
    async with pool.connection() as conn, conn.transaction():
        await volumes.begin_explicit_restore(conn, vol.id, version=version, actor="alice")


async def _save(pool, vol, trigger="user", slug=None):
    async with pool.connection() as conn, conn.transaction():
        return await volumes.request_save(
            conn, vol.id, trigger=trigger, session_slug=slug, actor="alice"
        )


def _jobs(kube):
    return [o for (kind, _ns, _n), o in kube.objects.items() if kind == "Job"]


def _outcome(**result):
    return json.dumps({"entries": [{"index": 0, **result}]})


def _uploaded(digest=DIGEST):
    return _outcome(state="uploaded", size=7, digest=digest, manifest=[{"path": "a"}])


async def test_restore_with_a_foreign_pod_aborts_and_creates_no_job(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    ver = await _version(pool, v)
    kube.seed(
        {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": "x-0", "labels": {volume_pod_label(v.id): "rw"}},
            "status": {"phase": "Running"},
        }
    )
    await _restore(pool, v, ver.id)
    await rec.tick()
    assert _jobs(kube) == []
    got = await _get(pool, v.id)
    assert (got.state, got.content_version) == ("ready", None)
    kinds = [k for k, *_ in await _kinds(pool, v.id)]
    assert kinds[-2:] == ["restore_started", "restore_skipped_mounted"]
    async with pool.connection() as conn:
        last = (await volumes.events(conn, v.id, limit=1)).items[0]
    assert last.detail == {"reason": "pod_found"}


async def test_successful_restore_sets_content_version(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    ver = await _version(pool, v)
    await _restore(pool, v, ver.id)
    await rec.tick()
    (job,) = _jobs(kube)
    assert await _state(pool, v.id) == "restoring"
    kube.finish_job(job["metadata"]["name"], log=_outcome(state="restored"))
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.content_version) == ("ready", ver.id)
    assert (await _kinds(pool, v.id))[-1][0] == "restored"
    assert _jobs(kube) == []


@pytest.mark.parametrize(
    "finish",
    [
        {"exit_code": 1},
        {"deadline_exceeded": True},
        {"log": _outcome(state="unreadable", reason="bad archive")},
    ],
)
async def test_failed_restore_keeps_content_version_and_turns_ready(pool, kube, rec, finish):
    v = await _ready(pool, kube, rec)
    ver = await _version(pool, v)
    await _restore(pool, v, ver.id)
    await rec.tick()
    (job,) = _jobs(kube)
    kube.finish_job(job["metadata"]["name"], **finish)
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.content_version) == ("ready", None)
    assert (await _kinds(pool, v.id))[-1][0] == "restore_failed"


async def test_save_with_a_new_digest_moves_current_and_content_version(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    await _version(pool, v, "a" * 64)
    await _save(pool, v, "agent", "s-1")
    await rec.tick()
    (job,) = _jobs(kube)
    assert job["metadata"]["name"].startswith("vol-data-save-")
    kube.finish_job(job["metadata"]["name"], log=_uploaded())
    await rec.tick()
    async with pool.connection() as conn:
        cur = await backups.current(conn, "volume-data")
    got = await _get(pool, v.id)
    assert (got.state, got.content_version) == ("ready", cur.id)
    assert cur.digest == DIGEST and cur.origin == "save"
    assert (cur.session_slug, cur.project, cur.preset_id) == ("s-1", "p", None)
    assert cur.manifest == {"files": [{"path": "a"}]}
    assert (await _kinds(pool, v.id))[-1][0] == "saved"


async def test_save_with_an_unchanged_digest_keeps_current_and_deletes_the_object(
    pool, kube, rec, monkeypatch
):
    deleted = []
    monkeypatch.setattr(volumes_mod, "delete_keys", lambda _s, keys: deleted.extend(keys) or [])
    v = await _ready(pool, kube, rec)
    cur = await _version(pool, v)
    await _save(pool, v)
    await rec.tick()
    (job,) = _jobs(kube)
    kube.finish_job(job["metadata"]["name"], log=_uploaded())
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.content_version) == ("ready", cur.id)
    assert (await _kinds(pool, v.id))[-1][0] == "save_unchanged"
    assert len(deleted) == 1
    async with pool.connection() as conn:
        assert (await backups.current(conn, "volume-data")).id == cur.id


async def test_failed_save_turns_ready_and_changes_nothing(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    await _save(pool, v)
    await rec.tick()
    (job,) = _jobs(kube)
    kube.finish_job(job["metadata"]["name"], exit_code=1)
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.content_version) == ("ready", None)
    assert (await _kinds(pool, v.id))[-1][0] == "save_failed"
    async with pool.connection() as conn:
        assert await backups.current(conn, "volume-data") is None


async def test_save_while_a_claim_exists_marks_the_manifest_live(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    await _claim(pool, v.id, uuid.uuid4())
    await _save(pool, v)
    await rec.tick()
    (job,) = _jobs(kube)
    kube.finish_job(job["metadata"]["name"], log=_uploaded())
    await rec.tick()
    async with pool.connection() as conn:
        cur = await backups.current(conn, "volume-data")
    assert cur.manifest["live"] is True


async def test_save_job_carries_the_volume_excludes_and_restore_ignores_them(pool, kube, rec):
    v = await _make(pool, exclude=["node_modules", "*.log"])
    await rec.tick()
    kube.set_pvc(v.pvc_name, "Bound", "10Gi")
    await rec.tick()
    ver = await _version(pool, v)
    await _save(pool, v)
    await rec.tick()
    (job,) = _jobs(kube)
    name = job["metadata"]["name"]

    def entry(job):
        (c,) = job["spec"]["template"]["spec"]["containers"]
        env = {e["name"]: e["value"] for e in c["env"]}
        return json.loads(env["CACHE_ENTRIES"])[0]

    assert entry(job)["exclude"] == ["node_modules", "*.log"]
    kube.finish_job(name, exit_code=1)
    await rec.tick()
    await _restore(pool, v, ver.id)
    await rec.tick()
    (rjob,) = _jobs(kube)
    assert entry(rjob)["exclude"] == []
