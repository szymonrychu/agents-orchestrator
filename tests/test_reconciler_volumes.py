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


async def _claim(pool, vid, session_id, *, backup_restore=False):
    async with pool.connection() as conn, conn.transaction():
        await volumes.claim(
            conn,
            volume_id=vid,
            session_id=session_id,
            preset_id=None,
            mode="rw",
            backup_restore=backup_restore,
            session_slug="s-x",
            current_version=None,
        )


async def _state(pool, vid):
    return (await _get(pool, vid)).state


def _owner_job(kube):
    (name,) = [n for n in kube.names("Job") if "-owner-" in n]
    return name


async def _bind(kube, rec, v):
    """The PVC binds, the owner Job runs and succeeds: the volume turns ready."""
    kube.set_pvc(v.pvc_name, "Bound", "10Gi")
    await rec.tick()
    kube.finish_job(_owner_job(kube))
    await rec.tick()


async def _ready(pool, kube, rec, name="data"):
    v = await _make(pool, name=name)
    await rec.tick()
    await _bind(kube, rec, v)
    assert await _state(pool, v.id) == "ready"
    return v


async def test_provisioning_applies_pvc_and_turns_ready_after_the_owner_job(pool, kube, rec):
    """An unseeded volume gets its root handed to the agent user before it turns ready (claims
    wait until then, so no pod mounts it first)."""
    v = await _make(pool)
    await rec.tick()
    pvc = kube.get(PVC, "vol-data")
    assert pvc["spec"]["accessModes"] == ["ReadWriteMany"]
    assert pvc["metadata"]["finalizers"] == [VOLUME_FINALIZER]
    assert await _state(pool, v.id) == "provisioning"
    kube.set_pvc("vol-data", "Bound", "10Gi")
    await rec.tick()
    owner = kube.get("Job", _owner_job(kube))
    (step,) = owner["spec"]["template"]["spec"]["containers"]
    assert step["command"] == ["chown", "1000:1000", "/volume"]
    assert await _state(pool, v.id) == "provisioning"
    await rec.tick()  # the Job is still running
    assert await _state(pool, v.id) == "provisioning"
    kube.finish_job(owner["metadata"]["name"])
    await rec.tick()
    assert await _state(pool, v.id) == "ready"
    assert kube.names("Job") == []


@pytest.mark.parametrize(
    ("setup", "reason"),
    [("apply_error", "pvc_apply_failed"), ("lost", "pvc_lost"), ("owner", "owner_failed")],
)
async def test_provisioning_failures_set_a_fixed_reason(pool, kube, rec, setup, reason):
    v = await _make(pool)
    if setup == "apply_error":
        kube.fail(PVC, "vol-data", KubeError("boom"), verbs=["patch"])
        await rec.tick()
    elif setup == "lost":
        await rec.tick()
        kube.set_pvc("vol-data", "Lost")
        await rec.tick()
    else:
        await rec.tick()
        kube.set_pvc("vol-data", "Bound", "10Gi")
        await rec.tick()
        kube.finish_job(_owner_job(kube), exit_code=1)
        await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.state_reason) == ("failed", reason)


async def _owner_running(pool, kube, rec):
    v = await _make(pool)
    await rec.tick()
    kube.set_pvc("vol-data", "Bound", "10Gi")
    await rec.tick()
    return v, _owner_job(kube)


def _failed(reason):
    return {"type": "Failed", "status": "True", "reason": reason}


@pytest.mark.parametrize(
    ("status", "state", "logged"),
    [
        ({"failed": 1, "active": 1}, "provisioning", None),  # a pod failed, the Job retries
        ({"failed": 3, "conditions": [_failed("BackoffLimitExceeded")]}, "failed", "job_failed"),
        ({"failed": 1, "conditions": [_failed("DeadlineExceeded")]}, "failed", "job_timeout"),
    ],
)
async def test_owner_job_ends_only_on_its_terminal_conditions(
    pool, kube, rec, caplog, status, state, logged
):
    v, name = await _owner_running(pool, kube, rec)
    kube.get("Job", name)["status"] = status
    await rec.tick()
    assert await _state(pool, v.id) == state
    assert kube.has("Job", name) is (state == "provisioning")
    if logged:
        assert f"volume data: the owner Job failed ({logged}" in caplog.text


async def test_owner_failure_logs_the_exit_code_of_the_newest_pod_of_its_job(
    pool, kube, rec, caplog
):
    _v, name = await _owner_running(pool, kube, rec)
    labels = kube.get("Pod", f"{name}-x7k2p")["metadata"]["labels"]

    def pod(pod_name, pod_labels, created, code):
        return {
            "apiVersion": "v1",
            "kind": "Pod",
            "metadata": {"name": pod_name, "labels": pod_labels, "creationTimestamp": created},
            "status": {
                "phase": "Failed",
                "containerStatuses": [{"name": "c", "state": {"terminated": {"exitCode": code}}}],
            },
        }

    kube.seed(
        pod("retry", labels, "2099-01-01T00:00:00Z", 3),  # the Job's newest try
        pod(  # a pod of an earlier owner Job of the volume, newer still
            "earlier",
            {**labels, "batch.kubernetes.io/controller-uid": "an-earlier-job"},
            "2099-01-02T00:00:00Z",
            9,
        ),
    )
    kube.finish_job(name, exit_code=1)
    await rec.tick()
    assert "volume data: the owner Job failed (job_failed, exit 3)" in caplog.text


async def test_owner_retry_runs_a_new_job_beside_the_failed_one_of_the_last_attempt(
    pool, kube, rec
):
    """Each owner attempt has its own Job name, from the row alone: a retry never takes the
    previous attempt's failed Job (still present, its delete failed) for its own."""
    now = [1000.0]
    rec._clock = lambda: now[0]
    v, first = await _owner_running(pool, kube, rec)
    assert first == f"vol-{v.id}-owner-0"
    kube.finish_job(first, exit_code=1)
    kube.fail("Job", first, KubeError("boom"), verbs=["delete"])
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.state_reason, got.owner_attempts) == ("failed", "owner_failed", 1)
    await rec.tick()  # the backoff starts
    now[0] += volumes_mod.RETRY_BASE_SECONDS + 1
    await rec.tick()  # back to provisioning
    await rec.tick()  # the next attempt's Job
    second = f"vol-{v.id}-owner-1"
    assert sorted(kube.names("Job")) == sorted([first, second])
    assert await _state(pool, v.id) == "provisioning"
    kube.finish_job(second)
    await rec.tick()
    assert await _state(pool, v.id) == "ready"


async def test_owner_failures_retry_until_their_attempts_run_out_then_refuse_claims(
    pool, kube, rec
):
    """Owner retries are bounded only by the attempt count on the row, not by the in-process
    retry budget (here partly spent on a PVC apply failure): each failure retries until
    OWNER_ATTEMPTS_MAX, then the volume is terminal, also for a fresh reconciler after a
    restart, and a claim is refused rather than left waiting."""
    now = [1000.0]
    rec._clock = lambda: now[0]
    v = await _make(pool)
    kube.fail(PVC, "vol-data", KubeError("boom"), verbs=["patch"], times=1)
    for _ in range(4):  # the apply fails, back to provisioning past the backoff, the PVC
        now[0] += 10_000
        await rec.tick()
    kube.set_pvc("vol-data", "Bound", "10Gi")
    await rec.tick()  # the first owner Job
    for _ in range(volumes.OWNER_ATTEMPTS_MAX):
        kube.finish_job(_owner_job(kube), exit_code=1)
        for _ in range(4):  # fail, back to provisioning past the backoff, the next Job
            now[0] += 10_000
            await rec.tick()
    assert (await _get(pool, v.id)).owner_attempts == volumes.OWNER_ATTEMPTS_MAX
    fresh = VolumeReconciler(pool, kube, rec.settings)
    fresh._clock = lambda: now[0]
    for _ in range(3):
        now[0] += 10_000
        await fresh.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.state_reason) == ("failed", "owner_failed")
    assert kube.names("Job") == []
    async with pool.connection() as conn, conn.transaction():
        d = await volumes.claim(
            conn,
            volume_id=v.id,
            session_id=uuid.uuid4(),
            preset_id=None,
            mode="rw",
            backup_restore=False,
            session_slug="s-x",
            current_version=None,
        )
    assert (d.decision, d.claim) == ("refuse", None)


async def test_an_api_error_creating_the_owner_job_retries_without_using_an_attempt(
    pool, kube, rec
):
    """Only an owner Job that ran and failed counts: a refused create (API blip, 409, 429) is
    retried next pass and the volume stays provisioning."""
    v = await _make(pool)
    await rec.tick()
    kube.set_pvc("vol-data", "Bound", "10Gi")
    kube.fail("Job", f"vol-{v.id}-owner-0", KubeError("429"), verbs=["create"], times=1)
    await rec.tick()
    got = await _get(pool, v.id)
    assert (got.state, got.owner_attempts, kube.names("Job")) == ("provisioning", 0, [])
    await rec.tick()
    assert _owner_job(kube) == f"vol-{v.id}-owner-0"


@pytest.mark.parametrize("leave", ["deleting", "lost"])
async def test_owner_job_is_deleted_when_the_volume_leaves_provisioning(pool, kube, rec, leave):
    v, name = await _owner_running(pool, kube, rec)
    if leave == "deleting":
        async with pool.connection() as conn, conn.transaction():
            await volumes.mark_deleting(conn, v.id)
    else:
        kube.set_pvc("vol-data", "Lost")
    await rec.tick()
    assert not kube.has("Job", name)


async def test_transient_pvc_apply_failure_is_retried_with_backoff_then_gives_up(pool, kube, rec):
    now = [1000.0]
    rec._clock = lambda: now[0]
    v = await _make(pool)
    kube.fail(PVC, "vol-data", KubeError("boom"), verbs=["patch"], times=1)
    await rec.tick()
    assert await _state(pool, v.id) == "failed"
    await rec.tick()  # still inside the backoff
    assert await _state(pool, v.id) == "failed"
    now[0] += volumes_mod.RETRY_BASE_SECONDS + 1
    await rec.tick()  # back to provisioning
    assert await _state(pool, v.id) == "provisioning"
    await rec.tick()
    await _bind(kube, rec, v)
    assert await _state(pool, v.id) == "ready"

    w = await _make(pool, name="other")
    kube.fail(PVC, "vol-other", KubeError("boom"), verbs=["patch"])
    for _ in range(volumes_mod.RETRY_MAX + 3):
        now[0] += 10_000
        await rec.tick()
        await rec.tick()
    got = await _get(pool, w.id)
    assert (got.state, got.state_reason) == ("failed", "pvc_apply_failed")
    assert rec._retries[w.id][0] == volumes_mod.RETRY_MAX


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


def _pod(name, **spec):
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "namespace": "agents"},
        "spec": spec,
        "status": {"phase": "Running"},
    }


async def test_delete_is_held_by_a_pod_mounting_the_pvc_without_a_claim(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    kube.seed(_pod("hand-made", volumes=[{"persistentVolumeClaim": {"claimName": "vol-data"}}]))
    async with pool.connection() as conn, conn.transaction():
        await volumes.mark_deleting(conn, v.id)
    await rec.tick()
    assert kube.has(PVC, "vol-data")
    assert kube.patches == []
    assert await _state(pool, v.id) == "deleting"


async def test_delete_keeps_the_finalizer_when_the_pod_lookup_errors(pool, kube, rec, monkeypatch):
    v = await _ready(pool, kube, rec)
    async with pool.connection() as conn, conn.transaction():
        await volumes.mark_deleting(conn, v.id)

    async def boom(namespace, selector):
        raise KubeError("api down")

    monkeypatch.setattr(kube, "list_pods", boom)
    await rec.tick()
    assert kube.has(PVC, "vol-data")
    assert kube.patches == []
    assert await _state(pool, v.id) == "deleting"


async def test_stale_snapshot_does_not_overwrite_a_concurrent_state_change(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    stale = await _get(pool, v.id)
    async with pool.connection() as conn, conn.transaction():
        await volumes.set_state(conn, v.id, "resizing")
    del kube.objects[(PVC, "agents", "vol-data")]
    await rec.reconcile(stale)  # _ready sees the PVC missing, but the row moved on
    assert await _state(pool, v.id) == "resizing"


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


@pytest.mark.parametrize(("backup_restore", "saves"), [(True, 1), (False, 0)])
async def test_the_last_orphan_release_saves_a_volume_with_backup_writes(
    pool, kube, rec, backup_restore, saves
):
    v = await _ready(pool, kube, rec)
    await _claim(pool, v.id, uuid.uuid4(), backup_restore=backup_restore)
    await rec.release_orphans()
    kinds = [k for k, _a, _b in await _kinds(pool, v.id)]
    assert (kinds.count("released"), kinds.count("save_started")) == (1, saves)


async def test_one_orphan_session_lookup_error_does_not_block_another(pool, kube, rec, monkeypatch):
    v1, v2 = await _ready(pool, kube, rec, "one"), await _ready(pool, kube, rec, "two")
    bad, good = uuid.uuid4(), uuid.uuid4()
    await _claim(pool, v1.id, bad)
    await _claim(pool, v2.id, good)
    real = kube.list_pods
    calls = 0

    async def flaky(namespace, selector):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise KubeError("api down")
        return await real(namespace, selector)

    monkeypatch.setattr(kube, "list_pods", flaky)
    await rec.release_orphans()
    async with pool.connection() as conn:
        left = {c.session_id for v in (v1, v2) for c in await volumes.unreleased_claims(conn, v.id)}
    assert len(left) == 1


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


async def test_restore_waits_while_a_foreign_pod_uses_the_volume_then_runs(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    ver = await _version(pool, v)
    pod = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": "x-0", "labels": {volume_pod_label(v.id): "rw"}},
        "status": {"phase": "Running"},
    }
    kube.seed(pod)
    await _restore(pool, v, ver.id)
    for _ in range(3):
        await rec.tick()
        assert _jobs(kube) == []
        assert await _state(pool, v.id) == "restoring"
    kinds = [k for k, *_ in await _kinds(pool, v.id)]
    assert kinds[-2:] == ["restore_started", "restore_skipped_mounted"]
    async with pool.connection() as conn:
        last = (await volumes.events(conn, v.id, limit=1)).items[0]
    assert last.detail == {"reason": "pod_found"}
    await kube.delete("Pod", "agents", "x-0")
    await rec.tick()
    assert len(_jobs(kube)) == 1


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


@pytest.mark.parametrize(
    ("msg", "fixed"),
    [
        ("OSError: [Errno 13] Permission denied: '/data/secret-dir/f'", "transfer_failed"),
        ("ClientError: NoSuchKey /data/x", "object_missing"),
    ],
)
async def test_job_failure_text_never_reaches_events(pool, kube, rec, msg, fixed):
    v = await _ready(pool, kube, rec)
    ver = await _version(pool, v)
    await _restore(pool, v, ver.id)
    await rec.tick()
    (job,) = _jobs(kube)
    kube.finish_job(job["metadata"]["name"], log=_outcome(state="unreadable", reason=msg))
    await rec.tick()
    async with pool.connection() as conn:
        page = await volumes.events(conn, v.id, limit=50)
    last = next(e for e in page.items if e.kind == "restore_failed")
    assert last.detail == {"reason": fixed}
    assert "/data" not in str(page.items)


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
    await _bind(kube, rec, v)
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


async def test_release_during_a_resize_saves_once_the_volume_is_ready(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    async with pool.connection() as conn, conn.transaction():
        await volumes.update(conn, v.id, size="20Gi", actor_kind="user", actor="alice")
    assert await _state(pool, v.id) == "resizing"
    await _save(pool, v, "release", "writer")
    await _save(pool, v, "release", "writer")
    await rec.tick()  # patches the PVC; capacity not grown yet
    assert await _state(pool, v.id) == "resizing"
    kube.set_pvc(v.pvc_name, "Bound", "20Gi")
    await rec.tick()
    assert await _state(pool, v.id) == "ready"
    await rec.tick()
    assert await _state(pool, v.id) == "saving"
    assert (await _kinds(pool, v.id)).count(("save_started", "system", "reconciler")) == 1


async def test_release_during_a_save_leads_to_exactly_one_follow_up_save(pool, kube, rec):
    v = await _ready(pool, kube, rec)
    await _save(pool, v, "user")
    await rec.tick()
    (job,) = _jobs(kube)
    await _save(pool, v, "release", "writer")
    await _save(pool, v, "release", "writer")
    kube.finish_job(job["metadata"]["name"], log=_uploaded())
    await rec.tick()
    assert await _state(pool, v.id) == "ready"
    await rec.tick()
    assert await _state(pool, v.id) == "saving"
    await rec.tick()
    (job2,) = _jobs(kube)
    kube.finish_job(job2["metadata"]["name"], log=_uploaded())
    await rec.tick()
    await rec.tick()
    assert await _state(pool, v.id) == "ready"
    assert [k for k, *_ in await _kinds(pool, v.id)].count("save_started") == 2
