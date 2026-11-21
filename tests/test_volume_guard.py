"""HARD RULE: never restore a volume that a not-fully-discarded session has claimed.

The DB guard (`db.volumes.claim`, `begin_explicit_restore`, `abort_restore`) against the real test
Postgres, and the Kubernetes pod check (`reconciler.volume_guard.pods_using`) against the fake kube.
"""

import asyncio
import uuid

import pytest
from factories import insert_session
from fakes.fake_kube import FakeKube

from agents_orchestrator.db import volumes
from agents_orchestrator.k8s.labels import SESSION_LABEL, volume_pod_label
from agents_orchestrator.reconciler.volume_guard import pods_using

CONTENT = uuid.uuid4()
NEWER = uuid.uuid4()


async def _volume(pool, *, state="ready", content_version=CONTENT, name="data"):
    async with pool.connection() as conn, conn.transaction():
        v = await volumes.create(
            conn,
            name=name,
            projects=["p"],
            size="10Gi",
            storage_class="rook-ceph-rwx",
            created_by="alice",
            actor_kind="user",
            actor="alice",
        )
        if state != "provisioning":
            await volumes.set_state(conn, v.id, state, content_version=content_version)
    return v


async def _claim(conn, vid, session_id, *, current=NEWER, backup_restore=True):
    async with conn.transaction():
        return await volumes.claim(
            conn,
            volume_id=vid,
            session_id=session_id,
            preset_id=uuid.uuid4(),
            mode="rw",
            backup_restore=backup_restore,
            session_slug=f"s-{session_id.hex[:6]}",
            current_version=current,
        )


async def _kinds(pool, vid):
    async with pool.connection() as conn:
        page = await volumes.events(conn, vid, limit=200)
    return [e.kind for e in reversed(page.items)]


async def test_two_claimants_race_exactly_one_restores(pool):
    for i in range(20):
        v = await _volume(pool, name=f"race-{i}")
        a, b = uuid.uuid4(), uuid.uuid4()

        async def go(session_id, vid=v.id):
            async with pool.connection() as conn:
                return await _claim(conn, vid, session_id)

        got = await asyncio.gather(go(a), go(b))
        assert sorted(d.decision for d in got) == ["restore", "wait"]
        assert (await _kinds(pool, v.id)).count("restore_started") == 1
        async with pool.connection() as conn:
            claims = await volumes.unreleased_claims(conn, v.id)
            assert (await volumes.get(conn, v.id)).state == "restoring"
        winner = next(d for d in got if d.decision == "restore")
        assert [c.session_id for c in claims] == [winner.claim.session_id]


async def test_second_claimant_blocks_on_the_row_lock_then_waits(pool):
    v = await _volume(pool)
    async with pool.connection() as first, pool.connection() as second:
        tx = first.transaction()
        await tx.__aenter__()
        try:
            won = await volumes.claim(
                first,
                volume_id=v.id,
                session_id=uuid.uuid4(),
                preset_id=None,
                mode="rw",
                backup_restore=True,
                session_slug="a",
                current_version=NEWER,
            )
            assert won.decision == "restore"
            pending = asyncio.create_task(_claim(second, v.id, uuid.uuid4()))
            await asyncio.sleep(0.5)
            assert not pending.done(), "the second claim did not wait for the volume row lock"
        finally:
            await tx.__aexit__(None, None, None)
        lost = await asyncio.wait_for(pending, 10)
    assert lost.decision == "wait"
    assert lost.claim is None


@pytest.mark.parametrize("other_desired", ["active", "deleting"])
async def test_other_unreleased_claim_skips_restore_and_records_it(pool, other_desired):
    v = await _volume(pool)
    other = await insert_session(pool, desired=other_desired)
    me = uuid.uuid4()
    async with pool.connection() as conn:
        await _claim(conn, v.id, other["id"], backup_restore=False)
        got = await _claim(conn, v.id, me)
        assert got.decision == "proceed"
        assert (await volumes.get(conn, v.id)).state == "ready"
        last = (await volumes.events(conn, v.id, limit=1)).items[0]
    assert last.kind == "restore_skipped_mounted"
    assert last.detail == {
        "session_id": str(me),
        "others": [str(other["id"])],
        "current_version": str(NEWER),
        "content_version": str(CONTENT),
    }


@pytest.mark.parametrize(
    ("content", "current", "backup_restore", "decision", "state"),
    [
        (CONTENT, CONTENT, True, "proceed", "ready"),  # PVC already holds the newest state
        (None, NEWER, True, "restore", "restoring"),  # fresh volume
        (CONTENT, NEWER, False, "proceed", "ready"),  # attachment does not restore
        (None, None, True, "proceed", "ready"),  # nothing saved yet
    ],
)
async def test_restore_condition(pool, content, current, backup_restore, decision, state):
    v = await _volume(pool, content_version=content)
    async with pool.connection() as conn:
        got = await _claim(conn, v.id, uuid.uuid4(), current=current, backup_restore=backup_restore)
        assert (got.decision, got.volume.state) == (decision, state)
        assert got.claim is not None
    kinds = await _kinds(pool, v.id)
    assert kinds[-1] == ("restore_started" if decision == "restore" else "mounted")
    assert "restore_skipped_mounted" not in kinds


@pytest.mark.parametrize(
    ("state", "decision"), [("restoring", "wait"), ("saving", "wait"), ("deleting", "refuse")]
)
async def test_busy_or_deleting_volume_inserts_no_claim(pool, state, decision):
    v = await _volume(pool, state=state)
    async with pool.connection() as conn:
        got = await _claim(conn, v.id, uuid.uuid4())
        assert (got.decision, got.claim) == (decision, None)
        assert await volumes.unreleased_claims(conn, v.id) == []


async def test_abort_restore_returns_to_ready_with_reason(pool):
    v = await _volume(pool, content_version=None)
    async with pool.connection() as conn:
        await _claim(conn, v.id, uuid.uuid4())
        async with conn.transaction():
            await volumes.abort_restore(conn, v.id, reason="pod_found")
        assert (await volumes.get(conn, v.id)).state == "ready"
        last = (await volumes.events(conn, v.id, limit=1)).items[0]
    assert (last.kind, last.detail["reason"]) == ("restore_skipped_mounted", "pod_found")


async def test_explicit_restore_refused_while_any_claim_is_unreleased(pool):
    v = await _volume(pool)
    s = uuid.uuid4()
    async with pool.connection() as conn:
        await _claim(conn, v.id, s, backup_restore=False)
        with pytest.raises(volumes.VolumeMounted):
            async with conn.transaction():
                await volumes.begin_explicit_restore(conn, v.id, version=NEWER, actor="alice")
        async with conn.transaction():
            await volumes.release_claims(conn, s, actor_kind="system", actor="reconciler")
        async with conn.transaction():
            await volumes.begin_explicit_restore(conn, v.id, version=NEWER, actor="alice")
        assert (await volumes.get(conn, v.id)).state == "restoring"
        last = (await volumes.events(conn, v.id, limit=1)).items[0]
    assert (last.kind, last.detail["version"]) == ("restore_started", str(NEWER))


@pytest.mark.parametrize("state", ["saving", "restoring", "deleting"])
async def test_explicit_restore_refused_while_busy(pool, state):
    v = await _volume(pool, state=state)
    async with pool.connection() as conn:
        with pytest.raises(volumes.VolumeBusy):
            async with conn.transaction():
                await volumes.begin_explicit_restore(conn, v.id, version=NEWER, actor="alice")
        assert (await volumes.get(conn, v.id)).state == state


VID = uuid.UUID("0123456789abcdef0123456789abcdef")


def _pod(name, *, labels=None, claim=None, phase="Running"):
    spec = (
        {"volumes": [{"name": "v", "persistentVolumeClaim": {"claimName": claim}}]} if claim else {}
    )
    return {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": name, "labels": labels or {}},
        "spec": spec,
        "status": {"phase": phase},
    }


async def test_pods_using_finds_label_or_claim_name_and_skips_finished_and_own():
    kube = FakeKube()
    label = volume_pod_label(VID)
    kube.seed(
        _pod("by-label", labels={label: "rw", SESSION_LABEL: "other"}),
        _pod("by-claim", claim="vol-data"),
        _pod("done", labels={label: "ro"}, phase="Succeeded"),
        _pod("own", labels={label: "rw", SESSION_LABEL: "me"}, claim="vol-data"),
        _pod("unrelated", claim="vol-other"),
    )
    got = await pods_using(
        kube, "agents", volume_id=VID, pvc_name="vol-data", exclude_session_slug="me"
    )
    assert sorted(got) == ["by-claim", "by-label"]
    assert label == "agents.szymonrichert.pl/vol-01234567"
