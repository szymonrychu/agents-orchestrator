"""Volume objects: the volume PVC, the pod and init Job mounts of attachments, prune exclusion."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest
from test_render import TOKENS, make_session, make_settings, sts_of

from agents_orchestrator.db.volumes import Volume
from agents_orchestrator.k8s import labels, render
from agents_orchestrator.reconciler.init import render_init_job
from agents_orchestrator.reconciler.loop import PRUNE_ORDER
from agents_orchestrator.spec import VolumeAttachment

NOW = datetime(2026, 11, 21, tzinfo=UTC)


def make_volume(name: str = "data", **over) -> Volume:
    base = {
        "id": uuid.uuid4(),
        "name": name,
        "projects": ["alpha", "beta"],
        "labels": [],
        "description": "",
        "exclude": [],
        "size": "10Gi",
        "storage_class": "rook-ceph-rwx",
        "pvc_name": f"vol-{name}",
        "state": "ready",
        "state_reason": None,
        "content_version": None,
        "created_by": "u",
        "created_at": NOW,
        "updated_at": NOW,
        "deleted_at": None,
    }
    base.update(over)
    return Volume(**base)


def attach(vol: Volume, mode: str, path: str | None = None):
    return (VolumeAttachment(volume=vol.id, mode=mode, mount_path=path), vol)


def pod_of(objs) -> dict:
    return sts_of(objs)["spec"]["template"]


def test_volume_pvc_labels_annotation_finalizer_and_no_session_label():
    vol = make_volume()
    pvc = render.render_volume_pvc(vol, make_settings())
    meta = pvc["metadata"]
    assert meta["name"] == "vol-data"
    assert meta["labels"] == {
        "app.kubernetes.io/managed-by": "agents-orchestrator",
        f"{labels.DOMAIN}/component": "volume",
        f"{labels.DOMAIN}/volume-id": str(vol.id),
    }
    assert meta["annotations"] == {f"{labels.DOMAIN}/projects": "alpha,beta"}
    assert meta["finalizers"] == [f"{labels.DOMAIN}/volume-protection"]
    assert pvc["spec"] == {
        "accessModes": ["ReadWriteMany"],
        "storageClassName": "rook-ceph-rwx",
        "resources": {"requests": {"storage": "10Gi"}},
    }


def test_session_prune_selector_never_matches_a_volume_pvc():
    pvc = render.render_volume_pvc(make_volume(), make_settings())
    assert "PersistentVolumeClaim" in PRUNE_ORDER
    for slug in ("alpha", "data", "vol-data"):
        selector = labels.session_labels(slug)
        assert not selector.items() <= pvc["metadata"]["labels"].items()


@pytest.mark.parametrize(("mode", "read_only"), [("ro", True), ("rw", False)], ids=["ro", "rw"])
def test_attachment_read_only_on_pod_volume_and_mount_and_label(mode, read_only):
    vol = make_volume()
    objs = render.render_session(
        make_session(), make_settings(), TOKENS, [attach(vol, mode, "/mnt/x")]
    )
    pod = pod_of(objs)
    volume = next(v for v in pod["spec"]["volumes"] if v["name"] == "vol-0")
    assert volume["persistentVolumeClaim"] == {"claimName": "vol-data", "readOnly": read_only}
    mount = next(m for m in pod["spec"]["containers"][0]["volumeMounts"] if m["name"] == "vol-0")
    assert mount == {"name": "vol-0", "mountPath": "/mnt/x", "readOnly": read_only}
    assert pod["metadata"]["labels"][labels.volume_pod_label(vol.id)] == mode


def test_one_pod_label_per_volume_and_default_path():
    a, b = make_volume("a"), make_volume("b")
    objs = render.render_session(
        make_session(), make_settings(), TOKENS, [attach(a, "ro"), attach(b, "rw")]
    )
    pod = pod_of(objs)
    assert pod["metadata"]["labels"][labels.volume_pod_label(a.id)] == "ro"
    assert pod["metadata"]["labels"][labels.volume_pod_label(b.id)] == "rw"
    mounts = {m["name"]: m["mountPath"] for m in pod["spec"]["containers"][0]["volumeMounts"]}
    assert (mounts["vol-0"], mounts["vol-1"]) == ("/volumes/a", "/volumes/b")
    assert pod["spec"]["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch"


def test_init_job_mounts_same_paths_and_modes():
    a, b = make_volume("a"), make_volume("b")
    atts = [attach(a, "ro", "/claude"), attach(b, "rw")]
    session, settings = make_session(), make_settings()
    pod = pod_of(render.render_session(session, settings, TOKENS, atts))["spec"]
    job = render_init_job(session, settings, ["true"], atts)["spec"]["template"]["spec"]

    def vol_parts(p):
        mounts = [m for m in p["containers"][0]["volumeMounts"] if m["name"].startswith("vol-")]
        vols = [v for v in p["volumes"] if v["name"].startswith("vol-")]
        return mounts, vols

    assert vol_parts(job) == vol_parts(pod)
    assert job["securityContext"]["fsGroupChangePolicy"] == "OnRootMismatch"
