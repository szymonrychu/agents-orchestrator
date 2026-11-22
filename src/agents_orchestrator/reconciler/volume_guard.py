"""HARD RULE second line of defence: the pods that use a volume, checked before any restore Job.

The DB guard (`db.volumes.claim`, `begin_explicit_restore`) decides from claims; this check
catches a pod the claims do not know about (a crashed release, a hand-made pod). Any pod it
returns holds the restore (`db.volumes.note_restore_wait`, reason `pod_found`): the volume
stays `restoring`, no Job is created and the check repeats each pass.
"""

from __future__ import annotations

import uuid

from agents_orchestrator.k8s.kube import KubeApi
from agents_orchestrator.k8s.labels import SESSION_LABEL, volume_pod_label

_FINISHED = ("Succeeded", "Failed")


async def pods_using(
    kube: KubeApi,
    namespace: str,
    *,
    volume_id: uuid.UUID,
    pvc_name: str,
    exclude_session_slug: str | None = None,
) -> list[str]:
    """Names of the pods in `namespace` that carry the volume's pod label or mount its PVC
    (`spec.volumes[].persistentVolumeClaim.claimName`), not Succeeded or Failed, and not of
    the session `exclude_session_slug` (the claimant). One unfiltered list: a label selector
    alone would miss a pod that mounts the PVC without the label."""
    label = volume_pod_label(volume_id)
    out = []
    for pod in await kube.list_pods(namespace, {}):
        meta = pod.get("metadata") or {}
        labels = meta.get("labels") or {}
        if (pod.get("status") or {}).get("phase") in _FINISHED:
            continue
        if exclude_session_slug is not None and labels.get(SESSION_LABEL) == exclude_session_slug:
            continue
        claims = {
            ((v.get("persistentVolumeClaim") or {}).get("claimName"))
            for v in (pod.get("spec") or {}).get("volumes") or []
        }
        if label in labels or pvc_name in claims:
            out.append(meta["name"])
    return out
