"""Labels, selectors and the reserved object-name prefix of one session (D26)."""

from __future__ import annotations

import uuid
from collections.abc import Mapping
from typing import Any

SESSION_LABEL = "agents.szymonrichert.pl/session"
SESSION_ID_LABEL = "agents.szymonrichert.pl/session-id"
MANAGED_BY_LABEL = "app.kubernetes.io/managed-by"
MANAGED_BY = "agents-orchestrator"


def prefix(slug: str) -> str:
    return f"agent-{slug}"


def session_labels(slug: str) -> dict[str, str]:
    return {SESSION_LABEL: slug, MANAGED_BY_LABEL: MANAGED_BY}


def kube_labels(slug: str, session_id: uuid.UUID) -> dict[str, str]:
    """Labels of the per-session Kubernetes access objects: the session labels plus its id."""
    return {**session_labels(slug), SESSION_ID_LABEL: str(session_id)}


def kube_service_account(slug: str, session_id: uuid.UUID) -> str:
    """The per-session ServiceAccount of granted Kubernetes access: unique per session id."""
    return f"{prefix(slug)}-{session_id.hex[:8]}"


def cluster_binding_name(slug: str, session_id: uuid.UUID) -> str:
    return f"{kube_service_account(slug, session_id)}-cluster-admin"


def namespace_binding_name(slug: str, session_id: uuid.UUID, tier: str) -> str:
    return f"{kube_service_account(slug, session_id)}-{tier}"


def selector(slug: str) -> dict[str, dict[str, str]]:
    return {"matchLabels": session_labels(slug)}


def object_names(slug: str) -> dict[str, str]:
    base = prefix(slug)
    names = {
        "pvc": f"{base}-workspace",
        "secret": f"{base}-ccw",
        "configmap": f"{base}-config",
        "service": base,
        "statefulset": base,
    }
    for name in names.values():
        assert name.startswith(base), f"{name} lacks the reserved prefix {base}"
    return names


def workspace_pvc(slug: str, status: Mapping[str, Any]) -> str:
    """The workspace PVC of a session: the name the reconciler pinned in `status.workspace_pvc`
    (`pin_workspace_pvc`), else the deterministic `agent-<slug>-workspace`."""
    return str(status.get("workspace_pvc") or object_names(slug)["pvc"])


def suffixed_workspace_pvc(slug: str, session_id: uuid.UUID) -> str:
    """The workspace PVC name of a session whose slug's deterministic name is held by a PVC kept
    from an earlier session of that slug (T-26): unique by the first 8 hex of the session id."""
    return f"{prefix(slug)}-{session_id.hex[:8]}-workspace"
