"""The api's seam to the preset secret store (superleader-crud D5, D7).

The only api module that imports `agents_orchestrator.k8s` (pinned by
`tests/test_api_sessions.py::test_api_never_imports_k8s_or_wrapper`), as `workspace.py` is for
wrappers. Routes call the store through here; the Kubernetes client is built on first use, so an
api that never writes a secret never needs Kubernetes access.
"""

from __future__ import annotations

from fastapi import FastAPI, Request

from agents_orchestrator.k8s.kube import KubeApi, LightkubeApi
from agents_orchestrator.k8s.secrets import (
    PresetNotFound,
    SecretMeta,
    SecretStoreError,
    SecretUnset,
    StagedStale,
    bind_staged,
    collect_bound,
    delete_webhook_credential,
    gc,
    has_rows,
    list_metas,
    promote_staged,
    put_webhook_credential,
    release,
    secret_name,
    set_secret,
    snapshot_versions,
    stage_secret,
    sync_declared,
)

__all__ = [
    "KubeApi",
    "PresetNotFound",
    "SecretMeta",
    "SecretStoreError",
    "SecretUnset",
    "StagedStale",
    "bind_staged",
    "close_kube",
    "collect_bound",
    "delete_webhook_credential",
    "gc",
    "secret_name",
    "has_rows",
    "kube_of",
    "list_metas",
    "promote_staged",
    "put_webhook_credential",
    "release",
    "set_secret",
    "snapshot_versions",
    "stage_secret",
    "sync_declared",
]


def kube_of(request: Request) -> KubeApi:
    """The app's Kubernetes client: the one given to `create_app`, else one built on first use."""
    state = request.app.state
    if state.kube is None:
        state.kube = LightkubeApi()
        state.kube_owned = True
    return state.kube


async def close_kube(app: FastAPI) -> None:
    if app.state.kube is not None and app.state.kube_owned:
        await app.state.kube.aclose()
        app.state.kube = None
