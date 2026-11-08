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
    forget_unsaved,
    gc,
    has_rows,
    list_metas,
    promote_staged,
    put_webhook_credential,
    reap_unreserved,
    releasable,
    release,
    secret_name,
    secret_version,
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
    "forget_unsaved",
    "gc",
    "secret_name",
    "secret_version",
    "has_rows",
    "kube_of",
    "kube_of_app",
    "list_metas",
    "promote_staged",
    "put_webhook_credential",
    "reap_unreserved",
    "releasable",
    "release",
    "set_secret",
    "snapshot_versions",
    "stage_secret",
    "sync_declared",
]


def kube_of(request: Request) -> KubeApi:
    """The Kubernetes client of the request's app (`kube_of_app`)."""
    return kube_of_app(request.app)


def kube_of_app(app: FastAPI) -> KubeApi:
    """The app's Kubernetes client: the one given to `create_app`, else one built on first use.
    Routes reach it through `kube_of`; the proposals sweeper, which has no request, directly."""
    state = app.state
    if state.kube is None:
        state.kube = LightkubeApi()
        state.kube_owned = True
    return state.kube


async def close_kube(app: FastAPI) -> None:
    if app.state.kube is not None and app.state.kube_owned:
        await app.state.kube.aclose()
        app.state.kube = None
