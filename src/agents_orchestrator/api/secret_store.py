"""The api's seam to Kubernetes for webhook endpoint credentials (superleader-crud D9).

The only api module that imports `agents_orchestrator.k8s` (pinned by
`tests/test_api_sessions.py::test_api_never_imports_k8s_or_wrapper`), as `workspace.py` is for
wrappers. The Kubernetes client is built on first use, so an api that never writes a credential
never needs Kubernetes access.
"""

from __future__ import annotations

from fastapi import FastAPI, Request

from agents_orchestrator.k8s.kube import KubeApi, LightkubeApi
from agents_orchestrator.k8s.secrets import (
    SecretStoreError,
    delete_webhook_credential,
    put_webhook_credential,
    webhook_secret_name,
)

__all__ = [
    "KubeApi",
    "SecretStoreError",
    "close_kube",
    "delete_webhook_credential",
    "kube_of",
    "kube_of_app",
    "put_webhook_credential",
    "webhook_secret_name",
]


def kube_of(request: Request) -> KubeApi:
    """The Kubernetes client of the request's app (`kube_of_app`)."""
    return kube_of_app(request.app)


def kube_of_app(app: FastAPI) -> KubeApi:
    """The app's Kubernetes client: the one given to `create_app`, else one built on first use.
    Routes reach it through `kube_of`."""
    state = app.state
    if state.kube is None:
        state.kube = LightkubeApi()
        state.kube_owned = True
    return state.kube


async def close_kube(app: FastAPI) -> None:
    if app.state.kube is not None and app.state.kube_owned:
        await app.state.kube.aclose()
        app.state.kube = None
