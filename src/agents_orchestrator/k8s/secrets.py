"""Webhook endpoint credentials (superleader-crud D9): the one Kubernetes Secret per
agents-webhook endpoint, `webhook-<endpoint id>-password`, holding the credential the endpoint
issued once.

A value is only ever held as a `SecretStr` until it goes into `stringData`; the apply response
is dropped, nothing here logs a value or an exception body, and errors are generic. Secrets are
written and deleted by name only (the Role has no get, list or watch on Secrets).
"""

from __future__ import annotations

import logging

from pydantic import SecretStr

from agents_orchestrator.k8s.kube import KubeApi, KubeError
from agents_orchestrator.k8s.labels import MANAGED_BY, MANAGED_BY_LABEL

log = logging.getLogger(__name__)


class SecretStoreError(Exception):
    """A secret write failed; the message never carries the value or a Kubernetes body."""


def webhook_secret_name(endpoint_id: int) -> str:
    return f"webhook-{endpoint_id}-password"


async def put_webhook_credential(
    kube: KubeApi, namespace: str, endpoint_id: int, key: str, value: SecretStr
) -> str:
    """Store the credential an agents-webhook endpoint issued once, as `stringData[key]`.

    `key` is `password` (basic mode) or `secret` (header, bearer, hmac). The Secret is replaced
    whole, so a rotate that changes the mode leaves no stale key behind. The apply response is
    dropped and errors carry no value. Returns the Secret name.
    """
    name = webhook_secret_name(endpoint_id)
    try:
        await kube.delete("Secret", namespace, name)
        await kube.apply(
            {
                "apiVersion": "v1",
                "kind": "Secret",
                "metadata": {
                    "name": name,
                    "namespace": namespace,
                    "labels": {MANAGED_BY_LABEL: MANAGED_BY},
                },
                "type": "Opaque",
                "stringData": {key: value.get_secret_value()},
            }
        )
    except KubeError as exc:
        log.warning("webhook credential write failed: %s (%s)", name, type(exc).__name__)
        raise SecretStoreError(f"could not store the credential of webhook {endpoint_id}") from None
    log.info("webhook credential set: %s key=%s", name, key)
    return name


async def delete_webhook_credential(kube: KubeApi, namespace: str, endpoint_id: int) -> None:
    """Remove the endpoint's credential Secret; a failure is logged, never raised."""
    name = webhook_secret_name(endpoint_id)
    try:
        await kube.delete("Secret", namespace, name)
    except KubeError as exc:
        log.warning("webhook credential delete failed: %s (%s)", name, type(exc).__name__)
