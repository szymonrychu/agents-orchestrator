"""The Kubernetes calls the reconciler makes, behind one protocol (D6, D9, D26).

Objects travel as plain Kubernetes JSON dicts, the shape `render_session` produces. Every call
names its namespace explicitly; the empty namespace means cluster scope, used for the kinds in
`CLUSTER_SCOPED`. The production Role grants Secrets only create, patch and
delete, so `get_object` and `list_objects` are never called for Secrets: a Secret is applied
and deleted by its deterministic name.

`LightkubeApi` is the production implementation; `tests/fakes/fake_kube.py:FakeKube` is the
in-memory one that also enforces the Role.
"""

from __future__ import annotations

import contextlib
import re
from collections.abc import Iterator, Mapping
from typing import Any, Protocol

import httpx2
from lightkube import ApiError, AsyncClient, codecs
from lightkube.core.client import CascadeType
from lightkube.core.exceptions import LoadResourceError
from lightkube.resources.apps_v1 import StatefulSet
from lightkube.resources.batch_v1 import Job
from lightkube.resources.core_v1 import (
    ConfigMap,
    PersistentVolumeClaim,
    Pod,
    Secret,
    Service,
    ServiceAccount,
)
from lightkube.resources.rbac_authorization_v1 import ClusterRoleBinding, RoleBinding
from lightkube.types import PatchType

from agents_orchestrator.k8s.labels import MANAGED_BY, MANAGED_BY_LABEL, SESSION_LABEL
from agents_orchestrator.spec import parse_quantity

FIELD_MANAGER = "agents-orchestrator"

Obj = dict[str, Any]
Selector = Mapping[str, str]

RESOURCES: dict[str, Any] = {
    "StatefulSet": StatefulSet,
    "Service": Service,
    "ConfigMap": ConfigMap,
    "PersistentVolumeClaim": PersistentVolumeClaim,
    "Secret": Secret,
    "Pod": Pod,
    "Job": Job,
    "ServiceAccount": ServiceAccount,
    "RoleBinding": RoleBinding,
    "ClusterRoleBinding": ClusterRoleBinding,
}

# Kinds that have no namespace: the KubeApi namespace argument is "" and lightkube gets None.
CLUSTER_SCOPED: frozenset[str] = frozenset({"ClusterRoleBinding"})


def obj_namespace(obj: Mapping[str, Any]) -> str:
    """The object's metadata.namespace, or "" for a cluster-scoped object."""
    return (obj.get("metadata") or {}).get("namespace") or ""


def _ns(kind: str, namespace: str) -> str | None:
    return None if kind in CLUSTER_SCOPED else namespace


class KubeError(Exception):
    """A Kubernetes call failed."""


class Forbidden(KubeError):
    """The reconciler's Role does not grant this call (HTTP 403)."""


class NotFound(KubeError):
    """The object does not exist (HTTP 404)."""


class KubeApi(Protocol):
    async def apply(
        self, obj: Obj, *, field_manager: str = FIELD_MANAGER, force: bool = True
    ) -> Obj:
        """Server-side apply `obj` into `obj.metadata.namespace`; returns the stored object."""
        ...

    async def get_object(self, kind: str, namespace: str, name: str) -> Obj | None:
        """The object, or None when it does not exist. Never called for Secrets."""
        ...

    async def list_objects(self, kind: str, namespace: str, selector: Selector) -> list[Obj]:
        """Objects of `kind` matching every label of `selector`. Never called for Secrets."""
        ...

    async def merge_patch(self, kind: str, namespace: str, name: str, patch: Obj) -> Obj:
        """JSON merge patch (RFC 7386, a null removes a field); returns the stored object."""
        ...

    async def delete(self, kind: str, namespace: str, name: str) -> bool:
        """Delete by name; False when it did not exist."""
        ...

    async def list_pods(self, namespace: str, selector: Selector) -> list[Obj]: ...

    async def delete_pod(self, namespace: str, name: str) -> bool: ...

    async def create_job(self, obj: Obj) -> Obj: ...

    async def get_job(self, namespace: str, name: str) -> Obj | None: ...

    async def read_pod_log(self, namespace: str, name: str, tail_lines: int) -> str: ...

    async def aclose(self) -> None: ...


def is_owned(obj: Mapping[str, Any], slug: str) -> bool:
    """True only when the object carries both the managed-by and this session's label (D26)."""
    labels = (obj.get("metadata") or {}).get("labels") or {}
    return labels.get(MANAGED_BY_LABEL) == MANAGED_BY and labels.get(SESSION_LABEL) == slug


def pod_ready(pod: Mapping[str, Any]) -> bool:
    for cond in (pod.get("status") or {}).get("conditions") or []:
        if cond.get("type") == "Ready":
            return cond.get("status") == "True"
    return False


def _resource(kind: str) -> Any:
    try:
        return RESOURCES[kind]
    except KeyError:
        raise KubeError(f"unsupported kind {kind!r}") from None


@contextlib.contextmanager
def _mapped() -> Iterator[None]:
    """Translate lightkube and transport errors into KubeError, Forbidden and NotFound."""
    try:
        yield
    except ApiError as exc:
        code = exc.status.code if exc.status is not None else None
        message = (exc.status.message if exc.status is not None else None) or str(exc)
        if code == 403:
            raise Forbidden(message) from exc
        if code == 404:
            raise NotFound(message) from exc
        raise KubeError(f"{code}: {message}") from exc
    except (httpx2.HTTPError, LoadResourceError) as exc:
        raise KubeError(str(exc)) from exc


class LightkubeApi:
    """KubeApi on the lightkube AsyncClient: in-cluster config, else the kubeconfig."""

    def __init__(self, client: AsyncClient | None = None) -> None:
        self.client = client if client is not None else AsyncClient(field_manager=FIELD_MANAGER)

    async def apply(
        self, obj: Obj, *, field_manager: str = FIELD_MANAGER, force: bool = True
    ) -> Obj:
        with _mapped():
            res = codecs.from_dict(obj)
            out = await self.client.apply(res, field_manager=field_manager, force=force)
        return out.to_dict()

    async def get_object(self, kind: str, namespace: str, name: str) -> Obj | None:
        res = _resource(kind)
        try:
            with _mapped():
                out = await self.client.get(res, name, namespace=_ns(kind, namespace))
        except NotFound:
            return None
        return out.to_dict()

    async def list_objects(self, kind: str, namespace: str, selector: Selector) -> list[Obj]:
        res = _resource(kind)
        with _mapped():
            return [
                o.to_dict()
                async for o in self.client.list(
                    res, namespace=_ns(kind, namespace), labels=dict(selector) or None
                )
            ]

    async def merge_patch(self, kind: str, namespace: str, name: str, patch: Obj) -> Obj:
        res = _resource(kind)
        with _mapped():
            out = await self.client.patch(
                res,
                name,
                patch,
                namespace=_ns(kind, namespace),
                patch_type=PatchType.MERGE,
                field_manager=FIELD_MANAGER,
            )
        return out.to_dict()

    async def delete(self, kind: str, namespace: str, name: str) -> bool:
        res = _resource(kind)
        try:
            with _mapped():
                # a Job deleted without a propagation policy would orphan its pods
                cascade = CascadeType.BACKGROUND if kind == "Job" else None
                await self.client.delete(res, name, namespace=_ns(kind, namespace), cascade=cascade)
        except NotFound:
            return False
        return True

    async def list_pods(self, namespace: str, selector: Selector) -> list[Obj]:
        return await self.list_objects("Pod", namespace, selector)

    async def delete_pod(self, namespace: str, name: str) -> bool:
        return await self.delete("Pod", namespace, name)

    async def create_job(self, obj: Obj) -> Obj:
        with _mapped():
            out = await self.client.create(codecs.from_dict(obj), field_manager=FIELD_MANAGER)
        return out.to_dict()

    async def get_job(self, namespace: str, name: str) -> Obj | None:
        return await self.get_object("Job", namespace, name)

    async def read_pod_log(self, namespace: str, name: str, tail_lines: int) -> str:
        with _mapped():
            lines = [
                line
                async for line in self.client.log(name, namespace=namespace, tail_lines=tail_lines)
            ]
        return "".join(lines).rstrip("\n")

    async def aclose(self) -> None:
        await self.client.close()


def _zero(value: Any) -> bool:
    return value is None or value in ("", 0, False) or value == [] or value == {}


# A resource quantity: cpu, memory, storage under `limits` / `requests`, an emptyDir sizeLimit.
_QUANTITY_PATH = re.compile(r"\.(?:(?:limits|requests)\.[^.\[]+|sizeLimit)$")


def _same_quantity(desired: str, live: str) -> bool:
    """True when both parse as Kubernetes quantities of equal value ("0.5" and "500m")."""
    try:
        return parse_quantity(desired) == parse_quantity(live)
    except ValueError:
        return False


def _at_least(live: Any, desired: Any) -> bool:
    try:
        return parse_quantity(live) >= parse_quantity(desired)
    except ValueError:
        return False


def drift(desired: Any, live: Any, path: str = "") -> str | None:
    """The path of the first field of `desired` the live object does not hold, or None.

    Maps compare key by key and extra live keys (server defaults, status, metadata the API
    server owns) are ignored; lists compare item by item at the same length. A desired zero
    value ("", 0, false, empty) also matches a missing live key, because the API server omits
    empty fields. A key the render dropped is not seen here (the live object still has it).
    Resource quantities compare by value: the API server canonicalises them on write ("0.5"
    is stored as "500m", "1024Mi" as "1Gi").
    """
    if isinstance(desired, dict):
        if not isinstance(live, dict):
            return path or "."
        for k, v in desired.items():
            sub = f"{path}.{k}"
            where = drift(v, live[k], sub) if k in live else (None if _zero(v) else sub)
            if where is not None:
                return where
        return None
    if isinstance(desired, list):
        if not isinstance(live, list) or len(desired) != len(live):
            return path or "."
        for i, (d, x) in enumerate(zip(desired, live, strict=True)):
            where = drift(d, x, f"{path}[{i}]")
            if where is not None:
                return where
        return None
    if desired == live and type(desired) is type(live):
        return None
    if (
        isinstance(desired, str)
        and isinstance(live, str)
        and _QUANTITY_PATH.search(path)
        and _same_quantity(desired, live)
    ):
        return None
    if path.endswith(".requests.storage") and _at_least(live, desired):
        return None  # a PVC only grows: a larger live size is not drift
    return path or "."
