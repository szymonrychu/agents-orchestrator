"""In-memory KubeApi that behaves like the reconciler's production Role (D9, D26).

- Every call outside `namespace` raises Forbidden, and so do get and list on Secrets (the Role
  grants secrets only create, patch and delete). Forbidden attempts are also recorded in
  `forbidden`, so a test can assert none were made even when the caller swallowed the error.
- RBAC kinds: a ServiceAccount is allowed in `namespace`; a RoleBinding only in `rbac_namespaces`;
  a ClusterRoleBinding only at cluster scope (namespace "") and with `cluster_rbac`. Applying a
  binding whose `roleRef.name` is not in `bindable` is Forbidden, like the API server's
  escalation check. Cluster-scoped objects are stored under namespace "".
- Applies, merge patches, deletes and every call are recorded (`applies`, `patches`, `deletes`,
  `calls`).
- Applying a StatefulSet simulates its controller: pods `<sts>-<i>` for i < replicas carrying the
  pod template labels; scaling down removes the higher ordinals, deleting the StatefulSet
  removes its pods, and deleting a pod of a live StatefulSet recreates it with a new uid.
  Readiness and phase are controlled with `set_pod`; `controller_paused` freezes the pods and
  the StatefulSet status.
- Revisions: `metadata.generation` counts spec changes; the controller sets
  `status.observedGeneration` and `status.updateRevision` (`<sts>-<hash of the template>`), and
  labels every pod it creates `controller-revision-hash: <updateRevision>`. Under
  `updateStrategy: OnDelete` a template change leaves existing pods on their old revision until
  they are deleted, and a deleted pod returns at the update revision. Under RollingUpdate a
  template change makes the controller replace every pod at the update revision (recorded in
  `rolled`); a change outside the template, such as a strategy-only merge patch, rolls nothing.
- The API server's StatefulSet defaulting and validation: a missing strategy becomes
  `{type: RollingUpdate, rollingUpdate: {partition: 0}}`; a server-side apply keeps that
  defaulted, unowned `rollingUpdate` when the applied object does not name it, and a
  `rollingUpdate` next to any other type is rejected, so an apply cannot move a defaulted
  RollingUpdate StatefulSet to OnDelete. `merge_patch` (RFC 7386, null removes) can.
- `graceful_pod_deletes` keeps a deleted pod terminating (`metadata.deletionTimestamp` set, its
  labels and readiness unchanged) until `finish_termination`, like a real grace period.
- `seed` places objects (the orchestrator's own, unlabelled ones, or foreign ones) without
  recording them; `seed_orchestrator_objects` seeds what the chart puts in the same namespace.
- `fail(kind, name)` injects an error for one object.
- Creating a Job simulates its controller: one Running pod `<job>-<suffix>` with the pod template
  labels; `finish_job` ends it (succeeded, failed with an exit code, or deadline exceeded) and
  gives the pod its log; deleting a Job removes its pods.
"""

from __future__ import annotations

import copy
import hashlib
import itertools
import json
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from typing import Any

from agents_orchestrator.k8s.kube import (
    CLUSTER_SCOPED,
    FIELD_MANAGER,
    Forbidden,
    KubeError,
    NotFound,
    obj_namespace,
)
from agents_orchestrator.reconciler.rollout import REVISION_LABEL
from agents_orchestrator.spec import parse_quantity

READ_VERBS = frozenset({"get", "list", "watch"})
ORCHESTRATOR_LABELS = {
    "app.kubernetes.io/name": "agents-orchestrator",
    "app.kubernetes.io/instance": "agents-orchestrator",
    "app.kubernetes.io/managed-by": "Helm",
}

_seq = itertools.count(1)


@dataclass(frozen=True)
class Call:
    verb: str
    kind: str
    namespace: str
    name: str | None = None
    selector: dict[str, str] | None = None


@dataclass(frozen=True)
class Apply:
    obj: dict[str, Any]
    field_manager: str
    force: bool

    @property
    def kind(self) -> str:
        return self.obj["kind"]

    @property
    def name(self) -> str:
        return self.obj["metadata"]["name"]


@dataclass
class _Failure:
    exc: Exception
    verbs: frozenset[str] | None
    times: int | None


@dataclass
class FakeKube:
    namespace: str = "agents"
    pods_ready: bool = False
    controller_paused: bool = False  # the StatefulSet controller lags: no pod or status changes
    graceful_pod_deletes: bool = False  # a deleted pod stays terminating until finish_termination
    rbac_namespaces: frozenset[str] = frozenset()  # namespaces where RoleBindings are allowed
    cluster_rbac: bool = False  # ClusterRoleBindings are allowed
    serviceaccounts: bool = False  # ServiceAccounts are allowed (chart: either kube access on)
    bindable: frozenset[str] = frozenset()  # role names a binding may reference
    objects: dict[tuple[str, str, str], dict[str, Any]] = field(default_factory=dict)
    applies: list[Apply] = field(default_factory=list)
    patches: list[tuple[str, str, dict[str, Any]]] = field(default_factory=list)
    rolled: list[str] = field(default_factory=list)  # pods the controller replaced (RollingUpdate)
    deletes: list[tuple[str, str, str]] = field(default_factory=list)
    calls: list[Call] = field(default_factory=list)
    forbidden: list[Call] = field(default_factory=list)
    _failures: dict[tuple[str, str], _Failure] = field(default_factory=dict)

    # ------------------------------------------------------------------ test controls

    def seed(self, *objs: Mapping[str, Any]) -> None:
        for obj in objs:
            o = copy.deepcopy(dict(obj))
            meta = o.setdefault("metadata", {})
            meta.setdefault("namespace", "" if o["kind"] in CLUSTER_SCOPED else self.namespace)
            meta.setdefault("labels", {})
            meta.setdefault("uid", str(uuid.uuid4()))
            self.objects[(o["kind"], meta["namespace"], meta["name"])] = o

    def seed_orchestrator_objects(self) -> list[tuple[str, str]]:
        """The chart's own objects in the shared namespace; returns their (kind, name)."""
        objs: list[dict[str, Any]] = []
        for component in ("api", "dispatcher", "ingester", "reconciler"):
            labels = {**ORCHESTRATOR_LABELS, "app.kubernetes.io/component": component}
            objs.append(_obj("apps/v1", "Deployment", f"agents-orchestrator-{component}", labels))
            objs.append(_pod(f"agents-orchestrator-{component}-7d9f8c-x2k4q", labels, ready=True))
        objs.append(
            _obj("v1", "ConfigMap", "agents-orchestrator-settings", dict(ORCHESTRATOR_LABELS))
        )
        for secret in ("agents-orchestrator-claude", "agents-orchestrator-master-key"):
            objs.append(_obj("v1", "Secret", secret, dict(ORCHESTRATOR_LABELS)))
        objs.append(_obj("v1", "Service", "agents-orchestrator", dict(ORCHESTRATOR_LABELS)))
        cnpg = {"cnpg.io/cluster": "agents-orchestrator-db"}
        for i in (1, 2):
            objs.append(_obj("v1", "PersistentVolumeClaim", f"agents-orchestrator-db-{i}", cnpg))
            objs.append(_pod(f"agents-orchestrator-db-{i}", cnpg, ready=True))
        self.seed(*objs)
        return [(o["kind"], o["metadata"]["name"]) for o in objs]

    def fail(
        self,
        kind: str,
        name: str,
        exc: Exception | None = None,
        *,
        verbs: Iterable[str] | None = None,
        times: int | None = None,
    ) -> None:
        """Make calls on one object raise `exc` (all verbs unless `verbs`; `times` then heal)."""
        self._failures[(kind, name)] = _Failure(
            exc or KubeError(f"injected failure for {kind}/{name}"),
            frozenset(verbs) if verbs is not None else None,
            times,
        )

    def heal(self, kind: str, name: str) -> None:
        self._failures.pop((kind, name), None)

    def set_pod(
        self,
        name: str,
        *,
        ready: bool | None = None,
        phase: str | None = None,
        pod_ip: str | None = None,
    ) -> None:
        pod = self.objects[("Pod", self.namespace, name)]
        if pod_ip is not None:
            pod["status"]["podIP"] = pod_ip
        if phase is not None:
            pod["status"]["phase"] = phase
        if ready is not None:
            _set_ready(pod, ready)

    def finish_job(
        self,
        name: str,
        *,
        exit_code: int = 0,
        log: str = "",
        deadline_exceeded: bool = False,
    ) -> None:
        """End a Job like its controller: Complete (exit 0), Failed, or DeadlineExceeded."""
        job = self.objects[("Job", self.namespace, name)]
        pod = next(p for p in self._job_pods(name))
        pod["log"] = log
        if deadline_exceeded:
            pod["status"]["phase"] = "Failed"
            job["status"] = {
                "failed": 1,
                "conditions": [{"type": "Failed", "status": "True", "reason": "DeadlineExceeded"}],
            }
            return
        ok = exit_code == 0
        pod["status"]["phase"] = "Succeeded" if ok else "Failed"
        pod["status"]["containerStatuses"] = [
            {"name": "check", "state": {"terminated": {"exitCode": exit_code}}}
        ]
        if ok:
            job["status"] = {
                "succeeded": 1,
                "conditions": [{"type": "Complete", "status": "True"}],
            }
        else:
            job["status"] = {
                "failed": 1,
                "conditions": [
                    {"type": "Failed", "status": "True", "reason": "BackoffLimitExceeded"}
                ],
            }

    def finish_termination(self, name: str) -> None:
        """A terminating pod is gone; its StatefulSet controller recreates it."""
        pod = self.objects.pop(("Pod", self.namespace, name))
        assert pod["metadata"].get("deletionTimestamp"), f"pod {name} is not terminating"
        self._recreate(pod)

    def has(self, kind: str, name: str, namespace: str | None = None) -> bool:
        return (kind, namespace or self.namespace, name) in self.objects

    def get(self, kind: str, name: str, namespace: str | None = None) -> dict[str, Any]:
        return self.objects[(kind, namespace or self.namespace, name)]

    def names(self, kind: str) -> list[str]:
        return sorted(n for (k, ns, n) in self.objects if k == kind and ns == self.namespace)

    def applied(self, kind: str | None = None) -> list[Apply]:
        return [a for a in self.applies if kind is None or a.kind == kind]

    def deleted(self, kind: str | None = None) -> list[str]:
        return [n for (k, _ns, n) in self.deletes if kind is None or k == kind]

    def reset_records(self) -> None:
        self.applies.clear()
        self.patches.clear()
        self.rolled.clear()
        self.deletes.clear()
        self.calls.clear()
        self.forbidden.clear()

    # ------------------------------------------------------------------ the Role and failures

    def _check(
        self, verb: str, kind: str, namespace: str, name: str | None = None, selector=None
    ) -> None:
        call = Call(verb, kind, namespace, name, dict(selector) if selector is not None else None)
        self.calls.append(call)
        if not self._in_scope(kind, namespace):
            self.forbidden.append(call)
            raise Forbidden(f"{verb} {kind} in namespace {namespace!r} is outside the Role")
        if kind == "Secret" and verb in READ_VERBS:
            self.forbidden.append(call)
            raise Forbidden(f"{verb} on secrets is not granted to the reconciler")
        if name is not None:
            failure = self._failures.get((kind, name))
            if failure is not None and (failure.verbs is None or verb in failure.verbs):
                if failure.times is not None:
                    failure.times -= 1
                    if failure.times <= 0:
                        self.heal(kind, name)
                raise failure.exc

    def _in_scope(self, kind: str, namespace: str) -> bool:
        if kind == "RoleBinding":
            return namespace in self.rbac_namespaces
        if kind == "ClusterRoleBinding":
            return namespace == "" and self.cluster_rbac
        if kind == "ServiceAccount":
            return namespace == self.namespace and self.serviceaccounts
        return namespace == self.namespace

    def _check_escalation(self, obj: dict[str, Any]) -> None:
        """The API server refuses a binding to a role the caller may not itself hold or bind."""
        if obj["kind"] not in ("RoleBinding", "ClusterRoleBinding"):
            return
        role = (obj.get("roleRef") or {}).get("name")
        if role not in self.bindable:
            self.forbidden.append(
                Call("patch", obj["kind"], obj_namespace(obj), obj["metadata"]["name"])
            )
            raise Forbidden(f"binding to role {role!r} is not permitted (escalation check)")

    # ------------------------------------------------------------------ KubeApi

    async def apply(
        self, obj: dict[str, Any], *, field_manager: str = FIELD_MANAGER, force: bool = True
    ) -> dict[str, Any]:
        meta = obj["metadata"]
        self._check("patch", obj["kind"], obj_namespace(obj), meta["name"])
        self._check_escalation(obj)
        key = (obj["kind"], obj_namespace(obj), meta["name"])
        stored = copy.deepcopy(obj)
        previous = self.objects.get(key)
        stored["metadata"]["uid"] = (
            previous["metadata"]["uid"] if previous is not None else str(uuid.uuid4())
        )
        stored.pop("stringData", None)  # a Secret's data is write-only for this fake
        _canonicalise_quantities(stored)
        if obj["kind"] == "StatefulSet":
            _keep_defaulted_rolling_update(stored, previous)
            _admit_statefulset(stored)
        if obj["kind"] == "PersistentVolumeClaim":
            _admit_pvc(stored, previous)
        self.applies.append(Apply(copy.deepcopy(obj), field_manager, force))
        return self._store(key, stored, previous)

    async def merge_patch(
        self, kind: str, namespace: str, name: str, patch: dict[str, Any]
    ) -> dict[str, Any]:
        self._check("patch", kind, namespace, name)
        key = (kind, namespace, name)
        previous = self.objects.get(key)
        if previous is None:
            raise NotFound(f"{kind} {name!r} not found")
        stored = _merge(previous, patch)
        if kind == "StatefulSet":
            _admit_statefulset(stored)
        self.patches.append((kind, name, copy.deepcopy(patch)))
        return self._store(key, stored, previous)

    async def get_object(self, kind: str, namespace: str, name: str) -> dict[str, Any] | None:
        self._check("get", kind, namespace, name)
        obj = self.objects.get((kind, namespace, name))
        return copy.deepcopy(obj) if obj is not None else None

    async def list_objects(
        self, kind: str, namespace: str, selector: Mapping[str, str]
    ) -> list[dict[str, Any]]:
        self._check("list", kind, namespace, selector=selector)
        return self._select(kind, namespace, selector)

    async def delete(self, kind: str, namespace: str, name: str) -> bool:
        self._check("delete", kind, namespace, name)
        obj = self.objects.pop((kind, namespace, name), None)
        if obj is None:
            return False
        self.deletes.append((kind, namespace, name))
        if kind == "StatefulSet":
            for pod in self._pods_of(name):
                self.objects.pop(("Pod", namespace, pod["metadata"]["name"]), None)
        if kind == "Job":
            for pod in self._job_pods(name):
                self.objects.pop(("Pod", namespace, pod["metadata"]["name"]), None)
        return True

    async def list_pods(self, namespace: str, selector: Mapping[str, str]) -> list[dict[str, Any]]:
        self._check("list", "Pod", namespace, selector=selector)
        return self._select("Pod", namespace, selector)

    async def delete_pod(self, namespace: str, name: str) -> bool:
        self._check("delete", "Pod", namespace, name)
        key = ("Pod", namespace, name)
        pod = self.objects.get(key)
        if pod is None:
            return False
        self.deletes.append(key)
        if self.graceful_pod_deletes:
            pod["metadata"].setdefault("deletionTimestamp", "2026-10-28T00:00:00Z")
            return True
        del self.objects[key]
        self._recreate(pod)
        return True

    async def create_job(self, obj: dict[str, Any]) -> dict[str, Any]:
        meta = obj["metadata"]
        self._check("create", "Job", meta.get("namespace", ""), meta["name"])
        key = ("Job", meta["namespace"], meta["name"])
        if key in self.objects:
            raise KubeError(f"jobs {meta['name']!r} already exists")
        stored = copy.deepcopy(obj)
        stored["metadata"]["uid"] = str(uuid.uuid4())
        stored.setdefault("status", {})
        self.objects[key] = stored
        template = stored["spec"]["template"]
        pod = _pod(
            f"{meta['name']}-x7k2p", dict(template["metadata"].get("labels", {})), ready=False
        )
        pod["metadata"]["namespace"] = meta["namespace"]
        pod["metadata"]["annotations"] = {"fake/job": meta["name"]}
        pod["status"]["phase"] = "Running"
        self.objects[("Pod", meta["namespace"], pod["metadata"]["name"])] = pod
        return copy.deepcopy(stored)

    async def get_job(self, namespace: str, name: str) -> dict[str, Any] | None:
        return await self.get_object("Job", namespace, name)

    async def read_pod_log(self, namespace: str, name: str, tail_lines: int) -> str:
        self._check("get", "Pod", namespace, name)
        pod = self.objects.get(("Pod", namespace, name))
        if pod is None:
            raise KubeError(f"pods {name!r} not found")
        lines = pod.get("log", "").splitlines()
        return "\n".join(lines[-tail_lines:]) if tail_lines else ""

    async def aclose(self) -> None:
        return None

    # ------------------------------------------------------------------ internals

    def _store(
        self, key: tuple[str, str, str], stored: dict[str, Any], previous: dict[str, Any] | None
    ) -> dict[str, Any]:
        self.objects[key] = stored
        if key[0] == "StatefulSet":
            self._sts_applied(stored, previous)
            self._sync_pods(stored)
        return copy.deepcopy(stored)

    def _select(
        self, kind: str, namespace: str, selector: Mapping[str, str]
    ) -> list[dict[str, Any]]:
        out = []
        for (k, ns, _name), obj in sorted(self.objects.items()):
            labels = obj["metadata"].get("labels", {})
            if (
                k == kind
                and ns == namespace
                and all(labels.get(a) == b for a, b in selector.items())
            ):
                out.append(copy.deepcopy(obj))
        return out

    def _pods_of(self, sts_name: str) -> list[dict[str, Any]]:
        return [
            o
            for (k, ns, _n), o in self.objects.items()
            if k == "Pod"
            and ns == self.namespace
            and o["metadata"].get("annotations", {}).get("fake/statefulset") == sts_name
        ]

    def _job_pods(self, job_name: str) -> list[dict[str, Any]]:
        return [
            o
            for (k, ns, _n), o in self.objects.items()
            if k == "Pod"
            and ns == self.namespace
            and o["metadata"].get("annotations", {}).get("fake/job") == job_name
        ]

    def _recreate(self, pod: dict[str, Any]) -> None:
        owner = pod["metadata"].get("annotations", {}).get("fake/statefulset")
        sts = self.objects.get(("StatefulSet", self.namespace, owner)) if owner else None
        if sts is not None:
            self._sync_pods(sts)  # the controller recreates the pod, unready, with a new uid

    def _sts_applied(self, sts: dict[str, Any], previous: dict[str, Any] | None) -> None:
        """Generation, status and, under RollingUpdate, the controller's replacement of pods."""
        meta = sts["metadata"]
        if previous is None:
            meta["generation"], status = 1, {}
        else:
            changed = previous.get("spec") != sts.get("spec")
            meta["generation"] = previous["metadata"].get("generation", 1) + changed
            status = copy.deepcopy(previous.get("status") or {})
        sts["status"] = status
        if self.controller_paused:
            return
        template = json.dumps(sts["spec"].get("template"), sort_keys=True).encode()
        revision = f"{meta['name']}-{hashlib.sha256(template).hexdigest()[:10]}"
        status["observedGeneration"] = meta["generation"]
        status["updateRevision"] = revision
        template_changed = previous is not None and (previous.get("spec") or {}).get(
            "template"
        ) != sts["spec"].get("template")
        if template_changed and sts["spec"]["updateStrategy"]["type"] != "OnDelete":
            for pod in sorted(self._pods_of(meta["name"]), key=lambda p: p["metadata"]["name"]):
                del self.objects[("Pod", self.namespace, pod["metadata"]["name"])]
                self.rolled.append(pod["metadata"]["name"])  # _sync_pods brings it back

    def _sync_pods(self, sts: dict[str, Any]) -> None:
        if self.controller_paused:
            return
        name = sts["metadata"]["name"]
        replicas = sts["spec"].get("replicas", 1)
        template = sts["spec"].get("template") or {}  # a seeded StatefulSet may be sparse
        labels = dict((template.get("metadata") or {}).get("labels", {}))
        revision = (sts.get("status") or {}).get("updateRevision")
        if revision is not None:
            labels[REVISION_LABEL] = revision
        wanted = {f"{name}-{i}" for i in range(replicas)}
        for pod in self._pods_of(name):
            if pod["metadata"]["name"] not in wanted:
                del self.objects[("Pod", self.namespace, pod["metadata"]["name"])]
        for pod_name in sorted(wanted):
            key = ("Pod", self.namespace, pod_name)
            if key not in self.objects:
                pod = _pod(pod_name, dict(labels), ready=self.pods_ready)
                pod["metadata"]["annotations"] = {"fake/statefulset": name}
                pod["metadata"]["namespace"] = self.namespace
                self.objects[key] = pod


def _obj(api_version: str, kind: str, name: str, labels: dict[str, str]) -> dict[str, Any]:
    return {
        "apiVersion": api_version,
        "kind": kind,
        "metadata": {"name": name, "labels": labels},
    }


def _merge(target: Any, patch: Any) -> Any:
    """RFC 7386 JSON merge patch: objects merge, null removes, anything else replaces."""
    if not isinstance(patch, dict):
        return copy.deepcopy(patch)
    out = copy.deepcopy(target) if isinstance(target, dict) else {}
    for key, value in patch.items():
        if value is None:
            out.pop(key, None)
        else:
            out[key] = _merge(out.get(key), value)
    return out


def _admit_pvc(stored: dict[str, Any], previous: dict[str, Any] | None) -> None:
    """PVC validation: a smaller request than before is a 422; the status survives an apply."""
    if previous is None:
        return
    floor = max(
        parse_quantity(previous["spec"]["resources"]["requests"]["storage"]),
        parse_quantity(((previous.get("status") or {}).get("capacity") or {}).get("storage", "0")),
    )
    if parse_quantity(stored["spec"]["resources"]["requests"]["storage"]) < floor:
        raise KubeError(
            "422: PersistentVolumeClaim is invalid: spec.resources.requests.storage: Forbidden:"
            " field can not be less than previous value"
        )
    if "status" in previous:
        stored["status"] = copy.deepcopy(previous["status"])


def _keep_defaulted_rolling_update(sts: dict[str, Any], previous: dict[str, Any] | None) -> None:
    """Server-side apply leaves the defaulted, unowned `rollingUpdate` the object does not name."""
    kept = ((previous or {}).get("spec", {}).get("updateStrategy") or {}).get("rollingUpdate")
    strategy = sts["spec"].setdefault("updateStrategy", {})
    if kept is not None and "rollingUpdate" not in strategy:
        strategy["rollingUpdate"] = copy.deepcopy(kept)


_BINARY = ("", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei")
_DECIMAL = ("n", "u", "m", "", "k", "M", "G", "T", "P", "E")


def canonical_quantity(value: str) -> str:
    """A quantity as the API server stores it: "0.5" is "500m", "1024Mi" is "1Gi", "1e3" is "1k".

    A binary suffix (Ki..Ei) stays binary at the largest power that divides the value; anything
    else becomes decimal SI, using the milli suffix for a fraction.
    """
    qty = parse_quantity(value)
    if qty == 0:
        return "0"
    if value.endswith(_BINARY[1:]) and qty == qty.to_integral_value():
        n = int(qty)
        for exp in range(len(_BINARY) - 1, 0, -1):
            if n % 1024**exp == 0:
                return f"{n // 1024**exp}{_BINARY[exp]}"
    if qty != qty.to_integral_value():
        return f"{int(qty * 1000)}m"
    n = int(qty)
    exp = 0
    while n % 1000 == 0 and exp < 6:
        n //= 1000
        exp += 1
    return f"{n}{_DECIMAL[3 + exp]}"


def _canonicalise_quantities(node: Any, parent: str = "") -> None:
    """Rewrite resource quantities in place, like the API server on write."""
    if isinstance(node, dict):
        for key, val in node.items():
            if isinstance(val, str) and (parent in ("limits", "requests") or key == "sizeLimit"):
                node[key] = canonical_quantity(val)
            else:
                _canonicalise_quantities(val, key)
    elif isinstance(node, list):
        for item in node:
            _canonicalise_quantities(item, parent)


def _admit_statefulset(sts: dict[str, Any]) -> None:
    """The API server's StatefulSet strategy defaulting, then its validation."""
    strategy = sts["spec"].setdefault("updateStrategy", {})
    strategy.setdefault("type", "RollingUpdate")
    if strategy["type"] == "RollingUpdate":
        strategy.setdefault("rollingUpdate", {"partition": 0})
    elif "rollingUpdate" in strategy:
        raise KubeError(
            f"422: StatefulSet.apps {sts['metadata']['name']!r} is invalid:"
            " spec.updateStrategy.rollingUpdate: only allowed for updateStrategy 'RollingUpdate'"
        )


def _set_ready(pod: dict[str, Any], ready: bool) -> None:
    pod["status"]["conditions"] = [{"type": "Ready", "status": "True" if ready else "False"}]


def _pod(name: str, labels: dict[str, str], *, ready: bool) -> dict[str, Any]:
    pod = _obj("v1", "Pod", name, labels)
    pod["metadata"]["uid"] = f"{next(_seq)}-{uuid.uuid4()}"
    pod["status"] = {
        "phase": "Running" if ready else "Pending",
        "podIP": f"10.0.{next(_seq) // 250}.{next(_seq) % 250 + 1}",
    }
    _set_ready(pod, ready)
    return pod
