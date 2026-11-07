"""LightkubeApi namespace mapping for cluster-scoped kinds, against a recording stub client."""

from __future__ import annotations

from typing import Any, cast

from lightkube import AsyncClient

from agents_orchestrator.k8s.kube import CLUSTER_SCOPED, RESOURCES, LightkubeApi, obj_namespace


class _Obj:
    def to_dict(self) -> dict[str, Any]:
        return {}


class StubClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def list(self, res, **kw):
        self.calls.append(("list", kw))
        if False:
            yield _Obj()

    async def get(self, res, name, **kw):
        self.calls.append(("get", kw))
        return _Obj()

    async def delete(self, res, name, **kw):
        self.calls.append(("delete", kw))

    async def patch(self, res, name, patch, **kw):
        self.calls.append(("patch", kw))
        return _Obj()


async def test_cluster_scoped_kind_passes_no_namespace():
    stub = StubClient()
    api = LightkubeApi(cast(AsyncClient, stub))
    await api.list_objects("ClusterRoleBinding", "", {"a": "b"})
    await api.get_object("ClusterRoleBinding", "", "x")
    await api.merge_patch("ClusterRoleBinding", "", "x", {})
    await api.delete("ClusterRoleBinding", "", "x")
    assert [kw["namespace"] for _, kw in stub.calls] == [None] * 4


async def test_namespaced_kind_passes_namespace():
    stub = StubClient()
    api = LightkubeApi(cast(AsyncClient, stub))
    await api.list_objects("RoleBinding", "media", {"a": "b"})
    await api.delete("RoleBinding", "media", "x")
    assert [kw["namespace"] for _, kw in stub.calls] == ["media", "media"]


def test_kinds_registered():
    assert {"ServiceAccount", "RoleBinding", "ClusterRoleBinding"} <= set(RESOURCES)
    assert frozenset({"ClusterRoleBinding"}) == CLUSTER_SCOPED


def test_obj_namespace():
    assert obj_namespace({"kind": "ClusterRoleBinding", "metadata": {"name": "x"}}) == ""
    assert obj_namespace({"metadata": {"name": "x", "namespace": "media"}}) == "media"
