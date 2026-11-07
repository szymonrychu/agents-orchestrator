"""The fake kube's RBAC model: where bindings may live and which roles they may reference."""

from __future__ import annotations

import pytest
from fakes.fake_kube import FakeKube

from agents_orchestrator.k8s.kube import Forbidden


def rb(namespace: str, role: str, name: str = "agent-x-1-read") -> dict:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "RoleBinding",
        "metadata": {"name": name, "namespace": namespace},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role},
        "subjects": [{"kind": "ServiceAccount", "name": "agent-x-1", "namespace": "agents"}],
    }


def crb(role: str) -> dict:
    return {
        "apiVersion": "rbac.authorization.k8s.io/v1",
        "kind": "ClusterRoleBinding",
        "metadata": {"name": "agent-x-1-cluster-admin"},
        "roleRef": {"apiGroup": "rbac.authorization.k8s.io", "kind": "ClusterRole", "name": role},
        "subjects": [{"kind": "ServiceAccount", "name": "agent-x-1", "namespace": "agents"}],
    }


async def test_rolebinding_only_in_rbac_namespaces():
    k = FakeKube(rbac_namespaces=frozenset({"media"}), bindable=frozenset({"view"}))
    await k.apply(rb("media", "view"))
    with pytest.raises(Forbidden):
        await k.apply(rb("games", "view"))


async def test_binding_to_unbindable_role_is_forbidden():
    k = FakeKube(rbac_namespaces=frozenset({"media"}), bindable=frozenset({"view"}))
    with pytest.raises(Forbidden):
        await k.apply(rb("media", "edit"))
    assert k.forbidden


async def test_cluster_binding_needs_cluster_rbac():
    with pytest.raises(Forbidden):
        await FakeKube(bindable=frozenset({"cluster-admin"})).apply(crb("cluster-admin"))
    k = FakeKube(cluster_rbac=True, bindable=frozenset({"cluster-admin"}))
    await k.apply(crb("cluster-admin"))
    assert await k.list_objects("ClusterRoleBinding", "", {}) != []


async def test_cluster_binding_delete_and_get_at_cluster_scope():
    k = FakeKube(cluster_rbac=True, bindable=frozenset({"cluster-admin"}))
    await k.apply(crb("cluster-admin"))
    assert await k.get_object("ClusterRoleBinding", "", "agent-x-1-cluster-admin") is not None
    assert await k.delete("ClusterRoleBinding", "", "agent-x-1-cluster-admin")
    assert await k.list_objects("ClusterRoleBinding", "", {}) == []


async def test_serviceaccount_only_in_own_namespace():
    k = FakeKube()
    sa = {
        "apiVersion": "v1",
        "kind": "ServiceAccount",
        "metadata": {"name": "a", "namespace": "agents"},
    }
    await k.apply(sa)
    sa2 = {**sa, "metadata": {"name": "a", "namespace": "media"}}
    with pytest.raises(Forbidden):
        await k.apply(sa2)
