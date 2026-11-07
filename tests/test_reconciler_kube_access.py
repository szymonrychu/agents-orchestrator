"""The reconciler applies, prunes and discards the per-session ServiceAccount and its bindings
across the three scopes: the orchestrator namespace, each allowlisted namespace and
the cluster scope (T-55, plan 3.4). Bookkeeping is keyed by (kind, namespace, name).

The pure tests drive `_foreign`, `_apply`, `_prune` and `_remaining` on a Reconciler without a
pool; the converge and discard tests run against Postgres (CI)."""

from __future__ import annotations

import uuid
from typing import Any

import pytest
import pytest_asyncio
from fakes.fake_kube import FakeKube
from test_reconciler import Env
from test_reconciler import make_settings as db_settings
from test_service_accounts import make_session, make_settings

from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s.kube import KubeError
from agents_orchestrator.k8s.labels import (
    SESSION_ID_LABEL,
    cluster_binding_name,
    kube_labels,
    kube_service_account,
    namespace_binding_name,
    session_labels,
)
from agents_orchestrator.k8s.render import kube_access_objects
from agents_orchestrator.reconciler import Reconciler
from agents_orchestrator.reconciler.loop import DISCARD_ORDER, obj_key
from agents_orchestrator.settings import Settings

ROLES = frozenset({"view", "edit", "cluster-admin"})
NS = {
    "kube_namespace_access_namespaces": ["media", "games"],
    "kube_namespace_access_tiers": ["read", "edit"],
}


def settings(**over: Any) -> Settings:
    return make_settings(**{**NS, **over})


def kube(**over: Any) -> FakeKube:
    base: dict[str, Any] = {
        "rbac_namespaces": frozenset({"media", "games"}),
        "bindable": ROLES,
        "serviceaccounts": True,
    }
    return FakeKube(namespace="agents", **{**base, **over})


def reconciler(k: FakeKube, s: Settings) -> Reconciler:
    """No pool: these steps make Kubernetes calls only (the constructor just stores it)."""
    return Reconciler(None, k, s)


def account(session: Session) -> str:
    return kube_service_account(session.slug, session.id)


async def apply_all(r: Reconciler, objs: list[dict[str, Any]]) -> None:
    for obj in objs:
        await r._apply(obj, None, "scale-down")


async def converge_rbac(r: Reconciler, session: Session) -> list[dict[str, Any]]:
    """The RBAC slice of a converge: guard, apply, prune what is no longer rendered."""
    objs = kube_access_objects(session, r.settings)
    found, live = await r._foreign(session, objs)
    assert found == []
    for obj in objs:
        await r._apply(obj, live.get(obj_key(obj)), "scale-down")
    await r._prune(session.slug, {obj_key(o) for o in objs}, reason="no longer rendered")
    return objs


async def test_same_binding_name_in_two_namespaces():
    k, s = kube(), settings()
    r = reconciler(k, s)
    both = make_session(kube_namespace_access={"media": ["read"], "games": ["read"]})
    sa = account(both)
    await converge_rbac(r, both)
    assert k.has("RoleBinding", f"{sa}-read", "media")
    assert k.has("RoleBinding", f"{sa}-read", "games")
    assert ("RoleBinding", "games", f"{sa}-read") in r._applied

    k.reset_records()
    media = make_session(kube_namespace_access={"media": ["read"]})
    await converge_rbac(r, media)
    assert k.deletes == [("RoleBinding", "games", f"{sa}-read")]
    assert k.has("RoleBinding", f"{sa}-read", "media")
    assert ("RoleBinding", "games", f"{sa}-read") not in r._applied
    assert ("RoleBinding", "media", f"{sa}-read") in r._applied
    assert k.applied() == []  # the media binding is unchanged: not applied again


@pytest.mark.parametrize(
    ("over", "scopes"),
    [
        ({}, []),
        ({"kube_cluster_access_enabled": True}, ["agents"]),
        ({"kube_namespace_access_namespaces": ["media"]}, ["agents"]),
    ],
)
def test_service_account_scope_follows_the_chart_grant(over, scopes):
    # the chart grants serviceaccounts only `if or kubeClusterAccess.enabled
    # kubeNamespaceAccess.namespaces` (role.yaml); the reconciler lists them only then
    s = make_settings(namespace="agents", **over)
    assert reconciler(FakeKube(namespace="agents"), s)._scopes("ServiceAccount") == scopes


async def test_default_settings_make_no_service_account_calls():
    k = FakeKube(namespace="agents")  # the chart defaults: no serviceaccounts verbs
    r = reconciler(k, make_settings(namespace="agents"))
    session = make_session()
    await r._prune(session.slug, set(), reason="no longer rendered")
    await r._prune(session.slug, set(), DISCARD_ORDER, reason="discard")
    assert await r._remaining(session.slug) == []
    assert k.forbidden == []
    assert not any(c.kind == "ServiceAccount" for c in k.calls)


async def test_prune_skips_unlisted_namespaces():
    # the allowlist dropped games: the reconciler's RBAC reaches media only, cluster access is off
    k = kube(rbac_namespaces=frozenset({"media"}))
    s = settings(kube_namespace_access_namespaces=["media"])
    r = reconciler(k, s)
    session = make_session(kube_namespace_access={"media": ["read"], "games": ["read"]})
    await converge_rbac(r, session)
    await r._prune(session.slug, set(), DISCARD_ORDER, reason="discard")
    assert await r._remaining(session.slug) == []
    assert k.forbidden == []
    assert {c.namespace for c in k.calls} <= {"media", "agents", ""}


async def test_switch_cluster_to_namespace_swaps_bindings():
    k, s = kube(cluster_rbac=True), settings(kube_cluster_access_enabled=True)
    r = reconciler(k, s)
    grants = {"kube_namespace_access": {"media": ["read"]}}
    wide = make_session(kube_cluster_access=True, **grants)
    sa, crb = account(wide), cluster_binding_name(wide.slug, wide.id)
    await converge_rbac(r, wide)
    assert ("ClusterRoleBinding", "", crb) in k.objects  # has() reads "" as the default namespace
    assert k.names("RoleBinding") == []

    k.reset_records()
    await converge_rbac(r, make_session(kube_cluster_access=False, **grants))
    assert k.deletes == [("ClusterRoleBinding", "", crb)]
    assert [(a.kind, a.obj["metadata"]["namespace"], a.name) for a in k.applied()] == [
        ("RoleBinding", "media", f"{sa}-read")
    ]
    assert k.has("ServiceAccount", sa)  # same name, untouched: the pods keep their account


async def test_live_binding_with_other_session_id_is_replaced(caplog):
    k, s = kube(), settings()
    r = reconciler(k, s)
    session = make_session(kube_namespace_access={"media": ["read"]})
    name = namespace_binding_name(session.slug, session.id, "read")
    other = uuid.uuid4()
    k.seed(
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "RoleBinding",
            "metadata": {"name": name, "namespace": "media", "labels": kube_labels("alpha", other)},
            "roleRef": {
                "apiGroup": "rbac.authorization.k8s.io",
                "kind": "ClusterRole",
                "name": "edit",
            },
        }
    )
    objs = kube_access_objects(session, s)
    with caplog.at_level("WARNING", logger="agents_orchestrator.reconciler.loop"):
        found, live = await r._foreign(session, objs)
    assert found == [] and ("RoleBinding", "media", name) not in live
    assert ("RoleBinding", "media", name) in k.deletes  # deleted, never adopted
    (warning,) = [m for m in caplog.messages if "belongs to session" in m]
    assert str(other) in warning and name in warning

    await apply_all(r, objs)
    stored = k.get("RoleBinding", name, "media")
    assert stored["metadata"]["labels"][SESSION_ID_LABEL] == str(session.id)
    assert stored["roleRef"]["name"] == "view"


async def test_unlabelled_service_account_is_foreign():
    k, s = kube(), settings()
    r = reconciler(k, s)
    session = make_session(kube_namespace_access={"media": ["read"]})
    sa = account(session)
    k.seed({"apiVersion": "v1", "kind": "ServiceAccount", "metadata": {"name": sa}})
    found, live = await r._foreign(session, kube_access_objects(session, s))
    assert found == [f"foreign_object ServiceAccount/{sa}"]
    assert k.deletes == [] and ("ServiceAccount", "agents", sa) not in live


async def test_discard_order_and_remaining_wait():
    k = kube(cluster_rbac=True)
    s = settings(kube_cluster_access_enabled=True)
    r = reconciler(k, s)
    session = make_session(kube_namespace_access={"media": ["read", "edit"], "games": ["read"]})
    await apply_all(r, kube_access_objects(session, s))
    sa = account(session)
    crb = cluster_binding_name(session.slug, session.id)
    labels = kube_labels(session.slug, session.id)
    k.seed(
        {
            "apiVersion": "rbac.authorization.k8s.io/v1",
            "kind": "ClusterRoleBinding",
            "metadata": {"name": crb, "labels": labels},
        },
        {
            "apiVersion": "apps/v1",
            "kind": "StatefulSet",
            "metadata": {"name": "agent-alpha", "labels": session_labels(session.slug)},
            "spec": {"replicas": 0},
        },
    )
    await r._prune(session.slug, set(), DISCARD_ORDER, reason="discard")
    assert k.deletes == [
        ("StatefulSet", "agents", "agent-alpha"),
        ("RoleBinding", "media", f"{sa}-edit"),  # the allowlist order, then by name
        ("RoleBinding", "media", f"{sa}-read"),
        ("RoleBinding", "games", f"{sa}-read"),
        ("ClusterRoleBinding", "", crb),
        ("ServiceAccount", "agents", sa),
    ]
    assert await r._remaining(session.slug) == []
    assert not any(key[0] in ("RoleBinding", "ServiceAccount") for key in r._applied)

    await apply_all(r, kube_access_objects(session, s))
    k.fail("RoleBinding", f"{sa}-edit", verbs={"delete"})
    with pytest.raises(KubeError):
        await r._prune(session.slug, set(), DISCARD_ORDER, reason="discard")
    assert f"RoleBinding/{sa}-edit" in await r._remaining(session.slug)


# ---------------------------------------------------------------- converge and discard (DB, CI)


@pytest_asyncio.fixture
async def rbac_env(pool, dsn) -> Env:
    env = Env(pool, dsn)
    env.kube = kube(rbac_namespaces=frozenset({"media"}))
    env.settings = db_settings(
        dsn,
        kube_namespace_access_namespaces=["media"],
        kube_namespace_access_tiers=["read", "edit"],
    )
    env.rec = Reconciler(pool, env.kube, env.settings, wrapper_factory=None)
    return env


async def test_default_settings_converge_reaches_ready_and_discard_purges(pool, dsn):
    env = Env(pool, dsn)  # chart defaults: no kube access, no serviceaccounts verbs
    env.kube.pods_ready = True
    c = await env.session("alpha")
    await env.rec.reconcile_once()
    assert env.kube.forbidden == []
    got = await env.get(c.id)
    assert got.status.get("error") is None
    assert got.status["phase"] == "ready"

    await env.delete(c.id)
    await env.rec.reconcile_once()
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None
    assert env.kube.forbidden == []
    assert not any(call.kind == "ServiceAccount" for call in env.kube.calls)


async def test_converge_namespace_session_creates_sa_and_bindings(rbac_env):
    env = rbac_env
    c = await env.session("alpha", kube_namespace_access={"media": ["read", "edit"]})
    await env.rec.reconcile_once()
    sa = kube_service_account("alpha", c.id)
    assert env.kube.has("ServiceAccount", sa)
    assert env.kube.has("RoleBinding", f"{sa}-read", "media")
    assert env.kube.has("RoleBinding", f"{sa}-edit", "media")
    sts = env.kube.get("StatefulSet", "agent-alpha")
    assert sts["spec"]["template"]["spec"]["serviceAccountName"] == sa
    assert env.kube.forbidden == []

    env.kube.reset_records()
    await env.rec.reconcile_once()
    assert env.kube.applied() == [] and env.kube.deletes == []

    await env.update(c.id, kube_namespace_access={"media": ["read"]})
    await env.rec.reconcile_once()
    assert env.kube.deletes == [("RoleBinding", "media", f"{sa}-edit")]
    assert env.kube.forbidden == []


async def test_discard_purges_only_after_bindings_gone(rbac_env):
    env = rbac_env
    c = await env.session("alpha", kube_namespace_access={"media": ["read"]})
    await env.rec.reconcile_once()
    sa = kube_service_account("alpha", c.id)
    await env.delete(c.id)
    env.kube.fail("RoleBinding", f"{sa}-read", verbs={"delete"})
    await env.rec.reconcile_once()
    got = await env.get(c.id)
    assert got is not None and got.status["phase"] == "error"
    assert env.kube.has("ServiceAccount", sa)  # bindings go before the account they name

    env.kube.heal("RoleBinding", f"{sa}-read")
    await env.rec.reconcile_once()
    assert await env.get(c.id) is None
    assert not env.kube.has("RoleBinding", f"{sa}-read", "media")
    assert not env.kube.has("ServiceAccount", sa)
    assert env.kube.forbidden == []
