"""The kube_namespace_access spec field, its settings, validate_spec checks and tier roles."""

import os

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings, SettingsError
from agents_orchestrator.spec import KUBE_TIERS, SessionSpec, SpecInvalid, validate_spec

DB = "postgresql://u:p@h/db"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for k in list(os.environ):
        if k.startswith("ORCH_"):
            monkeypatch.delenv(k)
    monkeypatch.setenv("ORCH_DATABASE_URL", DB)


def settings(namespaces="", tiers="", prefix=""):
    return Settings(
        agent_image="img:1",
        kube_namespace_access_namespaces=namespaces,
        kube_namespace_access_tiers=tiers,
        kube_namespace_access_role_prefix=prefix,
    )


def spec(access):
    return SessionSpec(kube_namespace_access=access)


def test_namespace_map_normalised():
    s = SessionSpec.model_validate({"kube_namespace_access": {"media": ["port-forward", "read"]}})
    assert s.kube_namespace_access == {"media": ["read", "port-forward"]}


@pytest.mark.parametrize(
    "bad",
    [
        {},
        {"media": []},
        {"media": ["read", "read"]},
        {"media": ["write"]},
        {"Media": ["read"]},
        {"a_b": ["read"]},
        {"x" * 64: ["read"]},
        {"media\n": ["read"]},
    ],
)
def test_namespace_map_refused(bad):
    with pytest.raises(ValidationError):
        SessionSpec.model_validate({"kube_namespace_access": bad})


def test_sparse_when_unset():
    assert SessionSpec().kube_namespace_access is None
    assert "kube_namespace_access" not in SessionSpec().model_dump(exclude_unset=True)


def test_all_five_tiers_and_cluster_together_accepted():
    s = SessionSpec.model_validate(
        {"kube_cluster_access": True, "kube_namespace_access": {"media": list(KUBE_TIERS)}}
    )
    assert s.kube_cluster_access and s.kube_namespace_access["media"] == list(KUBE_TIERS)


def test_validate_spec_allowlist_and_tiers():
    st = settings(namespaces="media", tiers="read,port-forward", prefix="o-ns-")
    validate_spec(spec({"media": ["read"]}), st)
    with pytest.raises(
        SpecInvalid, match="namespace 'games' is not in the Kubernetes namespace allowlist"
    ):
        validate_spec(spec({"games": ["read"]}), st)
    with pytest.raises(SpecInvalid, match="tier 'exec' is not enabled"):
        validate_spec(spec({"media": ["exec"]}), st)
    validate_spec(spec({"games": ["read"]}), st, stored=spec({"games": ["read"]}))


def test_validate_spec_one_error_per_namespace_and_tier():
    st = settings(namespaces="media", tiers="read")
    with pytest.raises(SpecInvalid) as e:
        validate_spec(spec({"games": ["exec", "admin"], "media": ["edit"]}), st)
    assert len(e.value.errors) == 4


def test_validate_spec_changed_value_is_checked_against_stored():
    st = settings(namespaces="media", tiers="read")
    with pytest.raises(SpecInvalid):
        validate_spec(spec({"games": ["read", "edit"]}), st, stored=spec({"games": ["read"]}))


def test_settings_tiers_validated(monkeypatch):
    monkeypatch.setenv("ORCH_KUBE_NAMESPACE_ACCESS_TIERS", "read,write")
    with pytest.raises(SettingsError) as e:
        Settings(agent_image="img:1")
    assert "ORCH_KUBE_NAMESPACE_ACCESS_TIERS" in str(e.value)


@pytest.mark.parametrize("tier", ["port-forward", "exec"])
def test_settings_custom_tiers_need_prefix(monkeypatch, tier):
    monkeypatch.setenv("ORCH_KUBE_NAMESPACE_ACCESS_TIERS", f"read,{tier}")
    with pytest.raises(SettingsError) as e:
        Settings(agent_image="img:1")
    assert "ORCH_KUBE_NAMESPACE_ACCESS_ROLE_PREFIX" in str(e.value)
    monkeypatch.setenv("ORCH_KUBE_NAMESPACE_ACCESS_ROLE_PREFIX", "o-ns-")
    assert Settings(agent_image="img:1").kube_namespace_access_tiers == ["read", tier]


def test_settings_builtin_tiers_need_no_prefix():
    assert settings(namespaces="media", tiers="read,edit,admin").kube_namespace_access_available


def test_available_needs_namespace_and_tier():
    assert settings().kube_namespace_access_available is False
    assert settings(namespaces="media").kube_namespace_access_available is False
    assert settings(tiers="read").kube_namespace_access_available is False


def test_kube_tier_role():
    st = settings(namespaces="media", tiers=",".join(KUBE_TIERS), prefix="o-ns-")
    assert [st.kube_tier_role(t) for t in KUBE_TIERS] == [
        "view",
        "o-ns-port-forward",
        "o-ns-exec",
        "edit",
        "admin",
    ]
    assert st.kube_namespace_access_available is True
