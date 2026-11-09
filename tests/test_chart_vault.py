"""T-72 O1: the agents-vault upstream. ORCH_VAULT_URL and ORCH_VAULT_TOKEN are one pair, env of
the api main container only."""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from helm_render import CHART, render, render_error
from test_chart_usage import BASE, FULL, _by_kind, _containers, _env_names, _is_api_main

VAULT_URL = "http://agents-vault-service:8001"
VAULT_VALUES = {
    "inline": {"url": VAULT_URL, "token": "t"},
    "existing": {"url": VAULT_URL, "existingSecret": "vault-s", "existingSecretKey": "k"},
}


def _docs(extra: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(extra or {})})


def test_values_defaults() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["vault"] == {
        "url": "",
        "token": "",
        "existingSecret": "",
        "existingSecretKey": "vault-token",
    }


def test_off_by_default() -> None:
    docs = _docs()
    assert f"{FULL}-vault" not in _by_kind(docs, "Secret")
    for items in _containers(docs).values():
        for c in items:
            assert "ORCH_VAULT_URL" not in _env_names(c)


@pytest.mark.parametrize(
    "half",
    [{"url": VAULT_URL}, {"token": "t"}, {"existingSecret": "vault-s"}],
    ids=["url-only", "token-only", "existing-secret-only"],
)
def test_pair_or_fail(half: dict[str, str]) -> None:
    err = render_error({**BASE, "vault": half})
    assert (
        "agents-orchestrator: set vault.url together with vault.token or vault.existingSecret, "
        "or none of them" in err
    )


@pytest.mark.parametrize("vault", VAULT_VALUES.values(), ids=VAULT_VALUES.keys())
def test_pair_only_in_api_main_container(vault: dict[str, str]) -> None:
    docs = _docs({"vault": vault})
    for dep, items in _containers(docs).items():
        for c in items:
            names = _env_names(c)
            assert ("ORCH_VAULT_URL" in names) == ("ORCH_VAULT_TOKEN" in names)
            assert ("ORCH_VAULT_URL" in names) == _is_api_main(dep, c), (dep, c["name"])
    for cm in _by_kind(docs, "ConfigMap").values():
        assert "ORCH_VAULT" not in yaml.safe_dump(cm)
    api = next(c for c in _containers(docs)[f"{FULL}-api"] if c["name"] == "api")
    env = {e["name"]: e for e in api["env"]}
    assert env["ORCH_VAULT_URL"]["value"] == VAULT_URL
    assert env["ORCH_VAULT_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": vault.get("existingSecret", f"{FULL}-vault"),
        "key": vault.get("existingSecretKey", "vault-token"),
    }


def test_chart_secret_and_checksum_on_api_only() -> None:
    tok = "".join(["marker", "vault", "value"])
    docs = _docs({"vault": {"url": VAULT_URL, "token": tok}})
    assert _by_kind(docs, "Secret")[f"{FULL}-vault"]["stringData"] == {"vault-token": tok}
    deps = _by_kind(docs, "Deployment")
    for name, d in deps.items():
        has = "checksum/vault-secret" in d["spec"]["template"]["metadata"]["annotations"]
        assert has == (name == f"{FULL}-api")


def test_existing_secret_wins() -> None:
    docs = _docs({"vault": {**VAULT_VALUES["existing"], "token": "t"}})
    assert f"{FULL}-vault" not in _by_kind(docs, "Secret")
