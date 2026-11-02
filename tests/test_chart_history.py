"""O4: the agents-history chart values. `admin.history` is a relay upstream like the others
(ORCH_HISTORY_URL and ORCH_HISTORY_TOKEN, env of the api main container only). `discardHook`
feeds the pre-discard settings hook: ORCH_DISCARD_HOOK_URL is in the ConfigMap (the api reports
the hook kind in /meta), ORCH_DISCARD_HOOK_TOKEN is env of the reconciler main container only."""

from __future__ import annotations

from typing import Any

import pytest
import yaml
from helm_render import CHART, render, render_error

FULL = "agents-orchestrator"
HISTORY_URL = "http://agents-history-api-service:8001"
HOOK_URL = "http://agents-history-api-ingest:8002/history/v1/ingest"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
    "postgres": {"url": "postgresql://u:p@db:5432/app"},
}
HOOK = {"url": HOOK_URL, "existingSecret": "agents-history-ingest", "existingSecretKey": "token"}


def _docs(extra: dict[str, Any] | None = None) -> list[dict]:
    return render({**BASE, **(extra or {})})


def _by_kind(docs: list[dict], kind: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def _main_containers(docs: list[dict]) -> dict[str, dict]:
    """The main container of each Deployment, by component."""
    out = {}
    for name, dep in _by_kind(docs, "Deployment").items():
        main = dep["spec"]["template"]["spec"]["containers"]
        assert len(main) == 1
        out[name.removeprefix(FULL + "-")] = main[0]
    return out


def _env(container: dict) -> dict[str, dict]:
    return {e["name"]: e for e in container.get("env", [])}


def _every_container(docs: list[dict]) -> list[tuple[str, dict]]:
    out = []
    for name, dep in _by_kind(docs, "Deployment").items():
        spec = dep["spec"]["template"]["spec"]
        for c in spec.get("initContainers", []) + spec["containers"]:
            out.append((name + "/" + c["name"], c))
    return out


def _settings(docs: list[dict]) -> dict[str, str]:
    return _by_kind(docs, "ConfigMap")[f"{FULL}-settings"]["data"]


def test_values_defaults() -> None:
    values = yaml.safe_load((CHART / "values.yaml").read_text())
    assert values["admin"]["history"] == {
        "url": "",
        "token": "",
        "existingSecret": "",
        "existingSecretKey": "token",
    }
    assert values["discardHook"] == {
        "url": "",
        "token": "",
        "existingSecret": "",
        "existingSecretKey": "token",
    }


def test_off_by_default_renders_nothing() -> None:
    docs = _docs()
    for where, c in _every_container(docs):
        assert not {"ORCH_HISTORY_URL", "ORCH_HISTORY_TOKEN", "ORCH_DISCARD_HOOK_TOKEN"} & set(
            _env(c)
        ), where
    assert "ORCH_DISCARD_HOOK_URL" not in _settings(docs)
    assert not [n for n in _by_kind(docs, "Secret") if "history" in n or "discard" in n]


def test_history_upstream_env_on_api_only() -> None:
    admin = {"history": {"url": HISTORY_URL, "existingSecret": "hist-s", "existingSecretKey": "k"}}
    docs = _docs({"admin": admin})
    for where, c in _every_container(docs):
        names = set(_env(c))
        assert ("ORCH_HISTORY_URL" in names) == ("ORCH_HISTORY_TOKEN" in names), where
        assert ("ORCH_HISTORY_URL" in names) == (where == f"{FULL}-api/api"), where
    env = _env(_main_containers(docs)["api"])
    assert env["ORCH_HISTORY_URL"]["value"] == HISTORY_URL
    assert env["ORCH_HISTORY_TOKEN"]["valueFrom"]["secretKeyRef"] == {"name": "hist-s", "key": "k"}
    data = _settings(docs)
    assert not [k for k in data if k.startswith("ORCH_HISTORY_")]
    assert f"{FULL}-admin-history" not in _by_kind(docs, "Secret")


def test_history_upstream_inline_token_builds_the_chart_secret() -> None:
    tok = "".join(["marker", "history", "value"])
    docs = _docs({"admin": {"history": {"url": HISTORY_URL, "token": tok}}})
    assert _by_kind(docs, "Secret")[f"{FULL}-admin-history"]["stringData"] == {"token": tok}
    ref = _env(_main_containers(docs)["api"])["ORCH_HISTORY_TOKEN"]["valueFrom"]["secretKeyRef"]
    assert ref == {"name": f"{FULL}-admin-history", "key": "token"}
    for cm in _by_kind(docs, "ConfigMap").values():
        assert tok not in yaml.safe_dump(cm)
    api = _by_kind(docs, "Deployment")[f"{FULL}-api"]
    assert "checksum/admin-history-secret" in api["spec"]["template"]["metadata"]["annotations"]


@pytest.mark.parametrize(
    "half",
    [{"url": HISTORY_URL}, {"token": "t"}, {"existingSecret": "hist-s"}],
    ids=["url-only", "token-only", "existing-secret-only"],
)
def test_history_upstream_pair_enforced(half: dict[str, str]) -> None:
    err = render_error({**BASE, "admin": {"history": half}})
    assert "admin.history.url together with admin.history.token or" in err


def test_discard_hook_url_in_configmap() -> None:
    docs = _docs({"discardHook": HOOK})
    assert _settings(docs)["ORCH_DISCARD_HOOK_URL"] == HOOK_URL
    assert "ORCH_DISCARD_HOOK_TOKEN" not in _settings(docs)


def test_discard_hook_token_on_reconciler_only() -> None:
    docs = _docs({"discardHook": HOOK})
    for where, c in _every_container(docs):
        assert ("ORCH_DISCARD_HOOK_TOKEN" in _env(c)) == (where == f"{FULL}-reconciler/reconciler")
    ref = _env(_main_containers(docs)["reconciler"])["ORCH_DISCARD_HOOK_TOKEN"]
    assert ref["valueFrom"]["secretKeyRef"] == {"name": "agents-history-ingest", "key": "token"}


@pytest.mark.parametrize(
    "half",
    [{"url": HOOK_URL}, {"token": "t"}, {"existingSecret": "agents-history-ingest"}],
    ids=["url-only", "token-only", "existing-secret-only"],
)
def test_discard_hook_pair_enforced(half: dict[str, str]) -> None:
    err = render_error({**BASE, "discardHook": half})
    assert "discardHook.url together with discardHook.token or discardHook.existingSecret" in err


def test_discard_hook_token_value_never_in_configmap() -> None:
    tok = "t0k3n"
    docs = _docs({"discardHook": {"url": HOOK_URL, "token": tok}})
    for cm in _by_kind(docs, "ConfigMap").values():
        assert tok not in yaml.safe_dump(cm)
    assert _by_kind(docs, "Secret")[f"{FULL}-discard-hook"]["stringData"] == {"token": tok}
    ref = _env(_main_containers(docs)["reconciler"])["ORCH_DISCARD_HOOK_TOKEN"]
    assert ref["valueFrom"]["secretKeyRef"] == {"name": f"{FULL}-discard-hook", "key": "token"}


def test_discard_hook_chart_secret_checksum_on_reconciler_only() -> None:
    def sums(token: str) -> dict[str, str | None]:
        deps = _by_kind(_docs({"discardHook": {"url": HOOK_URL, "token": token}}), "Deployment")
        return {
            n: d["spec"]["template"]["metadata"]["annotations"].get("checksum/discard-hook-secret")
            for n, d in deps.items()
        }

    one, two = sums("t1"), sums("t2")
    rec = f"{FULL}-reconciler"
    assert one[rec] and two[rec] and one[rec] != two[rec]
    assert {v for n, v in one.items() if n != rec} == {None}


def test_discard_hook_existing_secret_wins_over_inline_token() -> None:
    docs = _docs({"discardHook": {**HOOK, "token": "t0k3n"}})
    assert f"{FULL}-discard-hook" not in _by_kind(docs, "Secret")
    for d in _by_kind(docs, "Deployment").values():
        annotations = d["spec"]["template"]["metadata"]["annotations"]
        assert "checksum/discard-hook-secret" not in annotations
