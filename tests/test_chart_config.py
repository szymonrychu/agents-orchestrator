"""Settings, Secrets and the Postgres connection of the chart (D28, D7, D15)."""

from __future__ import annotations

import copy
import shutil
from pathlib import Path
from typing import Any

import pytest
import yaml
from helm_render import CHART, LINT_VALUES, render, render_error

FULL = "agents-orchestrator"
BASE: dict[str, Any] = {
    "agentImage": "registry.example/wrapper:1",
    "masterKey": {"value": "mk"},
    "claude": {"token": "tok"},
}
URL = "postgresql://u:p@db:5432/app"


@pytest.fixture
def env_chart(tmp_path: Path) -> Path:
    """A chart copy with a probe template that renders the shared container env."""
    dst = tmp_path / "agents-orchestrator"
    shutil.copytree(CHART, dst)
    (dst / "templates" / "env-probe.yaml").write_text(
        "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: env-probe\ndata:\n"
        "  env: |\n"
        '    {{- include "agents-orchestrator.env" . | nindent 4 }}\n'
    )
    return dst


def values(**extra: Any) -> dict[str, Any]:
    v = copy.deepcopy(BASE)
    v.update(extra)
    return v


def by_kind(docs: list[dict], kind: str) -> dict[str, dict]:
    return {d["metadata"]["name"]: d for d in docs if d["kind"] == kind}


def test_requires_postgres_connection():
    err = render_error(values())
    assert (
        "agents-orchestrator: a Postgres connection is required: set postgres.url, "
        "postgres.host/user/database/password, postgres.existingSecret, or cnpg.enabled=true" in err
    )


def test_postgres_url_and_parts():
    secrets = by_kind(render(values(postgres={"url": URL})), "Secret")
    assert yaml.safe_load(yaml.safe_dump(secrets[f"{FULL}-database"]["stringData"])) == {"uri": URL}

    parts = {
        "host": "db.example",
        "user": "us er@x",
        "password": "p:w/d?#",
        "database": "orch",
        "port": 6432,
        "sslmode": "require",
    }
    secrets = by_kind(render(values(postgres=parts)), "Secret")
    assert secrets[f"{FULL}-database"]["stringData"]["uri"] == (
        "postgresql://us%20er%40x:p%3Aw%2Fd%3F%23@db.example:6432/orch?sslmode=require"
    )
    parts.pop("sslmode")
    parts["password"] = ""
    secrets = by_kind(render(values(postgres=parts)), "Secret")
    assert secrets[f"{FULL}-database"]["stringData"]["uri"] == (
        "postgresql://us%20er%40x:@db.example:6432/orch"
    )


def test_postgres_parts_incomplete_is_not_a_source():
    err = render_error(values(postgres={"host": "db", "user": "u"}))
    assert "a Postgres connection is required" in err


def test_postgres_url_wired_from_chart_secret(env_chart):
    docs = render(values(postgres={"url": URL}), chart=env_chart)
    (probe,) = [d for d in docs if d["metadata"]["name"] == "env-probe"]
    block = yaml.safe_load(probe["data"]["env"])
    env = block["env"]
    db = next(e for e in env if e["name"] == "ORCH_DATABASE_URL")
    assert db["valueFrom"]["secretKeyRef"] == {"name": f"{FULL}-database", "key": "uri"}


def test_postgres_existing_secret_ref(env_chart):
    docs = render(
        values(postgres={"existingSecret": "pg-secret", "existingSecretKey": "dsn"}),
        chart=env_chart,
    )
    assert f"{FULL}-database" not in by_kind(docs, "Secret")
    (probe,) = [d for d in docs if d["metadata"]["name"] == "env-probe"]
    block = yaml.safe_load(probe["data"]["env"])
    env = block["env"]
    db = next(e for e in env if e["name"] == "ORCH_DATABASE_URL")
    assert db["valueFrom"]["secretKeyRef"] == {"name": "pg-secret", "key": "dsn"}
    mk = next(e for e in env if e["name"] == "ORCH_MASTER_KEY")
    assert mk["valueFrom"]["secretKeyRef"] == {"name": f"{FULL}-master-key", "key": "master-key"}
    assert block["envFrom"] == [{"configMapRef": {"name": f"{FULL}-settings"}}]


def test_multiple_postgres_sources_rejected():
    err = render_error(values(postgres={"url": URL, "existingSecret": "x"}))
    assert "postgres.url" in err and "postgres.existingSecret" in err
    err = render_error(
        values(
            postgres={"url": URL, "host": "h", "user": "u", "database": "d"}, cnpg={"enabled": True}
        )
    )
    assert "postgres.url" in err and "postgres.host/user/database" in err and "cnpg.enabled" in err


CNPG_KIND = "Cluster"  # the CloudNativePG database resource


def test_cnpg_optional_with_backup_passthrough(env_chart):
    docs = render(values(postgres={"url": URL}))
    assert CNPG_KIND not in {d["kind"] for d in docs}

    backup = {
        "retentionPolicy": "30d",
        "barmanObjectStore": {"destinationPath": "s3://b/p", "s3Credentials": {"x": {"name": "s"}}},
    }
    cnpg = {
        "enabled": True,
        "instances": 3,
        "imageName": "ghcr.io/cloudnative-pg/postgresql:17",
        "storage": {"size": "20Gi", "storageClass": "fast"},
        "walStorage": {"enabled": True, "size": "4Gi", "storageClass": "wal"},
        "resources": {"limits": {"memory": "1Gi"}},
        "backup": backup,
    }
    docs = render(values(cnpg=cnpg), chart=env_chart)
    (database,) = [d for d in docs if d["kind"] == CNPG_KIND]
    assert database["apiVersion"] == "postgresql.cnpg.io/v1"
    assert database["metadata"]["name"] == f"{FULL}-db"
    spec = database["spec"]
    assert spec["instances"] == 3
    assert spec["imageName"] == "ghcr.io/cloudnative-pg/postgresql:17"
    assert spec["storage"] == {"size": "20Gi", "storageClass": "fast"}
    assert spec["walStorage"] == {"size": "4Gi", "storageClass": "wal"}
    assert spec["resources"] == {"limits": {"memory": "1Gi"}}
    assert spec["bootstrap"]["initdb"] == {
        "database": "agents_orchestrator",
        "owner": "agents_orchestrator",
    }
    assert spec["backup"] == backup
    assert f"{FULL}-database" not in by_kind(docs, "Secret")
    (probe,) = [d for d in docs if d["metadata"]["name"] == "env-probe"]
    block = yaml.safe_load(probe["data"]["env"])
    env = block["env"]
    db = next(e for e in env if e["name"] == "ORCH_DATABASE_URL")
    assert db["valueFrom"]["secretKeyRef"] == {"name": f"{FULL}-db-app", "key": "uri"}

    minimal = render(values(cnpg={"enabled": True}))
    (database,) = [d for d in minimal if d["kind"] == CNPG_KIND]
    assert "backup" not in database["spec"]
    assert "walStorage" not in database["spec"]
    assert "imageName" not in database["spec"]
    assert "storageClass" not in database["spec"]["storage"]


def test_settings_configmap():
    cm = by_kind(
        render(
            values(postgres={"url": URL}, env={"ORCH_LOG_LEVEL": "debug", "ORCH_X": 5}),
            namespace="ns1",
        ),
        "ConfigMap",
    )[f"{FULL}-settings"]
    assert cm["data"] == {
        "ORCH_NAMESPACE": "ns1",
        "ORCH_AGENT_IMAGE": "registry.example/wrapper:1",
        "ORCH_AGENT_PORT": "8001",
        "ORCH_SELF_RESTART_COOLDOWN_SECONDS": "600",
        "ORCH_SERVICE_PORT": "8002",
        "ORCH_AGENT_API_URL": f"http://{FULL}-agents.ns1.svc:8001",
        "ORCH_CLAUDE_SECRET_NAME": f"{FULL}-claude",
        "ORCH_AGENT_SERVICE_ACCOUNT": f"{FULL}-agent",
        "ORCH_LOG_LEVEL": "debug",
        "ORCH_X": "5",
    }
    cm = by_kind(
        render(
            values(postgres={"url": URL}, claude={"existingSecret": "mine"}),
        ),
        "ConfigMap",
    )[f"{FULL}-settings"]
    assert cm["data"]["ORCH_CLAUDE_SECRET_NAME"] == "mine"

    err = render_error(values(postgres={"url": URL}, env={"LOG_LEVEL": "x"}))
    assert "LOG_LEVEL" in err and "ORCH_" in err

    err = render_error({**values(postgres={"url": URL}), "agentImage": ""})
    assert "agentImage" in err


def test_master_key_and_claude_secrets():
    docs = render(values(postgres={"url": URL}))
    secrets = by_kind(docs, "Secret")
    assert secrets[f"{FULL}-master-key"]["stringData"] == {"master-key": "mk"}
    assert secrets[f"{FULL}-claude"]["stringData"] == {"CLAUDE_CODE_OAUTH_TOKEN": "tok"}

    docs = render(
        values(
            postgres={"url": URL},
            masterKey={"existingSecret": "mk-s", "existingSecretKey": "k"},
            claude={"existingSecret": "cl-s"},
        )
    )
    secrets = by_kind(docs, "Secret")
    assert f"{FULL}-master-key" not in secrets and f"{FULL}-claude" not in secrets

    err = render_error({**values(postgres={"url": URL}), "masterKey": {}})
    assert "masterKey" in err
    err = render_error({**values(postgres={"url": URL}), "claude": {}})
    assert "claude" in err


def test_no_secret_values_in_configmap():
    # built at run time so no secret-shaped literal sits in the source
    pw, mk, tok, chat = ("".join(["marker", name, "value"]) for name in ("pw", "mk", "tok", "chat"))
    docs = render(
        values(
            postgres={"host": "h", "user": "u", "password": pw, "database": "d"},
            masterKey={"value": mk},
            claude={"token": tok},
            chat={"url": CHAT_URL, "token": chat},
        )
    )
    for cm in by_kind(docs, "ConfigMap").values():
        text = yaml.safe_dump(cm)
        for secret in (pw, mk, tok, chat, "postgresql://"):
            assert secret not in text


def test_secret_names_never_match_agent_pod_secrets():
    import re

    docs = render(values(postgres={"url": URL}), release="agent-x-ccw")
    for name in by_kind(docs, "Secret"):
        assert not re.fullmatch(r"agent-.+-ccw", name), name


def test_lint_values_render():
    assert render(yaml.safe_load(LINT_VALUES.read_text()))


def test_master_key_chart_secret_key_follows_existing_secret_key(env_chart):
    docs = render(
        values(postgres={"url": URL}, masterKey={"value": "mk", "existingSecretKey": "k"}),
        chart=env_chart,
    )
    assert by_kind(docs, "Secret")[f"{FULL}-master-key"]["stringData"] == {"k": "mk"}
    (probe,) = [d for d in docs if d["metadata"]["name"] == "env-probe"]
    env = yaml.safe_load(probe["data"]["env"])["env"]
    mk = next(e for e in env if e["name"] == "ORCH_MASTER_KEY")
    assert mk["valueFrom"]["secretKeyRef"] == {"name": f"{FULL}-master-key", "key": "k"}


CHAT_URL = "http://agents-matrix-connector-api-agents:8001"
COMPONENTS = ["api", "dispatcher", "ingester", "reconciler"]


def _containers(docs: list[dict]) -> dict[str, list[dict]]:
    """Every container (init containers included) of each Deployment, by Deployment name."""
    out = {}
    for name, dep in by_kind(docs, "Deployment").items():
        spec = dep["spec"]["template"]["spec"]
        out[name] = spec.get("initContainers", []) + spec["containers"]
    return out


def _env_names(container: dict) -> list[str]:
    return [e["name"] for e in container.get("env", [])]


CHAT_VALUES = {
    "inline": {"url": CHAT_URL, "token": "t"},
    "existing": {"url": CHAT_URL, "existingSecret": "chat-s", "existingSecretKey": "k"},
}


def _is_api_main(dep: str, container: dict) -> bool:
    return dep == f"{FULL}-api" and container["name"] == "api"


@pytest.mark.parametrize("chat", CHAT_VALUES.values(), ids=CHAT_VALUES.keys())
def test_configmap_has_no_chat_url(chat):
    cm = by_kind(render(values(postgres={"url": URL}, chat=chat)), "ConfigMap")[f"{FULL}-settings"]
    assert "ORCH_CHAT_URL" not in cm["data"]
    assert "ORCH_CHAT_TOKEN" not in cm["data"]


@pytest.mark.parametrize("chat", CHAT_VALUES.values(), ids=CHAT_VALUES.keys())
def test_chat_pair_rendered_together_per_container(chat):
    """Settings refuses a chat URL without its token, so no container may get one alone."""
    docs = render(values(postgres={"url": URL}, chat=chat))
    containers = _containers(docs)
    assert sorted(containers) == sorted(f"{FULL}-{c}" for c in COMPONENTS)
    for dep, items in containers.items():
        for c in items:
            names = _env_names(c)
            assert ("ORCH_CHAT_URL" in names) == ("ORCH_CHAT_TOKEN" in names), (dep, c["name"])
            assert ("ORCH_CHAT_URL" in names) == _is_api_main(dep, c), (dep, c["name"])


@pytest.mark.parametrize("chat", CHAT_VALUES.values(), ids=CHAT_VALUES.keys())
def test_api_container_has_chat_pair(chat):
    docs = render(values(postgres={"url": URL}, chat=chat))
    api = next(c for c in _containers(docs)[f"{FULL}-api"] if c["name"] == "api")
    env = {e["name"]: e for e in api["env"]}
    assert env["ORCH_CHAT_URL"] == {"name": "ORCH_CHAT_URL", "value": CHAT_URL}
    assert env["ORCH_CHAT_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": chat.get("existingSecret", f"{FULL}-chat"),
        "key": chat.get("existingSecretKey", "chat-token"),
    }


def test_chat_token_secret_on_api_only():
    tok = "".join(["marker", "chat", "value"])
    docs = render(values(postgres={"url": URL}, chat={"url": CHAT_URL, "token": tok}))
    assert by_kind(docs, "Secret")[f"{FULL}-chat"]["stringData"] == {"chat-token": tok}
    containers = _containers(docs)
    assert sorted(containers) == sorted(f"{FULL}-{c}" for c in COMPONENTS)
    for name, items in containers.items():
        for c in items:
            refs = [e for e in c.get("env", []) if e["name"] == "ORCH_CHAT_TOKEN"]
            if name == f"{FULL}-api" and c["name"] == "api":
                assert refs == [
                    {
                        "name": "ORCH_CHAT_TOKEN",
                        "valueFrom": {
                            "secretKeyRef": {"name": f"{FULL}-chat", "key": "chat-token"}
                        },
                    }
                ]
            else:
                assert refs == [], (name, c["name"])
    for cm in by_kind(docs, "ConfigMap").values():
        assert tok not in yaml.safe_dump(cm)

    def chat_sums(token: str) -> dict[str, str | None]:
        deps = by_kind(
            render(values(postgres={"url": URL}, chat={"url": CHAT_URL, "token": token})),
            "Deployment",
        )
        return {
            n: d["spec"]["template"]["metadata"]["annotations"].get("checksum/chat-secret")
            for n, d in deps.items()
        }

    one, two = chat_sums("t1"), chat_sums("t2")
    assert one[f"{FULL}-api"] and two[f"{FULL}-api"] and one[f"{FULL}-api"] != two[f"{FULL}-api"]
    assert {v for n, v in one.items() if n != f"{FULL}-api"} == {None}

    docs = render(
        values(
            postgres={"url": URL},
            chat={
                "url": CHAT_URL,
                "token": tok,
                "existingSecret": "chat-s",
                "existingSecretKey": "k",
            },
        )
    )
    assert f"{FULL}-chat" not in by_kind(docs, "Secret")
    api = next(c for c in _containers(docs)[f"{FULL}-api"] if c["name"] == "api")
    ref = next(e for e in api["env"] if e["name"] == "ORCH_CHAT_TOKEN")
    assert ref["valueFrom"]["secretKeyRef"] == {"name": "chat-s", "key": "k"}
    for name, items in _containers(docs).items():
        for c in items:
            if not (name == f"{FULL}-api" and c["name"] == "api"):
                assert "ORCH_CHAT_TOKEN" not in _env_names(c), (name, c["name"])
    deps = by_kind(docs, "Deployment")
    assert all(
        "checksum/chat-secret" not in d["spec"]["template"]["metadata"]["annotations"]
        for d in deps.values()
    )
    for cm in by_kind(docs, "ConfigMap").values():
        assert tok not in yaml.safe_dump(cm)


@pytest.mark.parametrize(
    "chat",
    [
        {"url": CHAT_URL},
        {"token": "t"},
        {"existingSecret": "chat-s"},
        {"url": CHAT_URL, "existingSecretKey": "k"},
    ],
)
def test_chat_url_and_token_together(chat):
    err = render_error(values(postgres={"url": URL}, chat=chat))
    assert "chat.url" in err and "chat.token" in err and "chat.existingSecret" in err


def test_chat_absent_by_default():
    docs = render(values(postgres={"url": URL}))
    assert "ORCH_CHAT_URL" not in by_kind(docs, "ConfigMap")[f"{FULL}-settings"]["data"]
    assert f"{FULL}-chat" not in by_kind(docs, "Secret")
    for items in _containers(docs).values():
        for c in items:
            assert "ORCH_CHAT_TOKEN" not in _env_names(c)
            assert "ORCH_CHAT_URL" not in _env_names(c)


ADMIN_UPSTREAMS = {
    "webhook": ("WEBHOOK", "http://agents-webhook-admin:8081"),
    "schedule": ("SCHEDULE", "http://agents-schedule-admin:8081"),
    "connector": ("CONNECTOR", "http://agents-matrix-connector-agents:8001/chat/v1/admin"),
}


def _admin_env(docs: list[dict]) -> dict[str, dict]:
    api = next(c for c in _containers(docs)[f"{FULL}-api"] if c["name"] == "api")
    return {e["name"]: e for e in api["env"]}


def test_admin_upstreams_off_by_default():
    docs = render(values(postgres={"url": URL}))
    for items in _containers(docs).values():
        for c in items:
            assert not [n for n in _env_names(c) if n.startswith(("ORCH_WEBHOOK", "ORCH_SCHEDULE"))]
            assert not [n for n in _env_names(c) if n.startswith("ORCH_CONNECTOR")]
    assert not [n for n in by_kind(docs, "Secret") if "-admin-" in n]


@pytest.mark.parametrize("name", ADMIN_UPSTREAMS)
def test_admin_upstream_pair_inline_token_on_api_main_only(name):
    prefix, url = ADMIN_UPSTREAMS[name]
    tok = "".join(["marker", name, "value"])
    docs = render(values(postgres={"url": URL}, admin={name: {"url": url, "token": tok}}))
    env = _admin_env(docs)
    assert env[f"ORCH_{prefix}_URL"] == {"name": f"ORCH_{prefix}_URL", "value": url}
    assert env[f"ORCH_{prefix}_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": f"{FULL}-admin-{name}",
        "key": "token",
    }
    assert by_kind(docs, "Secret")[f"{FULL}-admin-{name}"]["stringData"] == {"token": tok}
    for dep, items in _containers(docs).items():
        for c in items:
            if not _is_api_main(dep, c):
                assert f"ORCH_{prefix}_URL" not in _env_names(c)
                assert f"ORCH_{prefix}_TOKEN" not in _env_names(c)
    for cm in by_kind(docs, "ConfigMap").values():
        assert tok not in yaml.safe_dump(cm)
        assert url not in yaml.safe_dump(cm)


@pytest.mark.parametrize("name", ADMIN_UPSTREAMS)
def test_admin_upstream_existing_secret_wins(name):
    prefix, url = ADMIN_UPSTREAMS[name]
    admin = {name: {"url": url, "token": "t", "existingSecret": "mine", "existingSecretKey": "k"}}
    docs = render(values(postgres={"url": URL}, admin=admin))
    ref = _admin_env(docs)[f"ORCH_{prefix}_TOKEN"]["valueFrom"]["secretKeyRef"]
    assert ref == {"name": "mine", "key": "k"}
    assert f"{FULL}-admin-{name}" not in by_kind(docs, "Secret")


@pytest.mark.parametrize("name", ADMIN_UPSTREAMS)
@pytest.mark.parametrize("half", ["url", "token", "existingSecret"])
def test_admin_upstream_half_pair_fails_render(name, half):
    _, url = ADMIN_UPSTREAMS[name]
    err = render_error(
        values(postgres={"url": URL}, admin={name: {half: url if half == "url" else "x"}})
    )
    assert f"admin.{name}" in err


def test_self_restart_cooldown_default_and_override():
    cm = by_kind(render(values(postgres={"url": URL})), "ConfigMap")[f"{FULL}-settings"]
    assert cm["data"]["ORCH_SELF_RESTART_COOLDOWN_SECONDS"] == "600"
    docs = render(values(postgres={"url": URL}, selfRestartCooldownSeconds=90))
    cm = by_kind(docs, "ConfigMap")[f"{FULL}-settings"]
    assert cm["data"]["ORCH_SELF_RESTART_COOLDOWN_SECONDS"] == "90"
