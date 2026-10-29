"""Renderer: golden objects, config validation against the wrapper, the onStart script."""

from __future__ import annotations

import copy
import json
import os
import stat
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
import yaml
from claude_code_wrapper.config import load_config

from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s import labels, on_start, render
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import SessionTokens

GOLDEN = Path(__file__).parent / "golden"
TOKENS = SessionTokens(auth="a" * 32, peer="p" * 32, agent="g" * 32)
AGENT_ENV = ["ORCH_AGENT_TOKEN", "ORCH_AGENT_URL", "ORCH_SESSION_ID", "POD_NAME"]
AGENT_URL = "http://orchestrator-agents.agents.svc:8081"
IMAGE = "harbor.example/containers/claude-code-wrapper:ca2ddf3"


def make_settings(**over) -> Settings:
    base = {
        "database_url": "postgresql://u:p@db/orch",
        "master_key": "m" * 32,
        "agent_image": IMAGE,
        "default_model": "sonnet",
        "models": ["sonnet", "claude-opus-5-5"],
    }
    base.update(over)
    return Settings(**base)


def make_session(slug="alpha", **spec) -> Session:
    return Session(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        slug=slug,
        spec=SessionSpec.model_validate(spec),
        generation=1,
        observed_generation=0,
        desired="running",
        status={},
        created_at=datetime(2026, 10, 23, tzinfo=UTC),
    )


def by_kind(objs):
    return {o["kind"]: o for o in objs}


def config_of(objs) -> dict:
    return yaml.safe_load(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])


def sts_of(objs) -> dict:
    return by_kind(objs)["StatefulSet"]


def container_of(objs) -> dict:
    return sts_of(objs)["spec"]["template"]["spec"]["containers"][0]


def golden(name: str, objs: list[dict]) -> None:
    path = GOLDEN / name
    if os.environ.get("UPDATE_GOLDEN"):
        path.write_text(yaml.safe_dump(objs, sort_keys=False))
    assert yaml.safe_load(path.read_text()) == objs


def test_golden_two_replicas_with_peers():
    session = make_session(
        replicas=2,
        env={"GITHUB_TOKEN": "x", "AAA": "1"},
        resources={"requests": {"cpu": "500m"}, "limits": {"memory": "2Gi"}},
        any_mcp_config={"servers": {}},
        pvc_size="5Gi",
        model="opus",
        effort="high",
    )
    golden("session_two_peers.yaml", render.render_session(session, make_settings(), TOKENS))


def test_golden_single_without_peers():
    session = make_session(slug="solo", replicas=1, peers=False)
    objs = render.render_session(session, make_settings(), TOKENS)
    assert "peers" not in config_of(objs)
    golden("session_single.yaml", objs)


def test_rendered_ccw_config_validates(tmp_path, monkeypatch):
    cases = [
        (True, None),
        (False, None),
        (True, ["*"]),
        (False, ["_x", "my-plugin:do_it"]),
    ]
    for peers, agent_commands in cases:
        wrapper = None if agent_commands is None else {"agent_commands": agent_commands}
        objs = render.render_session(
            make_session(replicas=3, peers=peers, env={"FOO": "bar"}, wrapper=wrapper),
            make_settings(),
            TOKENS,
        )
        monkeypatch.setenv("CCW_AUTH_TOKEN", TOKENS.auth)
        monkeypatch.setenv("CCW_PEER_TOKEN", TOKENS.peer)
        monkeypatch.setenv("CCW_PEER_URL", "http://agent-alpha-0.agent-alpha.agents.svc:8080")
        monkeypatch.setenv("CLAUDE_CODE_OAUTH_TOKEN", "oauth")
        monkeypatch.setenv("POD_NAME", "agent-alpha-0")
        path = tmp_path / "config.yaml"
        path.write_text(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])
        cfg = load_config(path)
        assert cfg.hooks.onStart
        assert cfg.peers.enabled is peers
        assert cfg.claude.env.claude_autocompact_pct_override == 95
        assert cfg.sessions.commands.client == ["*"]
        assert cfg.sessions.commands.agent == (agent_commands or [])


def test_commands_client_wildcard_rendered():
    for wrapper in (None, {"system_append": "Be terse."}):
        session = make_session(wrapper=wrapper)
        cfg = config_of(render.render_session(session, make_settings(), TOKENS))
        assert cfg["sessions"]["commands"] == {"client": ["*"]}
    for names in (["compact", "context"], ["*"], ["_x"]):
        session = make_session(wrapper={"agent_commands": names})
        cfg = config_of(render.render_session(session, make_settings(), TOKENS))
        assert cfg["sessions"]["commands"] == {"client": ["*"], "agent": names}


def test_all_objects_labelled():
    objs = render.render_session(make_session(), make_settings(), TOKENS)
    assert [o["kind"] for o in objs] == [
        "PersistentVolumeClaim",
        "Secret",
        "ConfigMap",
        "Service",
        "StatefulSet",
    ]
    for o in objs:
        assert o["metadata"]["labels"]["agents.szymonrichert.pl/session"] == "alpha"
        assert o["metadata"]["labels"]["app.kubernetes.io/managed-by"] == "agents-orchestrator"


def test_sessions_subpathexpr_per_pod():
    mounts = {
        m["mountPath"]: m
        for m in container_of(
            render.render_session(make_session(workdir="proj"), make_settings(), TOKENS)
        )["volumeMounts"]
    }
    assert mounts["/sessions"]["subPathExpr"] == "sessions/$(POD_NAME)"
    assert mounts["/workdir"]["subPath"] == "workdir"
    assert "subPathExpr" not in mounts["/workdir"]
    assert mounts["/workdir"]["name"] == mounts["/sessions"]["name"]


def test_peer_static_urls_cover_every_ordinal():
    cfg = config_of(render.render_session(make_session(replicas=3), make_settings(), TOKENS))
    assert cfg["peers"]["enabled"] is True
    assert cfg["peers"]["name"] == "${POD_NAME}"
    assert cfg["peers"]["advertise_url"] == "${CCW_PEER_URL}"
    assert cfg["peers"]["token"] == "${CCW_PEER_TOKEN}"
    assert cfg["peers"]["static"] == [
        f"http://agent-alpha-{i}.agent-alpha.agents.svc.cluster.local:8080" for i in range(3)
    ]


def test_pull_secret_and_oauth_secret_ref():
    pod = sts_of(render.render_session(make_session(), make_settings(), TOKENS))["spec"][
        "template"
    ]["spec"]
    assert pod["imagePullSecrets"] == [{"name": "regcred"}]
    env = {e["name"]: e for e in pod["containers"][0]["env"]}
    assert env["CLAUDE_CODE_OAUTH_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "agents-orchestrator-claude",
        "key": "CLAUDE_CODE_OAUTH_TOKEN",
    }
    assert env["CCW_AUTH_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "agent-alpha-ccw",
        "key": "CCW_AUTH_TOKEN",
    }
    assert env["POD_NAME"]["valueFrom"]["fieldRef"]["fieldPath"] == "metadata.name"
    assert env["CCW_PEER_URL"]["value"] == (
        "http://$(POD_NAME).agent-alpha.agents.svc.cluster.local:8080"
    )
    empty = sts_of(
        render.render_session(make_session(), make_settings(image_pull_secret=""), TOKENS)
    )
    assert "imagePullSecrets" not in empty["spec"]["template"]["spec"]
    custom = sts_of(
        render.render_session(make_session(), make_settings(claude_secret_name="mine"), TOKENS)
    )
    env = {e["name"]: e for e in custom["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["CLAUDE_CODE_OAUTH_TOKEN"]["valueFrom"]["secretKeyRef"]["name"] == "mine"


def test_any_mcp_server_entry():
    objs = render.render_session(make_session(), make_settings(), TOKENS)
    assert config_of(objs)["claude"]["mcp_servers"]["any-mcp"] == {
        "type": "stdio",
        "command": "/opt/any-mcp/bin/any-mcp",
        "args": ["--config", "/opt/any-mcp/etc/any-mcp.yaml"],
    }
    pod = sts_of(objs)["spec"]["template"]["spec"]
    vols = {v["name"]: v for v in pod["volumes"]}
    mount = next(
        m for m in pod["containers"][0]["volumeMounts"] if m["mountPath"] == "/opt/any-mcp"
    )
    assert vols[mount["name"]]["emptyDir"] == {}
    mise = next(m for m in pod["containers"][0]["volumeMounts"] if m["mountPath"] == "/opt/mise")
    assert vols[mise["name"]]["emptyDir"] == {}


def test_config_hash_rolls_pods():
    def hash_of(session, **kw):
        objs = render.render_session(session, make_settings(**kw), TOKENS)
        return sts_of(objs)["spec"]["template"]["metadata"]["annotations"][
            "agents.szymonrichert.pl/config-hash"
        ]

    base = hash_of(make_session())
    assert hash_of(make_session()) == base
    assert hash_of(make_session(any_mcp_config={"a": 1})) != base
    assert hash_of(make_session(model="opus")) != base
    assert hash_of(make_session(), any_mcp_ref="v9.9.9") != base


def test_config_hash_follows_plugin_sources():
    def hash_of(session, **kw):
        settings = make_settings(agent_api_url=AGENT_URL, **kw)
        objs = render.render_session(session, settings, TOKENS)
        return sts_of(objs)["spec"]["template"]["metadata"]["annotations"][render.HASH_ANNOTATION]

    base = hash_of(make_session())
    assert hash_of(make_session()) == base
    assert hash_of(make_session(), any_mcp_plugins_ref="v9.9.9") != base
    assert hash_of(make_session(), any_mcp_plugins_repo="https://git.example/a/b") != base
    tools = hash_of(make_session(any_mcp_sources=["github.com/acme/tools"]))
    assert tools != base
    assert hash_of(make_session(any_mcp_sources=["github.com/acme/tools?ref=v2"])) != tools


def test_all_objects_in_namespace():
    objs = render.render_session(
        make_session(replicas=2), make_settings(namespace="ns1", cluster_domain="k.local"), TOKENS
    )
    for o in objs:
        assert o["metadata"]["namespace"] == "ns1"
    cfg = config_of(objs)
    assert all(".agent-alpha.ns1.svc.k.local:8080" in u for u in cfg["peers"]["static"])
    env = {e["name"]: e for e in container_of(objs)["env"]}
    assert ".agent-alpha.ns1.svc.k.local:8080" in env["CCW_PEER_URL"]["value"]
    assert sts_of(objs)["spec"]["serviceName"] == "agent-alpha"


def test_passthrough_env_lists_spec_env_names(tmp_path, monkeypatch):
    objs = render.render_session(
        make_session(env={"ZED": "1", "ALPHA": "2"}), make_settings(), TOKENS
    )
    cfg = config_of(objs)
    assert cfg["claude"]["passthrough_env"] == sorted(["ALPHA", "ZED", *AGENT_ENV])
    env = {e["name"]: e.get("value") for e in container_of(objs)["env"]}
    assert env["ZED"] == "1" and env["ALPHA"] == "2"
    for k, v in {
        "CCW_AUTH_TOKEN": "t" * 20,
        "CCW_PEER_TOKEN": "q" * 20,
        "CCW_PEER_URL": "http://x:1",
        "CLAUDE_CODE_OAUTH_TOKEN": "o",
        "POD_NAME": "p",
        "ORCH_AGENT_TOKEN": "g" * 20,
        "ORCH_AGENT_URL": "http://o:1",
        "ORCH_SESSION_ID": "c",
    }.items():
        monkeypatch.setenv(k, v)
    path = tmp_path / "c.yaml"
    path.write_text(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])
    assert load_config(path).claude.passthrough_env == sorted(["ALPHA", "ZED", *AGENT_ENV])


def test_workdir_path_matches_mount_and_session_workdir():
    spec = SessionSpec(workdir="proj")
    assert render.workdir_path(spec) == "/workdir/proj"
    mount = render.workdir_mount(spec)
    assert render.workdir_path(spec).startswith(mount["mountPath"] + "/")
    assert render.workdir_path(spec).removeprefix(mount["mountPath"] + "/") == spec.workdir
    objs = render.render_session(make_session(workdir="proj"), make_settings(), TOKENS)
    assert mount in container_of(objs)["volumeMounts"]
    assert mount["subPath"] == "workdir"


def test_every_object_name_uses_reserved_prefix():
    objs = render.render_session(make_session(slug="beta"), make_settings(), TOKENS)
    assert all(o["metadata"]["name"].startswith("agent-beta") for o in objs)
    names = labels.object_names("beta")
    assert set(names.values()) == {o["metadata"]["name"] for o in objs}
    assert labels.session_labels("beta") == {
        "agents.szymonrichert.pl/session": "beta",
        "app.kubernetes.io/managed-by": "agents-orchestrator",
    }
    assert labels.selector("beta") == {"matchLabels": labels.session_labels("beta")}


def test_on_start_hook_and_startup_probe():
    objs = render.render_session(
        make_session(), make_settings(on_start_timeout_seconds=600), TOKENS
    )
    assert config_of(objs)["hooks"]["onStart"] == [
        {
            "type": "command",
            "run": "sh /etc/ccw/on-start.sh",
            "blocking": True,
            "on_error": "allow",
            "timeout_seconds": 600,
        }
    ]
    assert "on-start.sh" in by_kind(objs)["ConfigMap"]["data"]
    c = container_of(objs)
    assert c["startupProbe"]["httpGet"]["path"] == "/healthz"
    assert c["startupProbe"]["periodSeconds"] == 5
    assert c["startupProbe"]["failureThreshold"] == 600 // 5 + 12
    assert c["livenessProbe"]["httpGet"]["path"] == "/healthz"
    assert c["readinessProbe"]["httpGet"]["path"] == "/readyz"
    odd = container_of(
        render.render_session(make_session(), make_settings(on_start_timeout_seconds=601), TOKENS)
    )
    assert odd["startupProbe"]["failureThreshold"] == 121 + 12


def _stub_bin(tmp_path: Path) -> tuple[Path, Path]:
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.log"
    root = tmp_path / "any-mcp"
    # the fake any-mcp logs its argv; `plugins install --config F` also logs F's content
    (tmp_path / "any-mcp.stub").write_text(
        f"""#!/bin/sh
echo "any-mcp $*" >> {log}
if [ "$1 $2 $3" = "plugins install --config" ]; then
  cat "$4" >> {log}
  echo >> {log}
fi
"""
    )
    python3 = bin_dir / "python3"
    python3.write_text(
        f"""#!/bin/sh
echo "python3 $*" >> {log}
if [ "$1 $2" = "-m venv" ]; then
  mkdir -p "$3/bin"
  printf '#!/bin/sh\\necho "pip $*" >> {log}\\n' > "$3/bin/pip"
  cp {tmp_path / "any-mcp.stub"} "$3/bin/any-mcp"
  chmod +x "$3/bin/pip" "$3/bin/any-mcp"
fi
"""
    )
    python3.chmod(python3.stat().st_mode | stat.S_IEXEC)
    return log, root


def _run(script: str, root: Path, tmp_path: Path) -> None:
    path = tmp_path / "on-start.sh"
    path.write_text(script.replace("/opt/any-mcp", str(root)))
    env = {"PATH": f"{tmp_path / 'bin'}:/usr/bin:/bin"}
    subprocess.run(["sh", str(path)], check=True, env=env, capture_output=True)


def test_on_start_script_installs_once_and_writes_json_config(tmp_path):
    settings = make_settings(any_mcp_ref="v0.1.0", any_mcp_repo="https://example.com/any-mcp")
    spec = SessionSpec(any_mcp_config={"name": "it's $HOME `x`", "n": [1, 2]})
    script = on_start.render_on_start(spec, settings)
    assert script.startswith("#!/bin/sh\n")
    assert "set -eu" in script
    log, root = _stub_bin(tmp_path)
    _run(script, root, tmp_path)
    calls = log.read_text()
    assert "pip install --no-cache-dir any-mcp @ git+https://example.com/any-mcp@v0.1.0" in calls
    assert calls.count("pip install") == 1
    assert "any-mcp --help" in calls
    assert (root / ".ref").read_text().strip() == "v0.1.0"
    assert json.loads((root / "etc" / "any-mcp.yaml").read_text()) == spec.any_mcp_config

    log.write_text("")
    spec2 = SessionSpec(any_mcp_config={"changed": True})
    _run(on_start.render_on_start(spec2, settings), root, tmp_path)
    calls = log.read_text()
    assert "pip install" not in calls and "-m venv" not in calls
    assert "any-mcp --help" in calls
    assert json.loads((root / "etc" / "any-mcp.yaml").read_text()) == {"changed": True}

    log.write_text("")
    _run(on_start.render_on_start(spec2, make_settings(any_mcp_ref="v0.2.0")), root, tmp_path)
    assert log.read_text().count("pip install") == 1
    assert (root / ".ref").read_text().strip() == "v0.2.0"


def test_on_start_installs_plugin_sources(tmp_path):
    """D7: after writing the config and before --help, on every run, not only on a new ref."""
    settings = make_settings(agent_api_url=AGENT_URL)
    spec = SessionSpec(replicas=2, any_mcp_sources=["github.com/acme/tools"])
    script = on_start.render_on_start(spec, settings, "alpha")
    install = "/opt/any-mcp/bin/any-mcp plugins install --config /opt/any-mcp/etc/any-mcp.yaml\n"
    assert script.count(install) == 1
    heredoc_end = script.index("\nANY_MCP_CONFIG_EOF\n")
    assert heredoc_end < script.index(install) < script.index("/opt/any-mcp/bin/any-mcp --help")

    log, root = _stub_bin(tmp_path)
    config = root / "etc" / "any-mcp.yaml"
    for run in range(2):
        log.write_text("")
        _run(script, root, tmp_path)
        lines = log.read_text().splitlines()
        assert ("pip install" in log.read_text()) is (run == 0)
        at = lines.index(f"any-mcp plugins install --config {config}")
        installed = json.loads("\n".join(lines[at + 1 : lines.index("any-mcp --help")]))
        assert installed == json.loads(config.read_text())
        assert {"name": "tools", "source": "github.com/acme/tools"} in installed["plugins"]
        assert lines[-1] == "any-mcp --help"


def test_on_start_config_sources(tmp_path):
    f = tmp_path / "mcp.yaml"
    f.write_text("a: 1\n")
    settings = make_settings(any_mcp_config_file=str(f))
    assert '"a": 1' in on_start.render_on_start(SessionSpec(), settings)
    spec = SessionSpec(any_mcp_config={"b": 2})
    assert '"b": 2' in on_start.render_on_start(spec, settings)
    assert "\n{}\n" in on_start.render_on_start(SessionSpec(), make_settings())


def test_on_start_refuses_delimiter_in_config(monkeypatch):
    monkeypatch.setattr(on_start, "DELIMITER", "}")
    with pytest.raises(ValueError, match="delimiter"):
        on_start.render_on_start(SessionSpec(any_mcp_config={"a": 1}), make_settings())


def test_ask_user_question_disabled():
    cfg = config_of(render.render_session(make_session(), make_settings(), TOKENS))
    assert cfg["sessions"]["ask_user_question"] == "deny"
    assert cfg["claude"]["permissions"]["deny"] == [
        "EnterPlanMode",
        "ExitPlanMode",
        "AskUserQuestion",
    ]
    assert cfg["sessions"]["max_sessions"] == 1
    assert cfg["sessions"]["resume_on_start"] is True
    assert cfg["sessions"]["idle_timeout_seconds"] == 0


def test_system_append_asks_in_writing():
    cfg = config_of(render.render_session(make_session(), make_settings(), TOKENS))
    text = cfg["prompts"]["system_append"]
    assert text == render.ASK_IN_WRITING_PROMPT
    low = text.lower()
    assert "numbered list" in low
    assert "recommended answer" in low
    assert '"yes"' in text
    assert "askuserquestion" in low


def test_shutdown_below_grace_and_resources():
    objs = render.render_session(
        make_session(resources={"limits": {"cpu": "1"}}),
        make_settings(agent_termination_grace_seconds=90),
        TOKENS,
    )
    pod = sts_of(objs)["spec"]["template"]["spec"]
    assert pod["terminationGracePeriodSeconds"] == 90
    assert config_of(objs)["limits"]["shutdown_timeout_seconds"] < 90
    assert pod["containers"][0]["resources"] == {"limits": {"cpu": "1"}}
    assert pod["securityContext"] == {"runAsUser": 1000, "fsGroup": 1000}


def test_render_is_pure():
    session = make_session(replicas=2)
    a = render.render_session(session, make_settings(), TOKENS)
    assert render.render_session(session, make_settings(), TOKENS) == copy.deepcopy(a)


def test_render_keeps_spec_resources_unchanged():
    c = container_of(render.render_session(make_session(), make_settings(), TOKENS))
    assert "resources" not in c
    spec = {"resources": {"requests": {"cpu": "2"}, "limits": {"memory": "2Gi"}}}
    c = container_of(render.render_session(make_session(**spec), make_settings(), TOKENS))
    assert c["resources"] == {"requests": {"cpu": "2"}, "limits": {"memory": "2Gi"}}


def test_agent_credential_env():
    session = make_session()
    objs = render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS)
    assert by_kind(objs)["Secret"]["stringData"]["ORCH_AGENT_TOKEN"] == TOKENS.agent
    env = {e["name"]: e for e in container_of(objs)["env"]}
    assert env["ORCH_AGENT_TOKEN"]["valueFrom"]["secretKeyRef"] == {
        "name": "agent-alpha-ccw",
        "key": "ORCH_AGENT_TOKEN",
    }
    assert env["ORCH_SESSION_ID"]["value"] == str(session.id)
    assert env["ORCH_AGENT_URL"]["value"] == AGENT_URL


def test_agent_env_session_id():
    """Agent pods get ORCH_SESSION_ID, pass it to any-mcp, and send it as X-Orch-Session."""
    session = make_session(replicas=2)
    objs = _render(session)
    env = {e["name"]: e for e in container_of(objs)["env"]}
    assert {k for k in env if k.startswith("ORCH_")} == {
        "ORCH_AGENT_TOKEN",
        "ORCH_AGENT_URL",
        "ORCH_SESSION_ID",
    }
    assert env["ORCH_SESSION_ID"] == {"name": "ORCH_SESSION_ID", "value": str(session.id)}
    cfg = config_of(objs)
    assert cfg["claude"]["passthrough_env"] == AGENT_ENV
    assert cfg["hooks"]["onAgentToolCall"][0]["headers"] == {
        "Authorization": "Bearer ${ORCH_AGENT_TOKEN}",
        "X-Orch-Session": "${ORCH_SESSION_ID}",
    }


def test_passthrough_includes_agent_env():
    objs = render.render_session(
        make_session(env={"ZED": "1"}), make_settings(agent_api_url=AGENT_URL), TOKENS
    )
    assert config_of(objs)["claude"]["passthrough_env"] == sorted(["ZED", *AGENT_ENV])
    solo = render.render_session(
        make_session(replicas=1), make_settings(agent_api_url=AGENT_URL), TOKENS
    )
    assert "ORCH_AGENT_TOKEN" in config_of(solo)["claude"]["passthrough_env"]


def test_no_agent_url_omits_env():
    objs = render.render_session(make_session(), make_settings(), TOKENS)
    names = [e["name"] for e in container_of(objs)["env"]]
    assert "ORCH_AGENT_URL" not in names
    assert "ORCH_AGENT_TOKEN" in names and "ORCH_SESSION_ID" in names
    assert "ORCH_AGENT_URL" in config_of(objs)["claude"]["passthrough_env"]


def test_agent_token_trust_domain_documented():
    """The token is plain pod env, readable by every process in the pod (Bash included)."""
    objs = render.render_session(make_session(), make_settings(agent_api_url=AGENT_URL), TOKENS)
    env = {e["name"]: e for e in container_of(objs)["env"]}
    assert "ORCH_AGENT_TOKEN" in env
    assert "ORCH_AGENT_TOKEN" in config_of(objs)["claude"]["passthrough_env"]
    readme = (Path(__file__).parent.parent / "README.md").read_text().lower()
    assert "best-effort" in readme and "orch_agent_token" in readme


def _render(session, **settings):
    return render.render_session(
        session, make_settings(agent_api_url=AGENT_URL, **settings), TOKENS
    )


def test_peer_seeds_from_cap():
    cfg = config_of(_render(make_session(replicas=1, max_replicas=3)))
    assert cfg["peers"]["static"] == [
        f"http://agent-alpha-{i}.agent-alpha.agents.svc.cluster.local:8080" for i in range(3)
    ]
    assert cfg["peers"]["name"] == "${POD_NAME}"
    assert cfg["peers"]["advertise_url"] == "${CCW_PEER_URL}"


def test_alone_session_no_hive_config():
    cfg = config_of(_render(make_session(replicas=1, static=True)))
    assert not cfg.get("peers", {}).get("enabled")
    assert cfg["notes"]["enabled"] is False
    assert "onAgentToolCall" not in cfg["hooks"]
    cfg = config_of(_render(make_session(replicas=3, max_replicas=3, peers=False)))
    assert not cfg.get("peers", {}).get("enabled")
    assert cfg["notes"]["enabled"] is True


def test_hive_session_config():
    cfg = config_of(_render(make_session(replicas=2)))
    assert cfg["peers"]["enabled"] is True
    assert cfg["notes"]["enabled"] is True
    assert cfg["hooks"]["onAgentToolCall"] == [
        {
            "type": "webhook",
            "url": "${ORCH_AGENT_URL}/agent/v1/locks/check",
            "headers": {
                "Authorization": "Bearer ${ORCH_AGENT_TOKEN}",
                "X-Orch-Session": "${ORCH_SESSION_ID}",
            },
            "matcher": "^(Write|Edit|MultiEdit|NotebookEdit)$",
            "blocking": True,
            "on_error": "allow",
            "timeout_seconds": 2,
            "retries": 0,
        }
    ]
    no_url = render.render_session(make_session(replicas=2), make_settings(), TOKENS)
    assert "onAgentToolCall" not in config_of(no_url)["hooks"]


def test_main_agent_only_tools_rendered():
    for session in (make_session(replicas=1, static=True), make_session(replicas=3)):
        cfg = config_of(_render(session))
        assert cfg["claude"]["main_agent_only_tools"] == ["mcp__any-mcp__orchestrator_*"]


@pytest.mark.parametrize(
    "low,high",
    [
        ({"replicas": 1}, {"replicas": 3}),
        ({"replicas": 2}, {"replicas": 4}),
        ({"replicas": 2, "init_scripts": ["echo a"]}, {"replicas": 2, "init_scripts": ["b", "c"]}),
    ],
)
def test_runtime_fields_change_only_statefulset_replicas(low, high):
    a = _render(make_session(max_replicas=4, **low))
    b = _render(make_session(max_replicas=4, **high))
    assert sts_of(a)["spec"]["replicas"] == low["replicas"]
    assert sts_of(b)["spec"]["replicas"] == high["replicas"]
    for objs in (a, b):
        sts_of(objs)["spec"]["replicas"] = 0
    assert a == b


def test_init_scripts_never_rendered():
    a = _render(make_session(init_scripts=["echo a"]))
    b = _render(make_session(init_scripts=["echo b", "echo c"]))
    c = _render(make_session())
    assert a == b == c


def _on_start_plugins(session) -> list[str]:
    objs = render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS)
    script = by_kind(objs)["ConfigMap"]["data"]["on-start.sh"]
    body = script.split("<<'ANY_MCP_CONFIG_EOF'\n")[1].split("\nANY_MCP_CONFIG_EOF")[0]
    return [p["name"] for p in json.loads(body).get("plugins", [])]


def test_on_start_orchestrator_entry_for_every_session():
    """D11: the session tools are rendered for every session, alone sessions included."""
    assert _on_start_plugins(make_session(replicas=2)) == ["orchestrator"]
    assert _on_start_plugins(make_session(replicas=1)) == ["orchestrator"]
    assert _on_start_plugins(make_session(replicas=1, static=True)) == ["orchestrator"]


def _on_start_body(session) -> tuple[str, dict]:
    objs = render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS)
    script = by_kind(objs)["ConfigMap"]["data"]["on-start.sh"]
    body = script.split("<<'ANY_MCP_CONFIG_EOF'\n")[1].split("\nANY_MCP_CONFIG_EOF")[0]
    return script, json.loads(body)


def _allow(session) -> list[str]:
    plugins = _on_start_body(session)[1].get("plugins", [])
    return plugins[0]["allow"] if plugins else []


@pytest.mark.parametrize(
    "spec,scalable",
    [
        ({"replicas": 1}, False),
        ({"replicas": 1, "static": True}, False),
        ({"replicas": 3, "static": True}, False),
        ({"replicas": 1, "max_replicas": 4}, True),
        ({"replicas": 4, "max_replicas": 4}, True),
    ],
)
def test_scale_tool_allowed_only_when_scalable(spec, scalable):
    assert ("scale" in _allow(make_session(**spec))) is scalable


def test_scale_allow_independent_of_replicas():
    low = _on_start_body(make_session(replicas=1, max_replicas=4))
    high = _on_start_body(make_session(replicas=4, max_replicas=4))
    assert low == high
    assert "scale" in low[1]["plugins"][0]["allow"]


FULL_WRAPPER = {
    "hooks": {
        "onStart": [{"type": "command", "run": "echo user-start"}],
        "onAgentToolCall": [{"type": "command", "run": "echo user-tool", "matcher": "^Bash$"}],
        "onStop": [{"type": "command", "run": "echo stop $HOME", "env": {"FOO": "x"}}],
        "onTick": [{"type": "webhook", "url": "https://h.example/t", "interval_seconds": 30}],
    },
    "auto_compact": {"idle_seconds": 120, "instructions": "keep notes", "min_context_tokens": 1000},
    "system_append": "Be terse.",
    "permissions": {"allow": ["Bash(ls:*)"], "deny": ["ExitPlanMode", "WebFetch"]},
    "mcp_servers": {
        "docs": {"type": "http", "url": "https://mcp.example/x", "headers": {"X-A": "b"}},
        "loc": {"type": "stdio", "command": "/bin/true", "args": ["-x"], "env": {"A": "b"}},
    },
    "plugins": {
        "marketplaces": [{"name": "mine", "source": "github:me/market"}],
        "install": [{"plugin": "tool@mine", "scope": "user", "required": False}],
    },
    "turn_stall_seconds": 90,
    "max_restarts": 3,
    "restart_continue_prompt": "continue please",
    "turn_max_seconds": 600,
    "agent_commands": ["compact", "context"],
}


def wrapper_cfg(wrapper, **spec) -> dict:
    session = make_session(wrapper=wrapper, max_replicas=2, replicas=2, **spec)
    return config_of(render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS))


def test_wrapper_settings_keep_reserved_entries():
    cfg = wrapper_cfg(FULL_WRAPPER)
    start = cfg["hooks"]["onStart"]
    assert start[0]["run"] == "sh /etc/ccw/on-start.sh"
    assert [h["run"] for h in start[1:]] == ["echo user-start"]
    tool = cfg["hooks"]["onAgentToolCall"]
    assert tool[0]["url"].endswith("/agent/v1/locks/check")
    assert tool[1]["run"] == "echo user-tool"
    assert cfg["claude"]["permissions"]["deny"] == [
        "EnterPlanMode",
        "ExitPlanMode",
        "AskUserQuestion",
        "WebFetch",
    ]
    assert list(cfg["claude"]["mcp_servers"]) == ["any-mcp", "docs", "loc"]
    assert cfg["prompts"]["system_append"] == render.ASK_IN_WRITING_PROMPT + "\n\nBe terse."


def test_user_strings_escaped_against_interpolation():
    from claude_code_wrapper.interpolation import interpolate

    lit = "${CLAUDE_CODE_OAUTH_TOKEN}"
    wrapper = {
        "hooks": {
            "onStop": [
                {"type": "webhook", "url": f"https://h.example/{lit}", "headers": {"X": lit}},
                {"type": "command", "run": f"echo {lit} $HOME"},
            ]
        },
        "auto_compact": {"idle_seconds": 10, "instructions": lit},
        "system_append": lit,
    }
    cfg = wrapper_cfg(wrapper)
    hooks = cfg["hooks"]["onStop"]
    assert hooks[0]["url"] == "https://h.example/$${CLAUDE_CODE_OAUTH_TOKEN}"
    assert hooks[0]["headers"]["X"] == "$${CLAUDE_CODE_OAUTH_TOKEN}"
    assert hooks[1]["run"] == "echo $${CLAUDE_CODE_OAUTH_TOKEN} $$HOME"
    assert cfg["sessions"]["auto_compact"]["instructions"] == "$${CLAUDE_CODE_OAUTH_TOKEN}"
    env = {"CLAUDE_CODE_OAUTH_TOKEN": "secret"}
    out = interpolate(cfg["hooks"]["onStop"], env)
    assert out[0]["url"] == f"https://h.example/{lit}"
    assert out[0]["headers"]["X"] == lit
    assert out[1]["run"] == f"echo {lit} $HOME"
    assert interpolate(cfg["sessions"]["auto_compact"], env)["instructions"] == lit
    assert interpolate(cfg["prompts"]["system_append"], env).endswith("\n\n" + lit)
    # rendered orchestrator values stay interpolable
    assert cfg["server"]["auth_token"] == "${CCW_AUTH_TOKEN}"
    assert cfg["hooks"]["onAgentToolCall"][0]["headers"]["Authorization"] == (
        "Bearer ${ORCH_AGENT_TOKEN}"
    )


def test_wrapper_settings_mapped():
    cfg = wrapper_cfg(FULL_WRAPPER)
    s = cfg["sessions"]
    assert s["auto_compact"] == FULL_WRAPPER["auto_compact"]
    assert s["turn_stall_seconds"] == 90
    assert s["max_restarts"] == 3
    assert s["restart_continue_prompt"] == "continue please"
    assert s["commands"] == {"client": ["*"], "agent": ["compact", "context"]}
    assert (s["max_sessions"], s["resume_on_start"], s["idle_timeout_seconds"]) == (1, True, 0)
    assert s["ask_user_question"] == "deny"
    assert cfg["limits"]["turn_max_seconds"] == 600
    assert cfg["plugins"]["install"][0]["plugin"] == "tool@mine"
    assert cfg["plugins"]["marketplaces"][0]["name"] == "mine"
    assert cfg["claude"]["permissions"]["allow"] == ["Bash(ls:*)"]
    assert cfg["hooks"]["onStop"][0]["run"] == "echo stop $$HOME"
    assert cfg["hooks"]["onTick"][0]["interval_seconds"] == 30
    assert "docs" in cfg["claude"]["mcp_servers"]


def test_rendered_wrapper_settings_validate(tmp_path, monkeypatch):
    session = make_session(wrapper=FULL_WRAPPER, max_replicas=2, replicas=2)
    objs = render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS)
    for k, v in {
        "CCW_AUTH_TOKEN": TOKENS.auth,
        "CCW_PEER_TOKEN": TOKENS.peer,
        "CCW_PEER_URL": "http://agent-alpha-0.agent-alpha.agents.svc:8080",
        "CLAUDE_CODE_OAUTH_TOKEN": "oauth",
        "POD_NAME": "agent-alpha-0",
        "ORCH_AGENT_TOKEN": "t",
        "ORCH_AGENT_URL": AGENT_URL,
        "ORCH_SESSION_ID": "c",
    }.items():
        monkeypatch.setenv(k, v)
    path = tmp_path / "config.yaml"
    path.write_text(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])
    cfg = load_config(path)
    assert cfg.sessions.auto_compact.idle_seconds == 120
    assert cfg.hooks.onStart[1].run == "echo user-start"


def test_no_wrapper_block_renders_unchanged():
    cfg = config_of(render.render_session(make_session(), make_settings(), TOKENS))
    assert cfg["hooks"] == {"onStart": [cfg["hooks"]["onStart"][0]]}
    assert cfg["sessions"] == {
        "max_sessions": 1,
        "resume_on_start": True,
        "idle_timeout_seconds": 0,
        "ask_user_question": "deny",
        "restart_continue_prompt": "Claude was restarted. Continue the task where you left off.",
        "commands": {"client": ["*"]},
    }
    assert cfg["claude"]["permissions"] == {
        "deny": ["EnterPlanMode", "ExitPlanMode", "AskUserQuestion"]
    }
    assert list(cfg["claude"]["mcp_servers"]) == ["any-mcp"]
    assert cfg["prompts"] == {"system_append": render.ASK_IN_WRITING_PROMPT}
    assert set(cfg["limits"]) == {"shutdown_timeout_seconds"}
    assert "plugins" not in cfg


def test_default_restart_continue_prompt_rendered(tmp_path, monkeypatch):
    default = "Claude was restarted. Continue the task where you left off."
    assert make_settings().default_restart_continue_prompt == default
    cfg = config_of(render.render_session(make_session(), make_settings(), TOKENS))
    assert cfg["sessions"]["restart_continue_prompt"] == default
    custom = make_settings(default_restart_continue_prompt="Resume ${HOME} now.")
    objs = render.render_session(make_session(), custom, TOKENS)
    assert config_of(objs)["sessions"]["restart_continue_prompt"] == "Resume $${HOME} now."
    for k, v in {
        "CCW_AUTH_TOKEN": TOKENS.auth,
        "CCW_PEER_TOKEN": TOKENS.peer,
        "CCW_PEER_URL": "http://agent-alpha-0.agent-alpha.agents.svc:8080",
        "CLAUDE_CODE_OAUTH_TOKEN": "oauth",
        "POD_NAME": "agent-alpha-0",
        "HOME": "/root",
    }.items():
        monkeypatch.setenv(k, v)
    path = tmp_path / "config.yaml"
    path.write_text(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])
    assert load_config(path).sessions.restart_continue_prompt == "Resume ${HOME} now."


def test_spec_restart_continue_prompt_overrides_default():
    settings = make_settings(default_restart_continue_prompt="operator default")
    for prompt in ("continue please", ""):
        session = make_session(wrapper={"restart_continue_prompt": prompt})
        cfg = config_of(render.render_session(session, settings, TOKENS))
        assert cfg["sessions"]["restart_continue_prompt"] == prompt


def test_hive_with_agent_commands_validates(tmp_path, monkeypatch):
    wrapper = {
        "agent_commands": ["compact"],
        "mcp_servers": {"docs": FULL_WRAPPER["mcp_servers"]["docs"]},
    }
    session = make_session(wrapper=wrapper, max_replicas=2, replicas=2)
    objs = render.render_session(session, make_settings(agent_api_url=AGENT_URL), TOKENS)
    for k, v in {
        "CCW_AUTH_TOKEN": TOKENS.auth,
        "CCW_PEER_TOKEN": TOKENS.peer,
        "CCW_PEER_URL": "http://agent-alpha-0.agent-alpha.agents.svc:8080",
        "CLAUDE_CODE_OAUTH_TOKEN": "oauth",
        "POD_NAME": "agent-alpha-0",
        "ORCH_AGENT_TOKEN": "t",
        "ORCH_AGENT_URL": AGENT_URL,
        "ORCH_SESSION_ID": "c",
    }.items():
        monkeypatch.setenv(k, v)
    path = tmp_path / "config.yaml"
    path.write_text(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])
    assert load_config(path).sessions.commands.agent == ["compact"]
