"""The orchestrator plugin entry merged into the rendered any-mcp config."""

from __future__ import annotations

import json
import re
import stat
import subprocess
import uuid
from datetime import UTC, datetime
from pathlib import Path

import pytest
from any_mcp.config import AnyMcpConfig
from any_mcp_tatara_orchestrator import OrchestratorPlugin

from agents_orchestrator.db.sessions import Session
from agents_orchestrator.k8s import on_start
from agents_orchestrator.k8s.on_start import render_on_start
from agents_orchestrator.k8s.render import render_session
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec
from agents_orchestrator.tokens import SessionTokens

HIVE_TOOLS = [
    "me",
    "activity",
    "leadership_handover",
    "lock_claim",
    "lock_release",
    "lock_list",
    "lock_reassign",
]
SESSION_TOOLS = ["sessions_list", "session_send"]
INIT_SCRIPTS_TOOLS = ["init_scripts_get", "init_scripts_edit"]
CHAT_TOOLS = ["chat_rooms", "chat_send", "chat_react", "chat_status"]
TAIL_TOOLS = [*SESSION_TOOLS, *INIT_SCRIPTS_TOOLS, *CHAT_TOOLS]
ADMIN_TOOLS = [
    "presets_list", "preset_get", "preset_create", "preset_replace", "preset_delete",
    "preset_copy", "session_get", "session_create", "session_discard", "session_restart",
    "agent_restart", "command_get", "webhooks_list", "webhook_get_meta", "webhook_create",
    "webhook_update", "webhook_clone", "webhook_rotate", "webhook_delete",
    "webhook_filters_set", "webhook_events", "webhook_event", "webhook_template_preview",
    "schedules_list", "schedule_get", "schedule_create", "schedule_update", "schedule_delete",
    "schedule_run", "schedule_runs", "schedule_runs_all", "schedule_run_get",
    "schedule_preview", "chat_admin_rooms", "chat_room_settings", "chat_rules",
    "chat_rule_add", "chat_rule_delete", "secret_set",
]  # fmt: skip
SECRET = "s3cr3t-token-value"
PLUGINS_REPO = "https://github.com/szymonrychu/any-mcp-tatara-plugins"
PLUGINS_REF = Settings.model_fields["any_mcp_plugins_ref"].default
RESERVED_SOURCE = f"git::{PLUGINS_REPO}//orchestration/orchestrator?ref={PLUGINS_REF}"


def make_settings(**over) -> Settings:
    base = {
        "database_url": "postgresql://u:p@db/orch",
        "master_key": "m" * 32,
        "agent_image": "img:1",
        "default_model": "sonnet",
        "models": ["sonnet"],
        "agent_api_url": "http://orch.example:8080",
    }
    base.update(over)
    return Settings(**base)


def written_config(script: str) -> dict:
    m = re.search(r"<<'ANY_MCP_CONFIG_EOF'\n(.*)\nANY_MCP_CONFIG_EOF\n", script, re.S)
    assert m
    return json.loads(m.group(1))


def entries(spec: SessionSpec, settings: Settings | None = None) -> dict:
    cfg = written_config(on_start.render_on_start(spec, settings or make_settings(), "alpha"))
    return {p["name"]: p for p in cfg.get("plugins", [])}


@pytest.mark.parametrize("messaging", [None, False, True])
@pytest.mark.parametrize("static", [False, True])
def test_alone_session_tools_entry(messaging, static):
    """D11, D13: an alone session gets the session, init-scripts and chat tools; the admin tools
    only with session_messaging."""
    spec = SessionSpec(replicas=1, static=static, session_messaging=messaging)
    expected = [*TAIL_TOOLS, *ADMIN_TOOLS] if messaging else TAIL_TOOLS
    assert on_start.orchestrator_allow(spec) == expected
    entry = entries(spec)["orchestrator"]
    assert entry["source"] == RESERVED_SOURCE and "plugin" not in entry
    assert entry["allow"] == expected
    assert entry["config"]["hive"] is False
    assert entry["config"]["max_replicas"] == 1
    cfg = written_config(on_start.render_on_start(spec, make_settings(), "alpha"))
    config = AnyMcpConfig.model_validate(cfg)
    plugin = next(p for p in config.plugins if p.name == "orchestrator")
    OrchestratorPlugin.Config.model_validate(plugin.config)


@pytest.mark.parametrize("messaging", [None, False, True])
@pytest.mark.parametrize(
    "spec_fields,hive_allow",
    [
        ({"replicas": 2, "static": True}, HIVE_TOOLS),
        ({"replicas": 2}, [*HIVE_TOOLS, "scale"]),
        ({"replicas": 1, "max_replicas": 3}, [*HIVE_TOOLS, "scale"]),
    ],
)
def test_hive_session_tools(messaging, spec_fields, hive_allow):
    """D11, D13: hive tools, scale when scalable, then the session, init-scripts and chat tools,
    then the admin tools only with session_messaging."""
    spec = SessionSpec(session_messaging=messaging, **spec_fields)
    allow = on_start.orchestrator_allow(spec)
    assert allow == [*hive_allow, *TAIL_TOOLS, *(ADMIN_TOOLS if messaging else [])]
    assert entries(spec)["orchestrator"]["allow"] == allow


def rendered_allow(created_by_service: str | None, **spec_fields) -> list[str]:
    """The orchestrator allow list a session created by `created_by_service` gets in its onStart
    script, rendered through the real session renderer."""
    session = Session(
        id=uuid.UUID("11111111-1111-1111-1111-111111111111"),
        slug="alpha",
        spec=SessionSpec.model_validate(spec_fields),
        generation=1,
        observed_generation=0,
        desired="active",
        status={},
        created_at=datetime(2026, 10, 28, tzinfo=UTC),
        created_by_service=created_by_service,
    )
    tokens = SessionTokens(auth="a" * 32, peer="p" * 32, agent="g" * 32)
    objs = render_session(session, make_settings(), tokens)
    script = next(o for o in objs if o["kind"] == "ConfigMap")["data"]["on-start.sh"]
    plugins = written_config(script)["plugins"]
    return next(p for p in plugins if p["name"] == "orchestrator")["allow"]


def test_every_creator_gets_the_chat_tools():
    """Who created the session does not change the allow list (an enabled flag is honoured)."""
    for service in (None, "agents-webhook", "agents-schedule", "agents-matrix-connector"):
        assert rendered_allow(service, replicas=1) == TAIL_TOOLS, service
        assert rendered_allow(service, replicas=2) == [*HIVE_TOOLS, "scale", *TAIL_TOOLS], service


def test_static_hive_has_no_scale_tool():
    spec = SessionSpec(replicas=2, static=True)
    assert on_start.orchestrator_allow(spec) == [*HIVE_TOOLS, *TAIL_TOOLS]


def test_hive_entry_and_allow():
    spec = SessionSpec(replicas=2, max_replicas=4, auto_compact_pct=70)
    assert on_start.orchestrator_allow(spec) == [*HIVE_TOOLS, "scale", *TAIL_TOOLS]
    entry = entries(spec)["orchestrator"]
    assert entry == {
        "name": "orchestrator",
        "source": RESERVED_SOURCE,
        "namespace": "orchestrator",
        "allow": [*HIVE_TOOLS, "scale", *TAIL_TOOLS],
        "config": {
            "base_url": "${ORCH_AGENT_URL}",
            "token": "${ORCH_AGENT_TOKEN}",
            "session": "${ORCH_SESSION_ID}",
            "agent": "${POD_NAME}",
            "slug": "alpha",
            "max_replicas": 4,
            "static": False,
            "hive": True,
            "auto_compact_pct": 70,
        },
    }
    cfg = written_config(on_start.render_on_start(spec, make_settings(), "alpha"))
    AnyMcpConfig.model_validate(cfg)


def test_plugin_config_effective_auto_compact():
    """D9: a spec without auto_compact_pct renders the settings default, so the advice shows."""
    spec = SessionSpec(replicas=2)
    assert spec.auto_compact_pct is None
    assert entries(spec)["orchestrator"]["config"]["auto_compact_pct"] == 95
    custom = entries(spec, make_settings(default_auto_compact_pct=88))["orchestrator"]
    assert custom["config"]["auto_compact_pct"] == 88


def test_plugin_entry_session_key():
    """The rendered entry names the session and validates under the installed any-mcp."""
    spec = SessionSpec(replicas=2, max_replicas=4)
    cfg = written_config(on_start.render_on_start(spec, make_settings(), "alpha"))
    config = AnyMcpConfig.model_validate(cfg)
    entry = next(p for p in config.plugins if p.name == "orchestrator")
    assert entry.config["session"] == "${ORCH_SESSION_ID}"
    plugin_config = OrchestratorPlugin.Config.model_validate(entry.config)
    assert plugin_config.session == "${ORCH_SESSION_ID}"


def test_user_config_merged():
    user = {"plugins": [{"name": "gh", "plugin": "x:Y", "config": {"a": 1}}], "tools": {}}
    spec = SessionSpec(replicas=2, any_mcp_config=user)
    got = entries(spec)
    assert got["gh"] == user["plugins"][0]
    assert "orchestrator" in got
    assert spec.any_mcp_config == {
        "plugins": [{"name": "gh", "plugin": "x:Y", "config": {"a": 1}}],
        "tools": {},
    }
    cfg = written_config(on_start.render_on_start(spec, make_settings(), "alpha"))
    assert cfg["tools"] == {}
    AnyMcpConfig.model_validate(cfg)


def test_default_config_used_when_spec_has_none(tmp_path):
    f = tmp_path / "mcp.yaml"
    f.write_text("plugins:\n  - {name: gh, plugin: 'x:Y'}\n")
    got = entries(SessionSpec(replicas=2), make_settings(any_mcp_config_file=str(f)))
    assert set(got) == {"gh", "orchestrator"}


@pytest.mark.parametrize("spec", [SessionSpec(replicas=1), SessionSpec(replicas=2)])
def test_omitted_without_agent_api_url_or_slug(spec):
    assert "orchestrator" in entries(spec)
    assert "orchestrator" not in entries(spec, make_settings(agent_api_url=None))
    for slug in (None, ""):
        cfg = written_config(on_start.render_on_start(spec, make_settings(), slug))
        assert "orchestrator" not in {p["name"] for p in cfg.get("plugins", [])}


def test_placeholders_not_secrets():
    spec = SessionSpec(replicas=2)
    script = on_start.render_on_start(spec, make_settings(master_key=SECRET * 2), "alpha")
    assert "${ORCH_AGENT_TOKEN}" in script and "${POD_NAME}" in script
    assert SECRET not in script


def test_invariant_to_replicas():
    a = on_start.render_on_start(SessionSpec(replicas=2, max_replicas=4), make_settings(), "alpha")
    b = on_start.render_on_start(SessionSpec(replicas=3, max_replicas=4), make_settings(), "alpha")
    assert a == b


def test_user_orchestrator_entry_replaced():
    user = {"plugins": [{"name": "orchestrator", "plugin": "x:Y"}, {"name": "gh", "plugin": "x:Z"}]}
    spec = SessionSpec(replicas=2, any_mcp_config=user)
    cfg = written_config(on_start.render_on_start(spec, make_settings(), "alpha"))
    names = [p["name"] for p in cfg["plugins"]]
    assert names.count("orchestrator") == 1 and "gh" in names
    entry = next(p for p in cfg["plugins"] if p["name"] == "orchestrator")
    assert entry["source"] == RESERVED_SOURCE and "plugin" not in entry
    AnyMcpConfig.model_validate(cfg)


@pytest.mark.parametrize("spec", [SessionSpec(replicas=1), SessionSpec(replicas=2, max_replicas=4)])
def test_reserved_entry_is_plugins_repo_source(spec):
    """D7: the reserved entry is a source in the plugins repo at the pinned ref, never a plugin."""
    settings = make_settings(
        any_mcp_plugins_repo="https://git.example/acme/plugins", any_mcp_plugins_ref="v7.8.9"
    )
    cfg = written_config(on_start.render_on_start(spec, settings, "alpha"))
    entry = next(p for p in cfg["plugins"] if p["name"] == "orchestrator")
    assert entry["source"] == (
        "git::https://git.example/acme/plugins//orchestration/orchestrator?ref=v7.8.9"
    )
    assert set(entry) == {"name", "namespace", "source", "allow", "config"}
    assert entry["namespace"] == "orchestrator"
    assert entry["allow"] == on_start.orchestrator_allow(spec)
    assert entry["config"]["slug"] == "alpha"
    config = AnyMcpConfig.model_validate(cfg)
    plugin = next(p for p in config.plugins if p.name == "orchestrator")
    assert plugin.plugin is None and plugin.source == entry["source"]
    OrchestratorPlugin.Config.model_validate(plugin.config)


def test_any_mcp_sources_rendered():
    """D8: each source is `{name, source}`, after the base entries and before the reserved one."""
    sources = ["github.com/acme/tools", "git::https://git.example/x/mono.git//plugins/Lint?ref=v1"]
    user = {"plugins": [{"name": "gh", "plugin": "x:Y"}], "tools": {}}
    spec = SessionSpec(replicas=2, any_mcp_config=user, any_mcp_sources=sources)
    cfg = written_config(on_start.render_on_start(spec, make_settings(), "alpha"))
    assert cfg["plugins"][:3] == [
        {"name": "gh", "plugin": "x:Y"},
        {"name": "tools", "source": sources[0]},
        {"name": "lint", "source": sources[1]},
    ]
    assert [p["name"] for p in cfg["plugins"]] == ["gh", "tools", "lint", "orchestrator"]
    assert cfg["tools"] == {}
    AnyMcpConfig.model_validate(cfg)

    for settings, slug in ((make_settings(agent_api_url=None), "alpha"), (make_settings(), None)):
        cfg = written_config(on_start.render_on_start(spec, settings, slug))
        assert [p["name"] for p in cfg["plugins"]] == ["gh", "tools", "lint"]
        AnyMcpConfig.model_validate(cfg)

    bare = SessionSpec(replicas=1, any_mcp_sources=sources[:1])
    cfg = written_config(on_start.render_on_start(bare, make_settings(agent_api_url=None)))
    assert cfg == {"plugins": [{"name": "tools", "source": sources[0]}]}


async def test_allowed_tools_exist_in_plugin():
    """Every tool name rendered or named to agents is offered by the pinned plugin."""
    from any_mcp.testing import PluginHarness

    from agents_orchestrator.dispatcher.session_frame import frame
    from agents_orchestrator.reconciler.init import render_init_sh

    specs = (
        SessionSpec(replicas=1),
        SessionSpec(replicas=2, static=True),
        SessionSpec(replicas=2, max_replicas=3),
    )
    offered: set[str] = set()
    allowed: set[str] = set()
    for spec in specs:
        entry = entries(spec)["orchestrator"]
        config = {
            **entry["config"],
            "base_url": "http://orch:8080",
            "token": "t",
            "session": "1",
            "agent": "a0",
        }
        async with PluginHarness(OrchestratorPlugin, config=config) as h:
            # the harness prefixes the class name; the rendered namespace is "orchestrator"
            offered |= {t["name"].removeprefix("orchestratorplugin_") for t in await h.list_tools()}
        allowed |= set(entry["allow"])
    named = set(
        re.findall(
            r"orchestrator_[a-z_]+", render_init_sh("alpha", ["true"]) + frame("x", "alpha", 1)
        )
    )
    assert named == {"orchestrator_init_scripts_edit", "orchestrator_session_send"}
    assert set(CHAT_TOOLS) <= allowed
    assert allowed | {n.removeprefix("orchestrator_") for n in named} <= offered


def _fake_tools(tmp_path: Path) -> tuple[Path, Path]:
    """Fake `python3 -m venv`, `pip` and `any-mcp` that log their argv to calls.log.

    `VENV_EXIT`, `PIP_EXIT` and `PLUGINS_EXIT` make the venv, the package install or
    `any-mcp plugins install` exit with that code; any-mcp exists only after a pip install
    succeeded, as with a real venv.
    """
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    log = tmp_path / "calls.log"
    any_mcp = tmp_path / "any-mcp.stub"
    any_mcp.write_text(
        f"""#!/bin/sh
echo "any-mcp $*" >> {log}
if [ "$1 $2" = "plugins install" ]; then
  exit "${{PLUGINS_EXIT:-0}}"
fi
"""
    )
    pip = tmp_path / "pip.stub"
    pip.write_text(
        f"""#!/bin/sh
echo "pip $*" >> {log}
if [ "${{PIP_EXIT:-0}}" != 0 ]; then
  exit "$PIP_EXIT"
fi
cp {any_mcp} "$(dirname "$0")/any-mcp"
chmod +x "$(dirname "$0")/any-mcp"
"""
    )
    python3 = bin_dir / "python3"
    python3.write_text(
        f"""#!/bin/sh
echo "python3 $*" >> {log}
if [ "${{VENV_EXIT:-0}}" != 0 ]; then
  exit "$VENV_EXIT"
fi
mkdir -p "$3/bin"
cp {pip} "$3/bin/pip"
chmod +x "$3/bin/pip"
"""
    )
    python3.chmod(python3.stat().st_mode | stat.S_IEXEC)
    return log, tmp_path / "any-mcp"


def _run_script(tmp_path: Path, **exits: int) -> subprocess.CompletedProcess[str]:
    script = render_on_start(
        SessionSpec(any_mcp_config={"a": 1}), make_settings(any_mcp_ref="v1.0.0")
    )
    path = tmp_path / "on-start.sh"
    path.write_text(script.replace("/opt/any-mcp", str(tmp_path / "any-mcp")))
    env = {"PATH": f"{tmp_path / 'bin'}:/usr/bin:/bin", **{k: str(v) for k, v in exits.items()}}
    return subprocess.run(["sh", str(path)], env=env, capture_output=True, text=True)


def test_plugin_install_failure_logged_and_exits_zero(tmp_path):
    """D6: a failed `any-mcp plugins install` is one stderr line, exit 0, no --help check."""
    log, root = _fake_tools(tmp_path)
    done = _run_script(tmp_path, PLUGINS_EXIT=1)
    assert done.returncode == 0
    assert done.stderr.splitlines() == ["on-start: any-mcp plugins install failed (exit 1)"]
    calls = log.read_text()
    assert f"any-mcp plugins install --config {root}/etc/any-mcp.yaml" in calls
    assert "any-mcp --help" not in calls
    assert (root / ".ref").read_text().strip() == "v1.0.0"
    assert json.loads((root / "etc" / "any-mcp.yaml").read_text()) == {"a": 1}


def test_any_mcp_install_failure_logged_and_retried_next_start(tmp_path):
    """D6: a failed package install writes no .ref, skips both any-mcp steps, retries next start."""
    log, root = _fake_tools(tmp_path)
    done = _run_script(tmp_path, PIP_EXIT=1)
    assert done.returncode == 0
    assert done.stderr.splitlines() == ["on-start: any-mcp package install failed (exit 1)"]
    calls = log.read_text()
    assert "pip install --no-cache-dir" in calls
    assert "any-mcp plugins install" not in calls and "any-mcp --help" not in calls
    assert not (root / ".ref").exists()
    assert json.loads((root / "etc" / "any-mcp.yaml").read_text()) == {"a": 1}

    log.write_text("")
    done = _run_script(tmp_path)
    assert done.returncode == 0 and done.stderr == ""
    calls = log.read_text()
    assert calls.count("pip install") == 1
    assert "any-mcp plugins install" in calls and calls.splitlines()[-1] == "any-mcp --help"
    assert (root / ".ref").read_text().strip() == "v1.0.0"


@pytest.mark.parametrize(
    "failing,step",
    [
        ("VENV_EXIT", "any-mcp package install"),
        ("PIP_EXIT", "any-mcp package install"),
        ("PLUGINS_EXIT", "any-mcp plugins install"),
    ],
)
def test_install_exit_two_never_propagates(tmp_path, failing, step):
    """The wrapper denies on exit 2 whatever on_error says, so an install exit 2 never escapes."""
    log, root = _fake_tools(tmp_path)
    done = _run_script(tmp_path, **{failing: 2})
    assert done.returncode == 0
    assert done.stderr.splitlines() == [f"on-start: {step} failed (exit 2)"]
    assert "any-mcp --help" not in log.read_text()


def test_config_write_failure_still_fails(tmp_path):
    """Only the installs are swallowed: a config that cannot be written fails the script."""
    _fake_tools(tmp_path)
    root = tmp_path / "any-mcp"
    root.mkdir()
    (root / ".ref").write_text("v1.0.0\n")
    (root / "etc").write_text("not a directory")
    assert _run_script(tmp_path).returncode != 0


def test_golden_allowlist_superleader_leader_and_service_session():
    """Golden rendered allowlists: the admin tools and secret_set exactly when session_messaging
    is on, whoever created the session; a plain leader never gets them."""
    hive = [*HIVE_TOOLS, "scale", *TAIL_TOOLS]
    assert rendered_allow(None, replicas=2, session_messaging=True) == [*hive, *ADMIN_TOOLS]
    assert rendered_allow(None, replicas=2) == hive
    assert rendered_allow(None, replicas=2, session_messaging=False) == hive
    assert rendered_allow("agents-webhook", replicas=2, session_messaging=True) == [
        *hive,
        *ADMIN_TOOLS,
    ]
    assert rendered_allow("agents-webhook", replicas=2) == hive
