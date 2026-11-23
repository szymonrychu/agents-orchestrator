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

# The section 4 matrix of S-83 (spec dreaming-in-hives), copied as literals so a code change cannot
# pass by agreeing with itself: M member, L hive leader or solo session, S superleader leader pod.
MEMBER = [
    "me", "activity", "lock", "init_scripts", "sessions_list", "session_labels",
    "session_events", "sessions_admin_list", "chat_history", "chat_notes",
    "task_create", "task_update", "task_list", "task_get", "task_comment",
    "decision_ask", "decision_withdraw", "approval_ask", "approval_list", "todo", "project",
    "spec_list", "spec_get", "spec_create", "spec_update", "spec_pull", "spec_push",
    "spec_comment", "spec_comments", "spec_queue",
    "plan_get", "plan_set", "plan_step_next", "plan_steps_push", "plan_export",
    "plan_questions_ask", "volumes", "volume_save", "history_search", "history_get", "handoffs",
]  # fmt: skip
LEADER_ONLY = [
    "handoff_submit", "scale", "session_send", "session_get", "session_patch",
    "session_discard", "chat_rooms", "chat_attachment", "project_chat_read", "proposal_put",
    "proposal_get", "preset_get", "usage", "self_close",
]  # fmt: skip
SUPERLEADER_ONLY = [
    "leadership_withdraw", "spec_delete", "volume_attach", "volume_create", "vault_list",
    "vault_attach", "preset_create", "preset_patch", "preset_delete", "preset_copy",
    "preset_manageable_request", "session_create", "session_restart", "session_message_cancel",
    "session_commands", "backups", "backups_copy", "webhook", "schedule", "chat_connector",
]  # fmt: skip
MATRIX = {
    "M": set(MEMBER),
    "L": {*MEMBER, *LEADER_ONLY},
    "S": {*MEMBER, *LEADER_ONLY, *SUPERLEADER_ONLY} - {"self_close"},
}
MINDMAP = [
    "mindmap_map", "mindmap_node", "mindmap_edge", "mindmap_direction", "mindmap_search",
    "mindmap_traverse", "mindmap_link", "mindmap_task_nodes", "mindmap_note_put",
    "mindmap_growth_area_update",
]  # fmt: skip
# Listed before the pinned plugin ships them; add a name when the role lists lead the plugin pin
# and remove it when the pin offers the tool. Empty at a release that ships every listed tool.
PLUGIN_PENDING: set[str] = set()
SUPERLEADER = {"replicas": 2, "max_replicas": 3, "session_messaging": True}

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


@pytest.mark.parametrize(
    "role,fields,leader",
    [
        ("M", {"replicas": 2}, False),
        ("M", SUPERLEADER, False),
        ("L", {"replicas": 2}, True),
        ("L", {"replicas": 1}, True),
        ("S", SUPERLEADER, True),
        ("S", {"replicas": 1, "session_messaging": True}, True),
    ],
    ids=["hive-member", "superleader-member", "hive-leader", "solo", "superleader", "solo-sl"],
)
def test_role_lists_match_matrix(role, fields, leader):
    """Each pod role renders exactly its section 4 column; a solo session is L, a solo
    superleader S; `scale` only where the session can scale."""
    config = entries(SessionSpec(**fields))["orchestrator"]["config"]
    listed = config["leader_tools"] if leader else config["member_tools"]
    expected = set(MATRIX[role])
    if role == "L" and not SessionSpec(**fields).scalable:
        expected.discard("scale")
    assert set(listed) == expected
    assert len(listed) == len(set(listed))


@pytest.mark.parametrize(
    "fields",
    [{"replicas": 1}, {"replicas": 2}, {"replicas": 2, "static": True}, SUPERLEADER],
    ids=["solo", "hive", "static-hive", "superleader"],
)
@pytest.mark.parametrize("mindmap", [False, True])
def test_allow_is_union_and_config_carries_lists(fields, mindmap):
    """The any-mcp allow is the union of both rendered lists, and the config carries both keys
    (a role with no key would list every tool); mindmap tools join every list only when the
    mindmap upstream is configured. The config validates against the pinned plugin."""
    over = {"mindmap_url": "http://mm:8082", "mindmap_token": "t"} if mindmap else {}
    entry = entries(SessionSpec(**fields), make_settings(**over))["orchestrator"]
    member, leader = entry["config"]["member_tools"], entry["config"]["leader_tools"]
    assert entry["allow"] == list(dict.fromkeys([*member, *leader]))
    for listed in (member, leader):
        assert (set(MINDMAP) <= set(listed)) is mindmap
        assert bool(set(MINDMAP) & set(listed)) is mindmap
    OrchestratorPlugin.Config.model_validate(entry["config"])


def test_superleader_member_pod_gets_member_list():
    """Decision 919: a member pod of a superleader session is not a superleader; its list is the
    member list of any other hive, the admin tools only in the leader list."""
    sl = entries(SessionSpec(**SUPERLEADER))["orchestrator"]["config"]
    hive = entries(SessionSpec(replicas=2))["orchestrator"]["config"]
    assert sl["member_tools"] == hive["member_tools"]
    assert not set(SUPERLEADER_ONLY) & set(sl["member_tools"])
    assert set(SUPERLEADER_ONLY) <= set(sl["leader_tools"])


def test_plugins_ref_is_release():
    """The plugin pin is the S-83 release (role lists, merged names) or later."""
    major, minor, patch = (int(p) for p in PLUGINS_REF.removeprefix("v").split("."))
    assert (major, minor, patch) >= (0, 62, 0)


def test_role_lists_do_not_depend_on_creator():
    """Who created the session does not change the rendered allow list."""
    for fields in ({"replicas": 1}, {"replicas": 2}):
        lists = {
            s: rendered_allow(s, **fields) for s in (None, "agents-webhook", "agents-schedule")
        }
        assert len({tuple(v) for v in lists.values()}) == 1, lists


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
    tokens = SessionTokens(auth="a" * 32, agent="g" * 32)
    objs = render_session(session, make_settings(), tokens)
    script = next(o for o in objs if o["kind"] == "ConfigMap")["data"]["on-start.sh"]
    plugins = written_config(script)["plugins"]
    return next(p for p in plugins if p["name"] == "orchestrator")["allow"]


def test_hive_entry_and_allow():
    spec = SessionSpec(replicas=2, max_replicas=4, auto_compact_pct=70)
    entry = entries(spec)["orchestrator"]
    member, leader = MEMBER, [*MEMBER, *LEADER_ONLY]
    assert entry == {
        "name": "orchestrator",
        "source": RESERVED_SOURCE,
        "namespace": "orchestrator",
        "allow": leader,
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
            "member_tools": member,
            "leader_tools": leader,
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


async def _offered(config: dict) -> set[str]:
    from any_mcp.testing import PluginHarness

    config = {**config, "base_url": "http://orch:8080", "token": "t", "session": "1", "agent": "a0"}
    # without role list keys the plugin lists every tool it ships
    config = {k: v for k, v in config.items() if k not in ("member_tools", "leader_tools")}
    async with PluginHarness(OrchestratorPlugin, config=config) as h:
        # the harness prefixes the class name; the rendered namespace is "orchestrator"
        return {t["name"].removeprefix("orchestratorplugin_") for t in await h.list_tools()}


async def test_released_plugin_offers_every_listed_tool():
    """Every tool of every role list, and every tool name the orchestrator writes for agents, is
    offered by the pinned plugin; nothing is pending at a release."""
    from agents_orchestrator.reconciler.init import render_init_sh

    assert not PLUGIN_PENDING
    settings = make_settings(mindmap_url="http://mm:8082", mindmap_token="t")
    listed: set[str] = set()
    for fields in ({"replicas": 1}, {"replicas": 2}, SUPERLEADER):
        entry = entries(SessionSpec(**fields), settings)["orchestrator"]
        listed |= set(entry["allow"])
    offered = await _offered(entry["config"])
    named = set(re.findall(r"orchestrator_([a-z_]+)", render_init_sh("alpha", ["true"])))
    assert named == {"init_scripts"}
    assert listed | named <= offered | PLUGIN_PENDING


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


# ---------------------------------------- the subagent reads (S-68)


async def _plugin_methods() -> dict[str, set[str]]:
    """Every HTTP method each plugin tool may call: every op route of a merged tool."""
    from any_mcp.testing import PluginHarness
    from any_mcp_tatara_orchestrator import _MERGED

    config = {"base_url": "http://orch:8080", "token": "t", "session": "1", "agent": "a0"}
    async with PluginHarness(OrchestratorPlugin, config={**config, "slug": "s"}) as h:
        plugin = h._loaded[0].instance
        tools = await plugin.load_tools()
    merged = {m.name: {r.method for r in m.ops.values()} for m in _MERGED.values()}
    return {s.name: merged.get(s.name) or ({s.http.method} if s.http else set()) for s in tools}


async def test_read_only_tools_map_to_read_routes():
    """Every tool of READ_ONLY_TOOLS calls GET routes only, every op of a merged tool included."""
    methods = await _plugin_methods()
    for tool in sorted(on_start.READ_ONLY_TOOLS):
        assert methods.get(tool) == {"GET"}, (tool, methods.get(tool))
