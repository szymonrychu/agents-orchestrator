"""The per-session onStart script: installs any-mcp once per ref, writes its config, installs its
plugin sources (D16, generic-any-mcp D7, D8)."""

from __future__ import annotations

import json
import shlex
from typing import Any

from agents_orchestrator.pressure_levels import compact_pct
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, source_plugin_name

DELIMITER = "ANY_MCP_CONFIG_EOF"
ROOT = "/opt/any-mcp"
CONFIG_PATH = f"{ROOT}/etc/any-mcp.yaml"


PLUGIN_NAME = "orchestrator"
PLUGIN_SUBDIR = "orchestration/orchestrator"
SCALE_TOOL = "scale"
# Per-pod role tool lists (S-83, decision 885, spec section 4 matrix). The any-mcp `allow` is the
# union of the member and the leader list; the plugin lists the subset of the pod's current role
# (read from GET /agent/v1/me). The lists are UX: every route still checks role and project scope.
# Mindmap tools (research-mindmaps) join every list only when the orchestrator has a mindmap
# upstream (`settings.mindmap_available`).
MINDMAP_TOOLS = [
    "mindmap_map", "mindmap_node", "mindmap_edge", "mindmap_direction", "mindmap_search",
    "mindmap_traverse", "mindmap_link", "mindmap_task_nodes", "mindmap_note_put",
    "mindmap_growth_area_update",
]  # fmt: skip
# M: reads of own + granted projects (P), the taskboard, specs and plans, own locks, labels and
# volumes. init_scripts and chat_notes carry writes the gates refuse a member.
MEMBER_TOOLS = [
    "me", "activity", "lock", "init_scripts", "sessions_list", "session_labels",
    "session_events", "sessions_admin_list", "chat_history", "chat_notes",
    "task_create", "task_update", "task_list", "task_get", "task_comment",
    "decision_ask", "decision_withdraw", "approval_ask", "approval_list", "todo", "project",
    "spec_list", "spec_get", "spec_create", "spec_update", "spec_pull", "spec_push",
    "spec_comment", "spec_comments", "spec_queue",
    "plan_get", "plan_set", "plan_step_next", "plan_steps_push", "plan_export",
    "plan_questions_ask", "volumes", "volume_save", "history_search", "history_get", "handoffs",
]  # fmt: skip
# L, a superset of M: the hive leader (and every solo session) also hands over, scales its own
# hive, messages and discards sessions of P, reads its own room and the project room, patches and
# proposes for its own session and presets of P, reads presets of P and the current usage window,
# and closes its own session.
LEADER_TOOLS = [
    *MEMBER_TOOLS,
    "handoff_submit", SCALE_TOOL, "session_send", "session_get", "session_patch",
    "session_discard", "chat_rooms", "chat_attachment", "project_chat_read", "proposal_put",
    "proposal_get", "preset_get", "usage", "self_close",
]  # fmt: skip
# S: the leader of a superleader session (`session_messaging`) gets every leader tool but
# self_close, plus the admin tools; a member pod of a superleader session gets MEMBER_TOOLS (919).
SUPERLEADER_TOOLS = [
    *(t for t in LEADER_TOOLS if t != "self_close"),
    "leadership_withdraw", "spec_delete", "volume_attach", "volume_create", "vault_list",
    "vault_attach", "preset_create", "preset_patch", "preset_delete", "preset_copy",
    "preset_manageable_request", "session_create", "session_restart", "session_message_cancel",
    "session_commands", "backups", "backups_copy", "webhook", "schedule", "chat_connector",
]  # fmt: skip
# Tools a subagent may call although every orchestrator tool is main-agent only (user decision
# m1223): every route behind them is a GET. A merged tool with a write op (lock, project,
# spec_queue, chat_notes, init_scripts, mindmap_map, ...) is not read-only, so its reads stay with
# the main agent. Rendered as `claude.subagent_allowed_tools`, intersected with the pod's role list
# at pod start (k8s/render.py). spec_pull and plan_export write files only in the working copy and
# call GET routes only.
READ_ONLY_TOOLS: frozenset[str] = frozenset(
    {
        "me", "activity", "sessions_list", "session_get", "session_events",
        "sessions_admin_list", "chat_rooms", "chat_history", "project_chat_read", "task_list",
        "task_get", "approval_list", "spec_list", "spec_get", "spec_comments", "spec_pull",
        "plan_get", "plan_export", "proposal_get", "preset_get", "usage", "history_search",
        "history_get", "handoffs", "volumes", "backups", "mindmap_search", "mindmap_traverse",
        "mindmap_task_nodes",
    }
)  # fmt: skip


def _with_mindmap(tools: list[str], mindmap: bool) -> list[str]:
    return [*tools, *MINDMAP_TOOLS] if mindmap else list(tools)


def member_tools(*, mindmap: bool = False) -> list[str]:
    """The tools of a member pod: MEMBER_TOOLS for every session, a superleader's included."""
    return _with_mindmap(MEMBER_TOOLS, mindmap)


def leader_tools(spec: SessionSpec, *, mindmap: bool = False) -> list[str]:
    """The tools of the leader pod: SUPERLEADER_TOOLS with `session_messaging`, else LEADER_TOOLS
    (a solo session is a leader), without `scale` when the session cannot scale."""
    if spec.session_messaging:
        return _with_mindmap(SUPERLEADER_TOOLS, mindmap)
    tools = [t for t in LEADER_TOOLS if spec.scalable or t != SCALE_TOOL]
    return _with_mindmap(tools, mindmap)


def start_role_tools(spec: SessionSpec, *, mindmap: bool = False) -> list[str]:
    """The role list of a pod at its start: every pod of a hive starts as a member (leadership is
    taken at run time), the one pod of a solo session leads it."""
    return member_tools(mindmap=mindmap) if spec.hive else leader_tools(spec, mindmap=mindmap)


def orchestrator_allow(spec: SessionSpec, *, mindmap: bool = False) -> list[str]:
    """The any-mcp allow list of the session: the union of its member and leader lists."""
    return list(
        dict.fromkeys([*member_tools(mindmap=mindmap), *leader_tools(spec, mindmap=mindmap)])
    )


def _orchestrator_entry(
    spec: SessionSpec, settings: Settings, slug: str | None
) -> dict[str, Any] | None:
    if not settings.agent_api_url or not slug:
        return None
    mindmap = settings.mindmap_available
    return {
        "name": PLUGIN_NAME,
        "source": (
            f"git::{settings.any_mcp_plugins_repo}//{PLUGIN_SUBDIR}"
            f"?ref={settings.any_mcp_plugins_ref}"
        ),
        "namespace": PLUGIN_NAME,
        "allow": orchestrator_allow(spec, mindmap=mindmap),
        "config": {
            "base_url": "${ORCH_AGENT_URL}",
            "token": "${ORCH_AGENT_TOKEN}",
            "session": "${ORCH_SESSION_ID}",
            "agent": "${POD_NAME}",
            "slug": slug,
            "max_replicas": spec.max_replicas or spec.replicas,
            "static": spec.static,
            "hive": spec.hive,
            "auto_compact_pct": compact_pct(spec.auto_compact_pct, settings),
            "member_tools": member_tools(mindmap=mindmap),
            "leader_tools": leader_tools(spec, mindmap=mindmap),
        },
    }


def any_mcp_config(
    spec: SessionSpec, settings: Settings, slug: str | None = None
) -> dict[str, Any]:
    base = (
        spec.any_mcp_config
        if spec.any_mcp_config is not None
        else settings.default_any_mcp_config()
    )
    entry = _orchestrator_entry(spec, settings, slug)
    sources = [{"name": source_plugin_name(s), "source": s} for s in spec.any_mcp_sources or []]
    if entry is None and not sources:
        return base
    plugins = base.get("plugins", [])
    if entry is None:
        return {**base, "plugins": [*plugins, *sources]}
    kept = [p for p in plugins if p.get("name") != PLUGIN_NAME]
    return {**base, "plugins": [*kept, *sources, entry]}


def render_on_start(spec: SessionSpec, settings: Settings, slug: str | None = None) -> str:
    """The onStart script; an install failure is one stderr line naming the step, never an exit.

    D6: the venv plus pip install and `any-mcp plugins install` are swallowed (the wrapper denies
    on exit 2 whatever on_error says); a failed package install writes no `.ref`, so the next
    start retries, and every later install step and the `--help` check are skipped.
    """
    body = json.dumps(any_mcp_config(spec, settings, slug), indent=2, sort_keys=True)
    if DELIMITER in body.splitlines():
        raise ValueError(f"any-mcp config contains the heredoc delimiter line {DELIMITER}")
    ref = shlex.quote(settings.any_mcp_ref)
    requirement = shlex.quote(f"any-mcp @ git+{settings.any_mcp_repo}@{settings.any_mcp_ref}")
    return f"""#!/bin/sh
set -eu
REF={ref}
ok=1
install() {{
  step=$1
  shift
  [ "$ok" = 1 ] || return 0
  "$@" && return 0
  echo "on-start: $step failed (exit $?)" >&2
  ok=0
}}
if ! grep -qxF -- "$REF" {ROOT}/.ref 2>/dev/null; then
  rm -rf {ROOT}/* {ROOT}/.ref
  install 'any-mcp package install' python3 -m venv {ROOT}
  install 'any-mcp package install' {ROOT}/bin/pip install --no-cache-dir {requirement}
  [ "$ok" = 0 ] || printf '%s\\n' "$REF" > {ROOT}/.ref
fi
mkdir -p {ROOT}/etc
cat > {CONFIG_PATH} <<'{DELIMITER}'
{body}
{DELIMITER}
install 'any-mcp plugins install' {ROOT}/bin/any-mcp plugins install --config {CONFIG_PATH}
[ "$ok" = 0 ] || {ROOT}/bin/any-mcp --help >/dev/null
"""
