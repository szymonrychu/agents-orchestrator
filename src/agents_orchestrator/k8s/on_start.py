"""The per-session onStart script: installs any-mcp once per ref, writes its config, installs its
plugin sources (D16, generic-any-mcp D7, D8)."""

from __future__ import annotations

import json
import shlex
from typing import Any

from agents_orchestrator.dispatcher.hive import compact_pct
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec, source_plugin_name

DELIMITER = "ANY_MCP_CONFIG_EOF"
ROOT = "/opt/any-mcp"
CONFIG_PATH = f"{ROOT}/etc/any-mcp.yaml"


PLUGIN_NAME = "orchestrator"
PLUGIN_SUBDIR = "orchestration/orchestrator"
SCALE_TOOL = "scale"
# The cooperative context-pressure handoff: every hive and solo session submits its own.
HANDOFF_TOOLS = ["handoff_submit"]
HIVE_TOOLS = [
    "me",
    "activity",
    "leadership_handover",
    *HANDOFF_TOOLS,
    "leadership_withdraw",
    "lock_claim",
    "lock_release",
    "lock_list",
    "lock_reassign",
]
SESSION_TOOLS = ["sessions_list", "session_send"]
INIT_SCRIPTS_TOOLS = ["init_scripts_get", "init_scripts_edit"]
CHAT_TOOLS = [
    "chat_rooms", "chat_history", "chat_attachment_fetch", "chat_attachment_status",
    "chat_notes_get", "chat_notes_update",
]  # fmt: skip
# Change proposals (preset-change-proposals plan 3.3): a superleader files them for any target it
# may see, the leader of any other session for its own session only (the API gate decides).
PROPOSAL_TOOLS = [
    "proposal_create", "proposal_revise", "proposals_list", "proposal_get", "proposal_withdraw",
]  # fmt: skip
# Read and patch the session's own spec (plan 3.2) and propose what it may not patch: every session
# gets them, the API gate admits the session's own leader (a regular session) or any superleader;
# a superleader lists them among its admin tools instead.
SESSION_EDIT_TOOLS = ["session_get", "session_patch", *PROPOSAL_TOOLS]
CLOSE_TOOL = "self_close"
# Taskboard tools (the agent-visible names are orchestrator_<tool>, like the chat tools): every
# session gets them, trigger-created ones included (a decision_ask they raise is answered on the
# web only).
TASK_TOOLS = [
    "task_create", "task_update", "task_list", "task_get", "task_comment",
    "decision_ask", "decision_withdraw", "approval_ask", "approval_list",
    "todo_set", "todo_update", "session_labels_set",
]  # fmt: skip
# Project tool (projects T-19): every session lists the projects its scope reaches
# (GET /agent/v1/projects: its own project, or every one for a superleader).
PROJECT_TOOLS = ["project_list", "project_summary"]
# Spec tools (T-116): the spec relay on the agent port; every session gets them, trigger-created
# ones included. Accept is user-only and has no tool; dismiss and delete are superleader admin
# tools (ADMIN_TOOLS), the agent relay refuses both. spec_queue_list reads the start queue (S-51);
# spec_queue_move is a superleader admin tool; queue removal has no tool and no relay path.
SPEC_TOOLS = [
    "spec_list", "spec_get", "spec_create", "spec_update", "spec_versions", "spec_comment",
    "spec_comments", "spec_comment_resolve", "spec_pull", "spec_push",
    "spec_queue_list",
]  # fmt: skip
# Plan tools (T-116 stage 4): the plan, steps, gate and section relay paths; every session.
PLAN_TOOLS = [
    "plan_get", "plan_set", "plan_steps", "plan_step_get", "plan_step_add", "plan_step_edit",
    "plan_step_deps", "plan_steps_order", "plan_step_next", "plan_step_update", "plan_gate",
    "spec_section", "plan_steps_push", "plan_export", "plan_questions_ask",
]  # fmt: skip
# Project room tools (T-117): every session can post to and read its project room; the routes
# themselves answer 403 not_leader to a worker.
PROJECT_CHAT_TOOLS = ["project_chat_send", "project_chat_read"]
# Mindmap tools (research-mindmaps): the plugin's mindmap module registers them statically; they
# reach a session only when the orchestrator has a mindmap upstream (`settings.mindmap_available`).
MINDMAP_TOOLS = [
    "mindmap_map_list", "mindmap_map_get", "mindmap_map_create", "mindmap_map_update",
    "mindmap_node_get", "mindmap_node_history", "mindmap_search", "mindmap_traverse",
    "mindmap_node_create", "mindmap_node_update", "mindmap_note_put",
    "mindmap_growth_area_update", "mindmap_edge_create", "mindmap_edge_delete", "mindmap_link",
    "mindmap_task_nodes", "mindmap_direction_list", "mindmap_direction_create",
    "mindmap_direction_update",
]  # fmt: skip
# Backup tools (S-68): every session saves one of its own backup entries on demand and reads its
# backup status (`POST /agent/v1/backup/save`, `GET /agent/v1/backup`; any agent of the session).
BACKUP_TOOLS = ["backup_save", "backup_status"]
# Volume tools (S-77): every session lists the volumes it mounts and saves one on demand (the
# route answers 403 backup_not_enabled unless its attachment has backup_restore).
VOLUME_TOOLS = ["volumes_status", "volume_save"]
# Volume reads of a superleader and a retrospective session (projects-scoped, names and ids only).
VOLUME_READ_TOOLS = ["volumes_list", "volume_get"]
# Superleader admin tools: the plugin's declarative admin rows (any-mcp-tatara-plugins
# orchestration/orchestrator admin.py), the vault tools included. History is read and search
# only: there is no history delete tool, deleting a stored session is for the user (web UI).
# The slice 2 reads (T-120): any session's live work, the deploy meta, the backups
# (no download) and the usage history. A project grant tool files a proposal the user reviews
# (T-120); revoke tools act at once. The slice 3 writes (labels, scale, lock reassign, message
# cancel, project update, cleanup settings, spec delete, suggestion dismiss) are
# superleader-alone and audited.
ADMIN_TOOLS = [
    "presets_list", "preset_get", "preset_create", "preset_replace", "preset_patch",
    "preset_delete", "preset_copy", "session_get", "session_patch", "session_create",
    "session_discard", "session_restart", "leadership_withdraw",
    "agent_restart", "command_get", "webhooks_list", "webhook_get_meta", "webhook_create",
    "webhook_update", "webhook_clone", "webhook_rotate", "webhook_delete",
    "webhook_filters_set", "webhook_events", "webhook_event", "webhook_template_preview",
    "schedules_list", "schedule_get", "schedule_create", "schedule_update", "schedule_delete",
    "schedule_run", "schedule_runs", "schedule_runs_all", "schedule_run_get",
    "schedule_preview", "chat_admin_rooms", "chat_room_settings", "chat_rules",
    "chat_rule_add", "chat_rule_delete",
    "chat_admin_room", "chat_admin_history", "chat_users", "chat_room_allow", "chat_open",
    "chat_open_status", "chat_open_confirm", "vault_list", "vault_attach", "vault_detach",
    "history_search", "history_get",
    "history_records", "usage_get", "project_create",
    "session_projects_grant", "session_projects_revoke", "preset_projects_grant",
    "preset_projects_revoke", "preset_manageable_request",
    "sessions_admin_list", "session_activity", "session_events", "session_messages",
    "session_locks", "session_commands", "meta_get", "backups_list", "backup_versions",
    "backup_manifest", "backups_copy", "backups_cleanup_get", "backups_cleanup_preview",
    "usage_history", "usage_sessions",
    "session_labels_update", "session_scale", "session_lock_reassign", "session_message_cancel",
    "project_update", "backups_cleanup_set", "spec_delete", "spec_suggestion_dismiss",
    "spec_queue_move", "handoffs_list", "handoff_get", "chat_admin_notes_get",
    "chat_admin_notes_update",
    *VOLUME_READ_TOOLS, "volume_attach", "volume_detach", "volume_create",
    *PROPOSAL_TOOLS,
]  # fmt: skip
# Retrospective sessions (S-68, decisions 769, 770, 775): the read set over histories, handoffs,
# chats, presets, usage and backups, plus two writes, the backup copy and the room notes update
# (scoped to rooms bound to a project the session holds). The API gates are `require_reader` and
# `require_notes_writer` (api/admin.py); mind map, spec and task writes come from the normal
# groups and the session's own and granted projects. No session patch or discard of others, no
# preset write.
RETRO_READ_TOOLS = [
    "history_search", "history_get", "history_records", "sessions_admin_list", "session_get",
    "session_activity", "session_events", "session_messages", "handoffs_list", "handoff_get",
    "chat_admin_rooms", "chat_admin_room", "chat_admin_history", "chat_admin_notes_get",
    "presets_list", "preset_get", "usage_get", "usage_history", "backups_list",
    "backup_versions", "backup_manifest", *VOLUME_READ_TOOLS,
]  # fmt: skip
RETRO_TOOLS = [*RETRO_READ_TOOLS, "backups_copy", "chat_admin_notes_update"]
# Tools a subagent may call although every orchestrator tool is main-agent only (user decision
# m1223): reads of notes, mind maps, tasks, specs, plans, projects, chats, sessions, locks,
# approvals, proposals, backups and handoffs; every route behind them is a GET. Rendered for every
# session as `claude.subagent_allowed_tools`, intersected with its own allow list (k8s/render.py).
# spec_pull and plan_export write files only in the working copy and call GET routes only.
READ_ONLY_TOOLS: frozenset[str] = frozenset(
    {
        *RETRO_READ_TOOLS,
        "me", "activity", "lock_list", "sessions_list", "chat_rooms",
        "chat_history", "chat_attachment_status", "chat_notes_get", "task_list", "task_get",
        "approval_list", "project_list", "project_summary", "project_chat_read", "spec_list",
        "spec_get", "spec_versions", "spec_comments", "spec_queue_list", "spec_section",
        "spec_pull", "plan_get", "plan_steps", "plan_step_get", "plan_gate", "plan_export",
        "proposals_list", "proposal_get", "mindmap_map_list", "mindmap_map_get",
        "mindmap_node_get", "mindmap_node_history", "mindmap_search", "mindmap_traverse",
        "mindmap_task_nodes", "mindmap_direction_list", "volumes_status",
    }
)  # fmt: skip


def _regular_tools(spec: SessionSpec) -> list[str]:
    """The tail of a session without `session_messaging`: the retrospective set when flagged,
    then its own spec and proposal tools, and `self_close` last."""
    retro = RETRO_TOOLS if spec.retrospective else []
    return [*retro, *SESSION_EDIT_TOOLS, CLOSE_TOOL]


def orchestrator_allow(spec: SessionSpec, *, mindmap: bool = False) -> list[str]:
    """The orchestrator tools this session's agents may call.

    Every session gets the session tools (D11), the init-scripts tools, the chat tools (D13), the
    task tools and `project_list`, alone sessions included, which also get `me` and
    `handoff_submit` (a hive also `leadership_handover`, a leader's own move); the agent API
    decides what each caller may list, send, edit and say in the chat (leader only). The admin
    tools are listed exactly when `session_messaging` is enabled, whoever
    created the session; every other session gets `SESSION_EDIT_TOOLS` (its spec and its
    proposals) and `self_close` last instead (a superleader is never offered `self_close`; it has
    the edit tools as admin tools). A `retrospective` session that is no superleader also gets
    `RETRO_TOOLS` right before its edit tools. A tool in two groups (`leadership_withdraw`, a hive
    and an admin tool; `session_get`, a retrospective and an edit tool) is listed once.
    """
    shared = [
        *SESSION_TOOLS,
        *INIT_SCRIPTS_TOOLS,
        *CHAT_TOOLS,
        *TASK_TOOLS,
        *PROJECT_TOOLS,
        *SPEC_TOOLS,
        *PLAN_TOOLS,
        *PROJECT_CHAT_TOOLS,
        *(MINDMAP_TOOLS if mindmap else []),
        *BACKUP_TOOLS,
        *VOLUME_TOOLS,
        *(ADMIN_TOOLS if spec.session_messaging else _regular_tools(spec)),
    ]
    if not spec.hive:
        return list(dict.fromkeys(["me", *HANDOFF_TOOLS, *shared]))
    scale = [SCALE_TOOL] if spec.scalable else []
    return list(dict.fromkeys([*HIVE_TOOLS, *scale, *shared]))


def _orchestrator_entry(
    spec: SessionSpec, settings: Settings, slug: str | None
) -> dict[str, Any] | None:
    if not settings.agent_api_url or not slug:
        return None
    return {
        "name": PLUGIN_NAME,
        "source": (
            f"git::{settings.any_mcp_plugins_repo}//{PLUGIN_SUBDIR}"
            f"?ref={settings.any_mcp_plugins_ref}"
        ),
        "namespace": PLUGIN_NAME,
        "allow": orchestrator_allow(spec, mindmap=settings.mindmap_available),
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
