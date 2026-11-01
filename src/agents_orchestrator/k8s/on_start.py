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


def orchestrator_allow(spec: SessionSpec) -> list[str]:
    """The orchestrator tools this session's agents may call.

    Every session gets the session tools (D11), the init-scripts tools and the chat tools (D13),
    alone sessions included; the agent API decides what each caller may list, send, edit and say
    in the chat (leader only), so the list never depends on `session_messaging`. Who created the
    session does not change the list.
    """
    shared = [*SESSION_TOOLS, *INIT_SCRIPTS_TOOLS, *CHAT_TOOLS]
    if not spec.hive:
        return shared
    scale = [SCALE_TOOL] if spec.scalable else []
    return [*HIVE_TOOLS, *scale, *shared]


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
        "allow": orchestrator_allow(spec),
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
