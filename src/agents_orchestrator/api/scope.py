"""Project scope of an agent session and the classification of every agent-port route (projects
plan 3.2, 3.3).

A superleader session (`spec.session_messaging`) is `*`, every project; any other session is
its home project followed by its extra projects (T-120), comma-separated, read live from the
session row on every request. The orchestrator computes
the scope and the service that owns the data enforces it on `X-Orch-Projects`, which the relay
sets itself (`forward` never passes an inbound header, so an agent cannot forge it).

`AGENT_SCOPE` pins every route of the agent port, built like `admin.ADMIN_VISIBILITY`: a new
agent route fails CI until it is classified.

- `own`: the caller's own session only; the project does not change the answer.
- `peers`: other sessions, filtered by the messaging rules (plan 3.3, Q5 C).
- `project`: data of the caller's project; the relay forwards `X-Orch-Projects` and the
  upstream filters.
- `superleader`: the superleader gate; unscoped by design. The admin relay (`admin_relay`)
  forwards are superleader-only and send `X-Orch-Projects: *`.
"""

from __future__ import annotations

from typing import Literal

from agents_orchestrator.db import sessions as sessions_repo

ScopeClass = Literal["own", "peers", "project", "superleader"]

PROJECTS_HEADER = "X-Orch-Projects"
ALL_PROJECTS = "*"

_A = "/agent/v1"
_ADMIN = f"{_A}/admin"


def scope_of(session: sessions_repo.Session) -> str:
    """`*` for a superleader session, else its projects comma-separated, home first (the
    taskboard reads the first as the home project)."""
    return ALL_PROJECTS if session.spec.session_messaging else ",".join(session.projects)


def projects_header(session: sessions_repo.Session) -> dict[str, str]:
    """The `X-Orch-Projects` header a relay sends for the calling session."""
    return {PROJECTS_HEADER: scope_of(session)}


AGENT_SCOPE: dict[tuple[str, str], ScopeClass] = {
    # identity, lifecycle, locks, labels, init scripts: the caller's own session
    ("GET", f"{_A}/me"): "own",  # answers `project`, `extra_projects` and `projects_scope`
    ("POST", f"{_A}/scale"): "own",
    ("POST", f"{_A}/close"): "own",
    ("GET", f"{_A}/init-scripts"): "own",
    # own + preset: the source preset is written only when it is in the session's project,
    # rechecked under the preset row lock (`db.sessions.set_init_scripts`)
    ("PUT", f"{_A}/init-scripts"): "own",
    ("POST", f"{_A}/leadership/handover"): "own",
    ("POST", f"{_A}/locks/claim"): "own",
    ("POST", f"{_A}/locks/release"): "own",
    ("GET", f"{_A}/locks"): "own",
    ("POST", f"{_A}/locks/reassign"): "own",
    ("POST", f"{_A}/locks/check"): "own",
    ("GET", f"{_A}/activity"): "own",
    ("PUT", f"{_A}/session/labels"): "own",
    # cross-session messaging
    ("GET", f"{_A}/sessions"): "peers",
    ("POST", f"{_A}/sessions/{{slug}}/messages"): "peers",
    # the project room: the caller's project, any project for a superleader
    ("GET", f"{_A}/project/messages"): "project",
    ("POST", f"{_A}/project/messages"): "project",
    # taskboard relay: forwards X-Orch-Projects, the taskboard filters (plan 3.6)
    ("GET", f"{_A}/tasks"): "project",
    ("POST", f"{_A}/tasks"): "project",
    ("PUT", f"{_A}/tasks"): "project",
    ("PATCH", f"{_A}/tasks"): "project",
    ("GET", f"{_A}/tasks/{{path:path}}"): "project",
    ("POST", f"{_A}/tasks/{{path:path}}"): "project",
    ("PUT", f"{_A}/tasks/{{path:path}}"): "project",
    ("PATCH", f"{_A}/tasks/{{path:path}}"): "project",
    ("GET", f"{_A}/projects/summary"): "project",
    # spec relay: same shape as the task relay (T-116)
    ("GET", f"{_A}/specs"): "project",
    ("POST", f"{_A}/specs"): "project",
    ("PUT", f"{_A}/specs"): "project",
    ("PATCH", f"{_A}/specs"): "project",
    ("GET", f"{_A}/specs/{{path:path}}"): "project",
    ("POST", f"{_A}/specs/{{path:path}}"): "project",
    ("PUT", f"{_A}/specs/{{path:path}}"): "project",
    ("PATCH", f"{_A}/specs/{{path:path}}"): "project",
    # mindmap relay: the service applies the read rule from the project header
    ("GET", f"{_A}/mindmap/{{path:path}}"): "project",
    ("POST", f"{_A}/mindmap/{{path:path}}"): "project",
    ("PATCH", f"{_A}/mindmap/{{path:path}}"): "project",
    ("DELETE", f"{_A}/mindmap/{{path:path}}"): "project",
    # the caller's projects, or every project for a superleader
    ("GET", f"{_A}/projects"): "project",
    # chat relay: project in the plan, but no filter is sent until the complete stage (X1)
    ("GET", f"{_A}/chat/rooms"): "project",
    ("POST", f"{_A}/chat/reactions"): "project",
    # a send and its delivery status are the caller's own; a send to a room bound to another
    # session posts in the occupier's name on relay (T-56, `chat_relay`)
    ("POST", f"{_A}/chat/messages"): "own",
    ("GET", f"{_A}/chat/messages/{{send_id}}"): "own",
    # room notes and skills: the upstream resolves the room from the caller session's own
    # binding, so a caller reaches only the room its session is bound to
    ("GET", f"{_A}/chat/notes"): "own",
    ("PUT", f"{_A}/chat/notes"): "own",
    ("GET", f"{_A}/chat/skills"): "own",
    ("GET", f"{_A}/chat/skills/{{name}}"): "own",
    ("PUT", f"{_A}/chat/skills/{{name}}"): "own",
    ("DELETE", f"{_A}/chat/skills/{{name}}"): "own",
    # session-spec-patch (`admin.session_router`): a leader edits its own session, a
    # superleader any it may manage
    ("GET", f"{_ADMIN}/sessions/{{slug}}"): "own",
    ("PATCH", f"{_ADMIN}/sessions/{{slug}}"): "own",
    ("POST", f"{_ADMIN}/sessions/{{slug}}/leadership/withdraw"): "own",
    # proposals: the proposer's own proposals only (`proposal_routes`); a superleader's GETs read
    # every proposal (T-120 slice 2), the scope class stays "own" for the write paths
    ("POST", f"{_ADMIN}/proposals"): "own",
    ("GET", f"{_ADMIN}/proposals"): "own",
    ("GET", f"{_ADMIN}/proposals/{{proposal_id}}"): "own",
    ("POST", f"{_ADMIN}/proposals/{{proposal_id}}/revise"): "own",
    ("POST", f"{_ADMIN}/proposals/{{proposal_id}}/withdraw"): "own",
    # admin: the superleader gate, unscoped
    ("GET", f"{_ADMIN}/presets"): "superleader",
    ("POST", f"{_ADMIN}/presets"): "superleader",
    ("GET", f"{_ADMIN}/presets/{{ref}}"): "superleader",
    ("PUT", f"{_ADMIN}/presets/{{ref}}"): "superleader",
    ("PATCH", f"{_ADMIN}/presets/{{ref}}"): "superleader",
    ("DELETE", f"{_ADMIN}/presets/{{ref}}"): "superleader",
    ("POST", f"{_ADMIN}/presets/{{ref}}/copy"): "superleader",
    ("GET", f"{_ADMIN}/vault"): "superleader",
    ("GET", f"{_ADMIN}/sessions"): "superleader",
    ("GET", f"{_ADMIN}/meta"): "superleader",
    ("GET", f"{_ADMIN}/archives"): "superleader",
    ("GET", f"{_ADMIN}/archives/settings"): "superleader",
    ("GET", f"{_ADMIN}/archives/cleanup/preview"): "superleader",
    ("PUT", f"{_ADMIN}/presets/{{ref}}/vault/{{entry_id}}"): "superleader",
    ("DELETE", f"{_ADMIN}/presets/{{ref}}/vault/{{entry_id}}"): "superleader",
    ("POST", f"{_ADMIN}/sessions"): "superleader",
    ("DELETE", f"{_ADMIN}/sessions/{{slug}}"): "superleader",
    ("POST", f"{_ADMIN}/sessions/{{slug}}/restart"): "superleader",
    ("POST", f"{_ADMIN}/sessions/{{slug}}/agents/{{pod}}/restart"): "superleader",
    ("GET", f"{_ADMIN}/sessions/{{slug}}/activity"): "superleader",
    ("GET", f"{_ADMIN}/sessions/{{slug}}/events"): "superleader",
    ("GET", f"{_ADMIN}/sessions/{{slug}}/messages"): "superleader",
    ("GET", f"{_ADMIN}/sessions/{{slug}}/locks"): "superleader",
    ("GET", f"{_ADMIN}/sessions/{{slug}}/commands"): "superleader",
    ("GET", f"{_ADMIN}/commands/{{command_id}}"): "superleader",
    ("GET", f"{_ADMIN}/usage"): "superleader",
    ("POST", f"{_ADMIN}/projects"): "superleader",
    # project grants (T-120): a grant files a proposal, a revoke is direct
    ("POST", f"{_ADMIN}/sessions/{{slug}}/projects"): "superleader",
    ("DELETE", f"{_ADMIN}/sessions/{{slug}}/projects/{{project}}"): "superleader",
    ("POST", f"{_ADMIN}/presets/{{ref}}/projects"): "superleader",
    ("DELETE", f"{_ADMIN}/presets/{{ref}}/projects/{{project}}"): "superleader",
    # admin relay (`admin_relay`): superleader-only forwards, sent with X-Orch-Projects: *
    ("GET", f"{_ADMIN}/webhooks/endpoints"): "superleader",
    ("POST", f"{_ADMIN}/webhooks/endpoints"): "superleader",
    ("PATCH", f"{_ADMIN}/webhooks/endpoints/{{id}}"): "superleader",
    ("DELETE", f"{_ADMIN}/webhooks/endpoints/{{id}}"): "superleader",
    ("POST", f"{_ADMIN}/webhooks/endpoints/{{id}}/clone"): "superleader",
    ("PUT", f"{_ADMIN}/webhooks/endpoints/{{id}}/filters"): "superleader",
    ("POST", f"{_ADMIN}/webhooks/endpoints/{{id}}/rotate"): "superleader",
    ("GET", f"{_ADMIN}/webhooks/events"): "superleader",
    ("GET", f"{_ADMIN}/webhooks/events/{{id}}"): "superleader",
    ("GET", f"{_ADMIN}/webhooks/meta"): "superleader",
    ("POST", f"{_ADMIN}/webhooks/templates/preview"): "superleader",
    ("GET", f"{_ADMIN}/schedules/runs"): "superleader",
    ("GET", f"{_ADMIN}/schedules/runs/{{run_id}}"): "superleader",
    ("GET", f"{_ADMIN}/schedules/schedules"): "superleader",
    ("POST", f"{_ADMIN}/schedules/schedules"): "superleader",
    ("POST", f"{_ADMIN}/schedules/schedules/preview"): "superleader",
    ("GET", f"{_ADMIN}/schedules/schedules/{{schedule_id}}"): "superleader",
    ("PUT", f"{_ADMIN}/schedules/schedules/{{schedule_id}}"): "superleader",
    ("DELETE", f"{_ADMIN}/schedules/schedules/{{schedule_id}}"): "superleader",
    ("POST", f"{_ADMIN}/schedules/schedules/{{schedule_id}}/run"): "superleader",
    ("GET", f"{_ADMIN}/schedules/schedules/{{schedule_id}}/runs"): "superleader",
    ("GET", f"{_ADMIN}/chats/rooms"): "superleader",
    ("GET", f"{_ADMIN}/chats/rooms/{{room_id}}"): "superleader",
    ("PUT", f"{_ADMIN}/chats/rooms/{{room_id}}/settings"): "superleader",
    ("POST", f"{_ADMIN}/chats/rooms/{{room_id}}/allow"): "superleader",
    ("GET", f"{_ADMIN}/chats/users"): "superleader",
    ("POST", f"{_ADMIN}/chats/room-requests"): "superleader",
    ("GET", f"{_ADMIN}/chats/room-requests/{{request_id}}"): "superleader",
    ("POST", f"{_ADMIN}/chats/room-requests/{{request_id}}/confirm"): "superleader",
    ("GET", f"{_ADMIN}/chats/rules"): "superleader",
    ("POST", f"{_ADMIN}/chats/rules"): "superleader",
    ("DELETE", f"{_ADMIN}/chats/rules/{{rule_id}}"): "superleader",
    ("GET", f"{_ADMIN}/history/sessions"): "superleader",
    ("GET", f"{_ADMIN}/history/sessions/{{session_id}}"): "superleader",
    ("GET", f"{_ADMIN}/history/sessions/{{session_id}}/records"): "superleader",
    ("GET", f"{_ADMIN}/usage/history"): "superleader",
    ("GET", f"{_ADMIN}/usage/sessions"): "superleader",
}
