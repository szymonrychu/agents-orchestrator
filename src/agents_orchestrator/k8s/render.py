"""Render a session spec into the Kubernetes objects of design D6 (pure, stable order)."""

from __future__ import annotations

import hashlib
import json
import math
from typing import Any

import yaml

from agents_orchestrator.db.sessions import Session
from agents_orchestrator.dispatcher.hive import compact_pct
from agents_orchestrator.k8s.labels import object_names, prefix, selector, session_labels
from agents_orchestrator.k8s.on_start import render_on_start
from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import WORKDIR_ROOT, SessionSpec, WrapperSettings
from agents_orchestrator.tokens import SessionTokens

HASH_ANNOTATION = "agents.szymonrichert.pl/config-hash"
SESSIONS_ROOT = "/sessions"
WORKSPACE_VOLUME = "workspace"
PROBE_PERIOD = 5
STARTUP_SLACK = 12

# Always passed to the any-mcp stdio child; ORCH_AGENT_URL is listed even when unset (the wrapper
# passes through only variables that exist).
KUBE_PASSTHROUGH_ENV = ("KUBERNETES_SERVICE_HOST", "KUBERNETES_SERVICE_PORT")
AGENT_PASSTHROUGH_ENV = ("ORCH_AGENT_TOKEN", "ORCH_AGENT_URL", "ORCH_SESSION_ID", "POD_NAME")

ASK_IN_WRITING_PROMPT = (
    "The AskUserQuestion tool is disabled and nobody answers interactive questions. "
    "When you need a decision or information from the user, ask in writing in your reply: "
    "write a numbered list of questions, and after each question give your recommended answer, "
    'worded so that replying "yes" accepts it. Then stop and wait for the reply. '
    "Never wait on an interactive question tool."
)


def workdir_path(spec: SessionSpec) -> str:
    return f"{WORKDIR_ROOT}/{spec.workdir}"


def workdir_mount(spec: SessionSpec) -> dict[str, str]:
    """The shared workspace root mount; sessions and check Jobs work in workdir_path(spec)."""
    return {"name": WORKSPACE_VOLUME, "mountPath": WORKDIR_ROOT, "subPath": "workdir"}


def agent_identity(settings: Settings) -> dict[str, Any]:
    """Every agent pod and Job runs as a named ServiceAccount with no token mounted (D4)."""
    return {
        "serviceAccountName": settings.agent_service_account,
        "automountServiceAccountToken": False,
    }


def kube_access_granted(spec: SessionSpec, settings: Settings) -> bool:
    """A kube_access spec gets the privileged identity only while the setting is set (D3)."""
    return bool(spec.kube_access and settings.kube_access_service_account)


def pod_identity(spec: SessionSpec, settings: Settings) -> dict[str, Any]:
    """The agent StatefulSet pod identity: the privileged account with its token (D5), else D4."""
    if kube_access_granted(spec, settings):
        return {
            "serviceAccountName": settings.kube_access_service_account,
            "automountServiceAccountToken": True,
        }
    return agent_identity(settings)


def pod_url(slug: str, pod: str, settings: Settings) -> str:
    """The per-pod DNS base URL of one wrapper: <pod>.agent-<slug>.<namespace>.svc.<domain>."""
    return (
        f"http://{pod}.{prefix(slug)}.{settings.namespace}.svc.{settings.cluster_domain}"
        f":{settings.wrapper_port}"
    )


def _shutdown_timeout(grace: int) -> int:
    return max(grace - 10, grace // 2)


def _lock_check_hook() -> dict[str, Any]:
    """Advisory file-lock warning; the wrapper interpolates ${VAR} from the pod env."""
    return {
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


def _escape(value: Any) -> Any:
    """Double each `$` in every string of a user subtree (keys excluded).

    The wrapper interpolates `${VAR}` in config values; `$$` makes it yield the literal text, so a
    user value can never read the pod environment. A hook shell still sees `$VAR` at run time.
    """
    if isinstance(value, str):
        return value.replace("$", "$$")
    if isinstance(value, dict):
        return {k: _escape(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_escape(v) for v in value]
    return value


def _apply_wrapper(cfg: dict[str, Any], wrapper: WrapperSettings) -> None:
    """Merge `spec.wrapper` into the config after the reserved entries, which stay first."""
    user = _escape(wrapper.model_dump(mode="json", exclude_none=True))
    hooks = cfg["hooks"]
    for event, actions in user.get("hooks", {}).items():
        hooks.setdefault(event, []).extend(actions)
    perms = user.get("permissions", {})
    deny = cfg["claude"]["permissions"]["deny"]
    deny.extend(d for d in perms.get("deny", []) if d not in deny)
    if "allow" in perms:
        cfg["claude"]["permissions"]["allow"] = perms["allow"]
    cfg["claude"]["mcp_servers"].update(user.get("mcp_servers", {}))
    if "system_append" in user:
        cfg["prompts"]["system_append"] += "\n\n" + user["system_append"]
    sessions = cfg["sessions"]
    for key in ("auto_compact", "turn_stall_seconds", "max_restarts", "restart_continue_prompt"):
        if key in user:
            sessions[key] = user[key]
    if "agent_commands" in user:
        sessions["commands"]["agent"] = user["agent_commands"]
    if "turn_max_seconds" in user:
        cfg["limits"]["turn_max_seconds"] = user["turn_max_seconds"]
    if "plugins" in user:
        cfg["plugins"] = user["plugins"]


def _ccw_config(session: Session, settings: Settings) -> dict[str, Any]:
    spec, slug = session.spec, session.slug
    kube_env = KUBE_PASSTHROUGH_ENV if kube_access_granted(spec, settings) else ()
    claude: dict[str, Any] = {
        "passthrough_env": sorted([*spec.env, *AGENT_PASSTHROUGH_ENV, *kube_env]),
        "env": {
            "claude_code_oauth_token": "${CLAUDE_CODE_OAUTH_TOKEN}",
            "disable_autoupdater": True,
            "claude_autocompact_pct_override": compact_pct(spec.auto_compact_pct, settings),
        },
    }
    model = spec.model or settings.default_model
    effort = spec.effort or settings.default_effort
    if model:
        claude["model"] = model
    if effort:
        claude["effort"] = effort
    claude["main_agent_only_tools"] = ["mcp__any-mcp__orchestrator_*"]
    claude["permissions"] = {"deny": ["EnterPlanMode", "ExitPlanMode", "AskUserQuestion"]}
    claude["mcp_servers"] = {
        "any-mcp": {
            "type": "stdio",
            "command": "/opt/any-mcp/bin/any-mcp",
            "args": ["--config", "/opt/any-mcp/etc/any-mcp.yaml"],
        }
    }
    cfg: dict[str, Any] = {
        "server": {"auth_token": "${CCW_AUTH_TOKEN}"},
        "paths": {"workdir_root": WORKDIR_ROOT, "sessions_root": SESSIONS_ROOT},
        "claude": claude,
        "hooks": {
            "onStart": [
                {
                    "type": "command",
                    "run": "sh /etc/ccw/on-start.sh",
                    "blocking": True,
                    "on_error": "allow",
                    "timeout_seconds": settings.on_start_timeout_seconds,
                }
            ]
        },
        "sessions": {
            "max_sessions": 1,
            "resume_on_start": True,
            "idle_timeout_seconds": 0,
            "ask_user_question": "deny",
            "restart_continue_prompt": _escape(settings.default_restart_continue_prompt),
            # The orchestrator is the wrapper's only client: every client command is allowed.
            "commands": {"client": ["*"]},
        },
        "prompts": {"system_append": ASK_IN_WRITING_PROMPT},
        "limits": {
            "shutdown_timeout_seconds": _shutdown_timeout(settings.agent_termination_grace_seconds)
        },
    }
    cfg["notes"] = {"enabled": spec.hive}
    if spec.hive and settings.agent_api_url:
        cfg["hooks"]["onAgentToolCall"] = [_lock_check_hook()]
    if spec.peers and spec.hive:
        cfg["peers"] = {
            "enabled": True,
            "name": "${POD_NAME}",
            "advertise_url": "${CCW_PEER_URL}",
            "token": "${CCW_PEER_TOKEN}",
            "static": [
                pod_url(slug, f"{prefix(slug)}-{i}", settings)
                for i in range(spec.max_replicas or spec.replicas)
            ],
        }
    if spec.wrapper:
        _apply_wrapper(cfg, spec.wrapper)
    return cfg


def _meta(slug: str, kind_name: str, settings: Settings) -> dict[str, Any]:
    return {
        "name": kind_name,
        "namespace": settings.namespace,
        "labels": session_labels(slug),
    }


def _secret_ref(name: str, key: str) -> dict[str, Any]:
    return {"valueFrom": {"secretKeyRef": {"name": name, "key": key}}}


def _container(session: Session, settings: Settings, names: dict[str, str]) -> dict[str, Any]:
    spec, slug = session.spec, session.slug
    port = settings.wrapper_port
    env: list[dict[str, Any]] = [
        {"name": "POD_NAME", "valueFrom": {"fieldRef": {"fieldPath": "metadata.name"}}},
        {"name": "CCW_PEER_URL", "value": pod_url(slug, "$(POD_NAME)", settings)},
        {"name": "CCW_AUTH_TOKEN", **_secret_ref(names["secret"], "CCW_AUTH_TOKEN")},
        {"name": "CCW_PEER_TOKEN", **_secret_ref(names["secret"], "CCW_PEER_TOKEN")},
        {
            "name": "CLAUDE_CODE_OAUTH_TOKEN",
            **_secret_ref(settings.claude_secret_name, "CLAUDE_CODE_OAUTH_TOKEN"),
        },
        {"name": "ORCH_AGENT_TOKEN", **_secret_ref(names["secret"], "ORCH_AGENT_TOKEN")},
        {"name": "ORCH_SESSION_ID", "value": str(session.id)},
        *(
            [{"name": "ORCH_AGENT_URL", "value": settings.agent_api_url}]
            if settings.agent_api_url
            else []
        ),
        *({"name": k, "value": v} for k, v in sorted(spec.env.items())),
    ]
    startup_failures = math.ceil(settings.on_start_timeout_seconds / PROBE_PERIOD) + STARTUP_SLACK
    container: dict[str, Any] = {
        "name": "ccw",
        "image": spec.image or settings.agent_image,
        "ports": [{"name": "http", "containerPort": port}],
        "env": env,
        "startupProbe": {
            "httpGet": {"path": "/healthz", "port": port},
            "periodSeconds": PROBE_PERIOD,
            "failureThreshold": startup_failures,
        },
        "livenessProbe": {"httpGet": {"path": "/healthz", "port": port}, "periodSeconds": 10},
        "readinessProbe": {"httpGet": {"path": "/readyz", "port": port}, "periodSeconds": 10},
        "volumeMounts": [
            workdir_mount(spec),
            {
                "name": WORKSPACE_VOLUME,
                "mountPath": SESSIONS_ROOT,
                "subPathExpr": "sessions/$(POD_NAME)",
            },
            {"name": "mise", "mountPath": "/opt/mise"},
            {"name": "any-mcp", "mountPath": "/opt/any-mcp"},
            {"name": "config", "mountPath": "/etc/ccw"},
        ],
    }
    if spec.resources is not None:
        resources = {k: v for k, v in spec.resources.model_dump().items() if v}
        if resources:
            container["resources"] = resources
    return container


def render_session(
    session: Session, settings: Settings, tokens: SessionTokens
) -> list[dict[str, Any]]:
    spec, slug = session.spec, session.slug
    names = object_names(slug)
    labels = session_labels(slug)

    config_data = {
        "config.yaml": yaml.safe_dump(_ccw_config(session, settings), sort_keys=False),
        "on-start.sh": render_on_start(spec, settings, slug, chat=not session.trigger_created),
    }
    config_hash = hashlib.sha256(json.dumps(config_data, sort_keys=True).encode()).hexdigest()

    pvc = {
        "apiVersion": "v1",
        "kind": "PersistentVolumeClaim",
        "metadata": _meta(slug, names["pvc"], settings),
        "spec": {
            "accessModes": ["ReadWriteMany"],
            "storageClassName": settings.pvc_storage_class,
            "resources": {"requests": {"storage": spec.pvc_size or settings.pvc_size}},
        },
    }
    secret = {
        "apiVersion": "v1",
        "kind": "Secret",
        "metadata": _meta(slug, names["secret"], settings),
        "type": "Opaque",
        "stringData": {
            "CCW_AUTH_TOKEN": tokens.auth,
            "CCW_PEER_TOKEN": tokens.peer,
            "ORCH_AGENT_TOKEN": tokens.agent,
        },
    }
    configmap = {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": _meta(slug, names["configmap"], settings),
        "data": config_data,
    }
    service = {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": _meta(slug, names["service"], settings),
        "spec": {
            "clusterIP": "None",
            "publishNotReadyAddresses": True,
            "selector": dict(labels),
            "ports": [
                {
                    "name": "http",
                    "port": settings.wrapper_port,
                    "targetPort": settings.wrapper_port,
                }
            ],
        },
    }
    pod_spec: dict[str, Any] = {
        **pod_identity(spec, settings),
        "securityContext": {"runAsUser": 1000, "fsGroup": 1000},
        "terminationGracePeriodSeconds": settings.agent_termination_grace_seconds,
        "containers": [_container(session, settings, names)],
        "volumes": [
            {"name": WORKSPACE_VOLUME, "persistentVolumeClaim": {"claimName": names["pvc"]}},
            {"name": "config", "configMap": {"name": names["configmap"]}},
            {"name": "mise", "emptyDir": {}},
            {"name": "any-mcp", "emptyDir": {}},
        ],
    }
    if settings.image_pull_secret:
        pod_spec = {"imagePullSecrets": [{"name": settings.image_pull_secret}], **pod_spec}
    statefulset = {
        "apiVersion": "apps/v1",
        "kind": "StatefulSet",
        "metadata": _meta(slug, names["statefulset"], settings),
        "spec": {
            "replicas": spec.replicas,
            "podManagementPolicy": "Parallel",
            # The controller never replaces a pod; the reconciler's replacer rolls idle ones
            # (reconciler/rollout.py). In this object, so it reaches Kubernetes in the same
            # server-side apply as any template change of the release that switches to it.
            "updateStrategy": {"type": "OnDelete"},
            "serviceName": names["service"],
            "selector": selector(slug),
            "template": {
                "metadata": {"labels": dict(labels), "annotations": {HASH_ANNOTATION: config_hash}},
                "spec": pod_spec,
            },
        },
    }
    return [pvc, secret, configmap, service, statefulset]
