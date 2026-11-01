"""C6: usage forwarding rendered into agent pods (claude-code-wrapper >= 0.8.0 `usage` section)."""

from __future__ import annotations

import copy

import yaml
from test_render import (
    IMAGE,
    TOKENS,
    by_kind,
    config_of,
    container_of,
    make_session,
    make_settings,
    sts_of,
)

from agents_orchestrator.k8s import render

URL = "http://agents-usage:8002"
SECRET = "agents-usage-ingest"
ON = {"agent_usage_url": URL, "agent_usage_secret": SECRET}
OTHER_IMAGE = "harbor.example/containers/claude-code-wrapper:0.7.0"


def env_names(objs) -> list[str]:
    return [e["name"] for e in container_of(objs)["env"]]


def rendered(session=None, **settings):
    return render.render_session(session or make_session(), make_settings(**settings), TOKENS)


def config_hash(objs) -> str:
    return sts_of(objs)["spec"]["template"]["metadata"]["annotations"][render.HASH_ANNOTATION]


def without_usage(objs, base):
    """The ON render with the usage env var, config section and its hash effect removed."""
    objs = copy.deepcopy(objs)
    annotations = sts_of(objs)["spec"]["template"]["metadata"]["annotations"]
    annotations[render.HASH_ANNOTATION] = config_hash(base)
    container = container_of(objs)
    container["env"] = [e for e in container["env"] if e["name"] != "CCW_USAGE_TOKEN"]
    cm = by_kind(objs)["ConfigMap"]
    config = yaml.safe_load(cm["data"]["config.yaml"])
    del config["usage"]
    cm["data"]["config.yaml"] = yaml.safe_dump(config, sort_keys=False)
    return objs


def test_off_by_default():
    objs = rendered()
    assert "CCW_USAGE_TOKEN" not in env_names(objs)
    assert "usage" not in config_of(objs)
    assert "usage" not in yaml.safe_load(by_kind(objs)["ConfigMap"]["data"]["config.yaml"])
    assert without_usage(rendered(**ON), objs) == objs


def test_on_for_default_image():
    objs = rendered(**ON)
    env = {e["name"]: e for e in container_of(objs)["env"]}
    assert env["CCW_USAGE_TOKEN"] == {
        "name": "CCW_USAGE_TOKEN",
        "valueFrom": {"secretKeyRef": {"name": SECRET, "key": "token"}},
    }
    names = env_names(objs)
    assert names.index("CCW_USAGE_TOKEN") == names.index("CLAUDE_CODE_OAUTH_TOKEN") + 1
    assert config_of(objs)["usage"] == {
        "url": URL,
        "token": "${CCW_USAGE_TOKEN}",
        "agent": "${POD_NAME}",
    }


def test_on_for_explicit_default_image():
    objs = rendered(make_session(image=IMAGE), agent_images=[IMAGE], **ON)
    assert "CCW_USAGE_TOKEN" in env_names(objs)
    assert config_of(objs)["usage"]["url"] == URL


def test_off_for_other_image():
    session = make_session(image=OTHER_IMAGE)
    objs = rendered(session, agent_images=[OTHER_IMAGE], **ON)
    assert "CCW_USAGE_TOKEN" not in env_names(objs)
    assert "usage" not in config_of(objs)
    settings = make_settings(agent_images=[OTHER_IMAGE], **ON)
    assert render.forwards_usage(session.spec, settings) is False


def test_forwards_usage_rule():
    on = make_settings(**ON)
    assert render.forwards_usage(make_session().spec, on) is True
    assert render.forwards_usage(make_session(image=IMAGE).spec, on) is True
    assert render.forwards_usage(make_session().spec, make_settings()) is False


def test_token_value_never_rendered():
    objs = rendered(**ON)
    assert config_of(objs)["usage"]["token"] == "${CCW_USAGE_TOKEN}"
    assert "value" not in {e["name"]: e for e in container_of(objs)["env"]}["CCW_USAGE_TOKEN"]
    assert "CCW_USAGE_TOKEN" not in by_kind(objs)["Secret"]["stringData"]


def test_not_passed_to_claude():
    objs = rendered(**ON)
    assert "CCW_USAGE_TOKEN" not in config_of(objs)["claude"]["passthrough_env"]


def test_config_hash_changes_when_enabled():
    base = config_hash(rendered())
    assert config_hash(rendered()) == base
    assert config_hash(rendered(**ON)) != base
    other = make_session(image=OTHER_IMAGE)
    assert config_hash(rendered(other, agent_images=[OTHER_IMAGE], **ON)) == config_hash(
        rendered(other, agent_images=[OTHER_IMAGE])
    )
