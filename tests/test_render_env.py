"""O4: how env variables render (protected -> secretKeyRef, expand gates `$`, ${NAME} refs)."""

from __future__ import annotations

import dataclasses
import secrets as stdlib_secrets

import pytest
import yaml
from test_render import (
    TOKENS,
    config_of,
    container_of,
    make_session,
    make_settings,
)

from agents_orchestrator.k8s import render
from agents_orchestrator.reconciler.init import render_init_job

VERSIONS = {"API_KEY": "ps-12345678-abcdef-v2"}


def sess(env, versions=None, **spec):
    s = make_session(env=env, **spec)
    return dataclasses.replace(s, secret_versions=VERSIONS if versions is None else versions)


def env_of(container):
    return {e["name"]: e for e in container["env"]}


ENV = {
    "API_KEY": {"protected": True},
    "LIT": {"value": "a$b ${X} $$"},
    "EXP": {"value": "$(LIT)-${X}", "expand": True},
}


def test_protected_renders_secret_key_ref_on_the_pod():
    objs = render.render_session(sess(ENV), make_settings(), TOKENS)
    got = env_of(container_of(objs))["API_KEY"]
    assert got == {
        "name": "API_KEY",
        "valueFrom": {"secretKeyRef": {"name": "ps-12345678-abcdef-v2", "key": "value"}},
    }
    assert "value" not in got


def test_protected_renders_secret_key_ref_on_the_init_job():
    job = render_init_job(sess(ENV, init_scripts=["true"]), make_settings())
    got = env_of(job["spec"]["template"]["spec"]["containers"][0])["API_KEY"]
    assert got["valueFrom"]["secretKeyRef"] == {"name": "ps-12345678-abcdef-v2", "key": "value"}


def test_expand_false_is_dollar_escaped_and_expand_true_is_not():
    for container in (
        container_of(render.render_session(sess(ENV), make_settings(), TOKENS)),
        render_init_job(sess(ENV, init_scripts=["true"]), make_settings())["spec"]["template"][
            "spec"
        ]["containers"][0],
    ):
        env = env_of(container)
        assert env["LIT"]["value"] == "a$$b $${X} $$$$"
        assert env["EXP"]["value"] == "$(LIT)-${X}"


def test_protected_names_are_passed_through_to_claude():
    cfg = config_of(render.render_session(sess(ENV), make_settings(), TOKENS))
    assert {"API_KEY", "LIT", "EXP"} <= set(cfg["claude"]["passthrough_env"])


def test_missing_secret_snapshot_is_refused_not_dropped():
    with pytest.raises(ValueError, match="API_KEY"):
        render.render_session(sess(ENV, versions={}), make_settings(), TOKENS)


def test_refs_in_hook_and_mcp_fields_are_emitted_unescaped():
    wrapper = {
        "hooks": {
            "onStart": [
                {
                    "type": "webhook",
                    "url": "http://h/$x",
                    "secret": "${API_KEY}",
                    "headers": {"Authorization": "Bearer ${API_KEY}", "X": "${OTHER:-d} $$ ${LIT}"},
                }
            ]
        },
        "mcp_servers": {
            "s": {"type": "stdio", "command": "c", "env": {"K": "${API_KEY}", "L": "a$b"}},
            "r": {"type": "http", "url": "http://m/x", "headers": {"H": "t ${API_KEY}"}},
        },
    }
    env = {**ENV, "OTHER": {"value": "o"}}
    cfg = config_of(render.render_session(sess(env, wrapper=wrapper), make_settings(), TOKENS))
    hook = cfg["hooks"]["onStart"][-1]
    assert hook["secret"] == "${API_KEY}"
    assert hook["headers"]["Authorization"] == "Bearer ${API_KEY}"
    # a default form naming an env variable stays a reference; `$$` stays an escape pair
    assert hook["headers"]["X"] == "${OTHER:-d} $$ ${LIT}"
    assert hook["url"] == "http://h/$$x"  # other fields stay escaped
    servers = cfg["claude"]["mcp_servers"]
    assert servers["s"]["env"] == {"K": "${API_KEY}", "L": "a$$b"}
    assert servers["r"]["headers"] == {"H": "t ${API_KEY}"}


def test_platform_refs_stay_escaped_so_a_user_cannot_read_pod_secrets():
    wrapper = {
        "hooks": {
            "onStart": [
                {"type": "webhook", "url": "http://h/x", "secret": "${CLAUDE_CODE_OAUTH_TOKEN}"}
            ]
        }
    }
    cfg = config_of(render.render_session(sess(ENV, wrapper=wrapper), make_settings(), TOKENS))
    assert cfg["hooks"]["onStart"][-1]["secret"] == "$${CLAUDE_CODE_OAUTH_TOKEN}"


def test_refs_in_other_text_fields_stay_escaped():
    wrapper = {"system_append": "token ${API_KEY}"}
    cfg = config_of(render.render_session(sess(ENV, wrapper=wrapper), make_settings(), TOKENS))
    assert "$${API_KEY}" in cfg["prompts"]["system_append"]


def test_the_wrapper_expands_the_rendered_reference(tmp_path, monkeypatch):
    from claude_code_wrapper.config import load_config

    canary = stdlib_secrets.token_hex(12)
    monkeypatch.setenv("API_KEY", canary)
    for k in ("CCW_AUTH_TOKEN", "CCW_PEER_TOKEN", "CLAUDE_CODE_OAUTH_TOKEN", "ORCH_AGENT_URL"):
        monkeypatch.setenv(k, "x" * 20)
    monkeypatch.setenv("ORCH_AGENT_TOKEN", "x" * 20)
    monkeypatch.setenv("ORCH_SESSION_ID", "s")
    wrapper = {
        "hooks": {
            "onStart": [
                {"type": "webhook", "url": "http://h/x", "secret": "${API_KEY}", "headers": {}}
            ]
        }
    }
    cfg = config_of(render.render_session(sess(ENV, wrapper=wrapper), make_settings(), TOKENS))
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(cfg))
    loaded = load_config(path)
    assert loaded.hooks.onStart[-1].secret == canary
