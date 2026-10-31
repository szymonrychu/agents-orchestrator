"""Jinja rendering of spec text fields: contexts per surface, render time, 422 on bad templates."""

from __future__ import annotations

import pytest
from test_api_sessions import assert_error, client_for
from test_api_sessions import make_settings as api_settings
from test_render import config_of, make_session, make_settings

from agents_orchestrator import rendering
from agents_orchestrator.api import templates
from agents_orchestrator.api.app import create_app
from agents_orchestrator.k8s import render
from agents_orchestrator.spec import SessionSpec, SpecInvalid, validate_spec
from agents_orchestrator.tokens import tokens_for

P = "/api/v1/presets"


def cfg_for(**spec):
    session = make_session("alpha", **spec)
    settings = make_settings()
    return config_of(render.render_session(session, settings, tokens_for(settings, session.id)))


def test_samples_are_the_one_definition():
    assert templates.SAMPLES is rendering.SAMPLES


def test_context_keys_equal_the_preview_samples():
    session = make_session("alpha", env={"A": "b"})
    for surface in ("preset_text", "init_script"):
        ctx = rendering.session_context(surface, session)
        assert set(ctx) == set(templates.SAMPLES[surface])
    ctx = rendering.session_context("preset_text", session)
    assert ctx["session_slug"] == "alpha"
    assert ctx["env"] == {"A": "b"}
    assert ctx["now"] == "2026-10-23T00:00:00+00:00"


def test_wrapper_text_fields_are_rendered():
    cfg = cfg_for(
        env={"TZ": "UTC"},
        wrapper={
            "auto_compact": {"instructions": "keep notes of {{ session_slug }} ({{ env.TZ }})"},
            "system_append": "You are in {{ session_slug }}.",
            "restart_continue_prompt": "continue {{ preset_name }}",
        },
    )
    assert cfg["sessions"]["auto_compact"]["instructions"] == "keep notes of alpha (UTC)"
    assert cfg["prompts"]["system_append"].endswith("\n\nYou are in alpha.")
    assert cfg["sessions"]["restart_continue_prompt"] == "continue "


def test_literal_braces_use_the_escape_rule():
    cfg = cfg_for(wrapper={"system_append": "write {{ '{{' }}name}} for a hole"})
    assert cfg["prompts"]["system_append"].endswith("write {{name}} for a hole")


def test_rendered_text_is_dollar_escaped_after_rendering():
    cfg = cfg_for(env={"X": "${CLAUDE_CODE_OAUTH_TOKEN}"}, wrapper={"system_append": "{{ env.X }}"})
    assert cfg["prompts"]["system_append"].endswith("$${CLAUDE_CODE_OAUTH_TOKEN}")


def test_render_is_stable_so_the_config_hash_does_not_move():
    spec = {"wrapper": {"system_append": "at {{ now }}"}}
    assert cfg_for(**spec) == cfg_for(**spec)


def test_plain_text_is_unchanged():
    cfg = cfg_for(wrapper={"system_append": "plain $HOME text\n"})
    assert cfg["prompts"]["system_append"].endswith("plain $$HOME text\n")


def test_start_prompt_is_a_sparse_spec_field():
    assert SessionSpec().start_prompt is None
    spec = SessionSpec.model_validate({"start_prompt": "hello {{ user }}"})
    assert spec.start_prompt == "hello {{ user }}"
    assert "start_prompt" not in SessionSpec().model_dump(exclude_unset=True)


def invalid(**spec):
    with pytest.raises(SpecInvalid) as exc:
        validate_spec(SessionSpec.model_validate(spec), make_settings())
    return "; ".join(exc.value.errors)


def test_undeclared_variable_fails_validation():
    assert "nope" in invalid(wrapper={"system_append": "{{ nope }}"})
    assert "wrapper.auto_compact.instructions" in invalid(
        wrapper={"auto_compact": {"instructions": "{{ user }}"}}
    )
    assert "init_scripts[1]" in invalid(init_scripts=["echo ok", "echo {{ now }}"])
    assert "start_prompt" in invalid(start_prompt="{{ env }}")


def test_syntax_error_fails_validation_with_line():
    msg = invalid(wrapper={"restart_continue_prompt": "ok\n{% if %}"})
    assert "wrapper.restart_continue_prompt" in msg and "line 2" in msg


def test_render_error_at_sample_fails_validation():
    assert "MISSING" in invalid(env={"A": "b"}, wrapper={"system_append": "{{ env.MISSING }}"})


def test_instructions_stay_one_line_after_rendering():
    msg = invalid(wrapper={"auto_compact": {"instructions": "{{ '\\n' }}x"}})
    assert "wrapper.auto_compact.instructions" in msg


def test_valid_templates_pass():
    validate_spec(
        SessionSpec.model_validate(
            {
                "env": {"TZ": "UTC"},
                "start_prompt": "hi {{ user }} from {{ preset_name }}",
                "init_scripts": ["echo {{ session_slug }} {{ env.TZ }}"],
                "wrapper": {
                    "system_append": "{{ now }}",
                    "auto_compact": {"instructions": "{{ session_slug }}"},
                    "restart_continue_prompt": "go {{ preset_name }}",
                },
            }
        ),
        make_settings(),
    )


async def test_api_422_on_undeclared_variable(pool, dsn):
    app = create_app(api_settings(dsn, max_replicas=4), pool)
    async with client_for(app) as http:
        r = await http.post(
            P, json={"name": "bad", "spec": {"wrapper": {"system_append": "{{ nope }}"}}}
        )
        assert_error(r, 422, "invalid_request")
        assert "nope" in r.json()["error"]["message"]


async def test_preset_round_trips_start_prompt_sparse(pool, dsn):
    app = create_app(api_settings(dsn, max_replicas=4), pool)
    async with client_for(app) as http:
        r = await http.post(P, json={"name": "sp", "spec": {"start_prompt": "hi {{ user }}"}})
        assert r.status_code == 201, r.text
        assert r.json()["spec"] == {"start_prompt": "hi {{ user }}"}
        r = await http.post(P, json={"name": "plain", "spec": {}})
        assert r.json()["spec"] == {}
