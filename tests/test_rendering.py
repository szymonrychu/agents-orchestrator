"""The `archive_name` surface: context, sample and the render + grammar check of a name."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from test_render import make_session

from agents_orchestrator import rendering
from agents_orchestrator.cache.keys import ArchiveNameError, validate_name
from agents_orchestrator.rendering import SAMPLES, render_name

NOW = datetime(2026, 10, 31, 12, 0, 0, tzinfo=UTC)


def test_archive_name_sample_keys_and_values():
    sample = SAMPLES["archive_name"]
    assert set(sample) == {"session_slug", "preset_name", "now", "date", "user"}
    assert sample["now"] == "20260101t000000z"
    assert sample["date"] == "2026-01-01"
    # the sample itself satisfies the segment alphabet, so a preview shows what will be accepted
    for key in ("session_slug", "preset_name", "user"):
        validate_name(sample[key], allow_prefix=False)


def test_format_now_is_fixed_width_utc_and_sortable():
    assert rendering.format_now(NOW) == "20261031t120000z"
    early = rendering.format_now(datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC))
    assert len(early) == len(rendering.format_now(NOW))
    assert early < rendering.format_now(NOW)


def test_session_context_for_archive_name():
    session = make_session("alpha")
    ctx = rendering.session_context("archive_name", session)
    assert set(ctx) == set(SAMPLES["archive_name"])
    assert ctx["session_slug"] == "alpha"
    assert ctx["now"] == "20261023t000000z"  # session creation time
    assert ctx["date"] == "2026-10-23"
    assert ctx["user"] == ""


def test_session_context_now_override_is_the_wall_clock():
    ctx = rendering.session_context("archive_name", make_session("alpha"), now=NOW)
    assert ctx["now"] == "20261031t120000z"
    assert ctx["date"] == "2026-10-31"


def test_session_context_slugifies_preset_name_and_user():
    session = make_session("alpha")
    session = replace(session, preset_name="My Preset (v2)!", created_by_service="Svc/Team")
    ctx = rendering.session_context("archive_name", session)
    assert ctx["preset_name"] == "my-preset-v2"
    assert ctx["user"] == "svc-team"
    validate_name(ctx["preset_name"], allow_prefix=False)


def test_session_context_slug_of_nothing_usable_is_empty():
    session = replace(make_session("alpha"), preset_name="!!!")
    assert rendering.session_context("archive_name", session)["preset_name"] == ""


def test_render_name_plain_and_templated():
    ctx = SAMPLES["archive_name"]
    assert render_name("fixed", ctx, allow_prefix=False) == "fixed"
    got = render_name("{{ preset_name }}/{{ now }}", ctx, allow_prefix=False)
    assert got == "default/20260101t000000z"


def test_render_name_prefix_form_only_when_allowed():
    ctx = SAMPLES["archive_name"]
    assert render_name("{{ preset_name }}/", ctx, allow_prefix=True) == "default/"
    with pytest.raises(ArchiveNameError):
        render_name("{{ preset_name }}/", ctx, allow_prefix=False)


@pytest.mark.parametrize("template", ["Bad Name", "sessions/x", "a//b", "{{ session_slug }}/../x"])
def test_render_name_validates_the_output(template):
    with pytest.raises(ArchiveNameError):
        render_name(template, SAMPLES["archive_name"], allow_prefix=False)


def test_render_name_empty_output_is_an_error():
    with pytest.raises(ArchiveNameError):
        render_name("{{ user }}", {**SAMPLES["archive_name"], "user": ""}, allow_prefix=False)


def test_render_name_unknown_variable_is_strict():
    with pytest.raises(ArchiveNameError) as err:
        render_name("{{ nope }}", SAMPLES["archive_name"], allow_prefix=False)
    assert "nope" in str(err.value)


def test_render_name_syntax_error_carries_the_line():
    with pytest.raises(ArchiveNameError) as err:
        render_name("ok\n{% if %}", SAMPLES["archive_name"], allow_prefix=False)
    assert err.value.line == 2


def test_render_name_sandbox_blocks_attribute_escapes():
    with pytest.raises(ArchiveNameError):
        render_name("{{ ''.__class__ }}", SAMPLES["archive_name"], allow_prefix=False)
