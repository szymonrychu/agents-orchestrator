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
    assert set(sample) == {"project", "session_slug", "preset_name", "now", "date", "user"}
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
    assert render_name("fixed", ctx) == "fixed"
    got = render_name("{{ preset_name }}/{{ now }}", ctx)
    assert got == "default/20260101t000000z"


@pytest.mark.parametrize("template", ["Bad Name", "sessions/x", "a//b", "{{ session_slug }}/../x"])
def test_render_name_validates_the_output(template):
    with pytest.raises(ArchiveNameError):
        render_name(template, SAMPLES["archive_name"])


def test_render_name_empty_output_is_an_error():
    with pytest.raises(ArchiveNameError):
        render_name("{{ user }}", {**SAMPLES["archive_name"], "user": ""})


def test_render_name_unknown_variable_is_strict():
    with pytest.raises(ArchiveNameError) as err:
        render_name("{{ nope }}", SAMPLES["archive_name"])
    assert "nope" in str(err.value)


def test_render_name_syntax_error_carries_the_line():
    with pytest.raises(ArchiveNameError) as err:
        render_name("ok\n{% if %}", SAMPLES["archive_name"])
    assert err.value.line == 2


def test_render_name_sandbox_blocks_attribute_escapes():
    with pytest.raises(ArchiveNameError):
        render_name("{{ ''.__class__ }}", SAMPLES["archive_name"])


def test_project_and_slug_in_context():
    sample = SAMPLES["archive_name"]
    ctx = {**sample, "project": "spellslinger", "preset_name": "mtg-group"}
    assert render_name("{{ project }}/skills", ctx) == "spellslinger/skills"
    assert render_name("{{ preset_name | slug }}", {**ctx, "preset_name": "MTG Group"}) == (
        "mtg-group"
    )
    built = rendering.archive_context("s", "MTG Group", "u", NOW, "Spell Slinger")
    assert built["project"] == "spell-slinger"
    assert "project" in sample


def test_sha256_call_detected():
    template = "k-{{ sha256('skills', 8) }}-{{ sha256('rules') }}"
    assert rendering.sha256_calls(template) == [("skills", 8), ("rules", 16)]
    assert rendering.sha256_calls("plain") == []
    with pytest.raises(rendering.NeedsDigests) as exc:
        render_name(template, SAMPLES["archive_name"])
    assert exc.value.calls == [("skills", 8), ("rules", 16)]


def test_sha256_substituted():
    digests = {"skills:8": "abcdef01", "rules:16": "0123456789abcdef"}
    got = render_name(
        "k-{{ sha256('skills', 8) }}-{{ sha256('rules') }}", SAMPLES["archive_name"], digests
    )
    assert got == "k-abcdef01-0123456789abcdef"


def test_sha256_without_a_digest_fails_and_bad_args_fail():
    with pytest.raises(ArchiveNameError):
        render_name("{{ sha256('x', 8) }}", SAMPLES["archive_name"], {})
    for bad in ("sha256('x', 7)", "sha256('x', 63)", "sha256('/x')", "sha256('../x')"):
        with pytest.raises(ArchiveNameError):
            render_name("{{ " + bad + " }}", SAMPLES["archive_name"], {"x:16": "a" * 16})
