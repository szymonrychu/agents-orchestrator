"""`orch presets-check`: lists the stored presets a session create would refuse, without values."""

from __future__ import annotations

import io
import secrets as stdlib_secrets

import psycopg
from psycopg.types.json import Jsonb
from test_api_sessions import make_settings

from agents_orchestrator import cli, presets_check

HOOK = {"hooks": {"onStart": [{"type": "webhook", "url": "http://h/x", "secret": "${GONE}"}]}}


def seed(dsn, name, spec):
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute(
            "INSERT INTO presets (id, name, spec) VALUES (gen_random_uuid(), %s, %s)",
            (name, Jsonb(spec)),
        )


def run(dsn, settings=None):
    out = io.StringIO()
    code = presets_check.run(dsn, settings or make_settings(dsn), out)
    return code, out.getvalue()


def test_valid_presets_pass(dsn):
    seed(dsn, "good", {"env": {"A": {"value": "1"}, "K": {"protected": True}}})
    code, text = run(dsn)
    assert code == 0, text
    assert "0 would fail" in text


def test_missing_reference_is_listed_by_preset_and_field(dsn):
    seed(dsn, "bad-hook", {"wrapper": HOOK, "env": {"OTHER": {"value": "v"}}})
    seed(dsn, "good", {})
    code, text = run(dsn)
    assert code == 1
    assert "bad-hook" in text and "GONE" in text
    assert "good:" not in text and "1 would fail" in text


def test_plain_string_env_is_listed(dsn):
    seed(dsn, "legacy", {"env": {"TOKEN": "plain"}})
    code, text = run(dsn)
    assert code == 1
    assert "env.TOKEN must be an object {value, protected, expand}" in text


def test_no_value_is_ever_printed(dsn):
    canary = "canary-" + stdlib_secrets.token_hex(16)
    seed(
        dsn,
        "leaky",
        {
            "env": {"TOKEN": canary, "OK": {"value": canary + "-b"}, "BAD NAME": {"value": canary}},
            "wrapper": HOOK,
        },
    )
    code, text = run(dsn)
    assert code == 1
    assert canary not in text


def test_a_setting_that_left_the_allow_list_is_listed(dsn):
    seed(dsn, "old-model", {"model": "retired-model"})
    code, text = run(dsn, make_settings(dsn, models="m1,m2"))
    assert code == 1 and "old-model" in text


def test_command_is_registered_and_prints(dsn, monkeypatch, capsys):
    seed(dsn, "bad", {"wrapper": HOOK})
    monkeypatch.setattr(cli, "_settings", lambda: make_settings(dsn))
    assert cli.main(["presets-check"]) == 1
    assert "bad" in capsys.readouterr().out
