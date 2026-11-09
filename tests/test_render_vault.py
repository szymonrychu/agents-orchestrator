"""O6: session env renders from the vault pins in `secret_versions`."""

from __future__ import annotations

import dataclasses

import pytest
from test_render import TOKENS, config_of, container_of, make_session, make_settings, sts_of

from agents_orchestrator.k8s import render
from agents_orchestrator.reconciler.init import render_init_job

ENTRY = "11111111-1111-4111-8111-111111111111"
REF = {"vault": ENTRY, "env": "TOKEN"}
SECRET_PIN = {"secret": f"vault-{ENTRY}-v3", "key": "TOKEN", "version": 3}


def sess(env, versions):
    return dataclasses.replace(make_session(env=env), secret_versions=versions)


def env_of(session):
    return {
        e["name"]: e
        for e in container_of(render.render_session(session, make_settings(), TOKENS))["env"]
    }


def test_render_vault_protected_pin():
    got = env_of(sess({"X": REF}, {"X": SECRET_PIN}))["X"]
    assert got == {
        "name": "X",
        "valueFrom": {"secretKeyRef": {"name": f"vault-{ENTRY}-v3", "key": "TOKEN"}},
    }


def test_render_readable_pin_escaped():
    got = env_of(sess({"Y": REF}, {"Y": {"value": "a$(B)$"}}))["Y"]
    assert got == {"name": "Y", "value": "a$$(B)$$"}


def test_render_ref_without_pin_raises():
    with pytest.raises(ValueError, match="vault variable X has no pin"):
        render.render_session(sess({"X": REF}, {}), make_settings(), TOKENS)


def test_render_entries_sorted_across_plain_and_pins():
    s = sess({"M": {"value": "1"}, "Z": REF, "A": REF}, {"Z": SECRET_PIN, "A": {"value": "v"}})
    assert [e["name"] for e in render.env_entries(s)] == ["A", "M", "Z"]


def test_render_legacy_pin_unchanged():
    old = dataclasses.replace(
        make_session(env={"API_KEY": {"protected": True}}),
        secret_versions={"API_KEY": "ps-00000000-0a0b0c-v2"},
    )
    want = {
        "name": "API_KEY",
        "valueFrom": {"secretKeyRef": {"name": "ps-00000000-0a0b0c-v2", "key": "value"}},
    }
    assert render.env_entries(old) == [want]
    assert sts_of(render.render_session(old, make_settings(), TOKENS))


def test_init_job_gets_the_pins_too():
    job = render_init_job(
        dataclasses.replace(
            sess({"X": REF}, {"X": SECRET_PIN}),
            spec=make_session(env={"X": REF}, init_scripts=["true"]).spec,
        ),
        make_settings(),
    )
    env = {e["name"]: e for e in job["spec"]["template"]["spec"]["containers"][0]["env"]}
    assert env["X"]["valueFrom"]["secretKeyRef"] == {"name": f"vault-{ENTRY}-v3", "key": "TOKEN"}


def test_pin_without_vault_ref_in_spec_is_ignored():
    s = sess({"X": {"value": "plain"}}, {"X": SECRET_PIN, "GONE": SECRET_PIN})
    assert render.env_entries(s) == [{"name": "X", "value": "plain"}]
    assert render.secret_env_names(s) == []


@pytest.mark.parametrize(
    "pin",
    [
        {"secret": "other-secret", "key": "TOKEN"},
        {"secret": f"vault-{ENTRY}-v3", "key": "bad-key"},
        {"secret": f"vault-{ENTRY}-v3"},
        {"secret": "vault-x-v3", "key": "TOKEN"},
        {"secret": 5, "key": "TOKEN"},
    ],
)
def test_invalid_pin_raises_with_env_name_only(pin):
    with pytest.raises(ValueError) as exc:
        render.env_entries(sess({"X": REF}, {"X": pin}))
    assert str(exc.value) == "vault variable X has an invalid pin"


def test_secret_env_lists_secret_pins_and_legacy_protected():
    s = dataclasses.replace(
        sess(
            {"Z": REF, "R": REF, "A": {"protected": True}, "P": {"value": "1"}},
            {"Z": SECRET_PIN, "R": {"value": "v"}, "A": "ps-00000000-0a0b0c-v2"},
        )
    )
    assert render.secret_env_names(s) == ["A", "Z"]
    cfg = config_of(render.render_session(s, make_settings(), TOKENS))
    assert cfg["claude"]["secret_env"] == ["A", "Z"]
    assert {"Z", "R", "A", "P"} <= set(cfg["claude"]["passthrough_env"])
