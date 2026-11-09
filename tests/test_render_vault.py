"""O6: session env renders from the vault pins in `secret_versions`."""

from __future__ import annotations

import dataclasses

import pytest
from test_render import TOKENS, container_of, make_session, make_settings, sts_of

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
    legacy = {"secret": "ps-legacy-v2", "key": "value", "version": 2}
    migrated = sess({}, {"API_KEY": legacy})
    old = dataclasses.replace(
        make_session(env={"API_KEY": {"protected": True}}),
        secret_versions={"API_KEY": "ps-legacy-v2"},
    )
    want = {
        "name": "API_KEY",
        "valueFrom": {"secretKeyRef": {"name": "ps-legacy-v2", "key": "value"}},
    }
    assert render.env_entries(migrated) == [want] == render.env_entries(old)
    assert sts_of(render.render_session(migrated, make_settings(), TOKENS)) == sts_of(
        render.render_session(old, make_settings(), TOKENS)
    )


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
