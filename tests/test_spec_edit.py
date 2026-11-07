"""Shared spec-edit helpers: the agent guard, the merge with the secret check, the audit note."""

from __future__ import annotations

import pytest
from fastapi import Request
from fastapi.exceptions import RequestValidationError

from agents_orchestrator.api.errors import ApiError
from agents_orchestrator.api.spec_edit import (
    guard_agent_edit,
    merged_spec_in,
    note_fields,
    secret_in_patch,
)


def make_request() -> Request:
    return Request({"type": "http"})


def test_note_fields_sorts_paths():
    request = make_request()
    note_fields(request, ["b", "a"])
    assert request.state.preset_fields == ["a", "b"]


def test_guard_not_privileged_passes_mcp_edit_on_kube_access_base():
    base = {"kube_cluster_access": True, "start_prompt": "a"}
    request = make_request()
    guard_agent_edit(base, {**base, "start_prompt": "b"}, [], request, privileged=False)
    assert not hasattr(request.state, "preset_fields")


def test_guard_not_privileged_still_refuses_grant_turned_on():
    request = make_request()
    with pytest.raises(ApiError) as caught:
        guard_agent_edit({}, {"kube_cluster_access": True}, [], request, privileged=False)
    assert caught.value.status == 403
    assert caught.value.code == "human_only_field"
    assert request.state.preset_fields == ["kube_cluster_access"]


def test_guard_privileged_refuses_a_prompt_change_on_a_privileged_base():
    base = {"kube_cluster_access": True, "start_prompt": "a"}
    request = make_request()
    with pytest.raises(ApiError) as caught:
        guard_agent_edit(base, {**base, "start_prompt": "b"}, [], request, privileged=True)
    assert caught.value.code == "privileged_target"
    assert request.state.preset_fields == ["start_prompt"]


def test_guard_privileged_refuses_a_secret_value_name():
    base = {"kube_cluster_access": True, "env": {"K": {"protected": True}}}
    with pytest.raises(ApiError) as caught:
        guard_agent_edit(base, base, ["K"], None, privileged=True)
    assert caught.value.code == "privileged_target"


def test_guard_without_request_still_raises():
    with pytest.raises(ApiError):
        guard_agent_edit({}, {"docker": True}, [], None, privileged=True)


def test_guard_human_only_runs_before_privileged():
    base = {"kube_cluster_access": True}
    with pytest.raises(ApiError) as caught:
        guard_agent_edit(
            base, {**base, "docker": True, "start_prompt": "x"}, [], None, privileged=True
        )
    assert caught.value.code == "human_only_field"


def test_secret_in_patch_refuses_a_value_for_a_protected_variable():
    merged = {"env": {"K": {"protected": True, "value": "v"}}}
    with pytest.raises(ApiError) as caught:
        secret_in_patch({"env": {"K": {"value": "v"}}}, merged)
    assert caught.value.code == "secret_in_spec"
    assert "protected variable K" in caught.value.message


def test_merged_spec_in_merges_and_returns_the_model():
    merged, spec = merged_spec_in({"start_prompt": "a", "replicas": 2}, {"start_prompt": "b"})
    assert merged == {"start_prompt": "b", "replicas": 2}
    assert spec.start_prompt == "b"


def test_merged_spec_in_removes_a_key_with_null():
    merged, _ = merged_spec_in({"start_prompt": "a", "model": "m"}, {"model": None})
    assert merged == {"start_prompt": "a"}


def test_merged_spec_in_relabels_a_bad_field():
    with pytest.raises(RequestValidationError) as caught:
        merged_spec_in({"start_prompt": "a"}, {"replicas": 0})
    assert [e["loc"] for e in caught.value.errors()] == [("body", "spec", "replicas")]


def test_merged_spec_in_refuses_a_value_for_a_protected_variable():
    stored = {"env": {"K": {"protected": True}}}
    with pytest.raises(ApiError) as caught:
        merged_spec_in(stored, {"env": {"K": {"value": "v"}}})
    assert caught.value.code == "secret_in_spec"
