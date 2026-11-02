import pytest

from agents_orchestrator.preset_policy import (
    changed_paths,
    human_only_changes,
    merge_patch,
    privileged,
    strip_grants,
)


@pytest.mark.parametrize("field", ["kube_access", "session_messaging", "docker"])
def test_grant_turned_on_is_refused(field):
    assert human_only_changes({}, {field: True}) == [field]
    assert human_only_changes({field: False}, {field: True}) == [field]


@pytest.mark.parametrize("field", ["kube_access", "session_messaging", "docker"])
def test_grant_turned_off_or_removed_is_allowed(field):
    assert human_only_changes({field: True}, {field: False}) == []
    assert human_only_changes({field: True}, {}) == []
    assert human_only_changes({field: True}, {field: True}) == []


def test_absent_false_null_are_equal():
    for new in ({}, {"kube_access": False}, {"kube_access": None}):
        assert human_only_changes({}, new) == []


def test_discard_hook_url():
    assert human_only_changes({}, {"discard_hook_url": "https://x"}) == ["discard_hook_url"]
    assert human_only_changes(
        {"discard_hook_url": "https://a"}, {"discard_hook_url": "https://b"}
    ) == ["discard_hook_url"]
    assert (
        human_only_changes({"discard_hook_url": "https://a"}, {"discard_hook_url": "https://a"})
        == []
    )
    assert human_only_changes({"discard_hook_url": "https://a"}, {}) == []
    assert human_only_changes({"discard_hook_url": "https://a"}, {"discard_hook_url": None}) == []


def test_permission_mode():
    def pm(v):
        return {"wrapper": {"permission_mode": v}} if v else {}

    assert human_only_changes(pm(None), pm("auto")) == []
    assert human_only_changes(pm("auto"), pm(None)) == ["wrapper.permission_mode"]
    assert human_only_changes(pm("auto"), pm("bypassPermissions")) == ["wrapper.permission_mode"]
    assert human_only_changes(pm(None), pm("bypassPermissions")) == []
    assert human_only_changes(pm("plan"), pm("auto")) == []
    assert human_only_changes(pm("plan"), pm("acceptEdits")) == ["wrapper.permission_mode"]


def test_permission_lists():
    def p(allow, deny):
        return {"wrapper": {"permissions": {"allow": allow, "deny": deny}}}

    assert human_only_changes(p(["A"], ["D"]), p([], ["D", "E"])) == []
    assert human_only_changes(p(["A"], ["D"]), p(["A", "B"], ["D"])) == [
        "wrapper.permissions.allow"
    ]
    assert human_only_changes(p(["A"], ["D"]), p(["A"], [])) == ["wrapper.permissions.deny"]
    assert human_only_changes({}, p(["A"], None)) == ["wrapper.permissions.allow"]


def test_protected_env_cannot_be_removed_or_unprotected():
    stored = {"env": {"K": {"protected": True}}}
    assert human_only_changes(stored, {"env": {}}) == ["env.K"]
    assert human_only_changes(stored, {}) == ["env.K"]
    assert human_only_changes(stored, {"env": {"K": {"value": "v"}}}) == ["env.K"]
    assert (
        human_only_changes(stored, {"env": {"K": {"protected": True}, "N": {"protected": True}}})
        == []
    )


def test_several_fields_sorted():
    assert human_only_changes({}, {"session_messaging": True, "kube_access": True}) == [
        "kube_access",
        "session_messaging",
    ]


def test_strip_grants():
    spec = {
        "kube_access": True,
        "docker": True,
        "docker_cpu": "1",
        "discard_hook_url": "https://x",
        "session_messaging": True,
        "wrapper": {"permission_mode": "auto"},
        "replicas": 2,
    }
    out, removed = strip_grants(spec)
    assert out == {"wrapper": {"permission_mode": "auto"}, "replicas": 2}
    assert removed == [
        "discard_hook_url",
        "docker",
        "docker_cpu",
        "kube_access",
        "session_messaging",
    ]
    assert spec["kube_access"] is True  # not mutated


def test_strip_grants_keeps_sizes_without_docker():
    out, removed = strip_grants({"kube_access": True, "docker_cpu": "1"})
    assert out == {"docker_cpu": "1"}
    assert removed == ["kube_access"]


def test_privileged():
    assert privileged({}) == []
    assert (
        privileged(
            {"kube_access": False, "session_messaging": None, "discard_hook_url": "https://x"}
        )
        == []
    )
    assert privileged({"docker": True, "kube_access": True}) == ["docker", "kube_access"]
    assert privileged({"session_messaging": True}) == ["session_messaging"]


def test_merge_patch_rfc7396():
    assert merge_patch({"a": {"b": 1, "c": 2}}, {"a": {"b": None, "d": 3}}) == {
        "a": {"c": 2, "d": 3}
    }
    assert merge_patch({"l": [1, 2]}, {"l": [3]}) == {"l": [3]}
    assert merge_patch({"a": 1}, "x") == "x"
    assert merge_patch({"a": {"b": 1}}, {"a": None}) == {}


def test_merge_patch_does_not_mutate():
    target = {"a": {"b": 1}}
    patch = {"a": {"c": [1]}}
    out = merge_patch(target, patch)
    out["a"]["c"].append(2)
    assert target == {"a": {"b": 1}}
    assert patch == {"a": {"c": [1]}}


def test_changed_paths():
    assert changed_paths(
        {"env": {"A": {"value": "1"}}, "replicas": 1},
        {"env": {"A": {"value": "2"}}, "replicas": 1, "wrapper": {"system_append": "x"}},
    ) == ["env.A", "wrapper.system_append"]


def test_privileged_target_changes():
    from agents_orchestrator.preset_policy import privileged_target_changes as ptc

    stored = {"kube_access": True, "env": {"K": {"protected": True}}, "start_prompt": "a"}
    assert ptc({"start_prompt": "a"}, {"start_prompt": "b"}) == []  # not privileged
    assert ptc(stored, {**stored, "usage_quota": {"five_hour_pct": 5}}) == []
    assert ptc(stored, {"env": {"K": {"protected": True}}, "start_prompt": "a"}) == []
    assert ptc(stored, {**stored, "start_prompt": "b", "init_scripts": ["x"]}) == [
        "init_scripts",
        "start_prompt",
    ]
    assert ptc(stored, {"env": {"K": {"protected": True}}}) == ["start_prompt"]
    assert ptc(stored, {**stored, "env": {**stored["env"], "N": {"value": "1"}}}) == ["env.N"]
