import pytest

from agents_orchestrator.preset_policy import (
    changed_paths,
    fixed_changes,
    human_only_changes,
    merge_patch,
    privileged,
    strip_grants,
    tighten_changes,
)


@pytest.mark.parametrize("field", ["kube_cluster_access", "session_messaging", "docker"])
def test_grant_turned_on_is_refused(field):
    assert human_only_changes({}, {field: True}) == [field]
    assert human_only_changes({field: False}, {field: True}) == [field]


@pytest.mark.parametrize("field", ["kube_cluster_access", "session_messaging", "docker"])
def test_grant_turned_off_or_removed_is_allowed(field):
    assert human_only_changes({field: True}, {field: False}) == []
    assert human_only_changes({field: True}, {}) == []
    assert human_only_changes({field: True}, {field: True}) == []


def test_absent_false_null_are_equal():
    for new in ({}, {"kube_cluster_access": False}, {"kube_cluster_access": None}):
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


def test_protected_env_removal_is_human_only():
    """Decision 7 b: removing or unprotecting a stored protected variable is human-only;
    adding one (and setting its value through the secret route) is not."""
    stored = {"env": {"K": {"protected": True}}}
    assert human_only_changes(stored, {"env": {}}) == ["env.K"]
    assert human_only_changes(stored, {}) == ["env.K"]
    assert human_only_changes(stored, {"env": {"K": {"value": "v"}}}) == ["env.K"]
    assert human_only_changes(stored, {"env": {"K": {"value": "v", "protected": False}}}) == [
        "env.K"
    ]
    assert human_only_changes(stored, {"env": {"K": {"protected": True}}}) == []
    assert human_only_changes({}, {"env": {"J": {"protected": True}}}) == []
    assert human_only_changes({"env": {"P": {"value": "x"}}}, {}) == []
    assert (
        human_only_changes(stored, {"env": {"K": {"protected": True}, "N": {"protected": True}}})
        == []
    )


def test_several_fields_sorted():
    assert human_only_changes({}, {"session_messaging": True, "kube_cluster_access": True}) == [
        "kube_cluster_access",
        "session_messaging",
    ]


def test_strip_grants():
    spec = {
        "kube_cluster_access": True,
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
        "kube_cluster_access",
        "session_messaging",
    ]
    assert spec["kube_cluster_access"] is True  # not mutated


def test_strip_grants_keeps_sizes_without_docker():
    out, removed = strip_grants({"kube_cluster_access": True, "docker_cpu": "1"})
    assert out == {"docker_cpu": "1"}
    assert removed == ["kube_cluster_access"]


def test_privileged():
    assert privileged({}) == []
    assert (
        privileged(
            {
                "kube_cluster_access": False,
                "session_messaging": None,
                "discard_hook_url": "https://x",
            }
        )
        == []
    )
    assert privileged({"docker": True, "kube_cluster_access": True}) == [
        "docker",
        "kube_cluster_access",
    ]
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

    stored = {"kube_cluster_access": True, "env": {"K": {"protected": True}}, "start_prompt": "a"}
    assert ptc({"start_prompt": "a"}, {"start_prompt": "b"}) == []  # not privileged
    assert ptc(stored, {**stored, "usage_quota": {"five_hour_pct": 5}}) == []
    assert ptc(stored, {"env": {"K": {"protected": True}}, "start_prompt": "a"}) == []
    assert ptc(stored, {**stored, "start_prompt": "b", "init_scripts": ["x"]}) == [
        "init_scripts",
        "start_prompt",
    ]
    assert ptc(stored, {"env": {"K": {"protected": True}}}) == ["start_prompt"]
    assert ptc(stored, {**stored, "env": {**stored["env"], "N": {"value": "1"}}}) == ["env.N"]


def test_privileged_target_refuses_secret_values():
    """A value for a protected variable of a privileged preset (credential substitution in a
    pod with a grant) is refused even though the stored spec does not change."""
    from agents_orchestrator.preset_policy import privileged_target_changes as ptc

    stored = {"kube_cluster_access": True, "env": {"K": {"protected": True}}}
    assert ptc(stored, stored, ["K"]) == ["env.K"]
    assert ptc(stored, {**stored, "start_prompt": "p"}, ["K"]) == ["env.K", "start_prompt"]
    assert (
        ptc({"env": {"K": {"protected": True}}}, {"env": {"K": {"protected": True}}}, ["K"]) == []
    )


NS = "kube_namespace_access"


@pytest.mark.parametrize(
    ("stored", "new"),
    [
        ({}, {NS: {"media": ["read"]}}),
        ({NS: {"media": ["read"]}}, {NS: {"media": ["read", "port-forward"]}}),
        ({NS: {"media": ["read"]}}, {NS: {"media": ["read"], "games": ["read"]}}),
    ],
)
def test_namespace_grant_growth_is_human_only(stored, new):
    assert NS in human_only_changes(stored, new)


@pytest.mark.parametrize("new", [{}, {NS: None}, {NS: {"media": ["read"]}}])
def test_namespace_grant_shrink_passes(new):
    stored = {NS: {"media": ["read", "port-forward"], "games": ["read"]}}
    assert NS not in human_only_changes(stored, new)


def test_namespace_grant_is_privileged_and_stripped():
    assert privileged({NS: {"media": ["read"]}}) == [NS]
    out, removed = strip_grants({NS: {"media": ["read"]}, "replicas": 1})
    assert out == {"replicas": 1} and removed == [NS]


@pytest.mark.parametrize(
    "field, old, new",
    [
        ("replicas", 1, 3),
        ("max_replicas", 3, 5),
        ("static", False, True),
        ("workdir", "a", "b"),
        ("pvc_size", "1Gi", "2Gi"),
        ("cache", {"save_key": "a"}, {"save_key": "b"}),
    ],
)
def test_fixed_changes_names_each_field(field, old, new):
    base = {"replicas": 3, "max_replicas": 5}
    assert fixed_changes({**base, field: old}, {**base, field: new}) == [field]


def test_fixed_changes_unchanged_is_empty():
    spec = {"replicas": 2, "max_replicas": 4, "workdir": "w", "pvc_size": "1Gi", "cache": {}}
    assert fixed_changes(spec, dict(spec)) == []
    assert fixed_changes({}, {}) == []


def test_fixed_changes_ignores_other_fields():
    assert fixed_changes({"start_prompt": "a"}, {"start_prompt": "b", "model": "m"}) == []


def test_fixed_changes_sorted():
    assert fixed_changes({}, {"workdir": "w", "pvc_size": "1Gi", "cache": {"save_key": "a"}}) == [
        "cache",
        "pvc_size",
        "workdir",
    ]


def test_fixed_changes_replicas_moves_the_unset_max_with_it():
    assert fixed_changes({}, {"replicas": 2}) == ["max_replicas", "replicas"]
    assert fixed_changes({"max_replicas": 2}, {"replicas": 2, "max_replicas": 2}) == ["replicas"]


def test_fixed_changes_max_replicas_absent_equals_replicas():
    assert fixed_changes({"replicas": 2}, {"replicas": 2, "max_replicas": 2}) == []
    assert fixed_changes({"replicas": 2, "max_replicas": 2}, {"replicas": 2}) == []
    assert fixed_changes({"replicas": 2}, {"replicas": 2, "max_replicas": 3}) == ["max_replicas"]
    assert fixed_changes({"replicas": 2, "max_replicas": 3}, {"replicas": 2}) == ["max_replicas"]


def test_fixed_changes_absent_versus_set():
    assert fixed_changes({}, {"pvc_size": "1Gi"}) == ["pvc_size"]
    assert fixed_changes({"pvc_size": "1Gi"}, {}) == ["pvc_size"]
    assert fixed_changes({}, {"static": True}) == ["static"]
    assert fixed_changes({"workdir": "w"}, {}) == ["workdir"]


def test_fixed_changes_defaults_equal_absent():
    assert fixed_changes({}, {"replicas": 1, "static": False, "workdir": "workdir"}) == []
    assert fixed_changes({"replicas": 1, "static": False}, {}) == []


def test_fixed_changes_null_equals_absent():
    assert fixed_changes({}, {"cache": None, "pvc_size": None}) == []
    assert fixed_changes({"cache": None}, {}) == []
    assert fixed_changes({"cache": {"save_key": "a"}}, {"cache": None}) == ["cache"]


DEFAULTS = {"quota_default": 100, "reminder_default": 0, "close_default": 0}


def tighten(stored, new, **defaults):
    return tighten_changes(stored, new, **{**DEFAULTS, **defaults})


@pytest.mark.parametrize("metric", ["five_hour_pct", "seven_day_pct"])
def test_tighten_quota(metric):
    path = f"usage_quota.{metric}"

    def q(v):
        return {"usage_quota": {metric: v}}

    assert tighten(q(50), q(80)) == [path]  # up is looser
    assert tighten(q(80), q(50)) == []  # down is tighter
    assert tighten(q(50), q(50)) == []
    assert tighten(q(50), {}) == [path]  # removed: back to the default 100
    assert tighten(q(50), {"usage_quota": {}}) == [path]
    assert tighten(q(50), q(None)) == [path]
    assert tighten({}, q(60)) == []  # unset stored is the default; below it is tighter
    assert tighten({}, q(100)) == []  # equal to the default
    assert tighten(q(100), {}) == []
    assert tighten({}, q(70), quota_default=60) == [path]  # above a lower default
    assert tighten(q(30), {}, quota_default=40) == [path]
    assert tighten(q(30), {}, quota_default=20) == []  # the default is tighter than the stored


def test_tighten_quota_metrics_are_independent():
    stored = {"usage_quota": {"five_hour_pct": 50, "seven_day_pct": 50}}
    new = {"usage_quota": {"five_hour_pct": 60, "seven_day_pct": 40}}
    assert tighten(stored, new) == ["usage_quota.five_hour_pct"]
    new = {"usage_quota": {"five_hour_pct": 60, "seven_day_pct": 70}}
    assert tighten(stored, new) == ["usage_quota.five_hour_pct", "usage_quota.seven_day_pct"]


@pytest.mark.parametrize("key", ["reminder_seconds", "close_seconds"])
def test_tighten_inactivity(key):
    path = f"inactivity.{key}"

    def i(v):
        return {"inactivity": {key: v}}

    assert tighten(i(3600), i(0)) == [path]  # 0 is off, infinite: looser
    assert tighten(i(0), i(3600)) == []  # on from off: tighter
    assert tighten(i(3600), i(7200)) == [path]  # longer is looser
    assert tighten(i(7200), i(3600)) == []  # shorter is tighter
    assert tighten(i(3600), i(3600)) == []
    assert tighten(i(3600), {}) == [path]  # unset with default 0 is off
    assert tighten({}, i(3600)) == []
    assert tighten({}, {}) == []
    assert tighten(i(0), {}) == []
    assert tighten(i(3600), {}, reminder_default=900, close_default=900) == []
    assert tighten(i(600), {}, reminder_default=900, close_default=900) == [path]


def test_tighten_inactivity_keys_are_independent():
    stored = {"inactivity": {"reminder_seconds": 600, "close_seconds": 3600}}
    new = {"inactivity": {"reminder_seconds": 300, "close_seconds": 0}}
    assert tighten(stored, new) == ["inactivity.close_seconds"]


def test_tighten_ignores_other_fields_and_null_blocks():
    assert tighten({"start_prompt": "a"}, {"start_prompt": "b"}) == []
    assert tighten({"usage_quota": None}, {"usage_quota": None, "inactivity": None}) == []
    assert tighten({"usage_quota": {"five_hour_pct": 50}}, {"usage_quota": None}) == [
        "usage_quota.five_hour_pct"
    ]


def test_tighten_does_not_mutate():
    stored = {"usage_quota": {"five_hour_pct": 50}}
    new = {"usage_quota": {"five_hour_pct": 80}}
    tighten(stored, new)
    assert stored == {"usage_quota": {"five_hour_pct": 50}}
    assert new == {"usage_quota": {"five_hour_pct": 80}}


def test_tighten_autoclose():
    path = "autoclose_after_seconds"

    def a(v):
        return {"autoclose_after_seconds": v}

    assert tighten(a(3600), a(7200)) == [path]  # raised is looser
    assert tighten(a(3600), {}) == [path]  # cleared: no autoclose, infinite
    assert tighten(a(3600), a(None)) == [path]
    assert tighten(a(7200), a(3600)) == []  # lowered is tighter
    assert tighten(a(3600), a(3600)) == []
    assert tighten({}, a(3600)) == []  # set from unset is tighter
    assert tighten({}, {}) == []


def _res(requests=None, limits=None):
    out = {}
    if requests is not None:
        out["requests"] = requests
    if limits is not None:
        out["limits"] = limits
    return {"resources": out}


@pytest.mark.parametrize("section", ["requests", "limits"])
@pytest.mark.parametrize(("key", "low", "high"), [("cpu", "500m", "1"), ("memory", "1Gi", "2Gi")])
def test_tighten_resources(section, key, low, high):
    path = f"resources.{section}.{key}"

    def r(v):
        return _res(**{section: {key: v}})

    assert tighten(r(low), r(high)) == [path]  # raised
    assert tighten(r(high), r(low)) == []  # lowered
    assert tighten(r(low), r(low)) == []  # equal
    assert tighten(r(low), _res(**{section: {}})) == [path]  # removed
    assert tighten(r(low), {}) == [path]
    assert tighten(r(low), {"resources": None}) == [path]


@pytest.mark.parametrize(
    ("same", "other"),
    [("1Gi", "1024Mi"), ("500m", "0.5"), ("1", "1000m"), ("1G", "1000M"), ("2Gi", "2147483648")],
)
def test_tighten_resources_compares_quantities_not_strings(same, other):
    for section in ("requests", "limits"):
        key = "memory" if "i" in same or "G" in same else "cpu"
        assert tighten(_res(**{section: {key: same}}), _res(**{section: {key: other}})) == []
        assert tighten(_res(**{section: {key: other}}), _res(**{section: {key: same}})) == []


def test_tighten_resources_mixed_units_order():
    assert tighten(_res(limits={"memory": "1Gi"}), _res(limits={"memory": "1025Mi"})) == [
        "resources.limits.memory"
    ]
    assert tighten(_res(limits={"memory": "1Gi"}), _res(limits={"memory": "1023Mi"})) == []
    assert tighten(_res(limits={"cpu": "0.5"}), _res(limits={"cpu": "501m"})) == [
        "resources.limits.cpu"
    ]
    assert tighten(_res(limits={"cpu": "0.5"}), _res(limits={"cpu": "499m"})) == []


def test_tighten_resources_adding_a_limit_is_tighter():
    assert tighten({}, _res(limits={"cpu": "2"})) == []
    assert tighten(_res(limits={"cpu": "2"}), _res(limits={"cpu": "2", "memory": "4Gi"})) == []


def test_tighten_resources_adding_a_request():
    """An absent request reads as the stored limit of the same key (Kubernetes defaults a
    request to its limit), else zero: adding one above that is a raise."""
    assert (
        tighten(_res(limits={"cpu": "2"}), _res(requests={"cpu": "1"}, limits={"cpu": "2"})) == []
    )
    assert (
        tighten(_res(limits={"cpu": "2"}), _res(requests={"cpu": "2"}, limits={"cpu": "2"})) == []
    )
    assert tighten({}, _res(requests={"cpu": "1"})) == ["resources.requests.cpu"]
    assert tighten(_res(limits={"memory": "1Gi"}), _res(requests={"memory": "2Gi"})) == [
        "resources.limits.memory",
        "resources.requests.memory",
    ]


def test_tighten_resources_keys_are_independent():
    stored = _res(requests={"cpu": "500m", "memory": "1Gi"}, limits={"cpu": "4", "memory": "8Gi"})
    new = _res(requests={"cpu": "250m", "memory": "2Gi"}, limits={"cpu": "8", "memory": "4Gi"})
    assert tighten(stored, new) == ["resources.limits.cpu", "resources.requests.memory"]


def test_tighten_all_rules_together():
    stored = {"autoclose_after_seconds": 600, **_res(limits={"cpu": "1"})}
    new = {"usage_quota": {"five_hour_pct": 101}, **_res(limits={})}
    assert tighten(stored, new) == [
        "autoclose_after_seconds",
        "resources.limits.cpu",
        "usage_quota.five_hour_pct",
    ]
