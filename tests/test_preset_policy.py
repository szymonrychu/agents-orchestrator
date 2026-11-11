import pytest

from agents_orchestrator.preset_policy import (
    AGENT_MAY_MOVE_PRIVILEGED_PRESETS,
    HUMAN_ONLY_PATHS,
    NON_SECURITY,
    changed_paths,
    classify,
    fixed_changes,
    human_only_changes,
    merge_patch,
    privileged,
    strip_grants,
    tighten_changes,
)
from agents_orchestrator.spec import SessionSpec, WrapperSettings


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


def test_env_changes_are_not_human_only():
    """The stored-protected rule is gone (decision Q4): removing or changing an env entry is
    not a human-only change; a privileged target limits it separately."""
    stored = {"env": {"K": {"protected": True}}}
    assert human_only_changes(stored, {"env": {}}) == []
    assert human_only_changes(stored, {}) == []
    assert human_only_changes(stored, {"env": {"K": {"value": "v"}}}) == []


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


def test_privileged_target_env_ref_paths_are_not_security():
    """An env.<NAME> path whose stored and new entries are each a ref or absent is allowed on a
    privileged target (Q4); a plain or protected entry on either side stays security."""
    from agents_orchestrator.preset_policy import privileged_target_changes as ptc

    ref = {"vault": "11111111-1111-4111-8111-111111111111", "env": "TOKEN"}
    other = {"vault": "22222222-2222-4222-8222-222222222222", "env": "TOKEN"}
    stored = {"kube_cluster_access": True, "env": {"R": ref, "P": {"value": "x"}}}
    keep = {"P": {"value": "x"}}
    assert ptc(stored, {**stored, "env": {**keep, "R": other}}) == []  # re-point
    assert ptc(stored, {**stored, "env": keep}) == []  # detach
    assert ptc(stored, {**stored, "env": {**stored["env"], "N": ref}}) == []  # attach
    assert ptc(stored, {**stored, "env": {**stored["env"], "N": {"value": "1"}}}) == ["env.N"]
    assert ptc(stored, {**stored, "env": {"R": ref}}) == ["env.P"]  # plain removed
    assert ptc(stored, {**stored, "env": {**stored["env"], "R": {"value": "v"}}}) == ["env.R"]
    assert ptc(stored, {**stored, "env": {**keep, "R": {"protected": True}}}) == ["env.R"]


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


DEFAULTS = {
    "quota_default": 100,
    "reminder_default": 0,
    "close_default": 0,
    "docker_cpu_default": "4",
    "docker_memory_default": "8Gi",
    "docker_storage_default": "30Gi",
}


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


# ---------------------------------- classification (preset-change-proposals plan 3.1)

# Plan 3.1 row 2, pinned here so a new spec field fails until it is classified.
SECURITY = frozenset(
    {
        "init_scripts",
        "start_prompt",
        "env",
        "any_mcp_config",
        "any_mcp_sources",
        "cache",
        "wrapper.hooks",
        "wrapper.mcp_servers",
        "wrapper.plugins",
        "wrapper.system_append",
        "wrapper.restart_continue_prompt",
        "wrapper.auto_compact",
    }
)


def _spec_paths() -> set[str]:
    top = set(SessionSpec.model_fields) - {"wrapper"}
    return top | {f"wrapper.{f}" for f in WrapperSettings.model_fields}


def test_every_spec_field_is_classified_exactly_once():
    lists = (NON_SECURITY, SECURITY, HUMAN_ONLY_PATHS)
    for path in sorted(_spec_paths()):
        assert sum(path in group for group in lists) == 1, path
    assert _spec_paths() == NON_SECURITY | SECURITY | HUMAN_ONLY_PATHS


def test_columns_are_not_spec_paths():
    assert not {"name", "labels", "project"} & (NON_SECURITY | HUMAN_ONLY_PATHS)


@pytest.mark.parametrize(
    ("path", "expected"),
    [
        ("model", (False, False)),
        ("usage_quota", (False, False)),
        ("wrapper.turn_max_seconds", (False, False)),
        ("env.FOO", (True, False)),
        ("wrapper.hooks", (True, False)),
        ("wrapper.not_a_field", (True, False)),
        ("not_a_field", (True, False)),
        ("init_scripts", (True, False)),
        ("kube_cluster_access", (True, True)),
        ("kube_namespace_access", (True, True)),
        ("session_messaging", (True, True)),
        ("docker", (True, True)),
        ("discard_hook_url", (True, True)),
        ("wrapper.permission_mode", (True, True)),
        ("wrapper.permissions", (True, True)),
    ],
)
def test_classify(path, expected):
    assert classify([path]) == {path: expected}


def test_classify_many_and_empty():
    assert classify([]) == {}
    assert classify(["model", "env.A"]) == {"model": (False, False), "env.A": (True, False)}


def test_privileged_target_allows_non_security():
    from agents_orchestrator.preset_policy import privileged_target_changes as ptc

    stored = {"kube_cluster_access": True}
    new = {
        **stored,
        "model": "opus",
        "peers": False,
        "inactivity": {"close_seconds": 60},
        "wrapper": {"turn_max_seconds": 600, "agent_commands": []},
    }
    assert ptc(stored, new) == []
    assert ptc(stored, {**new, "cache": {"key": "x"}}) == ["cache"]


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


def _dk(**sizes):
    return {"docker": True, **sizes}


@pytest.mark.parametrize(
    ("field", "low", "high"),
    [
        ("docker_cpu", "2", "6"),
        ("docker_memory", "4Gi", "12Gi"),
        ("docker_storage", "10Gi", "50Gi"),
    ],
)
def test_tighten_docker_sizes(field, low, high):
    assert tighten(_dk(**{field: low}), _dk(**{field: high})) == [field]  # raised
    assert tighten(_dk(**{field: high}), _dk(**{field: low})) == []  # lowered
    assert tighten(_dk(**{field: low}), _dk(**{field: low})) == []  # kept
    assert tighten(_dk(), _dk(**{field: low})) == []  # unset -> lower than default
    assert tighten(_dk(), _dk(**{field: high})) == [field]  # unset -> above the default
    assert tighten(_dk(), _dk()) == []


@pytest.mark.parametrize(
    ("field", "same", "other"),
    [
        ("docker_cpu", "1", "1000m"),
        ("docker_cpu", "500m", "0.5"),
        ("docker_memory", "1Gi", "1024Mi"),
        ("docker_storage", "2Gi", "2048Mi"),
    ],
)
def test_tighten_docker_sizes_compare_quantities_not_strings(field, same, other):
    assert tighten(_dk(**{field: same}), _dk(**{field: other})) == []
    assert tighten(_dk(**{field: other}), _dk(**{field: same})) == []


@pytest.mark.parametrize(
    ("field", "default", "below", "above"),
    [
        ("docker_cpu", "4", "2", "6"),
        ("docker_memory", "8Gi", "4Gi", "12Gi"),
        ("docker_storage", "30Gi", "10Gi", "50Gi"),
    ],
)
def test_tighten_docker_clearing_reads_as_the_deploy_default(field, default, below, above):
    key = f"{field}_default"
    # Cleared (absent or null): the default applies, looser only when it is higher.
    assert tighten(_dk(**{field: below}), _dk(), **{key: default}) == [field]
    assert tighten(_dk(**{field: below}), _dk(**{field: None}), **{key: default}) == [field]
    assert tighten(_dk(**{field: above}), _dk(), **{key: default}) == []
    assert tighten(_dk(**{field: default}), _dk(), **{key: default}) == []
    # An unset stored value reads as the default too, so a changed default moves the line.
    assert tighten(_dk(), _dk(**{field: below}), **{key: default}) == []
    assert tighten(_dk(), _dk(**{field: above}), **{key: default}) == [field]
    assert tighten(_dk(), _dk(**{field: default}), **{key: default}) == []


def test_tighten_docker_sizes_are_independent():
    stored = _dk(docker_cpu="2", docker_memory="8Gi", docker_storage="20Gi")
    new = _dk(docker_cpu="4", docker_memory="4Gi", docker_storage="40Gi")
    assert tighten(stored, new) == ["docker_cpu", "docker_storage"]


def test_tighten_docker_off_drops_the_sizes_and_is_tighter():
    stored = _dk(docker_cpu="2", docker_memory="4Gi", docker_storage="10Gi")
    assert tighten(stored, {}) == []
    assert tighten(stored, {"docker": None}) == []
    assert tighten(stored, {"docker": False}) == []


def test_tighten_docker_ignores_sizes_unless_both_sides_have_docker():
    # Turning docker on is the human-only grant guard's business, not this rule's.
    assert tighten({}, _dk(docker_cpu="64")) == []
    assert tighten({"docker": False}, _dk(docker_memory="64Gi")) == []
    # Without docker on the new side the sizes are not read at all.
    assert tighten(_dk(docker_cpu="2"), {"docker_cpu": "64"}) == []
    # Without docker on the stored side nothing is compared.
    assert tighten({"docker_cpu": "2"}, _dk(docker_cpu="64")) == []


def test_privileged_preset_move_switch_defaults_to_refuse():
    """One switch for a superleader moving a privileged preset between projects; the user's
    decision is pending (T-113), so the default refuses."""
    assert AGENT_MAY_MOVE_PRIVILEGED_PRESETS is False
