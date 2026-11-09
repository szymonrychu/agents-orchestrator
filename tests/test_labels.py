"""Label normalisation vectors. agents-taskboard keeps its own copy of these on purpose."""

from __future__ import annotations

import pytest

from agents_orchestrator.labels import normalize_labels


def test_normalize_trims_lowercases_dedupes():
    assert normalize_labels(["  Foo ", "bar", "FOO", "Bar", "a:b_c.d-e"]) == [
        "foo",
        "bar",
        "a:b_c.d-e",
    ]


def test_normalize_empty_and_order():
    assert normalize_labels([]) == []
    assert normalize_labels(["b", "a", "b"]) == ["b", "a"]


def test_normalize_accepts_bounds():
    assert normalize_labels(["x"]) == ["x"]
    assert normalize_labels(["a" * 40]) == ["a" * 40]
    twenty = [f"l{i}" for i in range(20)]
    assert normalize_labels(twenty) == twenty


@pytest.mark.parametrize(
    "bad",
    ["", "   ", "a" * 41, "has space", "slash/no", "unié", "semi;colon", "a,b", "x\ny"],
)
def test_invalid_label_rejected(bad):
    with pytest.raises(ValueError):
        normalize_labels([bad])


def test_more_than_twenty_labels_rejected():
    with pytest.raises(ValueError):
        normalize_labels([f"l{i}" for i in range(21)])


def test_cap_counts_after_dedupe():
    assert len(normalize_labels([f"l{i}" for i in range(20)] + ["L0", " l1 "])) == 20


@pytest.mark.parametrize("bad", ["project:a", " Project:B ", "project:", "PROJECT:x:y"])
def test_reserved_project_prefix_rejected(bad):
    with pytest.raises(ValueError, match="project:"):
        normalize_labels(["ok", bad])


def test_reserved_prefix_allowed_for_filters_and_only_as_prefix():
    assert normalize_labels(["project:a"], allow_reserved=True) == ["project:a"]
    assert normalize_labels(["x-project:a", "myproject:b"]) == ["x-project:a", "myproject:b"]
