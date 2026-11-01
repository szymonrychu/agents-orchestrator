import pytest

from agents_orchestrator.cache import keys
from agents_orchestrator.cache.keys import (
    ArchiveNameError,
    group_of,
    object_name_for,
    validate_name,
)


@pytest.mark.parametrize(
    "name",
    [
        "a",
        "proj",
        "proj/20261031t120000z",
        "a/b/c/d",
        "preset-0123456789abcdef",
        "x.y_z-1",
        "a" * 63,
        "/".join(["a" * 63] * 3),  # 191 chars
    ],
)
def test_valid_save_names(name):
    assert validate_name(name, allow_prefix=False) == name
    assert validate_name(name, allow_prefix=True) == name


@pytest.mark.parametrize(
    "name",
    [
        "",
        "/",
        ".",
        "..",
        "a/./b",
        "a/../b",
        "a//b",
        "/a",
        ".hidden",
        "a/.hidden",
        "Upper",
        "a b",
        "a\nb",
        "a/b/c/d/e",  # five segments
        "sessions",
        "sessions/abc",
        "a" * 64,  # segment too long
        "/".join(["a" * 63] * 4),  # 255 chars total
        "a\\b",
        "a/b/",  # prefix form only on restore
    ],
)
def test_invalid_save_names(name):
    with pytest.raises(ArchiveNameError):
        validate_name(name, allow_prefix=False)


@pytest.mark.parametrize("name", ["proj/", "a/b/c/", "a/"])
def test_restore_may_end_in_one_slash(name):
    assert validate_name(name, allow_prefix=True) == name


@pytest.mark.parametrize("name", ["/", "a//", "sessions/", "a/b/c/d/e/", "./", "../", ""])
def test_restore_prefix_still_validated(name):
    with pytest.raises(ArchiveNameError):
        validate_name(name, allow_prefix=True)


def test_prefix_form_counts_four_segments_without_the_empty_tail():
    assert validate_name("a/b/c/d/", allow_prefix=True) == "a/b/c/d/"


def test_length_limit_is_200():
    long_ok = "/".join(["a" * 49] * 4)  # 49*4 + 3 = 199
    assert len(long_ok) == 199
    assert validate_name(long_ok, allow_prefix=False) == long_ok
    long_bad = "/".join(["a" * 50] * 3 + ["b" * 50])  # 203
    with pytest.raises(ArchiveNameError):
        validate_name(long_bad, allow_prefix=False)


def test_first_segment_sessions_is_reserved_but_later_segments_are_not():
    assert validate_name("proj/sessions", allow_prefix=False) == "proj/sessions"


def test_non_string_is_rejected():
    with pytest.raises(ArchiveNameError):
        validate_name(None, allow_prefix=False)  # type: ignore[arg-type]


def test_object_name_for_adds_prefix_and_suffix():
    assert object_name_for("workspace-cache/", "proj/20261031t120000z") == (
        "workspace-cache/proj/20261031t120000z.tar.zst"
    )


def test_object_name_for_rejects_the_prefix_form():
    with pytest.raises(ArchiveNameError):
        object_name_for("workspace-cache/", "proj/")


def test_list_prefix_for_keeps_the_trailing_slash():
    assert keys.list_prefix_for("workspace-cache/", "proj/") == "workspace-cache/proj/"
    assert keys.list_prefix_for("workspace-cache/", "foo/") != "workspace-cache/foo-bar/"


def test_list_prefix_for_requires_the_prefix_form():
    with pytest.raises(ArchiveNameError):
        keys.list_prefix_for("workspace-cache/", "proj")


@pytest.mark.parametrize(
    ("key", "group"),
    [
        ("workspace-cache/proj/20261031t120000z.tar.zst", "workspace-cache/proj/"),
        ("workspace-cache/a/b/20261031t120000z.tar.zst", "workspace-cache/a/b/"),
        ("workspace-cache/flat.tar.zst", "workspace-cache/flat.tar.zst"),
        ("workspace-cache/sessions/s1.tar.zst", "workspace-cache/sessions/"),
        ("flat.tar.zst", "flat.tar.zst"),
    ],
)
def test_group_of(key, group):
    assert group_of(key, "workspace-cache/" if key.startswith("workspace-cache/") else "") == group


def test_fixed_width_now_names_sort_chronologically():
    names = [
        "proj/20261031t120000z",
        "proj/20260101t000000z",
        "proj/20261031t115959z",
        "proj/20271231t235959z",
    ]
    assert sorted(names)[0] == "proj/20260101t000000z"
    assert sorted(names)[-1] == "proj/20271231t235959z"
    assert sorted(names)[-2] == "proj/20261031t120000z"
