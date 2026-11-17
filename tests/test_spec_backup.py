"""spec.backup: entry shape, path rules, cross-entry rules and the removed `cache` field."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import (
    BackupEntry,
    SessionSpec,
    SpecInvalid,
    backups,
    has_workspace_entry,
    restores,
    validate_spec,
)


def settings(**over: object) -> Settings:
    return Settings(
        database_url="postgresql://u:p@h/db",
        cache_s3_bucket="b",
        **over,  # type: ignore[arg-type]
    )


def entry(**over: object) -> dict[str, object]:
    return {"root": "claude", "path": "skills", "mode": "both", "name": "skills", **over}


def spec(*entries: dict[str, object]) -> SessionSpec:
    return SessionSpec.model_validate({"backup": list(entries)})


def errors(s: SessionSpec) -> list[str]:
    with pytest.raises(SpecInvalid) as exc:
        validate_spec(s, settings())
    return exc.value.errors


@pytest.mark.parametrize("path", ["/etc", "/", "a/../b", "..", "../x", "a//b", ""])
def test_entry_rejects_absolute_and_dotdot(path: str) -> None:
    with pytest.raises(ValidationError):
        BackupEntry.model_validate(entry(path=path))


def test_entry_accepts_whole_root_and_nested_paths() -> None:
    assert BackupEntry.model_validate(entry(path=".")).path == "."
    assert BackupEntry.model_validate(entry(path="a/b")).path == "a/b"


def test_exclude_keeps_the_old_limits() -> None:
    for bad in (["/abs"], ["a/../b"], [""], ["x"] * 51):
        with pytest.raises(ValidationError):
            BackupEntry.model_validate(entry(exclude=bad))


def test_cache_field_rejected() -> None:
    with pytest.raises(ValidationError, match="cache"):
        SessionSpec.model_validate({"cache": {"restore_key": "a"}})


def test_backup_is_sparse() -> None:
    assert SessionSpec().backup is None
    assert "backup" not in SessionSpec().model_dump(exclude_none=True)


def test_overlap_same_direction_rejected() -> None:
    got = errors(
        spec(entry(path="skills", mode="backup"), entry(path="skills/x", mode="backup", name="x"))
    )
    assert any("overlap" in e for e in got)
    # both counts in both directions
    got = errors(spec(entry(path="a", mode="both"), entry(path="a/b", mode="restore", name="b")))
    assert any("overlap" in e for e in got)
    # the whole root contains everything under it
    got = errors(spec(entry(path=".", mode="restore"), entry(path="a", mode="restore", name="b")))
    assert any("overlap" in e for e in got)


def test_restore_and_backup_entry_same_path_allowed() -> None:
    s = spec(entry(path="skills", mode="restore"), entry(path="skills", mode="backup", name="o"))
    validate_spec(s, settings())


def test_different_roots_do_not_overlap() -> None:
    validate_spec(
        spec(entry(root="claude", path="."), entry(root="workspace", path=".", name="w")),
        settings(),
    )


@pytest.mark.parametrize("flag", ["live", "after_init"])
def test_live_needs_restore_direction(flag: str) -> None:
    got = errors(spec(entry(mode="backup", **{flag: True})))
    assert any(flag in e and "restore" in e for e in got)
    validate_spec(spec(entry(mode="restore", **{flag: True})), settings())
    validate_spec(spec(entry(mode="both", **{flag: True})), settings())


def test_max_16_entries() -> None:
    many = [entry(path=f"d{i}", name=f"n{i}") for i in range(16)]
    validate_spec(spec(*many), settings())
    got = errors(spec(*many, entry(path="d16", name="n16")))
    assert any("at most 16" in e for e in got)


def test_name_must_render_to_a_valid_name() -> None:
    assert any("name" in e for e in errors(spec(entry(name="{{ nope }}"))))
    assert any("name" in e for e in errors(spec(entry(name="{{ 'A B' }}"))))
    validate_spec(spec(entry(name="{{ project }}/{{ preset_name | slug }}")), settings())


def test_sha256_length_bounds_in_a_name() -> None:
    validate_spec(spec(entry(name="k-{{ sha256('a/b', 8) }}")), settings())
    assert errors(spec(entry(name="k-{{ sha256('a/b', 7) }}")))
    assert errors(spec(entry(name="k-{{ sha256('a/b', 63) }}")))


def test_backup_needs_the_s3_bucket() -> None:
    s = spec(entry())
    with pytest.raises(SpecInvalid, match="not configured"):
        validate_spec(s, Settings(database_url="postgresql://u:p@h/db"))


def test_direction_helpers_and_workspace_coupling() -> None:
    s = spec(
        entry(path="a", mode="restore"),
        entry(path="b", mode="backup", name="b"),
        entry(root="workspace", path=".", mode="both", name="w"),
    )
    assert [i for i, _ in restores(s)] == [0, 2]
    assert [i for i, _ in backups(s)] == [1, 2]
    assert has_workspace_entry(s)
    assert not has_workspace_entry(spec(entry()))
    assert not has_workspace_entry(SessionSpec())
    assert restores(SessionSpec()) == [] and backups(SessionSpec()) == []


def test_preset_prefix_no_longer_reserved() -> None:
    validate_spec(spec(entry(name="preset-0123456789abcdef")), settings())
    assert BackupEntry.model_validate(entry(name="preset-x")).name == "preset-x"
