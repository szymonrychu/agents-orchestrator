import shutil
import uuid

import psycopg

from agents_orchestrator.db.migrate import MIGRATIONS_DIR, run_migrations

T0 = "2026-11-20 10:00:00+00"

# claim (mode, backup_restore, released minutes after T0 or None), save (started, ended) or None
CASES = {
    "open": (("rw", True, None), (10, 20), True),
    "released_after_save": (("rw", True, 30), (10, 20), True),
    "released_during_save": (("rw", True, 15), (10, 20), True),
    "released_before_save": (("rw", True, 5), (10, 20), False),
    "never_saved": (("rw", True, 5), None, True),
    "ro_backup": (("ro", True, 30), (10, 20), False),
    "rw_no_backup": (("rw", False, 30), (10, 20), False),
}


def _seed(c, name, claim, save):
    vid = uuid.uuid4()
    c.execute(
        "INSERT INTO volumes (id, name, projects, size, storage_class, pvc_name, state,"
        " created_by) VALUES (%s, %s, '{p}', '1Gi', 'sc', %s, 'ready', 'alice')",
        (vid, name, f"vol-{name}"),
    )
    mode, backup, released = claim
    c.execute(
        "INSERT INTO volume_mounts (volume_id, session_id, mode, backup_restore, claimed_at,"
        " released_at) VALUES (%s, %s, %s, %s, %s::timestamptz,"
        " %s::timestamptz + make_interval(mins => %s))",
        (vid, uuid.uuid4(), mode, backup, T0, None if released is None else T0, released or 0),
    )
    if save is not None:
        for kind, minutes in (("save_started", save[0]), ("saved", save[1])):
            c.execute(
                "INSERT INTO volume_events (volume_id, at, actor_kind, actor, kind)"
                " VALUES (%s, %s::timestamptz + make_interval(mins => %s), 'system', 'r', %s)",
                (vid, T0, minutes, kind),
            )


def test_0061_marks_volumes_with_backup_writes_after_the_last_save_dirty(empty_dsn, tmp_path):
    for path in MIGRATIONS_DIR.glob("*.sql"):
        if path.name < "0061":
            shutil.copy(path, tmp_path / path.name)
    run_migrations(empty_dsn, tmp_path)
    with psycopg.connect(empty_dsn) as c:
        for name, (claim, save, _dirty) in CASES.items():
            _seed(c, name, claim, save)
    assert run_migrations(empty_dsn) == ["0061", "0062"]
    with psycopg.connect(empty_dsn) as c:
        got = dict(c.execute("SELECT name, backup_dirty_at IS NOT NULL FROM volumes"))
    assert got == {name: dirty for name, (_c, _s, dirty) in CASES.items()}
