"""User volume routes: create, list, detail, grow, delete, save, explicit restore."""

from __future__ import annotations

import asyncio
import logging
import uuid

import pytest
import pytest_asyncio
from factories import insert_session
from fakes.fake_kube import FakeKube
from test_api_backups import add_version
from test_api_sessions import assert_error, client_for, make_settings

from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import volumes
from agents_orchestrator.reconciler.volumes import VolumeReconciler

V = "/api/v1/volumes"
P = "/api/v1/presets"
PVC = "PersistentVolumeClaim"
BODY = {"name": "data", "projects": ["default"], "size": "1Gi"}


@pytest_asyncio.fixture
async def http(pool, dsn):
    async with client_for(create_app(make_settings(dsn), pool)) as c:
        yield c


async def make(http, **over):
    r = await http.post(V, json={**BODY, **over})
    assert r.status_code == 201, r.text
    return r.json()


async def set_state(pool, vid, state):
    async with pool.connection() as conn:
        await volumes.set_state(conn, uuid.UUID(vid), state)


async def ready(http, pool, **over):
    v = await make(http, **over)
    await set_state(pool, v["id"], "ready")
    return v


async def attach(http, v, name="p", **over):
    r = await http.post(
        P,
        json={"name": name, "spec": {"volumes": [{"volume": v["id"], "mode": "ro", **over}]}},
    )
    assert r.status_code == 201, r.text
    return r.json()


async def claim(pool, vid, mode="rw"):
    s = await insert_session(pool)
    async with pool.connection() as conn, conn.transaction():
        await volumes.claim(
            conn,
            volume_id=uuid.UUID(vid),
            session_id=s["id"],
            preset_id=None,
            mode=mode,
            backup_restore=False,
            session_slug=s["slug"],
            current_version=None,
        )
    return s


async def kinds(http, vid):
    r = await http.get(f"{V}/{vid}/events")
    return [e["kind"] for e in r.json()["items"]]


async def test_create_returns_a_provisioning_volume_with_the_default_size(http):
    r = await http.post(V, json={"name": "d1", "projects": ["default"], "exclude": ["*.tmp"]})
    assert r.status_code == 201, r.text
    got = r.json()
    assert (got["state"], got["size"], got["storage_class"]) == (
        "provisioning",
        "10Gi",
        "rook-ceph-rwx",
    )
    assert (got["exclude"], got["attachments"], got["mounted"]) == (["*.tmp"], 0, 0)


@pytest.mark.parametrize(
    ("over", "status", "code"),
    [
        ({}, 409, "volume_exists"),
        ({"name": "Bad_Name"}, 422, "invalid_request"),
        ({"size": "201Gi"}, 422, "invalid_request"),
        ({"size": "200Gi"}, 201, None),
        ({"labels": ["project:x"]}, 422, "invalid_request"),
        ({"projects": []}, 422, "invalid_request"),
        ({"exclude": ["/abs"]}, 422, "invalid_request"),
    ],
)
async def test_create_validation(http, over, status, code):
    if code == "volume_exists":
        await make(http)
    r = await http.post(V, json={**BODY, **({"name": "other"} if status == 201 else {}), **over})
    if code:
        assert_error(r, status, code)
    else:
        assert r.status_code == status, r.text


async def test_list_filters_by_project(http):
    await make(http, name="a", projects=["default", "x"])
    await make(http, name="b", projects=["y"])
    assert [v["name"] for v in (await http.get(V)).json()] == ["a", "b"]
    assert [v["name"] for v in (await http.get(V, params={"project": "x"})).json()] == ["a"]


async def test_detail_shows_presets_claims_and_versions(http, pool):
    v = await ready(http, pool)
    await attach(http, v, backup_restore=True)
    s = await claim(pool, v["id"])
    ver = await add_version(pool, "volume-data", manifest={"live": True})
    got = (await http.get(f"{V}/{v['id']}")).json()
    assert got["attachments"] == 1 and got["mounted"] == 1
    assert got["presets"] == [
        {
            "preset_id": got["presets"][0]["preset_id"],
            "preset_name": "p",
            "mode": "ro",
            "mount_path": "/volumes/data",
            "backup_restore": True,
        }
    ]
    assert got["claims"] == [{"session_slug": s["slug"], "mode": "rw"}]
    assert [(x["id"], x["origin"], x["live"]) for x in got["versions"]] == [
        (str(ver.id), "save", True)
    ]
    assert got["current_version"] == str(ver.id)


@pytest.mark.parametrize("method", ["get", "patch", "delete"])
async def test_unknown_volume_is_404(http, method):
    r = await http.request(
        method.upper(), f"{V}/{uuid.uuid4()}", **({"json": {}} if method == "patch" else {})
    )
    assert_error(r, 404, "volume_not_found")


@pytest.mark.parametrize(
    ("size", "status", "code", "state", "shown"),
    [
        ("2Gi", 200, None, "resizing", "2Gi"),
        ("1Gi", 200, None, "ready", "1Gi"),
        ("1024Mi", 200, None, "ready", "1Gi"),
        ("512Mi", 409, "shrink_refused", "ready", "1Gi"),
        ("201Gi", 422, "invalid_request", "ready", "1Gi"),
    ],
)
async def test_patch_size(http, pool, size, status, code, state, shown):
    v = await ready(http, pool)
    r = await http.patch(f"{V}/{v['id']}", json={"size": size})
    if code:
        assert_error(r, status, code)
    else:
        assert r.status_code == 200, r.text
    got = (await http.get(f"{V}/{v['id']}")).json()
    assert (got["state"], got["size"]) == (state, shown)


async def test_patch_updates_description_labels_projects_and_exclude(http, pool):
    v = await ready(http, pool)
    r = await http.patch(
        f"{V}/{v['id']}",
        json={"description": "d", "labels": ["a"], "projects": ["x"], "exclude": ["*.o"]},
    )
    got = r.json()
    assert (got["description"], got["labels"], got["projects"], got["exclude"]) == (
        "d",
        ["a"],
        ["x"],
        ["*.o"],
    )


async def test_patch_takes_the_volume_lock_before_checking_attaching_presets(http, pool):
    v = await ready(http, pool, projects=["default", "x"])
    await attach(http, v)
    async with pool.connection() as conn, conn.transaction():
        await conn.execute("SELECT 1 FROM volumes WHERE id = %s FOR UPDATE", (v["id"],))
        task = asyncio.create_task(http.patch(f"{V}/{v['id']}", json={"projects": ["x"]}))
        await asyncio.sleep(0.5)
        assert not task.done()
    assert_error(await task, 409, "project_in_use")


async def test_patch_refuses_removing_a_project_an_attaching_preset_needs(http, pool):
    v = await ready(http, pool, projects=["default", "x"])
    await attach(http, v)
    assert_error(
        await http.patch(f"{V}/{v['id']}", json={"projects": ["x"]}), 409, "project_in_use"
    )
    assert (
        await http.patch(f"{V}/{v['id']}", json={"projects": ["default", "y"]})
    ).status_code == 200


@pytest.mark.parametrize("state", ["restoring", "saving"])
async def test_delete_while_restoring_or_saving_is_409_busy(http, pool, state):
    v = await ready(http, pool)
    await set_state(pool, v["id"], state)
    assert_error(await http.delete(f"{V}/{v['id']}"), 409, "volume_busy")
    assert (await http.get(f"{V}/{v['id']}")).json()["state"] == state


@pytest.mark.parametrize("body", [{"size": "2Gi"}, {"description": "x"}, {"labels": ["a"]}])
async def test_patch_on_a_deleting_volume_is_409_busy(http, pool, body):
    v = await ready(http, pool)
    assert (await http.delete(f"{V}/{v['id']}")).status_code == 202
    assert_error(await http.patch(f"{V}/{v['id']}", json=body), 409, "volume_busy")


async def test_recreated_name_does_not_auto_restore_the_deleted_volumes_data(http, pool):
    old = await ready(http, pool)
    await add_version(pool, "volume-data")
    async with pool.connection() as conn, conn.transaction():
        await volumes.finish_delete(conn, uuid.UUID(old["id"]))
    new = await ready(http, pool)
    got = (await http.get(f"{V}/{new['id']}")).json()
    assert got["content_version"] == got["current_version"] is not None
    async with pool.connection() as conn, conn.transaction():
        decision = await volumes.claim(
            conn,
            volume_id=uuid.UUID(new["id"]),
            session_id=uuid.uuid4(),
            preset_id=None,
            mode="rw",
            backup_restore=True,
            session_slug="s",
            current_version=uuid.UUID(got["current_version"]),
        )
    assert decision.decision == "proceed"


async def test_patch_grow_while_busy_is_409(http, pool):
    v = await make(http)  # still provisioning
    assert_error(await http.patch(f"{V}/{v['id']}", json={"size": "2Gi"}), 409, "volume_busy")


async def test_delete_refused_while_attached_or_mounted(http, pool):
    a = await ready(http, pool, name="att")
    await attach(http, a)
    m = await ready(http, pool, name="mnt")
    await claim(pool, m["id"])
    assert_error(await http.delete(f"{V}/{a['id']}"), 409, "volume_attached")
    assert_error(await http.delete(f"{V}/{m['id']}"), 409, "volume_mounted")
    for v in (a, m):
        got = (await http.get(f"{V}/{v['id']}")).json()
        assert got["state"] == "ready"
        assert "delete_refused" in await kinds(http, v["id"])


async def test_delete_removes_the_pvc_and_keeps_backup_versions(http, pool, dsn):
    kube = FakeKube()
    rec = VolumeReconciler(pool, kube, make_settings(dsn))
    v = await make(http)
    await rec.tick()
    kube.set_pvc("vol-data", "Bound", "1Gi")
    await rec.tick()
    ver = await add_version(pool, "volume-data")
    r = await http.delete(f"{V}/{v['id']}")
    assert r.status_code == 202, r.text
    assert r.json()["state"] == "deleting"
    await rec.tick()
    await rec.tick()
    assert not kube.has(PVC, "vol-data")
    assert (await http.get(f"{V}/{v['id']}")).status_code == 404
    again = await make(http)  # the name is reusable; the old versions are still there
    detail = (await http.get(f"{V}/{again['id']}")).json()
    assert [x["id"] for x in detail["versions"]] == [str(ver.id)]


async def test_events_are_paged_newest_first(http, pool):
    v = await ready(http, pool)
    for size in ("2Gi", "3Gi"):
        await set_state(pool, v["id"], "ready")
        await http.patch(f"{V}/{v['id']}", json={"size": size})
    first = (await http.get(f"{V}/{v['id']}/events", params={"limit": 1})).json()
    assert [e["kind"] for e in first["items"]] == ["resized"]
    rest = (
        await http.get(
            f"{V}/{v['id']}/events", params={"before": first["next_cursor"], "limit": 10}
        )
    ).json()
    assert [e["kind"] for e in rest["items"]] == ["resized", "created"]


async def test_save_is_202_then_409_busy(http, pool):
    v = await ready(http, pool)
    r = await http.post(f"{V}/{v['id']}/save")
    assert r.status_code == 202, r.text
    assert r.json()["state"] == "saving"
    assert_error(await http.post(f"{V}/{v['id']}/save"), 409, "volume_busy")


async def test_restore_checks_version_mount_and_busy(http, pool):
    v = await ready(http, pool)
    mine = await add_version(pool, "volume-data")
    foreign = await add_version(pool, "volume-other")
    path = f"{V}/{v['id']}/restore"
    assert_error(await http.post(path, json={"version": str(foreign.id)}), 404, "version_not_found")
    assert_error(
        await http.post(path, json={"version": str(uuid.uuid4())}), 404, "version_not_found"
    )
    s = await claim(pool, v["id"])
    assert_error(await http.post(path, json={"version": str(mine.id)}), 409, "volume_mounted")
    assert (await http.get(f"{V}/{v['id']}")).json()["state"] == "ready"
    async with pool.connection() as conn, conn.transaction():
        await volumes.release_claims(conn, s["id"], actor_kind="system", actor="t")
    await set_state(pool, v["id"], "ready")
    r = await http.post(path, json={"version": str(mine.id)})
    assert r.status_code == 202, r.text
    assert r.json()["state"] == "restoring"
    assert_error(await http.post(path, json={"version": str(mine.id)}), 409, "volume_busy")


async def test_description_and_labels_never_reach_logs(http, pool, caplog):
    caplog.set_level(logging.DEBUG)
    marker = "CANARY-VOLUME-TEXT-5e1d"
    v = await http.post(V, json={**BODY, "description": marker, "labels": [marker.lower()]})
    vid = v.json()["id"]
    await set_state(pool, vid, "ready")
    await http.patch(f"{V}/{vid}", json={"description": marker, "size": "2Gi"})
    await http.post(V, json={**BODY, "labels": [marker.lower()]})  # 409
    await http.get(f"{V}/{vid}")
    events = (await http.get(f"{V}/{vid}/events")).text
    await http.delete(f"{V}/{vid}")
    assert marker not in events and marker.lower() not in events
    assert marker not in caplog.text
    assert marker.lower() not in caplog.text


DECLARED = {
    ("get", f"{V}/{{volume_id}}"): {404},
    ("patch", f"{V}/{{volume_id}}"): {404, 409},
    ("delete", f"{V}/{{volume_id}}"): {404, 409},
    ("get", f"{V}/{{volume_id}}/events"): {404},
    ("post", f"{V}/{{volume_id}}/save"): {404, 409},
    ("post", f"{V}/{{volume_id}}/restore"): {404, 409},
    ("post", V): {409},
}


@pytest.mark.parametrize(("method", "path"), sorted(DECLARED))
def test_volume_route_declares_its_error_statuses(method, path, dsn, pool):
    op = create_app(make_settings(dsn), pool).openapi()["paths"][path][method]
    codes = {int(c) for c in op["responses"]} - {200, 201, 202, 204, 422}
    assert codes == DECLARED[(method, path)]
