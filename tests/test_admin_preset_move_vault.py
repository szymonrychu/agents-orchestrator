"""T-113: a superleader moving a preset between projects re-runs the vault label check
(effective labels are the preset's labels plus its project); running sessions are untouched."""

# ruff: noqa: F811
from __future__ import annotations

from test_admin_vault import ENTRY, A, app, boss, call, vault  # noqa: F401
from test_api_presets_vault import ref
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client


async def _project(http_app, slug):
    async with main_client(http_app) as main:
        r = await main.post("/api/v1/projects", json={"slug": slug, "name": slug.title()})
    assert r.status_code == 201, r.text


async def test_move_out_of_the_label_project_is_vault_label_mismatch(app, call, vault):
    vault.entries[ENTRY] = ("github", {"TOKEN", "USER"}, "project:team-a")
    await _project(app, "team-a")
    async with main_client(app) as main:
        r = await main.post(
            "/api/v1/presets",
            json={
                "name": "mv",
                "spec": {"env": {"TOKEN": ref(ENTRY)}},
                "superleader_manageable": True,
                "project": "team-a",
            },
        )
    assert r.status_code == 201, r.text
    made = r.json()
    # the entry needs the label project:team-a, which only that project gives the preset
    r = await call("PATCH", f"{A}/mv", json={"project": "default"})
    assert_error(r, 422, "vault_label_mismatch")
    r = await call(
        "PUT",
        f"{A}/mv",
        json={"name": "mv", "spec": {"env": {"TOKEN": ref(ENTRY)}}, "project": "default"},
    )
    assert_error(r, 422, "vault_label_mismatch")
    async with main_client(app) as main:
        assert (await main.get(f"/api/v1/presets/{made['id']}")).json()["project"] == "team-a"
