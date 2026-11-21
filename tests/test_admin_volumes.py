"""Admin volume routes (S-77 step 9, spec 9.3 and 12): reads scoped to the caller's projects with
names and ids only; attach and detach on manageable presets under the cross-trust rule (Q7, a
security attach is a proposal); the rule inside every superleader preset save; create only by a
user-approved proposal (Q10)."""

from __future__ import annotations

import logging

import pytest
from factories import two_project_fixture
from test_api_agent import client_for, headers, make_settings
from test_api_sessions import assert_error
from test_api_sessions import client_for as main_client

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import presets as presets_repo
from agents_orchestrator.db import volumes

A = ADMIN_PREFIX
PRIV = {"docker": True}


@pytest.fixture
def app(pool, dsn):
    return create_app(make_settings(dsn), pool)


@pytest.fixture
async def boss(pool):
    return (await two_project_fixture(pool))["S"]  # a superleader of project a


async def vol(pool, name, projects=("a",), description="", labels=()):
    async with pool.connection() as conn:
        v = await volumes.create(
            conn,
            name=name,
            projects=list(projects),
            size="1Gi",
            description=description,
            labels=list(labels),
            storage_class="sc",
            created_by="u",
            actor_kind="user",
            actor="u",
        )
        await conn.execute("UPDATE volumes SET state = 'ready' WHERE id = %s", (v.id,))
    return v


async def preset(pool, name, spec=None, manageable=True, project="a"):
    async with pool.connection() as conn:
        return await presets_repo.create(conn, name, spec or {}, manageable, None, project)


def att(v, mode="ro", **over):
    return {"volume": str(v.id), "mode": mode, **over}


async def stored(pool, p):
    async with pool.connection() as conn:
        got = await presets_repo.get(conn, p.id)
    assert got is not None
    return got


async def kinds(pool, v):
    async with pool.connection() as conn:
        return [(e.kind, e.actor_kind) for e in (await volumes.events(conn, v.id, limit=50)).items]


async def agent(app, boss, method, path, **kw):
    async with client_for(app, boss.ip) as http:
        return await http.request(method, f"{A}{path}", headers=headers(boss.id), **kw)


async def main(app, method, path, **kw):
    async with main_client(app) as http:
        return await http.request(method, f"/api/v1{path}", **kw)


async def approve(app, proposal):
    items = [{"path": i["path"], "decision": "approve"} for i in proposal["revisions"][0]["items"]]
    return await main(
        app,
        "POST",
        f"/proposals/{proposal['id']}/review",
        json={"revision": proposal["revision"], "items": items},
    )


# ---------------------------------------------------------------- reads


async def test_reads_are_scoped_to_the_callers_projects_and_hold_no_free_text(app, pool, boss):
    own = await vol(pool, "own", ("a",), description="PRIVATE-NOTE", labels=("secret-label",))
    both = await vol(pool, "both", ("b", "a"))
    other = await vol(pool, "other", ("b",))
    await preset(pool, "uses", {"volumes": [att(own, mount_path="/o", backup_restore=True)]})
    listed = await agent(app, boss, "GET", "/volumes")
    assert listed.status_code == 200, listed.text
    assert [v["name"] for v in listed.json()] == ["both", "own"]
    assert "PRIVATE-NOTE" not in listed.text and "secret-label" not in listed.text
    got = await agent(app, boss, "GET", f"/volumes/{both.id}")
    assert got.status_code == 200 and got.json()["name"] == "both"
    detail = await agent(app, boss, "GET", f"/volumes/{own.id}")
    assert detail.json()["presets"] == [
        {
            "preset_id": detail.json()["presets"][0]["preset_id"],
            "preset_name": "uses",
            "mode": "ro",
            "mount_path": "/o",
            "backup_restore": True,
        }
    ]
    assert "PRIVATE-NOTE" not in detail.text
    assert_error(await agent(app, boss, "GET", f"/volumes/{other.id}"), 404, "volume_not_found")


# ---------------------------------------------------------------- attach and detach


@pytest.mark.parametrize(
    ("spec", "mode"),
    [({}, "ro"), ({}, "rw"), (PRIV, "ro")],
    ids=["plain-ro", "plain-rw", "privileged-ro-alone"],
)
async def test_free_attach_applies_and_is_audited(app, pool, boss, spec, mode):
    v = await vol(pool, "data")
    p = await preset(pool, "p", spec)
    r = await agent(app, boss, "POST", f"/presets/{p.id}/volumes/{v.id}", json={"mode": mode})
    assert r.status_code == 200, r.text
    assert r.json()["proposal"] is None
    [saved] = (await stored(pool, p)).spec["volumes"]
    assert (saved["volume"], saved["mode"], saved["mount_path"]) == (
        str(v.id),
        mode,
        "/volumes/data",
    )
    assert ("attached", "agent") in await kinds(pool, v)


@pytest.mark.parametrize(
    ("case", "mode"),
    [("share-into-privileged", "ro"), ("share-from-privileged", "ro"), ("rw-to-privileged", "rw")],
)
async def test_security_attach_files_a_proposal_and_changes_nothing(app, pool, boss, case, mode):
    v = await vol(pool, "data")
    if case == "share-into-privileged":  # a plain preset already has it; the privileged one gets it
        await preset(pool, "plain", {"volumes": [att(v)]})
        target = await preset(pool, "priv", PRIV)
    elif case == "share-from-privileged":  # a privileged preset has it; a plain one gets it
        await preset(pool, "priv", {**PRIV, "volumes": [att(v)]})
        target = await preset(pool, "plain")
    else:  # nobody has it; read-write to a privileged preset
        target = await preset(pool, "priv", PRIV)
    before = (await stored(pool, target)).spec
    r = await agent(app, boss, "POST", f"/presets/{target.id}/volumes/{v.id}", json={"mode": mode})
    assert r.status_code == 202, r.text
    proposal = r.json()["proposal"]
    assert proposal["kind"] == "preset" and r.json()["preset"] is None
    assert proposal["revisions"][0]["items"][0]["path"] == "volumes"
    assert (await stored(pool, target)).spec == before
    # the user's approval applies it, whatever the rule
    assert (await approve(app, proposal)).status_code == 200
    assert any(a["volume"] == str(v.id) for a in (await stored(pool, target)).spec["volumes"])


async def test_detach_applies_at_once_even_on_a_privileged_preset(app, pool, boss):
    v = await vol(pool, "data")
    p = await preset(pool, "priv", {**PRIV, "volumes": [att(v, mount_path="/volumes/data")]})
    r = await agent(app, boss, "DELETE", f"/presets/{p.id}/volumes/{v.id}")
    assert r.status_code == 200, r.text
    assert "volumes" not in (await stored(pool, p)).spec
    assert ("detached", "agent") in await kinds(pool, v)
    again = await agent(app, boss, "DELETE", f"/presets/{p.id}/volumes/{v.id}")
    assert_error(again, 404, "not_attached")


@pytest.mark.parametrize("method", ["POST", "DELETE"])
async def test_attach_and_detach_refuse_a_non_manageable_preset(app, pool, boss, method):
    v = await vol(pool, "data")
    p = await preset(pool, "mine", manageable=False)
    r = await agent(
        app,
        boss,
        method,
        f"/presets/{p.id}/volumes/{v.id}",
        **({"json": {"mode": "ro"}} if method == "POST" else {}),
    )
    assert_error(r, 403, "not_manageable")


async def test_attach_refuses_a_volume_of_other_projects(app, pool, boss):
    v = await vol(pool, "far", ("b",))
    p = await preset(pool, "p")
    r = await agent(app, boss, "POST", f"/presets/{p.id}/volumes/{v.id}", json={"mode": "ro"})
    assert_error(r, 404, "volume_not_found")


# ---------------------------------------------------------------- the rule inside every save


async def shared(pool):
    """A volume a privileged preset attaches; `plain` is a manageable preset without it."""
    v = await vol(pool, "data")
    await preset(pool, "priv", {**PRIV, "volumes": [att(v, mount_path="/volumes/data")]})
    return v, await preset(pool, "plain")


@pytest.mark.parametrize("route", ["patch", "put", "create", "copy"])
async def test_every_superleader_preset_save_refuses_a_cross_trust_volume(app, pool, boss, route):
    v, plain = await shared(pool)
    spec = {"volumes": [att(v)]}
    call = {
        "patch": ("PATCH", f"/presets/{plain.id}", {"spec": spec}),
        "put": ("PUT", f"/presets/{plain.id}", {"name": "plain", "spec": spec}),
        "create": ("POST", "/presets", {"name": "fresh", "spec": spec, "project": "a"}),
        "copy": (
            "POST",
            f"/presets/{(await preset(pool, 'src', spec)).id}/copy",
            {},
        ),  # a new share
    }[route]
    r = await agent(app, boss, call[0], call[1], json=call[2])
    assert_error(r, 403, "volume_cross_trust")


async def test_a_share_the_user_made_does_not_block_other_superleader_edits(app, pool, boss):
    v, _ = await shared(pool)
    other = await preset(pool, "other", {"volumes": [att(v, mount_path="/volumes/data")]})
    r = await agent(
        app, boss, "PATCH", f"/presets/{other.id}", json={"spec": {"model": "claude-sonnet-5-5"}}
    )
    assert r.status_code == 200, r.text
    assert (await stored(pool, other)).spec["volumes"][0]["volume"] == str(v.id)


async def test_detach_then_attach_is_refused_while_a_live_session_still_mounts(app, pool, boss):
    """Laundering path: detaching from an unprivileged preset leaves its session mounting."""
    from factories import insert_session

    v = await vol(pool, "data")
    q = await preset(pool, "q", {"volumes": [att(v, mount_path="/volumes/data")]})
    sess = await insert_session(pool, spec={})
    async with pool.connection() as conn:
        await volumes.set_state(conn, v.id, "ready")
        await volumes.claim(
            conn,
            volume_id=v.id,
            session_id=sess["id"],
            preset_id=q.id,
            mode="rw",
            backup_restore=False,
            session_slug=sess["slug"],
            current_version=None,
        )
    assert (await agent(app, boss, "DELETE", f"/presets/{q.id}/volumes/{v.id}")).status_code == 200
    p = await preset(pool, "p", PRIV)
    body = {"mode": "ro"}
    r = await agent(app, boss, "POST", f"/presets/{p.id}/volumes/{v.id}", json=body)
    assert r.status_code == 202, r.text  # a proposal for the user, the preset is unchanged
    assert "volumes" not in (await stored(pool, p)).spec
    async with pool.connection() as conn:
        await volumes.release_claims(conn, sess["id"], actor_kind="system", actor="t")
    r = await agent(app, boss, "POST", f"/presets/{p.id}/volumes/{v.id}", json=body)
    assert r.status_code == 200, r.text


async def test_detail_hides_attaching_presets_of_other_projects(app, pool, boss):
    v = await vol(pool, "both", ("a", "b"))
    await preset(pool, "mine", {"volumes": [att(v)]})
    await preset(pool, "theirs", {"volumes": [att(v)]}, project="b")
    detail = await agent(app, boss, "GET", f"/volumes/{v.id}")
    assert [p["preset_name"] for p in detail.json()["presets"]] == ["mine"]


# ---------------------------------------------------------------- create by proposal

BODY = {"name": "fresh", "projects": ["a"], "size": "2Gi", "reason": "the hive needs a cache"}


async def test_create_files_a_proposal_and_the_users_approval_creates_the_volume(app, pool, boss):
    r = await agent(
        app, boss, "POST", "/volumes", json={**BODY, "exclude": ["node_modules"], "labels": ["x"]}
    )
    assert r.status_code == 202, r.text
    proposal = r.json()
    assert proposal["kind"] == "volume_create"
    async with pool.connection() as conn:
        assert await volumes.by_name(conn, "fresh") is None
    done = await approve(app, proposal)
    assert done.status_code == 200 and done.json()["status"] == "applied", done.text
    async with pool.connection() as conn:
        made = await volumes.by_name(conn, "fresh")
        assert made is not None
        assert (made.size, made.projects, made.exclude) == ("2Gi", ["a"], ["node_modules"])
        [created] = [
            e for e in (await volumes.events(conn, made.id, limit=10)).items if e.kind == "created"
        ]
    assert created.actor_kind == "user" and created.detail["proposal"] == proposal["id"]


async def test_create_proposal_is_stale_when_the_name_is_taken_meanwhile(app, pool, boss):
    proposal = (await agent(app, boss, "POST", "/volumes", json=BODY)).json()
    await vol(pool, "fresh")
    done = await approve(app, proposal)
    assert_error(done, 409, "proposal_stale")


@pytest.mark.parametrize(
    ("over", "status", "code"),
    [
        ({"projects": ["b"]}, 403, "project_out_of_scope"),
        ({"projects": ["a", "b"]}, 403, "project_out_of_scope"),
        ({"size": "100Ti"}, 422, "invalid_request"),
        ({"name": "Bad_Name"}, 422, "invalid_request"),
        ({"name": "taken"}, 409, "volume_exists"),
    ],
)
async def test_create_refuses_what_the_user_route_would(app, pool, boss, over, status, code):
    await vol(pool, "taken")
    r = await agent(app, boss, "POST", "/volumes", json={**BODY, **over})
    assert_error(r, status, code)


# ---------------------------------------------------------------- canary


async def test_free_text_of_a_volume_never_reaches_admin_answers_or_logs(app, pool, boss, caplog):
    caplog.set_level(logging.DEBUG)
    marker = "CANARY-ADMIN-VOLUME-8b2f"
    v = await vol(pool, "canary", description=marker, labels=(marker.lower(),))
    p = await preset(pool, "p")
    texts = [
        (await agent(app, boss, "GET", "/volumes")).text,
        (await agent(app, boss, "GET", f"/volumes/{v.id}")).text,
        (
            await agent(app, boss, "POST", f"/presets/{p.id}/volumes/{v.id}", json={"mode": "ro"})
        ).text,
        (await agent(app, boss, "POST", "/volumes", json={**BODY, "description": marker})).text,
    ]
    async with pool.connection() as conn:
        events = str([e.detail for e in (await volumes.events(conn, v.id, limit=50)).items])
    for text in [*texts[:3], events, caplog.text]:
        assert marker.lower() not in text.lower()


# ---------------------------------------------------------------- OpenAPI


DECLARED = {
    ("delete", f"{A}/presets/{{ref}}/volumes/{{volume_id}}"): {403, 404, 409},
    ("post", f"{A}/presets/{{ref}}/volumes/{{volume_id}}"): {403, 404, 409},
    ("get", f"{A}/volumes"): {403},
    ("post", f"{A}/volumes"): {403, 409},
    ("get", f"{A}/volumes/{{volume_id}}"): {403, 404},
    ("post", "/agent/v1/volumes/{name}/save"): {403, 404, 409},
}


@pytest.mark.parametrize(("method", "path"), sorted(DECLARED))
def test_volume_route_declares_its_error_statuses(method, path, dsn, pool):
    op = create_app(make_settings(dsn), pool).openapi()["paths"][path][method]
    codes = {int(c) for c in op["responses"]} - {200, 201, 202, 204, 422}
    assert codes == DECLARED[(method, path)]
