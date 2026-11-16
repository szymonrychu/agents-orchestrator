"""GET /agent/v1/admin/handoffs and /handoffs/{id}: the superleader reads the handoff history."""

from __future__ import annotations

import uuid

from factories import insert_agent, insert_session
from test_agent_session_messaging import alone
from test_api_agent import KEY, client_for, headers, make_settings
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX
from agents_orchestrator.api.app import create_app
from agents_orchestrator.db import handoffs

H = f"{ADMIN_PREFIX}/handoffs"


async def get(pool, dsn, path, *, superleader=True, **params):
    boss = await alone(pool, f"ho-{uuid.uuid4().hex[:8]}", superleader=superleader)
    settings = make_settings(dsn).model_copy(update={"master_key": KEY})
    app = create_app(settings, pool)
    async with client_for(app, boss.leader_ip) as http:
        return await http.get(path, headers=headers(boss.id), params=params)


async def seed(pool, slug: str, texts: list[str]) -> tuple[dict, list[int]]:
    session = await insert_session(pool, slug=slug)
    ids = []
    for n, text in enumerate(texts):
        agent = await insert_agent(pool, session["id"], ordinal=n, pod_name=f"{slug}-{n}")
        async with pool.connection() as conn:
            row = await handoffs.insert(
                conn, session["id"], agent["id"], "voluntary", "done", handoff_text=text
            )
        ids.append(row.id)
    return session, ids


async def test_handoffs_list_and_get(pool, dsn):
    s1, ids1 = await seed(pool, "ho-one", ["first text", "second text"])
    _, ids2 = await seed(pool, "ho-two", ["other"])
    r = await get(pool, dsn, H)
    assert r.status_code == 200, r.text
    body = r.json()
    assert [h["id"] for h in body["handoffs"]] == sorted([*ids1, *ids2], reverse=True)
    assert body["has_more"] is False
    first = next(h for h in body["handoffs"] if h["id"] == ids1[0])
    assert first["session_id"] == str(s1["id"]) and first["session_slug"] == "ho-one"
    assert first["kind"] == "voluntary" and first["size"] == len("first text")
    assert set(first) == {
        "id",
        "session_id",
        "session_slug",
        "agent_id",
        "kind",
        "created_at",
        "size",
    }
    assert "handoff_text" not in first

    for ref in ("ho-one", str(s1["id"])):
        r = await get(pool, dsn, H, session=ref)
        assert [h["id"] for h in r.json()["handoffs"]] == sorted(ids1, reverse=True)

    page = (await get(pool, dsn, H, limit=2)).json()
    assert len(page["handoffs"]) == 2 and page["has_more"] is True
    rest = (await get(pool, dsn, H, limit=2, before_id=page["handoffs"][-1]["id"])).json()
    assert [h["id"] for h in rest["handoffs"]] == [min(ids1)] and rest["has_more"] is False

    one = await get(pool, dsn, f"{H}/{ids1[1]}")
    assert one.status_code == 200, one.text
    assert one.json()["handoff_text"] == "second text" and one.json()["id"] == ids1[1]


async def test_handoffs_unknown_ids_are_404(pool, dsn):
    assert_error(await get(pool, dsn, f"{H}/999999"), 404, "handoff_not_found")
    assert_error(await get(pool, dsn, H, session="no-such-slug"), 404, "not_found")


async def test_handoffs_forbidden_for_plain_session(pool, dsn):
    _, ids = await seed(pool, "ho-plain", ["secretish"])
    for path in (H, f"{H}/{ids[0]}"):
        r = await get(pool, dsn, path, superleader=False)
        assert_error(r, 403, "not_superleader")
