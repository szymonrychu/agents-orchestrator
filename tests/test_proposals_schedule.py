"""api.proposals for `kind: "schedule"` (preset-change-proposals plan 3.1, 3.5 schedules): create,
revise and the human review that applies to agents-schedule with `If-Match`.

The schedule upstream is a stateful `FakeUpstream` held to the vendored agents-schedule document
(as `test_admin_relay` builds it): a PUT checks `If-Match` against the stored `updated_at` (412)
and bumps it. Service level: real Postgres, no kube (a proposal holds no value)."""

from __future__ import annotations

import copy
import secrets as stdlib_secrets
import uuid
from types import SimpleNamespace

import httpx
import pydantic
import pytest
from fakes import fake_upstream as fu
from fastapi.exceptions import RequestValidationError
from psycopg.types.json import Jsonb
from test_api_agent import make_settings
from test_proposals_service import _error, _rows, _session

from agents_orchestrator.api import admin_relay, proposals, schemas

HOST = "http://schedule.test"
ONE = admin_relay.SERVICES["schedule"][1] + "/schedules/{schedule_id}"
GET, PUT = ("GET", ONE), ("PUT", ONE)


class Schedules:
    """One stored schedule behind the fake: GET answers it (404 when `gone`), PUT replaces it
    under `If-Match` and bumps `updated_at`; `put_answer` overrides the PUT's answer."""

    def __init__(self, fake: fu.FakeUpstream, preset_id: str) -> None:
        self.fake = fake
        self.gone = False
        self.put_answer: tuple[int, object] | None = None
        self.stamp = 0
        self.obj = fu.sample(fake.doc, {"$ref": "#/components/schemas/ScheduleView"})
        self.obj.update(
            id=str(uuid.uuid4()),
            name="nightly",
            cron="0 3 * * *",
            timezone="UTC",
            preset_id=preset_id,
            prompt="run the nightly checks",
            enabled=True,
        )
        self.bump()
        fake.answers[GET] = self.get
        fake.answers[PUT] = self.put

    def bump(self) -> None:
        self.stamp += 1
        self.obj["updated_at"] = f"2026-11-08T03:00:{self.stamp:02d}.123456Z"

    def puts(self) -> list[fu.Call]:
        return [c for c in self.fake.calls if c.method == "PUT"]

    def get(self, call, params):
        if self.gone or params["schedule_id"] != self.obj["id"]:
            return 404, {"error": {"code": "not_found", "message": "schedule not found"}}
        return 200, copy.deepcopy(self.obj)

    def put(self, call, params):
        if self.put_answer is not None:
            return self.put_answer
        if self.gone:
            return 404, {"error": {"code": "not_found", "message": "schedule not found"}}
        if call.headers.get("if-match") != f'"{self.obj["updated_at"]}"':
            return 412, {"error": {"code": "precondition_failed", "message": "changed"}}
        body = call.json()
        # agents-schedule stores the cron whitespace-normalised and fills its default timezone
        body["cron"] = " ".join(body["cron"].split())
        self.obj.update(body, timezone=body.get("timezone") or "UTC")
        self.bump()
        return 200, copy.deepcopy(self.obj)


async def _preset(pool, manageable: bool) -> str:
    pid = uuid.uuid4()
    async with pool.connection() as conn:
        await conn.execute(
            "INSERT INTO presets (id, name, spec, superleader_manageable) VALUES (%s,%s,%s,%s)",
            (pid, f"p-{pid.hex[:8]}", Jsonb({}), manageable),
        )
    return str(pid)


@pytest.fixture
async def rig(pool, dsn):
    token = "schedule-" + stdlib_secrets.token_hex(16)
    settings = make_settings(dsn).model_copy(update={"schedule_url": HOST, "schedule_token": token})
    fake = fu.FakeUpstream(fu.load("schedule"), token)
    http = httpx.AsyncClient(transport=httpx.ASGITransport(app=fake.app))
    r = SimpleNamespace(
        settings=settings,
        fake=fake,
        boss=await _session(pool, "sched-boss"),
        vis=await _preset(pool, True),
        hid=await _preset(pool, False),
        app=SimpleNamespace(state=SimpleNamespace(admin_http=http)),
    )
    r.sched = Schedules(fake, r.vis)
    yield r
    await http.aclose()


def _request(rig) -> SimpleNamespace:
    return SimpleNamespace(state=SimpleNamespace(admin_secrets=[]), app=rig.app)


async def _create(pool, rig, **body):
    body = {
        "kind": "schedule",
        "target": rig.sched.obj["id"],
        "reason": "move it",
        "patch": {"cron": "0 4 * * *", "prompt": "run the nightly checks twice"},
        **body,
    }
    async with pool.connection() as conn:
        view, _ = await proposals.create(
            _request(rig),
            conn,
            rig.settings,
            schemas.ProposalIn.model_validate(body),
            caller=rig.boss,
            superleader=True,
        )
    return view


async def _review(pool, rig, view, decision="approve", note=None):
    items = [{"path": i.path, "decision": decision, "note": note} for i in view.revisions[-1].items]
    body = schemas.ReviewIn.model_validate({"revision": view.revision, "items": items})
    async with pool.connection() as conn:
        return await proposals.review(_request(rig), conn, rig.settings, view.id, body)


async def _state(pool, proposal_id):
    [(status,)] = await _rows(
        pool, "SELECT status FROM change_proposals WHERE id = %s", (proposal_id,)
    )
    decisions = await _rows(
        pool,
        "SELECT path, decision FROM change_proposal_items WHERE proposal_id = %s ORDER BY path",
        (proposal_id,),
    )
    return status, dict(decisions)


async def _notices(pool, session_id) -> list[str]:
    rows = await _rows(
        pool,
        "SELECT text FROM messages WHERE session_id = %s AND notice = 'proposal' ORDER BY id",
        (session_id,),
    )
    return [t for (t,) in rows]


# ---------------------------------------------------------------- create


async def test_create_records_one_classified_item_per_field_and_the_updated_at(pool, rig):
    stamp = rig.sched.obj["updated_at"]
    view = await _create(pool, rig)
    assert (view.kind, view.target_id, view.target_name) == (
        "schedule",
        rig.sched.obj["id"],
        "nightly",
    )
    assert view.status == "open" and view.stale is False
    items = {i.path: i for i in view.revisions[0].items}
    assert sorted(items) == ["cron", "prompt"]
    assert (items["cron"].before, items["cron"].after) == ("0 3 * * *", "0 4 * * *")
    assert (items["cron"].security, items["prompt"].security) == (False, True)
    assert not any(i.grant for i in items.values())
    [(fingerprint, base)] = await _rows(
        pool,
        "SELECT fingerprint, base FROM change_proposal_revisions WHERE proposal_id = %s",
        (view.id,),
    )
    assert fingerprint == stamp  # byte for byte what the view said, the If-Match value
    assert base == {
        "name": "nightly",
        "cron": "0 3 * * *",
        "timezone": "UTC",
        "preset_id": rig.vis,
        "prompt": "run the nightly checks",
        "enabled": True,
    }
    assert rig.sched.puts() == []


async def test_a_preset_id_change_is_security_and_needs_a_visible_preset(pool, rig):
    other = await _preset(pool, True)
    view = await _create(pool, rig, patch={"preset_id": other, "enabled": False})
    items = {i.path: i.security for i in view.revisions[0].items}
    assert items == {"preset_id": True, "enabled": False}
    with pytest.raises(proposals.ApiError) as exc:
        await _create(pool, rig, patch={"preset_id": rig.hid})
    _error(exc, 404, "not_found")


async def test_a_key_outside_schedule_in_is_422(pool, rig):
    with pytest.raises(proposals.ApiError) as exc:
        await _create(pool, rig, patch={"cron": "0 4 * * *", "id": "x"})
    assert "id" in _error(exc, 422, "invalid_request").message


async def test_secrets_are_an_unknown_field(pool, rig):
    with pytest.raises(pydantic.ValidationError) as exc:
        await _create(pool, rig, secrets={"TOKEN": "v"})
    assert [e["type"] for e in exc.value.errors(include_input=False)] == ["extra_forbidden"]
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_a_merged_object_failing_schedule_in_is_422(pool, rig):
    with pytest.raises(RequestValidationError):
        await _create(pool, rig, patch={"name": ""})
    with pytest.raises(RequestValidationError):
        await _create(pool, rig, patch={"prompt": None})  # RFC 7396 removal of a required field


async def test_an_unchanged_patch_is_nothing_changed(pool, rig):
    with pytest.raises(proposals.ApiError) as exc:
        await _create(pool, rig, patch={"cron": "0 3 * * *"})
    _error(exc, 422, "nothing_changed")


async def test_an_unknown_or_hidden_schedule_is_404(pool, rig):
    for target in (str(uuid.uuid4()), "not-a-uuid"):
        with pytest.raises(proposals.ApiError) as exc:
            await _create(pool, rig, target=target)
        _error(exc, 404, "not_found")
    rig.sched.obj["preset_id"] = rig.hid
    with pytest.raises(proposals.ApiError) as exc:
        await _create(pool, rig)
    _error(exc, 404, "not_found")
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


# ---------------------------------------------------------------- review


async def test_approve_puts_the_full_object_with_if_match_and_records_applied(pool, rig):
    stamp = rig.sched.obj["updated_at"]
    view = await _create(pool, rig)
    done = await _review(pool, rig, view)
    [put] = rig.sched.puts()
    assert put.headers["if-match"] == f'"{stamp}"'
    assert put.json() == {
        "name": "nightly",
        "cron": "0 4 * * *",
        "timezone": "UTC",
        "preset_id": rig.vis,
        "prompt": "run the nightly checks twice",
        "enabled": True,
    }
    assert done.status == "applied"
    assert await _state(pool, view.id) == ("applied", {"cron": "approved", "prompt": "approved"})
    [notice] = await _notices(pool, rig.boss.id)
    assert notice.startswith(f"Proposal P-{view.id} r1 (schedule nightly) was reviewed: applied")


async def test_a_partial_approve_puts_only_the_approved_field(pool, rig):
    view = await _create(pool, rig)
    items = [
        {"path": "cron", "decision": "approve", "note": None},
        {"path": "prompt", "decision": "deny", "note": "keep it"},
    ]
    body = schemas.ReviewIn.model_validate({"revision": 1, "items": items})
    async with pool.connection() as conn:
        await proposals.review(_request(rig), conn, rig.settings, view.id, body)
    [put] = rig.sched.puts()
    assert (put.json()["cron"], put.json()["prompt"]) == ("0 4 * * *", "run the nightly checks")


async def test_a_schedule_changed_since_create_is_409_stale_and_nothing_is_put(pool, rig):
    view = await _create(pool, rig)
    rig.sched.obj["name"] = "renamed"
    rig.sched.bump()
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    assert "schedule changed" in _error(exc, 409, "proposal_stale").message
    assert rig.sched.puts() == []
    assert (await _state(pool, view.id))[0] == "stale"
    [notice] = await _notices(pool, rig.boss.id)
    assert "is stale: the schedule changed" in notice


async def test_a_412_at_the_put_is_409_stale(pool, rig):
    view = await _create(pool, rig)
    rig.sched.put_answer = (412, {"error": {"code": "precondition_failed", "message": "changed"}})
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    _error(exc, 409, "proposal_stale")
    assert len(rig.sched.puts()) == 1
    assert await _state(pool, view.id) == ("stale", {"cron": "pending", "prompt": "pending"})


async def test_a_deleted_schedule_is_409_target_gone(pool, rig):
    view = await _create(pool, rig)
    rig.sched.gone = True
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    _error(exc, 409, "target_gone")
    assert rig.sched.puts() == []
    assert (await _state(pool, view.id))[0] == "target_gone"


async def test_an_upstream_422_is_invalid_spec_and_the_proposal_stays_open(pool, rig):
    view = await _create(pool, rig)
    rig.sched.put_answer = (422, {"error": {"code": "invalid_cron", "message": "bad cron"}})
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    assert _error(exc, 422, "invalid_spec").message == "bad cron"
    assert await _state(pool, view.id) == ("open", {"cron": "pending", "prompt": "pending"})
    assert await _notices(pool, rig.boss.id) == []


async def test_a_failed_commit_after_the_put_is_recorded_applied_on_retry(pool, rig, monkeypatch):
    view = await _create(pool, rig)
    real = proposals._notify

    async def broken(conn, p, text):
        raise RuntimeError("commit forced to fail")

    monkeypatch.setattr(proposals, "_notify", broken)
    with pytest.raises(RuntimeError):
        await _review(pool, rig, view)
    assert len(rig.sched.puts()) == 1  # the PUT landed upstream
    assert (await _state(pool, view.id))[0] == "open"  # the orchestrator rolled back
    monkeypatch.setattr(proposals, "_notify", real)
    done = await _review(pool, rig, view)
    assert done.status == "applied"
    assert len(rig.sched.puts()) == 1  # idempotent: no second PUT
    assert await _state(pool, view.id) == ("applied", {"cron": "approved", "prompt": "approved"})


async def test_a_changed_schedule_not_matching_the_approval_is_still_stale(pool, rig):
    view = await _create(pool, rig)
    rig.sched.obj["cron"] = "0 4 * * *"  # one approved field landed, the other did not
    rig.sched.bump()
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    _error(exc, 409, "proposal_stale")


async def test_rework_and_deny_write_nothing_upstream(pool, rig):
    view = await _create(pool, rig)
    done = await _review(pool, rig, view, "rework", "use 05:00")
    assert done.status == "rework"
    assert rig.sched.puts() == []
    body = schemas.ProposalRevise.model_validate({"patch": {"cron": "0 5 * * *"}})
    async with pool.connection() as conn:
        revised = await proposals.revise(
            _request(rig), conn, rig.settings, view.id, body, caller=rig.boss
        )
    assert revised.revision == 2 and revised.status == "open"
    assert [i.path for i in revised.revisions[-1].items] == ["cron"]
    done = await _review(pool, rig, revised, "deny")
    assert done.status == "denied"
    assert rig.sched.puts() == []


async def test_revise_rebases_on_the_schedule_now(pool, rig):
    view = await _create(pool, rig)
    rig.sched.obj["name"] = "renamed"
    rig.sched.bump()
    stamp = rig.sched.obj["updated_at"]
    body = schemas.ProposalRevise.model_validate({"patch": {"cron": "0 5 * * *"}})
    async with pool.connection() as conn:
        revised = await proposals.revise(
            _request(rig), conn, rig.settings, view.id, body, caller=rig.boss
        )
    [(fingerprint,)] = await _rows(
        pool,
        "SELECT fingerprint FROM change_proposal_revisions WHERE proposal_id = %s AND revision = 2",
        (view.id,),
    )
    assert fingerprint == stamp
    done = await _review(pool, rig, revised)
    assert done.status == "applied"
    assert rig.sched.puts()[0].json()["name"] == "renamed"


async def test_applying_one_schedule_proposal_makes_another_on_it_stale(pool, rig):
    first = await _create(pool, rig)
    second = await _create(pool, rig, patch={"enabled": False})
    await _review(pool, rig, first)
    assert (await _state(pool, second.id))[0] == "stale"


# ---------------------------------------------------------------- review follow-ups (B3 review)


async def _fail_commit_then_retry(pool, rig, view, monkeypatch):
    """The PUT lands, the orchestrator commit fails; then the review is retried."""
    real = proposals._notify

    async def broken(conn, p, text):
        raise RuntimeError("commit forced to fail")

    monkeypatch.setattr(proposals, "_notify", broken)
    with pytest.raises(RuntimeError):
        await _review(pool, rig, view)
    monkeypatch.setattr(proposals, "_notify", real)
    return await _review(pool, rig, view)


async def test_a_cron_with_extra_whitespace_is_normalised_and_the_retry_is_applied(
    pool, rig, monkeypatch
):
    view = await _create(pool, rig, patch={"cron": "0  4 * * *"})
    [item] = view.revisions[-1].items
    assert item.after == "0 4 * * *"  # what agents-schedule stores
    done = await _fail_commit_then_retry(pool, rig, view, monkeypatch)
    assert done.status == "applied"
    assert len(rig.sched.puts()) == 1
    assert await _state(pool, view.id) == ("applied", {"cron": "approved"})


async def test_a_null_timezone_in_the_patch_is_refused_with_422(pool, rig):
    with pytest.raises(proposals.ApiError) as exc:
        await _create(pool, rig, patch={"timezone": None})
    assert "timezone" in _error(exc, 422, "invalid_request").message
    assert await _rows(pool, "SELECT id FROM change_proposals") == []


async def test_an_approved_preset_that_stopped_being_manageable_is_stale_at_review(pool, rig):
    other = await _preset(pool, True)
    view = await _create(pool, rig, patch={"preset_id": other})
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (other,)
        )
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    _error(exc, 409, "proposal_stale")
    assert rig.sched.puts() == []
    assert (await _state(pool, view.id))[0] == "stale"


async def test_a_schedule_whose_preset_became_hidden_is_target_gone_at_review(pool, rig):
    view = await _create(pool, rig)
    async with pool.connection() as conn:
        await conn.execute(
            "UPDATE presets SET superleader_manageable = false WHERE id = %s", (rig.vis,)
        )
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    _error(exc, 409, "target_gone")
    assert rig.sched.puts() == []
    assert (await _state(pool, view.id))[0] == "target_gone"


async def test_an_upstream_409_is_invalid_spec_and_the_proposal_stays_open(pool, rig):
    view = await _create(pool, rig)
    rig.sched.put_answer = (409, {"error": {"code": "name_taken", "message": "name taken"}})
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    assert _error(exc, 422, "invalid_spec").message == "name taken"
    assert await _state(pool, view.id) == ("open", {"cron": "pending", "prompt": "pending"})


async def test_an_upstream_503_is_503_and_the_proposal_stays_open(pool, rig):
    view = await _create(pool, rig)
    rig.sched.put_answer = (503, {"error": {"code": "down", "message": "down"}})
    with pytest.raises(proposals.ApiError) as exc:
        await _review(pool, rig, view)
    _error(exc, 503, "upstream_unavailable")
    assert await _state(pool, view.id) == ("open", {"cron": "pending", "prompt": "pending"})
    assert await _notices(pool, rig.boss.id) == []


async def test_revise_is_404_when_the_schedule_is_deleted_or_its_preset_hidden(pool, rig):
    view = await _create(pool, rig)
    body = schemas.ProposalRevise.model_validate({"patch": {"cron": "0 5 * * *"}})

    async def revise():
        async with pool.connection() as conn:
            await proposals.revise(
                _request(rig), conn, rig.settings, view.id, body, caller=rig.boss
            )

    rig.sched.obj["preset_id"] = rig.hid
    with pytest.raises(proposals.ApiError) as exc:
        await revise()
    _error(exc, 404, "not_found")
    rig.sched.gone = True
    with pytest.raises(proposals.ApiError) as exc:
        await revise()
    _error(exc, 404, "not_found")
