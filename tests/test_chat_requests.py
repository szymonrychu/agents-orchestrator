"""matrix-integration O3: a chat open or invite needs the user's yes on a taskboard decision.

The connector is a `FakeUpstream` held to the vendored OpenAPI document (admin and internal
routes), the taskboard a `FakeTaskboard`.
"""

from __future__ import annotations

import httpx
import pytest
from fakes import fake_upstream as fu
from test_admin_relay import DOCS
from test_admin_relay import rig as admin_rig  # noqa: F401
from test_agent_session_messaging import alone
from test_api_agent import client_for, headers
from test_api_sessions import assert_error

from agents_orchestrator.api.admin import ADMIN_PREFIX


@pytest.fixture
def rig(admin_rig):  # noqa: F811
    return admin_rig


CREATE = ("POST", "/chat/v1/admin/room-requests")
GET = ("GET", "/chat/v1/admin/room-requests/{request_id}")
DECISION = ("PUT", "/chat/v1/internal/room-requests/{request_id}/decision")
CONFIRM = ("POST", "/chat/v1/internal/room-requests/{request_id}/confirm")
URL = ADMIN_PREFIX + "/chats/room-requests"
BODY = {"target": "100012345", "action": "open", "task_key": "T-1"}


def view(rid: int = 5, **over) -> dict:
    row = fu.sample(DOCS["connector"], {"$ref": "#/components/schemas/RoomRequestView"})
    row.update(
        id=rid,
        action="open",
        status="pending_confirmation",
        target_kind="facebook",
        target="100012345",
        display_name="Jane Doe",
        task_key="T-1",
        decision_id=None,
    )
    row.update(over)
    return row


class Connector:
    """One room request held by the fake connector, with the guarded transitions."""

    def __init__(self, rig, **over) -> None:
        self.row = view(**over)
        fake = rig.fakes["connector"]
        self.fake = fake
        fake.answers[CREATE] = lambda call, p: (201, self.row)
        fake.answers[GET] = lambda call, p: (200, self.row)
        fake.answers[DECISION] = self.attach
        fake.answers[CONFIRM] = self.settle

    def attach(self, call, params):
        body = call.json()
        if self.row["status"] != "pending_confirmation" or self.row["decision_id"] is not None:
            return 409, {"error": {"code": "not_pending", "message": "x"}}
        self.row = {**self.row, "decision_id": body["decision_id"]}
        return 200, self.row

    def settle(self, call, params):
        body = call.json()
        if (
            self.row["status"] != "pending_confirmation"
            or self.row["decision_id"] != body["decision_id"]
        ):
            return 409, {"error": {"code": "not_pending", "message": "x"}}
        status = "confirmed" if body["approved"] else "rejected"
        self.row = {**self.row, "status": status}
        return 200, self.row

    def sent(self, method: str, suffix: str) -> list:
        return [c for c in self.fake.calls if c.method == method and c.path.endswith(suffix)]


def confirm_url(rid: int = 5) -> str:
    return f"{URL}/{rid}/confirm"


# ------------------------------------------------------------------------------- create


async def test_open_asks_decision_as_caller(rig):
    conn = Connector(rig)
    r = await rig.call("POST", URL, json=BODY)
    assert r.status_code == 201, r.text
    (decision,) = rig.app.state.fake_taskboard.decisions
    assert decision["asking_session"] == str(rig.boss.id)
    assert decision["asking_slug"] == "rel-boss"
    assert decision["client_key"] == "room-request-5"
    assert [o["label"] for o in decision["options"]] == ["Yes, open it", "No"]
    assert [bool(o.get("recommended")) for o in decision["options"]] == [True, False]
    assert decision["question"] == (
        "Open a Messenger chat with Jane Doe (facebook id 100012345) as Hermes?"
    )
    assert "Requested by session rel-boss with chat_open; room request 5." in decision["context"]
    (attach,) = conn.sent("PUT", "/room-requests/5/decision")
    assert attach.json() == {"task_key": "T-1", "decision_id": decision["id"]}
    assert r.json()["decision_id"] == decision["id"]
    assert r.json()["decision"] == {"id": decision["id"], "url": decision["url"]}


async def test_open_question_texts(rig):
    Connector(rig, target_kind="matrix", target="@bob:example.org", display_name=None)
    await rig.call("POST", URL, json=BODY)
    assert rig.app.state.fake_taskboard.decisions[0]["question"] == (
        "Open a Matrix DM with @bob:example.org?"
    )
    Connector(rig, rid=6, action="invite", target="@bob:example.org", room_id="!r:example.org")
    await rig.call("POST", URL, json={**BODY, "action": "invite", "room_id": "!r:example.org"})
    assert rig.app.state.fake_taskboard.decisions[1]["question"] == (
        "Invite @bob:example.org into room !r:example.org?"
    )


async def test_open_unknown_task_422_not_forwarded(rig):
    Connector(rig)
    r = await rig.call("POST", URL, json={**BODY, "task_key": "T-99"})
    assert_error(r, 422, "unknown_task")
    assert not rig.fakes["connector"].calls


@pytest.mark.parametrize("status", ["candidates", "resolving", "failed"])
async def test_open_without_pending_confirmation_no_decision(rig, status):
    Connector(rig, status=status)
    r = await rig.call("POST", URL, json=BODY)
    assert r.status_code == 201, r.text
    assert rig.app.state.fake_taskboard.decisions == []
    assert not [c for c in rig.fakes["connector"].calls if "/internal/" in c.path]


async def test_open_taskboard_unconfigured_503_not_forwarded(rig):
    Connector(rig)
    rig.app.state.settings = rig.app.state.settings.model_copy(
        update={"taskboard_url": "", "taskboard_token": ""}
    )
    r = await rig.call("POST", URL, json=BODY)
    assert_error(r, 503, "taskboard_unconfigured")
    assert not rig.fakes["connector"].calls


async def test_open_taskboard_down_503_not_forwarded(rig):
    Connector(rig)
    rig.app.state.fake_taskboard.down = True
    r = await rig.call("POST", URL, json=BODY)
    assert_error(r, 503, "taskboard_unavailable")
    assert not rig.fakes["connector"].calls


async def test_open_decision_failure_keeps_the_row_without_decision(rig):
    """The row exists upstream: it is answered with `decision: null`, confirm asks again."""
    conn = Connector(rig)
    board = rig.app.state.fake_taskboard
    real = board.handler

    def flaky(request):
        if request.method == "POST":
            raise httpx.ConnectError("refused", request=request)
        return real(request)

    rig.app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(flaky))
    r = await rig.call("POST", URL, json=BODY)
    assert r.status_code == 201, r.text
    assert r.json()["decision"] is None and r.json()["decision_id"] is None
    rig.app.state.taskboard_http = httpx.AsyncClient(transport=httpx.MockTransport(real))
    r = await rig.call("POST", confirm_url())
    assert r.status_code == 202, r.text
    assert conn.row["decision_id"] == board.decisions[0]["id"]


# ------------------------------------------------------------------------------- confirm


async def test_confirm_asks_when_missing_202(rig):
    conn = Connector(rig)
    r = await rig.call("POST", confirm_url())
    assert r.status_code == 202, r.text
    (decision,) = rig.app.state.fake_taskboard.decisions
    assert r.json()["decision"]["id"] == decision["id"]
    assert conn.row["decision_id"] == decision["id"]
    assert not conn.sent("POST", "/confirm")


async def _asked(rig, **over):
    conn = Connector(rig, **over)
    assert (await rig.call("POST", URL, json=BODY)).status_code == 201
    return conn, rig.app.state.fake_taskboard.decisions[0]["id"]


async def test_confirm_open_decision_409(rig):
    conn, _ = await _asked(rig)
    r = await rig.call("POST", confirm_url())
    assert_error(r, 409, "decision_open")
    assert r.json()["error"]["url"].startswith("https://tasks.test/")
    assert conn.row["status"] == "pending_confirmation"
    assert not conn.sent("POST", "/confirm")


async def test_confirm_yes_confirms(rig):
    conn, did = await _asked(rig)
    rig.app.state.fake_taskboard.answer(did, {"choice": 0, "label": "Yes, open it"})
    r = await rig.call("POST", confirm_url())
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "confirmed"
    (sent,) = conn.sent("POST", "/confirm")
    assert sent.json() == {"decision_id": did, "approved": True}


@pytest.mark.parametrize(
    "answer", [{"choice": 1, "label": "No"}, {"text": "yes please"}, {"choice": 3}]
)
async def test_confirm_other_answer_rejects(rig, answer):
    conn, did = await _asked(rig)
    rig.app.state.fake_taskboard.answer(did, answer)
    r = await rig.call("POST", confirm_url())
    assert r.status_code == 200, r.text
    assert r.json()["status"] == "rejected"
    assert conn.sent("POST", "/confirm")[0].json() == {"decision_id": did, "approved": False}


async def test_confirm_withdrawn_rejects(rig):
    conn, did = await _asked(rig)
    rig.app.state.fake_taskboard.withdraw(did)
    r = await rig.call("POST", confirm_url())
    assert r.json()["status"] == "rejected"
    assert conn.sent("POST", "/confirm")[0].json()["approved"] is False


async def test_confirm_uses_attached_decision_only(rig):
    """A second decision the agent asked itself on the same task, answered yes, confirms nothing."""
    conn, did = await _asked(rig)
    board = rig.app.state.fake_taskboard
    own = board.ask(session=str(rig.boss.id))
    board.answer(own["id"], {"choice": 0})
    r = await rig.call("POST", confirm_url())
    assert_error(r, 409, "decision_open")  # the attached one is still open
    assert not conn.sent("POST", "/confirm")
    board.answer(did, {"choice": 1})
    r = await rig.call("POST", confirm_url())
    assert r.json()["status"] == "rejected"


async def test_confirm_not_pending_409(rig):
    Connector(rig, status="done")
    r = await rig.call("POST", confirm_url())
    assert_error(r, 409, "not_pending")
    assert r.json()["error"]["status"] == "done"


async def test_confirm_unknown_request_404_passes_through(rig):
    missing = (404, {"error": {"code": "not_found", "message": "no such request"}})
    rig.fakes["connector"].answers[GET] = missing
    assert_error(await rig.call("POST", confirm_url(77)), 404, "not_found")


async def test_confirm_taskboard_down_503_row_unchanged(rig):
    conn, _ = await _asked(rig)
    rig.app.state.fake_taskboard.down = True
    r = await rig.call("POST", confirm_url())
    assert_error(r, 503, "taskboard_unavailable")
    assert conn.row["status"] == "pending_confirmation"
    assert not conn.sent("POST", "/confirm")


async def test_confirm_connector_down_503(rig):
    await _asked(rig)
    rig.hosts.broken.add("connector.test")
    assert_error(await rig.call("POST", confirm_url()), 503, "upstream_unavailable")


async def test_confirm_connector_conflict_passes_through(rig):
    conn, did = await _asked(rig)
    rig.app.state.fake_taskboard.answer(did, {"choice": 0})
    conn.row = {**conn.row, "decision_id": did + 1}  # the connector holds another decision
    conn.fake.answers[GET] = lambda call, p: (200, {**conn.row, "decision_id": did})
    r = await rig.call("POST", confirm_url())
    assert_error(r, 409, "not_pending")


async def test_confirm_needs_superleader(rig, pool):
    plain = await alone(pool, "rel-plain", superleader=False)
    async with client_for(rig.app, plain.leader_ip) as http:
        r = await http.post(confirm_url(), headers=headers(plain.id))
    assert_error(r, 403, "not_superleader")
    assert not rig.fakes["connector"].calls


async def test_internal_routes_not_relayed(rig):
    paths = rig.app.openapi()["paths"]
    assert not [p for p in paths if "internal" in p or p.endswith("/decision")]
    assert ADMIN_PREFIX + "/chats/room-requests/{request_id}/confirm" in paths
    assert (await rig.call("PUT", URL + "/5/decision", json={})).status_code in (404, 405)
    assert not rig.fakes["connector"].calls
