"""Human-only dialog relay: GET and POST /api/v1/agents/{agent_id}/dialog (T-98, O5)."""

from __future__ import annotations

import base64
import json
import logging
import uuid
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
from factories import insert_session
from fakes.fake_wrapper import FakeWrapper
from test_api_files import KEY, token_of
from test_api_sessions import assert_error, client_for, make_settings
from test_api_terminals import GOOD_ORIGIN, TermRig
from test_service_port import TOKENS, client
from test_vault_canary import table_texts

from agents_orchestrator.api import service_auth, workspace
from agents_orchestrator.api.app import create_app

RID = "perm_0123456789ab"
SINCE = "2026-11-09T10:00:00Z"
SECRET_TEXT = "please-do-not-log-this-message"


def permission_state(request_id: str = RID) -> dict:
    return {
        "kind": "permission",
        "since": SINCE,
        "permission": {
            "request_id": request_id,
            "tool_name": "Bash",
            "tool_input": '{"command": "ls"}',
            "truncated": False,
            "suggestions": [],
            "agent_id": None,
            "since": SINCE,
            "expires_at": "2026-11-09T10:30:00Z",
        },
        "tail": None,
    }


def dialog_state(kind: str = "dialog") -> dict:
    return {"kind": kind, "since": SINCE, "permission": None, "tail": "Select a model"}


def dialog_calls(fake: FakeWrapper) -> list[tuple[str, str]]:
    return [call for call in fake.requests if call[1].endswith("/dialog")]


@pytest_asyncio.fixture
async def rig(pool, dsn) -> AsyncIterator[TermRig]:
    settings = make_settings(dsn, master_key=KEY, allowed_origins=GOOD_ORIGIN)
    app = create_app(settings, pool)
    session = await insert_session(pool)
    state = TermRig(None, pool, session, [], app)
    async with client_for(app) as http:
        state.http = http
        try:
            yield state
        finally:
            await workspace.close_client(app)
            for fake in state.fakes:
                await fake.stop()


def url(row: dict) -> str:
    return f"/api/v1/agents/{row['id']}/dialog"


async def test_get_dialog_forwards(rig):
    fake, sid, row = await rig.agent(0)
    r = await rig.http.get(url(row))
    assert r.status_code == 200, r.text
    assert r.json() == {
        "kind": "none",
        "since": None,
        "permission": None,
        "tail": None,
        "agent_id": str(row["id"]),
        "session_id": str(rig.session["id"]),
    }
    fake.set_dialog(permission_state(), sid)
    r = await rig.http.get(url(row))
    body = r.json()
    assert body["kind"] == "permission" and body["since"] == SINCE
    assert body["permission"]["request_id"] == RID
    assert body["permission"]["tool_name"] == "Bash"
    assert body["agent_id"] == str(row["id"])
    assert body["session_id"] == str(rig.session["id"])
    assert dialog_calls(fake) == [("GET", f"/v1/sessions/{sid}/dialog")] * 2


async def test_post_allow_forwards_body(rig):
    fake, sid, row = await rig.agent(0)
    fake.set_dialog(permission_state(), sid)
    sent = {"action": "allow", "request_id": RID}
    r = await rig.http.post(url(row), json=sent)
    assert r.status_code == 200, r.text
    assert r.json() == {
        "status": "answered",
        "request_id": RID,
        "behavior": "allow",
        "escape_sent": None,
        "agent_id": str(row["id"]),
    }
    assert fake.dialog_answers == [sent]
    assert dialog_calls(fake) == [("POST", f"/v1/sessions/{sid}/dialog")]


async def test_post_deny_forwards_message_and_dismiss_fields(rig):
    fake, sid, row = await rig.agent(0)
    fake.set_dialog(permission_state(), sid)
    sent = {"action": "deny", "request_id": RID, "message": "not now"}
    assert (await rig.http.post(url(row), json=sent)).status_code == 200
    fake.set_dialog(dialog_state(), sid)
    sent = {"action": "dismiss", "expect_since": SINCE}
    r = await rig.http.post(url(row), json=sent)
    assert r.json()["status"] == "dismissed" and r.json()["escape_sent"] is True
    assert fake.dialog_answers[-1] == sent


@pytest.mark.parametrize("code", ["no_dialog", "dialog_changed", "permission_resolved"])
async def test_post_conflicts_pass_through(rig, code):
    fake, sid, row = await rig.agent(0)
    body = {"action": "allow", "request_id": RID}
    if code == "dialog_changed":
        fake.set_dialog(permission_state("perm_ffffffffffff"), sid)
    elif code == "permission_resolved":
        fake.set_dialog(permission_state(), sid)
        assert (await rig.http.post(url(row), json=body)).status_code == 200
    r = await rig.http.post(url(row), json=body)
    assert_error(r, 409, code)


async def test_post_not_dismissable_passes_through(rig):
    fake, sid, row = await rig.agent(0)
    fake.set_dialog(dialog_state("usage_wait"), sid)
    r = await rig.http.post(url(row), json={"action": "dismiss"})
    assert_error(r, 409, "not_dismissable")


async def test_post_extra_key_422_not_forwarded(rig):
    fake, _, row = await rig.agent(0)
    for body in (
        {"action": "dismiss", "force": True},
        {"action": "explode"},
        {"action": "deny", "message": "x" * 2001},
    ):
        assert_error(await rig.http.post(url(row), json=body), 422, "invalid_request")
    assert dialog_calls(fake) == []


async def test_unknown_agent_404(rig):
    await rig.agent(0)
    missing = f"/api/v1/agents/{uuid.uuid4()}/dialog"
    assert_error(await rig.http.get(missing), 404, "not_found")
    assert_error(await rig.http.post(missing, json={"action": "dismiss"}), 404, "not_found")


async def test_agent_not_ready_409(rig):
    _, _, row = await rig.agent(0, ready=False)
    assert_error(await rig.http.get(url(row)), 409, "agent_not_ready")
    assert_error(await rig.http.post(url(row), json={"action": "dismiss"}), 409, "agent_not_ready")


async def test_unreachable_502(rig):
    fake, _, row = await rig.agent(0)
    await fake.set_unreachable(True)
    assert_error(await rig.http.get(url(row)), 502, "wrapper_unreachable")
    r = await rig.http.post(url(row), json={"action": "dismiss"})
    assert_error(r, 502, "wrapper_unreachable")
    await fake.set_unreachable(False)


async def test_unexpected_wrapper_body_502(rig):
    fake, sid, row = await rig.agent(0)
    fake.set_dialog({"kind": "bogus", "since": None}, sid)
    assert_error(await rig.http.get(url(row)), 502, "wrapper_error")


async def test_origin_guard(rig):
    fake, sid, row = await rig.agent(0)
    fake.set_dialog(permission_state(), sid)
    bad = {"Origin": "https://evil.example"}
    r = await rig.http.post(url(row), json={"action": "allow", "request_id": RID}, headers=bad)
    assert_error(r, 403, "forbidden_origin")
    assert dialog_calls(fake) == []
    assert (await rig.http.get(url(row), headers=bad)).status_code == 200
    good = {"Origin": GOOD_ORIGIN}
    r = await rig.http.post(url(row), json={"action": "allow", "request_id": RID}, headers=good)
    assert r.status_code == 200, r.text


async def test_token_not_in_logs_or_responses(rig, caplog):
    fake, sid, row = await rig.agent(0)
    token = token_of(rig.session["id"])
    caplog.set_level(logging.DEBUG)
    fake.set_dialog(permission_state(), sid)
    responses = [await rig.http.get(url(row))]
    body = {"action": "allow", "request_id": RID}
    responses.append(await rig.http.post(url(row), json=body))
    responses.append(await rig.http.post(url(row), json=body))  # 409 permission_resolved
    fake.token = "different"
    responses.append(await rig.http.get(url(row)))
    responses.append(await rig.http.post(url(row), json=body))
    fake.token = token
    await fake.set_unreachable(True)
    responses.append(await rig.http.get(url(row)))
    responses.append(await rig.http.post(url(row), json=body))
    await fake.set_unreachable(False)

    assert [r.status_code for r in responses[:3]] == [200, 200, 409]
    assert caplog.records
    for record in caplog.records:
        assert token not in record.getMessage()
        assert token not in str(record.args) and token not in (record.exc_text or "")
    for resp in responses:
        assert token not in resp.text
        assert token not in str(dict(resp.headers))


async def test_answer_logged_without_message(rig, caplog):
    fake, sid, row = await rig.agent(0)
    caplog.set_level(logging.INFO)
    fake.set_dialog(permission_state(), sid)
    r = await rig.http.post(
        url(row), json={"action": "deny", "request_id": RID, "message": SECRET_TEXT}
    )
    assert r.status_code == 200, r.text
    # a refused answer is logged too, with the http status
    await rig.http.post(url(row), json={"action": "allow", "request_id": RID})
    lines = [rec.getMessage() for rec in caplog.records if "dialog answer" in rec.getMessage()]
    assert len(lines) == 2, lines
    first = lines[0]
    assert f"agent={row['id']}" in first and f"pod={row['pod_name']}" in first
    assert "action=deny" in first and f"request_id={RID}" in first and "status=answered" in first
    assert "status=409" in lines[1]
    assert all(SECRET_TEXT not in rec.getMessage() for rec in caplog.records)
    assert all("tool_input" not in m and '"command"' not in m for m in lines)


async def test_dismiss_log_has_dash_request_id(rig, caplog):
    fake, sid, row = await rig.agent(0)
    caplog.set_level(logging.INFO)
    fake.set_dialog(dialog_state(), sid)
    await rig.http.post(url(row), json={"action": "dismiss"})
    lines = [rec.getMessage() for rec in caplog.records if "dialog answer" in rec.getMessage()]
    assert len(lines) == 1 and "request_id=- " in lines[0] and "status=dismissed" in lines[0]


async def test_not_on_agent_or_service_port(rig, pool, dsn):
    _, _, row = await rig.agent(0)
    path = f"/api/v1/agents/{row['id']}/dialog"
    # the agent port serves /agent/v1 only
    async with client(rig.app, 8001) as agent_port:
        assert_error(await agent_port.get(path), 404, "not_found")
        assert_error(await agent_port.get(f"/agent/v1/agents/{row['id']}/dialog"), 404, "not_found")
        assert_error(
            await agent_port.post(f"/agent/v1/agents/{row['id']}/dialog", json={}), 404, "not_found"
        )
    # neither the service-token allowlist nor any relay allowlist lists it
    for method in ("GET", "POST"):
        assert service_auth.route_allowed(method, path) is False
    app = create_app(make_settings(dsn, service_tokens=TOKENS), pool)
    async with client(app, 8002) as service_port:
        assert_error(await service_port.get(path), 403, "forbidden")
        assert_error(await service_port.post(path, json={"action": "dismiss"}), 403, "forbidden")
    allowed = " ".join(p.pattern for _, p in service_auth.ALLOWED_ROUTES)
    assert "dialog" not in allowed


def test_dialog_route_only_on_main_api_prefix(rig):
    """No /agent/v1 route and no relay allowlist entry mentions the dialog."""
    paths = set(rig.app.openapi()["paths"])
    assert {p for p in paths if "dialog" in p} == {"/api/v1/agents/{agent_id}/dialog"}
    assert not any(p.startswith("/agent/v1") and "dialog" in p for p in paths)


@pytest.mark.parametrize(
    "bad", ["perm_x\nforged", "perm_0123456789AB", "perm_0123456789abc", "x" * 5000], ids=range(4)
)
async def test_bad_request_id_422_not_forwarded(rig, bad):
    fake, sid, row = await rig.agent(0)
    fake.set_dialog(permission_state(), sid)
    r = await rig.http.post(url(row), json={"action": "allow", "request_id": bad})
    assert r.status_code == 422, r.text
    assert dialog_calls(fake) == []


async def test_refused_answers_are_audited(rig, caplog):
    fake, sid, row = await rig.agent(0)
    caplog.set_level(logging.INFO)
    fake.set_dialog(permission_state(), sid)
    unknown = "00000000-0000-4000-8000-000000000000"
    r = await rig.http.post(f"/api/v1/agents/{unknown}/dialog", json={"action": "dismiss"})
    assert r.status_code == 404
    bad = {"Origin": "https://evil.example"}
    r = await rig.http.post(url(row), json={"action": "deny", "request_id": RID}, headers=bad)
    assert r.status_code == 403
    lines = [rec.getMessage() for rec in caplog.records if "dialog answer" in rec.getMessage()]
    assert len(lines) == 2, lines
    for want in ("status=404", f"agent={unknown}", "action=dismiss"):
        assert want in lines[0]
    for want in ("status=403", f"agent={row['id']}", "action=deny"):
        assert want in lines[1]
    assert all("tool_input" not in m for m in lines)


CANARY_INPUT = "CANARY-DIALOG-TOOL-INPUT-5d1e"
CANARY_MESSAGE = "CANARY-DIALOG-DENY-MESSAGE-b82c"


async def test_dialog_canary_never_leaks(rig, pool, caplog):
    """Secrets canary (secrets-model, "New route or tool"). Listener: the human main port only
    (see test_not_on_agent_or_service_port). Actor: the peer address, logged by the audit line;
    the main port has no user identity. The relayed GET body is the human's own view and may
    carry the permission's tool_input (positive control); nothing else may: no log record
    (audit line included), no row of any table, no event, no error body, not even base64."""
    fake, sid, row = await rig.agent(0)
    caplog.set_level(logging.DEBUG)
    state = permission_state()
    state["permission"]["tool_input"] = json.dumps({"command": f"echo {CANARY_INPUT}"})
    fake.set_dialog(state, sid)
    allowed: list[str] = []
    errors: list[str] = []

    got = await rig.http.get(url(row))
    assert got.status_code == 200 and CANARY_INPUT in got.text  # the human's view
    allowed.append(got.text)
    ok = await rig.http.post(
        url(row), json={"action": "deny", "request_id": RID, "message": CANARY_MESSAGE}
    )
    assert ok.status_code == 200, ok.text
    allowed.append(ok.text)
    body = {"action": "allow", "request_id": RID, "message": CANARY_MESSAGE}
    errors.append((await rig.http.post(url(row), json=body)).text)  # 409 permission_resolved
    errors.append((await rig.http.post(url(row), json={**body, "request_id": "bad"})).text)  # 422
    bad = {"Origin": "https://evil.example"}
    errors.append((await rig.http.post(url(row), json=body, headers=bad)).text)  # 403
    fake.set_dialog(state, sid)
    errors.append(
        (await rig.http.post(url(row), json={**body, "request_id": "perm_ffffffffffff"})).text
    )
    fake.set_dialog({"kind": "bogus", "since": None, "tool_input": CANARY_INPUT}, sid)
    errors.append((await rig.http.get(url(row))).text)  # 502 wrapper_error
    fake.set_dialog(None, sid)
    errors.append((await rig.http.post(url(row), json=body)).text)  # 409 no_dialog
    await fake.set_unreachable(True)
    errors.append((await rig.http.get(url(row))).text)
    errors.append((await rig.http.post(url(row), json=body)).text)
    await fake.set_unreachable(False)

    forbidden = [CANARY_INPUT, CANARY_MESSAGE]
    forbidden += [base64.b64encode(x.encode()).decode().rstrip("=") for x in forbidden]
    rows = await table_texts(pool)
    assert any("dialog answer peer=" in r.getMessage() for r in caplog.records)  # audit line
    for needle in forbidden:
        assert not [e for e in errors if needle in e], f"{needle} in an error body"
        assert needle not in caplog.text, f"{needle} in a log record"
        assert not [t for t, r in rows if needle in r], f"{needle} in a row"
    assert not [a for a in allowed[1:] if CANARY_INPUT in a]  # the answer echoes no tool input
