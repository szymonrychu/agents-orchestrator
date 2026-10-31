"""The files the kind roll-mid-turn e2e depends on (orch-e2e-rollout-mid-turn, design D11).

tests/e2e/test_e2e_kind.py rolls the agent image while a long turn runs. It needs a long
scenario turn of its own (the 4 s "slow" turn other tests rely on stays as it is), a turn that
answers the default restart continue prompt, a second image tag the kind node can pull, and an api
port-forward that survives the api pod being replaced by the helm upgrade. None of that can be
exercised without kind, so these checks pin the pieces the e2e reads.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest
import yaml
from e2e_kind_loader import e2e_kind as e2e_module

from agents_orchestrator.settings import Settings

ROOT = Path(__file__).resolve().parents[1]
SCENARIO = ROOT / "e2e" / "agent-test.scenario.json"
SCRIPT = ROOT / "scripts" / "e2e.sh"
VALUES = ROOT / "e2e" / "values-kind.yaml"
MARATHON_MIN_SECONDS = 300  # the roll (helm upgrade, reconciler restart) plus the checks


def turns() -> list[dict[str, Any]]:
    return json.loads(SCENARIO.read_text())["turns"]


def turn_for(prompt: str) -> dict[str, Any] | None:
    """The turn the wrapper's fake claude runs for `prompt`: the first whose regex matches."""
    return next((t for t in turns() if re.search(t["match"], prompt)), None)


def slept(turn: dict[str, Any]) -> float:
    return sum(step["sleep"] for step in turn["steps"] if "sleep" in step)


def test_slow_turn_keeps_its_four_seconds() -> None:
    slow = next(t for t in turns() if t["match"] == "slow")
    assert {"sleep": 4} in slow["steps"]
    turn = turn_for(e2e_module().SLOW_PROMPT.format(n=1))
    assert turn is slow or turn == slow


def test_marathon_turn_outlasts_the_roll() -> None:
    module = e2e_module()
    turn = turn_for(module.MARATHON_PROMPT.format(n=1))
    assert turn is not None and turn["match"] == "marathon", turn
    assert slept(turn) >= MARATHON_MIN_SECONDS, turn
    assert module.marathon_seconds() == slept(turn)


def test_typed_messages_queue_into_the_running_turn() -> None:
    """Regular-lane messages are typed into a busy agent's running turn (wrapper 0.5.0 lanes).

    Without `queue_input` the fake ends a "sleep" step on any typed byte and drops the text, so
    the message queued for the busy follower during the roll ended the marathon turn in seconds
    and status.rollout never held the busy pod. Real claude keeps the turn and queues the text.
    """
    assert json.loads(SCENARIO.read_text()).get("queue_input") is True


def test_no_other_prompt_runs_the_marathon_turn() -> None:
    module = e2e_module()
    default_continue = Settings.model_fields["default_restart_continue_prompt"].default
    for prompt in (
        module.SLOW_PROMPT.format(n=1),
        module.COMPACT_PROMPT,
        module.WRITE_PROMPT,
        "say hello",
        default_continue,
    ):
        turn = turn_for(prompt)
        assert turn is None or turn["match"] != "marathon", prompt


def test_default_continue_prompt_completes_a_continued_turn() -> None:
    prompt = Settings.model_fields["default_restart_continue_prompt"].default
    turn = turn_for(prompt)
    assert turn is not None, "the continue prompt would run the fake's default echo turn"
    assert turn["match"] != "slow" and slept(turn) < 10, turn
    assert any("text" in step for step in turn["steps"]), turn


def test_scenario_keeps_at_least_five_turns() -> None:
    assert len(turns()) >= 5


def test_values_allow_both_agent_images() -> None:
    values = yaml.safe_load(VALUES.read_text())
    allowed = set(values["env"]["ORCH_AGENT_IMAGES"].split(","))
    assert {values["agentImage"], e2e_module().AGENT_IMAGE_NEXT} <= allowed, allowed
    assert values["agentImage"] != e2e_module().AGENT_IMAGE_NEXT


def test_script_tags_loads_and_exports_the_second_image() -> None:
    text = SCRIPT.read_text()
    next_image = e2e_module().AGENT_IMAGE_NEXT
    assert f"AGENT_IMAGE_NEXT={next_image}" in text
    assert 'docker tag "$AGENT_IMAGE" "$AGENT_IMAGE_NEXT"' in text
    assert 'load_image "$AGENT_IMAGE"' in text
    assert 'load_image "$AGENT_IMAGE_NEXT"' in text
    assert '"ORCH_E2E_AGENT_IMAGE_NEXT=$AGENT_IMAGE_NEXT"' in text


def test_script_port_forward_reconnects() -> None:
    """The helm upgrade replaces the api pod, which ends a port-forward bound to it."""
    text = SCRIPT.read_text()
    assert "port_forward &" in text
    body = text.split("port_forward() {", 1)[1].split("\n}\n", 1)[0]
    assert "while true" in body and 'port-forward "svc/$RELEASE"' in body, body
    assert "trap" in body, "the loop must take its kubectl child down with it"


def test_replacement_order_reads_the_pod_names_from_json_log_lines(monkeypatch) -> None:
    """The reconciler logs JSON lines: the pod name ends at the message, not at the line end."""
    module = e2e_module()
    lines = [
        {"ts": "2026-10-28T11:20:00.000Z", "level": "INFO", "msg": "migrations applied: none"},
        {
            "ts": "2026-10-28T11:21:00.000Z",
            "level": "INFO",
            "logger": "agents_orchestrator.reconciler.rollout",
            "msg": "session e2e-scale-roll: replacing idle outdated pod agent-e2e-scale-roll-1",
        },
        {
            "ts": "2026-10-28T11:22:00.000Z",
            "level": "INFO",
            "logger": "agents_orchestrator.reconciler.rollout",
            "msg": "session e2e-scale-roll: replacing idle outdated pod agent-e2e-scale-roll-0",
        },
    ]
    text = "\n".join([*(json.dumps(line) for line in lines), "not a json line"])
    monkeypatch.setattr(module, "reconciler_log", lambda: text)
    assert module.replacement_order() == ["agent-e2e-scale-roll-1", "agent-e2e-scale-roll-0"]


class Flaky:
    """A transport handler that fails the first `failures` requests with `exc`."""

    def __init__(self, exc: type[httpx.TransportError], failures: int = 1) -> None:
        self.exc, self.failures, self.calls = exc, failures, 0

    def __call__(self, request: httpx.Request) -> httpx.Response:
        self.calls += 1
        if self.calls <= self.failures:
            raise self.exc("the port-forward dropped it", request=request)
        return httpx.Response(202 if request.method == "POST" else 200, json={})


def resilient(handler: Flaky, monkeypatch) -> httpx.Client:
    module = e2e_module()
    monkeypatch.setattr(module.time, "sleep", lambda _s: None)
    return module.ResilientClient(base_url="http://api", transport=httpx.MockTransport(handler))


@pytest.mark.parametrize("method", ["GET", "HEAD", "DELETE"])
@pytest.mark.parametrize("exc", [httpx.ConnectError, httpx.ReadError, httpx.RemoteProtocolError])
def test_resilient_client_resends_idempotent_requests(method, exc, monkeypatch) -> None:
    handler = Flaky(exc)
    with resilient(handler, monkeypatch) as client:
        assert client.request(method, "/meta").status_code == 200
    assert handler.calls == 2


def test_resilient_client_resends_a_post_that_never_connected(monkeypatch) -> None:
    handler = Flaky(httpx.ConnectError)
    with resilient(handler, monkeypatch) as client:
        assert client.post("/sessions/x/messages", json={"text": "t"}).status_code == 202
    assert handler.calls == 2


@pytest.mark.parametrize("exc", [httpx.ReadError, httpx.RemoteProtocolError, httpx.WriteError])
def test_resilient_client_never_resends_a_post_that_may_have_been_accepted(
    exc, monkeypatch
) -> None:
    """A message POST the api may have accepted is not sent again: it would be a duplicate."""
    handler = Flaky(exc)
    with resilient(handler, monkeypatch) as client, pytest.raises(exc):
        client.post("/sessions/x/messages", json={"text": "t"})
    assert handler.calls == 1
