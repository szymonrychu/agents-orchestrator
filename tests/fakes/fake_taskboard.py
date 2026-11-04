"""A small stand-in for agents-taskboard: tasks, decisions asked as a session, `client_key`
idempotency, answers and withdrawals. Used as an `httpx.MockTransport` handler."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import httpx

BASE = "/taskboard/api/v1"


@dataclass
class FakeTaskboard:
    token: str
    tasks: set[str] = field(default_factory=lambda: {"T-1"})
    decisions: list[dict[str, Any]] = field(default_factory=list)
    calls: list[httpx.Request] = field(default_factory=list)
    down: bool = False

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.down:
            raise httpx.ConnectError("refused", request=request)
        assert request.headers["authorization"] == f"Bearer {self.token}"
        path = request.url.path.removeprefix(BASE)
        if m := re.fullmatch(r"/tasks/(T-\d+)", path):
            if m[1] not in self.tasks or request.method != "GET":
                return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})
            mine = [d for d in self.decisions if d["task_key"] == m[1]]
            return httpx.Response(200, json={"key": m[1], "decisions": mine})
        if m := re.fullmatch(r"/tasks/(T-\d+)/decisions", path):
            if m[1] not in self.tasks or request.method != "POST":
                return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})
            body = json.loads(request.content)
            session = request.headers["x-orch-session"]
            same = [
                d
                for d in self.decisions
                if d["task_key"] == m[1]
                and d["asking_session"] == session
                and d["client_key"] == body.get("client_key")
                and body.get("client_key")
            ]
            if same:
                return httpx.Response(200, json=same[0])
            decision = {
                "id": len(self.decisions) + 1,
                "task_key": m[1],
                "asking_session": session,
                "asking_slug": request.headers["x-orch-slug"],
                "question": body["question"],
                "context": body.get("context", ""),
                "options": body["options"],
                "client_key": body.get("client_key"),
                "status": "open",
                "answer": None,
                "url": f"https://tasks.test/tasks/{m[1]}",
            }
            self.decisions.append(decision)
            return httpx.Response(201, json=decision)
        raise AssertionError(f"unexpected taskboard call {request.method} {path}")

    def ask(self, task: str = "T-1", session: str = "other", text: str = "q") -> dict[str, Any]:
        """A decision asked by some other session (an agent asking on its own)."""
        decision = {
            "id": len(self.decisions) + 1,
            "task_key": task,
            "asking_session": session,
            "asking_slug": "other",
            "question": text,
            "context": "",
            "options": [{"label": "Yes, open it"}, {"label": "No"}],
            "client_key": None,
            "status": "open",
            "answer": None,
            "url": f"https://tasks.test/tasks/{task}",
        }
        self.decisions.append(decision)
        return decision

    def answer(self, decision_id: int, answer: dict[str, Any]) -> None:
        d = self.decisions[decision_id - 1]
        d.update(status="answered", answer=answer)

    def withdraw(self, decision_id: int) -> None:
        self.decisions[decision_id - 1]["status"] = "withdrawn"
