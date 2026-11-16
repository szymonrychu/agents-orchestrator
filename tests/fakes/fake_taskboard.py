"""A small stand-in for agents-taskboard: tasks, decisions asked as a session, `client_key`
idempotency, answers and withdrawals, task assignees (the session filter of the task list),
created notice tasks and comments. Used as an `httpx.MockTransport` handler."""

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
    # task key -> the assignee session id (its claimed task)
    assignees: dict[str, str] = field(default_factory=dict)
    # task key -> extra fields of its list item (`title`, `status`, `todo`)
    info: dict[str, dict[str, Any]] = field(default_factory=dict)
    created: list[dict[str, Any]] = field(default_factory=list)
    comments: list[dict[str, Any]] = field(default_factory=list)

    def handler(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if self.down:
            raise httpx.ConnectError("refused", request=request)
        assert request.headers["authorization"] == f"Bearer {self.token}"
        path = request.url.path.removeprefix(BASE)
        if path == "/tasks" and request.method == "GET":
            session = request.url.params.get("session")
            items = [
                {"key": key, "assignee_session": who, "decisions": [], **self.info.get(key, {})}
                for key, who in sorted(self.assignees.items())
                if key in self.tasks and (session is None or who == session)
            ]
            return httpx.Response(200, json={"items": items, "next_cursor": None})
        if path == "/tasks" and request.method == "POST":
            body = json.loads(request.content)
            same = [t for t in self.created if t["client_key"] == body.get("client_key")]
            if same and body.get("client_key"):
                return httpx.Response(200, json=same[0])
            key = f"T-{100 + len(self.created)}"
            task = {
                "key": key,
                "session": request.headers["x-orch-session"],
                "projects": request.headers["x-orch-projects"],
                **body,
            }
            self.created.append(task)
            return httpx.Response(201, json=task)
        if m := re.fullmatch(r"/tasks/(T-\d+)/comments", path):
            if m[1] not in self.tasks or request.method != "POST":
                return httpx.Response(404, json={"error": {"code": "not_found", "message": "x"}})
            comment = {
                "task_key": m[1],
                "session": request.headers["x-orch-session"],
                "body": json.loads(request.content)["body"],
            }
            self.comments.append(comment)
            return httpx.Response(201, json={"key": m[1]})
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
