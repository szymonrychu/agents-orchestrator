"""The in-flight gate of a submitted handoff: what still blocks a clear or a leadership move.

`items` names every blocking thing of a wrapper's `Inflight` answer with a stable id; `evaluate`
drops the items the agent deliberately abandoned (with a reason) and reports the rest. Session
crons never block: the handoff package carries them to the successor.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

from agents_orchestrator.wrapper.models import Inflight

UNREACHABLE = "inflight:unknown (wrapper unreachable)"
STOP_ADVICE = "finish it, stop it with TaskStop, or abandon it with a reason"


def refusal_words(item_id: str, *, stored: bool = False) -> str:
    """What to do about one blocking item, by the kind its id starts with (`stored`: the 409
    gate, where only an abandon clears a scratch file; the 422 check also takes `results`)."""
    kind, _, what = item_id.partition(":")
    if kind == "scratch":
        path = what.split(" (", 1)[0]
        tail = "abandon it with a reason" if stored else "list it in results"
        return f"{path} is not persisted: move it under .scratchpad/<name>/ or {tail}"
    if kind == "subagent":
        who = what.removesuffix(" running")
        return (
            f"subagent(s) {who} running (name an id or the word subagent in in_flight): "
            + STOP_ADVICE
        )
    if kind == "task":
        return f"{item_id} is running: {STOP_ADVICE}"
    return f"{item_id} is still in flight: finish it or abandon it with a reason"


class AbandonLike(Protocol):
    item: str
    why: str


@dataclass(frozen=True)
class Item:
    id: str
    #: Other exact spellings the agent may use to name the item (bare task id, subject path).
    keys: tuple[str, ...] = ()
    #: The path of a worktree or scratch item: abandoning the path or a parent dir matches.
    path: str | None = None
    kind: str = "other"


@dataclass(frozen=True)
class GateResult:
    clear: bool
    waiting_on: list[str] = field(default_factory=list)
    abandoned: list[dict[str, str]] = field(default_factory=list)


def _task(task: dict[str, Any]) -> Item:
    tid = str(task.get("id") or "?")
    desc = str(task.get("description") or "").strip()
    label = f"task:{tid} ({desc})" if desc else f"task:{tid}"
    return Item(label, (tid, f"task:{tid}", *([desc] if desc else [])), kind="task")


def _worktree(w: dict[str, Any]) -> list[Item]:
    path = str(w.get("path") or "?")
    out: list[Item] = []

    def add(what: str) -> None:
        out.append(Item(f"worktree:{path} {what}", path=path, kind="worktree"))

    if w.get("error"):
        add(f"unknown ({w['error']})")
        return out
    if w.get("dirty"):
        add(f"dirty {w['dirty']}")
    if w.get("ahead"):
        add(f"ahead {w['ahead']}")
    if w.get("upstream") is False and not w.get("ahead"):
        add("unpushed (no upstream)")
    return out


def items(inflight: Inflight | None) -> list[Item]:
    """The blocking items of `inflight` (None: the wrapper did not answer)."""
    if inflight is None:
        return [Item(UNREACHABLE)]
    out: list[Item] = []
    if inflight.subagents:
        ids = inflight.subagent_ids
        label = ",".join(ids) if ids else str(inflight.subagents)
        out.append(Item(f"subagent:{label} running", ("subagent", *ids), kind="subagent"))
    out.extend(_task(t) for t in inflight.background_tasks)
    for w in inflight.worktrees:
        out.extend(_worktree(w))
    for s in inflight.scratch:
        path = str(s.get("path") or "?")
        out.append(Item(f"scratch:{path} (pod-local, not persisted)", path=path, kind="scratch"))
    if inflight.unknown:
        out.append(Item("inflight:unknown (git scan incomplete)"))
    return out


def _covers(abandoned: str, item: Item) -> bool:
    name = abandoned.strip()
    if name == item.id or name in item.keys:
        return True
    if item.path is None or not name:
        return False
    root = name.rstrip("/")
    return item.path == name or (root != "" and item.path.startswith(root + "/"))


def abandon_for(item: Item, abandon: list[Any]) -> Any | None:
    """The abandon entry that names `item` (exact id, bare key, its path or a parent dir)."""
    return next((a for a in abandon if _covers(a.item, item)), None)


def evaluate(inflight: Inflight | None, abandon: list[Any]) -> GateResult:
    waiting: list[str] = []
    dropped: list[dict[str, str]] = []
    for item in items(inflight):
        entry = abandon_for(item, abandon)
        if entry is None:
            waiting.append(item.id)
        else:
            dropped.append({"item": item.id, "why": entry.why})
    return GateResult(clear=not waiting, waiting_on=waiting, abandoned=dropped)
