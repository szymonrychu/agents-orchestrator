"""Texts of the graceful-handover notes sent to agents (plain ASCII, one place)."""

WEAK_WRAP_UP = (
    "Context limit (weak): wrap up. Finish the step you are in, commit and push, update your "
    "task and todo, and leave notes a successor can act on. Nothing will happen if you keep "
    "working."
)
WEAK_LEADER_CANDIDATES = (
    "At a natural boundary, orchestrator_leadership_handover(to=..., epoch=..., reason=...) "
    "is welcome."
)
WEAK_LEADER_NONE = "No ready successor; keep working."
WEAK_SOLO = (
    "To hand off now on your own terms, call orchestrator_leadership_handover("
    "reason=<your handoff>): your context is cleared and the handoff comes back to you."
)

STRONG_REQUEST = (
    "Write your handoff now as your reply: what you were doing, what is done (commits, PRs), "
    "what is next and in which order, open decisions and gotchas. Your reply text is the "
    "handoff; do not start new work."
)
RETURN_EMPTY = (
    "Your context was cleared at the strong limit; no handoff text was captured. Check your "
    "task, todo and the repo state."
)


def strong_notice(minutes: int, extension_minutes: int, *, hive: bool) -> str:
    """The strong-limit countdown notice (message and keyed note `handoff_notice`)."""
    moves = " and leadership moves to a successor" if hive else ""
    return (
        f"Context limit (strong): in {minutes} minutes you will be asked for a handoff, then "
        f"your context is cleared{moves}. Finish or park your current step now: commit and "
        "push, update your task and todo. Need more time? Call "
        f"orchestrator_leadership_handover(extend=true) once for {extension_minutes} more "
        "minutes."
    )


def strong_return(text: str | None) -> str:
    """The handoff sent back to a cleared solo agent (blank text: RETURN_EMPTY)."""
    if text is None or not text.strip():
        return RETURN_EMPTY
    return f"Handoff from your previous context (strong limit, cleared just now):\n\n{text}"
