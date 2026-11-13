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
RETURN_EMPTY_KEPT = (
    "The clear at the strong limit failed and your context was kept; no handoff text was "
    "captured. Carry on, and wrap up soon: the context may be compacted."
)


def strong_notice(minutes: int, extension_minutes: int, *, hive: bool) -> str:
    """The strong-limit countdown notice (message and keyed note `handoff_notice`).

    `hive`: leadership will move to a successor (decided at send time; a hive whose leader is
    the only live agent follows the solo order). No extension offer when extensions are off."""
    moves = " and leadership moves to a successor" if hive else ""
    text = (
        f"Context limit (strong): in {minutes} minutes you will be asked for a handoff, then "
        f"your context is cleared{moves}. Finish or park your current step now: commit and "
        "push, update your task and todo."
    )
    if extension_minutes <= 0:
        return text
    return (
        f"{text} Need more time? Call orchestrator_leadership_handover(extend=true) once for "
        f"{extension_minutes} more minutes."
    )


def strong_return(text: str | None, *, cleared: bool = True) -> str:
    """The handoff sent back to a solo agent (blank text: RETURN_EMPTY or RETURN_EMPTY_KEPT);
    `cleared` False when the clear failed and the agent kept its context."""
    if text is None or not text.strip():
        return RETURN_EMPTY if cleared else RETURN_EMPTY_KEPT
    if not cleared:
        return f"Handoff from your current context (strong limit, the clear failed):\n\n{text}"
    return f"Handoff from your previous context (strong limit, cleared just now):\n\n{text}"
