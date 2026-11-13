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
