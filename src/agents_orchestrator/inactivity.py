"""Inactivity limits of a session: the effective reminder and close limits, and the notice texts
the sweeper sends (hive-inactivity-limits, spec 3.3 and 3.4). Pure: no database, no I/O."""

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from agents_orchestrator.settings import Settings
from agents_orchestrator.spec import SessionSpec

NOTICE_REMINDER = "inactivity_reminder"
NOTICE_CLOSE = "inactivity_close"
NOTICE_AUTOCLOSE = "autoclose"
# The web recognises the notices in the chat log by these prefixes.
REMINDER_PREFIX = "Orchestrator notice (inactivity reminder):"
CLOSE_PREFIX = "Orchestrator notice (inactivity limit):"
AUTOCLOSE_PREFIX = "Orchestrator notice (session lifetime):"


@dataclass(frozen=True)
class Limits:
    """Quiet seconds before the reminder and before the hard close; 0 = off."""

    reminder_seconds: int
    close_seconds: int


def effective(spec: SessionSpec, settings: Settings) -> Limits:
    """The spec's limits where set, else the settings'. Superleaders are exempt from both."""
    if spec.session_messaging:
        return Limits(0, 0)
    own = spec.inactivity
    reminder = own.reminder_seconds if own and own.reminder_seconds is not None else None
    close = own.close_seconds if own and own.close_seconds is not None else None
    return Limits(
        settings.inactivity_reminder_seconds if reminder is None else reminder,
        settings.inactivity_close_seconds if close is None else close,
    )


def humanize(seconds: int) -> str:
    """`30 s`, `55 min` (floored), else hours floored to one decimal without a trailing `.0`."""
    if seconds < 60:
        return f"{seconds} s"
    if seconds < 3600:
        return f"{seconds // 60} min"
    whole, tenth = divmod(seconds // 360, 10)
    return f"{whole} h" if tenth == 0 else f"{whole}.{tenth} h"


def stamp(t: datetime) -> str:
    """UTC date and minute, `2026-11-02 14:05Z`."""
    return t.astimezone(UTC).strftime("%Y-%m-%d %H:%MZ")


def clock(t: datetime) -> str:
    """UTC minute of the day, `14:05Z`."""
    return t.astimezone(UTC).strftime("%H:%MZ")


def reminder_text(
    *,
    last_update: datetime,
    now: datetime,
    limits: Limits,
    pods: int,
    chat: bool,
    taskboard: bool,
) -> str:
    """The reminder notice; the update tools named are only those configured (`chat`,
    `taskboard`), and the close sentence appears only when the close limit is on."""
    quiet = max(0, int((now - last_update).total_seconds()))
    tools = " or ".join(
        name
        for name, on in (
            ("orchestrator_chat_send", chat),
            ("a comment on your task (orchestrator_task_comment)", taskboard),
        )
        if on
    )
    if tools:
        going = (
            f"If the work is still going, please post a short progress update now with {tools},"
            f" and keep posting at least every {humanize(limits.reminder_seconds)}."
        )
    else:
        going = (
            "If the work is still going, report progress in your answer to the next user message."
        )
    parts = [
        f"{REMINDER_PREFIX} this session has sent no update to the user"
        f" and done no tool work for {humanize(quiet)} (last activity {stamp(last_update)})"
        f" while holding {pods} agent"
        f" pod{'' if pods == 1 else 's'} and a session slot.",
        going,
        "If the work is finished, post a final status and call orchestrator_self_close to free"
        " the hive.",
    ]
    if limits.close_seconds > 0:
        closes_at = last_update + timedelta(seconds=limits.close_seconds)
        parts.append(
            "Without an update or activity the orchestrator will ask this session to close at"
            f" about {stamp(closes_at)}; it is closed only once idle, never mid-turn."
        )
    parts.append("Replying here alone does not count as an update.")
    return " ".join(parts)


def close_text(
    *,
    quiet_seconds: int,
    deadline: datetime,
    grace_seconds: int,
    chat: bool,
    taskboard: bool,
    workdir_saved: bool,
) -> str:
    """The inactivity ask; the hand-off sentence names only the configured update tools and
    the workdir only when the session saves its workspace, and is left out when neither applies."""
    steps = []
    if chat and taskboard:
        steps.append("post a final status (orchestrator_chat_send or a task comment)")
    elif chat:
        steps.append("post a final status (orchestrator_chat_send)")
    elif taskboard:
        steps.append("post a final status (a task comment with orchestrator_task_comment)")
    if workdir_saved:
        steps.append("since this session saves its workspace, write handoff notes into the workdir")
    parts = [
        f"{CLOSE_PREFIX} this session has sent no update and done no tool work for"
        f" {humanize(quiet_seconds)}, so the orchestrator asks you to close it."
    ]
    if steps:
        parts.append(
            f"If your work is finished: {' and '.join(steps)}, then call orchestrator_self_close."
        )
    else:
        parts.append("If your work is finished, call orchestrator_self_close.")
    parts.append(
        "If you are still working, post a progress update now to keep the session. It is never"
        f" closed while you work; if it is idle after {clock(deadline)} without a self-close or"
        f" an update, it is discarded {humanize(grace_seconds)} later."
    )
    return " ".join(parts)


def close_reason(quiet_seconds: int, close_seconds: int) -> str:
    """The `close_reason` recorded with an inactivity close."""
    return f"No update for {humanize(quiet_seconds)} (inactivity limit {close_seconds} s)"
