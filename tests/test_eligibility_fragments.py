"""The eligibility and handover fragments are spelled once, in `db/eligibility.py`."""

from __future__ import annotations

import inspect

from agents_orchestrator.db import eligibility, leadership, queue, sessions
from agents_orchestrator.dispatcher import hive


def test_no_limit_is_the_only_spelling_of_the_literal() -> None:
    assert str(eligibility.NO_LIMIT) in eligibility.REPLICAS
    expected = f"coalesce((c.spec->>'replicas')::int, {eligibility.NO_LIMIT})"
    assert expected == eligibility.REPLICAS
    assert eligibility.STAYS.endswith(f"{eligibility.NO_LIMIT})")


def test_fragments_compose_from_each_other() -> None:
    assert eligibility.STAYS in eligibility.READY_STAYS
    assert eligibility.READY_STAYS in eligibility.ELIGIBLE
    assert eligibility.LEAVING in eligibility.HANDOVER_TRIGGER
    assert not hasattr(eligibility, "COMPACTED")


def test_call_sites_embed_the_shared_fragments() -> None:
    assert eligibility.ELIGIBLE in leadership._CANDIDATES
    assert eligibility.HANDOVER_TRIGGER in leadership._CANDIDATES
    assert eligibility.ELIGIBLE in leadership._FREE_LONGEST
    assert eligibility.ELIGIBLE in hive._CANDIDATES
    assert eligibility.HANDOVER_TRIGGER in queue._HANDOVER_PENDING
    assert eligibility.ELIGIBLE in queue._HANDOVER_PENDING


def test_eligible_excludes_dead_agents() -> None:
    # D4: one fragment, embedded by both the pick predicate and the manual make-leader target
    assert "'dead'" in eligibility.DEAD
    assert f"NOT {eligibility.DEAD}" in eligibility.READY_STAYS
    assert eligibility.READY_STAYS in eligibility.ELIGIBLE
    assert eligibility.DEAD in leadership._CANDIDATES
    # spelled once: no other module respells the dead-state test
    for module in (leadership, queue, sessions):
        assert "'dead'" not in inspect.getsource(module)
