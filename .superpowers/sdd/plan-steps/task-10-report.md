# Task 10 report

Implemented: ChatAttachmentIn, routes GET /history, POST /attachments (202), GET /attachments/{request_id} in api/chat.py (docstring updated); CHAT_TOOLS + 3 names; scope.py "own" entries for the 3 routes (needed by test_every_agent_route_is_classified); README three #### headings; openapi.json regenerated; all three connector doc copies refreshed from cfbb4065f8b90cdaf76e870582c0deea7924af29 (chat_openapi.json, connector_openapi.json, fixtures/upstream/...openapi-agent.json) and both .source files carry the full sha; FakeChat serves history and attachments; tests added (history query, attachment body, one-segment id, leader only, unconfigured/unavailable, upstream refusals, no session touch, canary-not-logged).

TDD: RED = test_history_relayed_with_query_string 404 before routes; GREEN after.

Tests: full `mise run test`: 4724 passed, 4 failed (before the scope.py fix, which resolved one):
- test_admin_relay.py::test_routes_map_one_to_one_to_the_document[connector] and test_admin_relay_contract.py::test_every_upstream_admin_route_is_relayed[connector]: the refreshed connector doc has a new admin route GET /chat/v1/admin/rooms/{room_id}/messages, which the admin relay (Task 11, superleader history) does not relay yet. Expected red until Task 11 adds the relay entry. Unavoidable given A23.
- test_reconciler.py::test_lightkube_default_client_uses_env_config: environment (in-cluster kube env), unrelated.
Lint: pass.

Existing tests extended (route set grew): test_readme_documents_chat_routes, test_chat_contract (relay route count 10->13, concrete() for {request_id}, fake coverage for new routes), test_on_start (CHAT_TOOLS, PLUGIN_PENDING, alone allow literal), tests/e2e/test_e2e_kind.py literal list mirror.

Concern: httpx INFO logs print full upstream URLs including the history query string (from=, before=), in the real relay too (pre-existing, any route). Canary test therefore checks only agents_orchestrator.* records. Not widened.

## Fix report (review findings)

- run.py: new `quiet_http_loggers()` sets httpx and httpcore to WARNING; `configure_logging` calls it.
- tests/test_agent_chat.py: canary test applies `quiet_http_loggers()` and asserts the canary is in NO caplog record of any logger (verified it fails with the helper neutered). tests/test_runtime_logging.py: configure_logging sets both levels.
- MEMORY.md: dated 2026-11-16 entry. tests/fakes/fake_chat.py: lambda dropped.
- Command: uv run pytest tests/test_agent_chat.py tests/test_chat_contract.py tests/test_agent_scope.py tests/test_on_start.py tests/test_runtime_logging.py -q: 121 dots, no failures (pass); mise run lint: All checks passed.
