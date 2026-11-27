# Implementation review

Request path: registered Codex MCP → existing tool runtime → definition receipt → existing Oracle view contract/query. Demo preparation uses the existing SemanticFederationTool, decide_candidate and bind_view; it does not reimplement approval or SQL execution. Missing definitions now send the model to the same candidate workflow used in the demo.

| Principle | Result | Evidence |
|---|---|---|
| Reuse | O | Existing runtime, approval, view binding and long_context.run_turn reused |
| Scope | O | One product response correction; CO fixtures, operator helpers and runbook only |
| Logging | O | Existing business audit retained; operator preparation/approval and real-turn evidence printed/saved |
| Error handling | O | Candidate mismatch/duplicate state fail; subprocess exit codes preserved; failed runs retained |
| Environment boundary | O | Private env read at CLI entry/composition; no new production environment variable |
| Issue comments | O | No issue IDs embedded in source comments |
| Verification | O | 217 tests including Oracle metadata passed; changed Python Black/Ruff and diff checks passed; repository has no gen_code_map.py |
| Approval | O | Operator approval retained; no merge or release; only scoped demo accounts/views changed |
| Explanation | O | README follows registration → query → clarify → candidate → approval → reuse |
| Sync/async | O | One existing business path; async operator helper awaits it; synchronous Codex subprocess lives in a standalone CLI, not an event loop |
| Constants/config | O | CO fixture names/definitions are explicit demonstration constants; model timeout CLI-configurable; no deployment setting added |

One initial combined test run inherited the business Oracle URL and made a fake-database unit test query the real reader account. Removing business URL/schema from the test process while keeping LANG2SQL_TEST_METADATA_URL produced 217 passing tests. This was test invocation contamination, not a change to production access controls.

The demo helpers use Python 3.11+ for tomllib (the tested local interpreter is 3.13). Native screen recording and updating the user's original Documents artifact depend on macOS application permissions; they are not repository test results.
