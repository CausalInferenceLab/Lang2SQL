# Implementation verification — accuracy improvement

Reuse: SQLAlchemy reflection worker, Table/ExploreSchema, safety comment parser,
ContextConcierge/agent_loop, MeasuredLLM and official grading primitives, plus the existing Codex demo mcp_settings. No second
production query executor or provider retry loop. Python source/test delta is +1035 lines (415 test lines); metadata maps and evidence are not product logic.

| Principle | Result | Evidence |
|---|---|---|
| Reuse | O | Existing schema tool, threaded adapter, agent loop, core message/tool codecs, measured provider retry loop and graders extended. |
| Cause-focused changes | O | Missing keys, missing official context, incorrect result semantics and leading-comment rejection and account-quota retry errors mapped in README. |
| Logging | O | Case IDs, review round, original/final answers, all calls, errors, source hashes, final/aborted status and per-case Codex/MCP events persist. |
| Error levels | O | Unsupported constraint reflection warns; other reflection failures propagate. API errors log types only; account quota aborts the batch, transient rate limits retain bounded retries. |
| Configuration injection | O | Schema/API/profile input supplied at the CLI boundary; no new env reads in adapters. |
| No issue IDs in comments | O | Comments describe constraints rather than tickets. |
| Verification exit codes | O | Latest focused evaluation/MCP/safety checks: 56 passed; changed-code Ruff and diff checks passed. Earlier full checks are historical. No CODE MAP generator exists in this checkout. |
| Authorization | O | Requested implementation and PR only; no merge, release, messages or reviewer notifications. |
| Connected explanation | O | Reflection → schema tool → prompt → guarded SQL → final result → official score path documented. |
| One async logic path | O | DB reflection stays in existing to_thread worker; review reuses async agent_loop and the existing provider call counter; both API protocols use the same async retry loop. |
| Values/configuration/deployment | O | Review count and budgets are CLI/profile inputs; schema SHA values are named integrity constants. Env additions 0; benchmark options require no deployment change. |

Latest review: reused executed_sql and shared BASE_INSTRUCTIONS; pending source/test delta +29 lines, no new functions or environment variables. Successful result SQL footer accepted; invented LIMIT and blocked responses rejected in regression tests. Official graders unchanged. Full traces and fixed development results are retained. Spider remains 10/20; semantic guidance is not a correctness guarantee. No merge performed.
