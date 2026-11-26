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
| Verification exit codes | O | 239 passed, 1 skipped; changed-code Ruff and diff checks passed. No CODE MAP generator exists in this checkout. |
| Authorization | O | Requested implementation and PR only; no merge, release, messages or reviewer notifications. |
| Connected explanation | O | Reflection → schema tool → prompt → guarded SQL → final result → official score path documented. |
| One async logic path | O | DB reflection stays in existing to_thread worker; review reuses async agent_loop and the existing provider call counter; both API protocols use the same async retry loop. |
| Values/configuration/deployment | O | Review count and budgets are CLI/profile inputs; schema SHA values are named integrity constants. Env additions 0; benchmark options require no deployment change. |
