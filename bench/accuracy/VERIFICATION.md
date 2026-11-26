# Implementation verification

Reused `bench.spider2.evaluate` (read-only connections, final SQL extraction,
CSV export, atomic records and official Spider grader), `MeasuredLLM` (existing
Lang2SQL message/tool codecs), and the real `ContextConcierge` / `agent_loop`.
Python source delta before evidence snapshots: +882 lines, of which 418 are
adapted existing runner/provider code, 307 are runner integration/product prompt,
and 157 are integrity tests. JSON manifests and archived source snapshots are
provenance artifacts, not new product logic.

| Principle | Result | Evidence |
|---|---|---|
| Reuse first | O | Existing Spider execution and grading path reused; BIRD adds input adapter and pinned official comparator. |
| Fix evidenced causes | O | Development outputs showed extra projection/filter/type errors; only six prompt lines changed. Logged API timeouts motivated bounded transport retries. |
| Logging | O | Case-specific transcripts, SQL, state, API attempts, error classes, usage and time persist; raw record/CSV hashes accompany published summaries. |
| Error handling | O | Recoverable transport errors log warning; exhausted/nontransport errors raise into a recorded failed case. Failed cases remain in denominator. Provider messages are intentionally redacted to avoid credential leakage. |
| Configuration injection | O | Credential file read at CLI input boundary; key/profile injected into provider. No new production env variables. |
| No issue IDs in code comments | O | New code comments describe constraints, not ticket numbers. |
| Real exit codes | O | Locked environment: 222 passed, 1 skipped. Benchmark Ruff and diff whitespace checks pass. Existing system_prompt BLE001 is outside changed prompt text and explicitly excluded for that file. No CODE MAP script exists in this checkout. |
| Authorization | O | Implementation, tests and PR within user request; no merge or issue creation. |
| Explain request path | O | README identifies actual harness path and explicitly distinguishes it from Codex MCP, Oracle dialect and memory-effect claims. |
| One async logic path | O | One provider call/retry implementation; API calls and retry delay async; existing DB adapter uses worker threads. BIRD scoring is a standalone synchronous CLI. |
| Tunable values | O | Limits use CLI/profile configuration; exact settings frozen and archived. No deployment/env changes required for benchmark-only configuration. |

The integrity tests exercise fixed/disjoint splits, gold-field rejection,
read-only SQL, row budgets, official comparator hash/semantics, failed cases in
the denominator, missing-record failure, and logged/bounded transport retries.
The skipped test requires an optional external integration; no fake model output
is included in benchmark accuracy.

After measurement, unused copied legacy CLI entrypoints were removed. All eight retained execution/grading function ASTs and the final SQL instruction are unchanged; see `results/primitives-equivalence.json`. As-executed source bytes are archived under `results/source-final/`.
