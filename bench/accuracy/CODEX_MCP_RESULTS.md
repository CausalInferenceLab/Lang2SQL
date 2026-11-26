# Codex MCP comparison — fixed development questions

Executed with ChatGPT-authenticated Codex CLI, not the exhausted API credential.
Each suite uses exactly the same original 20 development IDs as the API baseline.
These are development diagnostics, not full 135/500 scores or independent holdout
accuracy. Model/API/input differences mean this is not an isolated MCP-effect study.

| Same 20 questions | Previous API baseline | Codex final SQL, executed offline | Codex → Lang2SQL MCP end-to-end |
|---|---:|---:|---:|
| spider | 10/20 (50%) | 3/20 (15%) | 0/20 (0%) |
| bird | 18/20 (90%) | 12/20 (60%) | 0/20 (0%) |

The offline column re-executes only the final fenced SQL from the same Codex
trajectory, unchanged, using the official comparator. It is a secondary diagnostic
added after observing the MCP blockade, **not successful MCP execution**. Missing
SQL/clarifications stay in the full 20-question denominator. No model was rerun
or answer chosen using gold. The MCP column additionally requires final SQL to
have actually executed through the production MCP server.

## Why the result differs

- spider: 20/20 blocked by definition confirmation; 0 run_sql calls; 4/20 final SQL outputs. Compared with baseline final SQL, 0 previously wrong answers became correct and 7 previously correct became wrong.
- bird: 20/20 blocked by definition confirmation; 0 run_sql calls; 13/20 final SQL outputs. Compared with baseline final SQL, 1 previously wrong answers became correct and 7 previously correct became wrong.

The MCP runtime is deliberately fresh per case: no registered business definitions
or prior answers. `ResolveBusinessTerms` requires a nonempty list of exact confirmed
metadata entries; its schema rejects an empty list even for questions expressed
only in literal SQL fields. Question text and official schema/evidence do not
become approved entries. Thus Codex may understand the request and write SQL, but
cannot execute it through this MCP contract. This is a product integration limit,
not evidence that Codex intrinsically has zero SQL accuracy.

No definition guard was disabled and no fabricated/gold-derived definitions were
registered. A full 635-case MCP replay was not launched after all 40 pilot cases
failed to reach SQL execution; the same deterministic blocker must be addressed
before a meaningful full end-to-end comparison.

The next implementation needs to distinguish literal field queries from ambiguous
business definitions and safely accept explicit user-provided criteria without
inventing policy. Test that missing definitions still stop execution, then repeat
this same fixed comparison before the full run. The 95% target remains unmet.

## Protocol and evidence

- Codex CLI default model, no model override; exact resolved model is not exposed in captured events.
- User configuration, shell, unified execution, apps and web search disabled for these invocations. Only the three read-only Lang2SQL MCP tools were enabled.
- Same pinned SQLite DBs and official graders; official schema metadata and per-question evidence supplied. Gold SQL/results never enter Codex input.
- Two concurrent cases per suite; 600s case deadline; 30s offline SQL deadline; 100,000 rows.
- ChatGPT login checked locally; no API credential file loaded. Each question has a new ephemeral session and in-memory metadata store.
- All 40 Codex invocations completed. CLI usage is recorded separately; internal API-call counts are unavailable and must not be reported as zero calls.
- [Case-by-case comparison](codex-mcp-results/comparison.json), manifests, settings, scores and compact records retain both successful and failed results. Original events remain under `/Users/seyoung/workspace/benchmarks/codex-mcp-20261126`.

Reproduce with `python -m bench.accuracy.codex_mcp --help`, then the existing
`bench.accuracy.evaluate score` command with the same DBs and official grader.
The extracted-SQL diagnostic derives from those recorded answers, not a new run.

Validation: 239 tests passed, 1 skipped. Runner checks forbid counting unexecuted
SQL, non-MCP tools or a diagnostic-query fallback after clarification as MCP success.
