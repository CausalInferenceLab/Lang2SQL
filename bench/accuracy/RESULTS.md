# Spider2 / BIRD accuracy — 2026-11-26

**99% was not reached.** These are measured execution-result accuracies, not a
claim that arbitrary business questions can be answered with 99% reliability.

## Primary: questions withheld from this development process

| Benchmark | Correct / total | EX | 95% Wilson interval | Failed predictions | API error attempts |
|---|---:|---:|---:|---:|---:|
| Spider 2.0-Lite SQLite | 71/115 | 61.74% | 52.61–70.11% | 11 | 0 |
| BIRD Mini-Dev SQLite | 416/480 | 86.67% | 83.33–89.42% | 4 | 0 |

The model/configuration was selected before scoring these 115/480 questions.
No fixes were made from their answers or scores. The Wilson intervals assume
independent Bernoulli questions; they do not establish production/Oracle accuracy
or eliminate public-benchmark training contamination. This is a question-level
split, not a previously unseen-database split.

## Full public SQLite subsets (include the 20 development questions)

| Benchmark | Correct / total | EX | 95% Wilson interval | Failed predictions | API error attempts |
|---|---:|---:|---:|---:|---:|
| Spider 2.0-Lite SQLite | 81/135 | 60.00% | 51.57–67.88% | 12 | 0 |
| BIRD Mini-Dev SQLite | 434/500 | 86.80% | 83.55–89.49% | 4 | 0 |

All 135/500 planned questions have exactly one final-config trajectory. The full
results combine the disjoint development/evaluation partitions; no best answer
was selected from the development trials. Full-subset figures include questions
used to guide prompt/model selection and must not be described as held-out test
scores. They are not official leaderboard submissions.

## What ran

- GPT-6.2 Sol (`openai/gpt-6.2-sol`, returned model ID `gpt-6.2-sol`), high reasoning,
  8,192 output tokens/call. Actual `ContextConcierge → agent_loop → ExplorerPort`.
- Up to 16 API attempts including auxiliary tools and transport retries per
  trajectory, API timeout 180s, agent-loop timeout 900s, each SQL timeout 30s,
  result limit 100,000 rows. Final SQL export is outside the agent-loop timer.
- At most one logged retry for timeout/connection/rate-limit failure, identical
  input, counted in the budget. Successful answers/SQL mistakes are not retried
  for selection. Concurrency 4 per benchmark; benchmark runs overlap.
- Product source `4e1b48a`, runner/provider hashes in each settings file and
  [frozen configuration](frozen-final.json). Runtime versions in
  [runtime.json](results/runtime.json). Every question has isolated memory.
- Official Spider2 execution-result evaluator and unmodified, hash-pinned BIRD
  `calculate_ex`. Clarification, API/SQL failure and missing final SQL remain
  incorrect in the denominator. Missing records would block publication.
- [Protocol and dataset versions](README.md). Spider 2.0-Lite SQLite is not
  Spider 1.0 or the full Spider2 cloud benchmark; BIRD Mini-Dev 500 is not the
  full BIRD dev/hidden test set.

## Development record (all trials retained)

| Run | Model profile | Correct / total | EX |
|---|---|---:|---:|
| spider-development-v1 | gpt62-sol | 12/20 | 60.00% |
| bird-development-v1 | gpt62-sol | 17/20 | 85.00% |
| spider-development-v2 | gpt62-sol | 8/20 | 40.00% |
| bird-development-v2 | gpt62-sol | 18/20 | 90.00% |
| spider-development-astra | gpt62-astra | 8/20 | 40.00% |
| bird-development-astra | gpt62-astra | 15/20 | 75.00% |

The initial run used product `e7f0b064`. The treatment added general output-grain,
projection, join direction, filtering and numeric-type checks to the system
prompt. The second development phase suffered API timeouts; those failures were
not removed or replaced. It motivated logged transport retry support and the
final per-benchmark concurrency setting. The development results do not establish
a consistent accuracy gain from the prompt changes. No causal memory/semantic
layer benefit is claimed, and historical GPT-4.1 scores under other budgets are
not treated as a matched before/after comparison.

The held-out Spider evaluation had 11 failed predictions, including 8 trajectories
that invoked `ask_user`. The clarification safeguard was kept; these are still
incorrect under the noninteractive benchmark metric.

## Evidence and limits

[Machine-readable final report](results/final-report.json), per-run manifests,
settings, official scores, final SQL and compact records are in `results/`.
Full local transcripts/CSVs are retained and their SHA-256 hashes are published.
The ordinary harness file-ingestion tool remains present; this is input separation,
not OS-level sandboxing. Actual file-tool attempts were audited and no successful
filesystem reads occurred; `OKF_BUNDLE_DIR` was absent. See `results/file-input-audit.json`.
Source snapshots preserve initial and final as-executed bytes. After measurement,
unused copied legacy CLI entrypoints were removed; all retained execution/grading
function ASTs and the final SQL instruction are unchanged (see
`results/primitives-equivalence.json`). Executed runner revision: `0260422`. The public data
license/attribution is retained; DB files and credentials are not committed.

This measures the Lang2SQL harness on SQLite. It does **not** measure the Codex
MCP client, Oracle SQL dialect, multi-user authorization, memory retention, or
the improvement from supplying company-specific definitions. The Oracle demo
remains separate evidence for its concrete scenario.

Locked-environment tests: 222 passed, 1 optional integration skipped. Benchmark
Ruff, diff whitespace and credential-pattern checks passed. See
[implementation verification](VERIFICATION.md). PR:
https://github.com/CausalInferenceLab/Lang2SQL/pull/320
