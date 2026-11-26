# Accuracy investigation and incomplete follow-up

**The requested 95% on both benchmarks was NOT reached.** This PR contains
verified implementation fixes and all measured experiments, not a claim of
successful benchmark improvement. It remains draft pending a complete replay.

| Benchmark | Previous complete baseline | Completed intermediate replay | Latest revised product |
|---|---:|---:|---|
| Spider 2.0-Lite SQLite 135 | 81/135 (60.00%) | 78/135 (57.78%), regression | Full-v3 interrupted; no score |
| BIRD Mini-Dev SQLite 500 | 434/500 (86.80%) | Full-v2 interrupted; no score | Full-v3 interrupted; no score |

Spider v2 used product `6f3baff`, Sol high reasoning, official schema metadata,
32 API attempts, 16,384 output tokens, no extra review. It had zero API errors
and 12 failed predictions, all retained in the denominator. **Do not call this
setting an accuracy improvement or replace the baseline with a best-of-runs score.**

Latest product `7a51b9f` additionally fixes unnecessary clarification and provides
an optional Responses API path. Its full-v3 runs were stopped after an isolated
API probe confirmed HTTP 429, code `credit_balance_exhausted`, type
`insufficient_quota`, with no Retry-After. This was account credit exhaustion,
not evidence of model failure or an ordinary transient request-rate limit.
The old retry logic treated it as a general RateLimitError and emitted many
failed attempts before diagnosis. No API calls are left running.

## Interrupted attempts (not accuracy)

| Run | Recorded / planned | Failed records | API error attempts |
|---|---:|---:|---:|
| bird-full-v2 | 421/500 | 102 | 190 |
| bird-full-v3 | 348/500 | 267 | 545 |
| spider-full-v3 | 35/135 | 35 | 70 |

All existing records are preserved. Missing/cancelled cases are not silently
removed or scored as successes. No partial-run percentage is published. Full-v3
increased concurrency to 12 per suite; this did not remedy the account quota.
Resumption requires restored API credits and a **fresh complete run**, not
cherry-picking successful answers from these attempts.

## Development experiments (all reported)

| Suite | Official metadata | Model | Initial output | After extra review |
|---|---|---|---:|---:|
| spider | False | sol-reviewed (high Chat) | 11/20 | 10/20 |
| spider | False | astra-reviewed (high Chat) | 11/20 | 10/20 |
| spider | True | sol-reviewed (high Chat) | 11/20 | 10/20 |
| spider | True | astra-reviewed (high Chat) | 11/20 | 11/20 |
| spider | True | sol-responses-max | 12/20 | Not run |
| spider | True | astra-responses-max | 9/20; 5 quota-related failures | Not run |
| bird | False | sol-reviewed (high Chat) | 19/20 | 19/20 |
| bird | False | astra-reviewed (high Chat) | 19/20 | 19/20 |
| bird | True | sol-reviewed (high Chat) | 18/20 | 18/20 |
| bird | True | astra-reviewed (high Chat) | 19/20 | 19/20 |
| bird | True | sol-responses-max | 17/20 | Not run |
| bird | True | astra-responses-max | 18/20 | Not run |

The BIRD 19/20 result is only a 20-question selection pilot: **not 95% over
BIRD500**. Spider Astra Responses was contaminated by credit exhaustion and is
not a fair model-quality comparison. All its failures remain in the denominator.
Review evaluates the initial and final answer of the same trajectory; initial
logs include later review costs and are not a no-review latency estimate.

Selections were frozen from the original development pilots before each full
run; no full-set score or per-question gold-selected answer routing was used.
V3 selected BIRD Astra high Chat with official metadata and Spider Sol max
Responses with metadata, both without review. Because an alternative trial was
quota-limited, this is provisional, not evidence of the optimal model.
Exact limits and source hashes are in each settings/selection file. Changes in
model, API, budgets and metadata prevent an isolated causal interpretation.

## Implemented fixes and evidence

- Preserve primary/composite foreign keys through SQLAlchemy reflection and the existing schema tool.
- Accept SELECT/WITH answers beginning with SQL comments; still reject writes.
- Supply optional pinned official schema descriptions/examples, without gold annotations.
- Check source columns, join multiplicity, aggregation grain, rate denominators, missing-value representations and exact output types.
- Eliminate clarification about background prose when the actual query already specifies its constraints. Real-API bird140 check: 1/1 correct. Undefined actor-ranking policy local195: still asks, officially 0/1.
- Add optional review and opaque Responses reasoning continuity; reuse the existing agent loop and provider retry budget. No frontend default change.
- After the quota incident, fail immediately on structured insufficient-quota/credit-exhausted errors, cancel queued/in-flight batch tasks, and persist an aborted status. Ordinary rate limits retain bounded retries. Partial runs cannot be scored. This fix is unit-tested; a paid API replay is blocked by the exhausted account.

## Remaining work

[Baseline diagnosis](diagnostics/README.md) inventories all 120 original errors.
The completed Spider v2 still has 57 errors. Some are wrong SQL result semantics;
others are clarification about genuinely missing definitions. Those questions
remain incorrect under the official noninteractive benchmark. Increasing model
budget cannot supply an absent business policy. No ambiguity guard was disabled.

The entire old evaluation set has now been inspected, so subsequent same-set
scores are **development-exposed public regression scores**, not independent
holdout accuracy. Official graders, tolerances and reference answers are
unchanged. No benchmark gold was supplied to inference. No 95% Oracle,
Codex-MCP, production accuracy or memory-effect claim is justified.

Next: restore API credits, rerun quota-contaminated model comparison and then a
fresh frozen full 135/500 evaluation. Investigate residual semantic errors from
that complete run; validate future claims on independent data. This PR does not
promise that those steps alone will reach 95%.

## Evidence and reproduction

[Machine-readable report](improvement-results/report.json) records completed and
interrupted attempts. Every trial includes manifests/settings, available
per-case scores, compact records with SQL/answers/calls, and hashes of raw
transcripts/CSVs. Original full transcripts stay local under
`/Users/seyoung/workspace/benchmarks/accuracy-improvement-20261126`.
All recorded `ingest_doc` audits were empty; no file reads through that tool were
observed. The harness is not an OS sandbox, so this is an input/audit claim, not
an OS isolation guarantee.

Use the [benchmark CLI](README.md#reproduce), copied profile/selection files,
matching pinned DBs and fresh output directories. Never overwrite these
attempts or combine their correct answers. All API tasks were stopped on quota
exhaustion; no unattended paid work remains.

Validation: **237 passed, 1 skipped**, changed-code Ruff and whitespace checks.
[Implementation verification](diagnostics/VERIFICATION.md).
[Draft PR #321](https://github.com/CausalInferenceLab/Lang2SQL/pull/321).
