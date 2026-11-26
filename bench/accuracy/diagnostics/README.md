# Accuracy improvement investigation

Baseline: PR #320, Spider 2.0-Lite SQLite 81/135 and BIRD Mini-Dev 434/500.
This iteration targets 95% on each exact public subset. It does not claim that target is reached.

## Protocol

All baseline failures were inspected offline. Consequently the old holdout is now
**development-exposed**; new scores on these same questions are public-development
regression scores, not an independent generalization estimate. Gold SQL/results
are used only for offline diagnosis and scoring. They are never passed to the
agent, SQL review, model selection input or persisted agent memory. No per-ID
SQL templates, answer cache or score-based answer selection is used.

Start with the original fixed 20 development questions in each suite and compare
Sol and Astra under a larger, equal budget. Each trajectory gets one subsequent
SQL review using the same agent loop and available DB tools. Review is counted
in API/time budgets and its final answer is always scored, even when worse than
the initial answer. Clarification/non-SQL output stops the trajectory; review
must not override it. Freeze the chosen configuration before full replay.

## Exhaustive mechanical baseline audit

`baseline-errors.json` covers all 54 Spider and 66 BIRD errors, including status,
explicit clarification calls, database and model-call count. For executed BIRD
errors it also records result dimensions. These observations are not an automatic
claim of semantic root cause.

| Suite | Executed SQL, wrong result | Failed prediction | Total wrong |
|---|---:|---:|---:|
| Spider | 42 | 12 | 54 |
| BIRD | 62 | 4 | 66 |

9 Spider and 2 BIRD failures invoked `ask_user`; other failures include plain-text
clarifications, SQL extraction and execution errors. API transport errors were
zero in both baseline final runs.

## Verified causes and scoped changes

| Evidence | Cause | Change / disposition |
|---|---|---|
| `SqlAlchemyExplorer._describe_table_sync` returned only columns | Declared relational keys were lost before reaching the agent | Preserve ordered primary/composite foreign keys in the existing Table port; expose them in `explore_schema`; legacy adapters retain empty defaults |
| Spider local274 final block starts with a SQL comment, followed by SELECT | `final_sql` required SELECT/WITH at byte zero | Reuse the safety comment parser for statement-kind detection; preserve the original SQL bytes; regression tests cover comments and write rejection |
| BIRD bird024 used SOCType/School instead of the explicitly named FRPM fields | Similar column names caused source substitution | General source/field verification instruction and measured second review |
| BIRD bird355/357/360/363 included unknown-weight sentinel values | SQL NULL was treated as the only missing representation | Inspect actual stored values and documented semantics before deciding missing-value handling; no superhero-specific rule or threshold in code |
| BIRD bird000/002 used a stored percentage instead of numerator/denominator | Stored rounding is not equivalent at requested precision | General denominator/grain/precision verification |
| Spider local008 used maximum single-season values rather than career totals | Aggregation grain mismatch | Review the query against the original request and verify aggregation |
| BIRD bird127 has an unindexed correlated EXISTS over posts and failed execution | Execution budget / query plan | Review can run diagnostics and reformulate SQL; failures stay in denominator |
| BIRD bird094 changed stored lowercase labels to title case | Unrequested presentation transformation | Preserve categorical output values verbatim |
| Spider local195 asks what defines top actors; local277 asks for missing regression weights | Ambiguity cannot always be resolved by schema inspection | Keep clarification; no fabricated business definitions or gold-driven resolution |

Some official comparisons are stricter or less specified than a business
answer contract. For example local130 asks for surname and quintile, while its
reference condition columns include surname and the numeric grade. BIRD bird120
can differ by floating-point operation order. These remain official errors;
neither comparator tolerances nor gold annotations are changed to raise scores.

## Reuse / execution map

- DB introspection: existing SQLAlchemy sync worker behind `asyncio.to_thread`.
- Tool rendering: existing `ExploreSchema`, no second schema tool.
- Generation and review: existing `ContextConcierge → agent_loop → run_sql`.
- Retry/budget: existing `MeasuredLLM`; review shares the same call counter.
- SQL extraction/read-only execution/grading: existing benchmark primitives.
- Configuration: existing CLI/profile input; no new environment variables or deployment changes.
- Review budget: CLI `--review-rounds` (0/1/2); default 0 preserves baseline behavior.

The first pilot uses commit 8e374a5 (before the comment-extraction fix). Final
source/configuration and all outcomes are recorded separately. No baseline
artifacts are overwritten.
