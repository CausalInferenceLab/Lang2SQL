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


## Missing official BIRD metadata

The original run supplied per-question evidence but not the published database
schema descriptions and value examples. `../bird-schema.json` is copied unchanged
from https://huggingface.co/datasets/birdsql/bird_mini_dev/blob/5b256b5a9c17478f06ed6912612e9a14d17065c8/baseline_inference/schemas/sqlite.json
(SHA-256 `5176e324a72e80a8a37a12285596d123e0ba6c310dceaa9f13a1e413ee88ff5b`).
The upstream authors explicitly separate this map from question/gold annotations.
The file retains the BIRD CC BY-SA 4.0 attribution in the parent README. Optional
`--schema-docs bench/accuracy/bird-schema.json` loads only this pinned map; any
changed file, annotated answer input or cross-suite use is rejected. The setting
and source hash are recorded. This changes supplied context, so results must
identify it and not attribute all change to the product prompt or memory.


Spider's published per-table schema/sample JSONs were also absent from the
initial prompt. `../spider2-schema.json` is a deterministic database-to-text map
of the upstream `table_name`, `column_names`, `column_types`, `description`, and
`sample_rows` fields, taken with `git show` at the pinned upstream revision.
`spider-schema-sources.json` records every original file hash and the map hash.
No evaluation directory, gold SQL, task-specific selected table list or previous
answer is read. Examples are explicitly marked non-exhaustive. Database folder
matching only normalizes case and hyphens to underscores. Spider's upstream MIT
notice is retained in the parent directory. Both schema maps are optional and
hash-validated, and both will be tested under the same pilot protocol.


## Follow-up: unnecessary clarification

The frozen full-v2 run exposed a false clarification in BIRD bird140: the
introduction mentioned active badge-holders, but the actual query explicitly
specified adults aged 19–65 with the Supporter badge, over all time. The agent
invented an additional activity filter and asked for its definition. A follow-up
prompt change distinguishes background motivation from actual query constraints,
and treats explicit user/document definitions as valid even without a semantic
layer entry. Genuine missing business definitions still require clarification.
This follow-up is a **new product revision**, not part of the frozen full-v2
predictions. Targeted checks and any subsequent full replay must identify it.


## Follow-up: reasoning continuity experiment

The benchmark Chat Completions translation retained tool calls but had no
Responses reasoning-item continuity path. An optional Responses codec now
replays the original opaque output items only for unchanged message prefixes
and tool definitions, using the existing provider budget/retry loop. A real API
smoke confirmed six successive calls with reasoning items replayed. The first
smoke exposed SDK null/alias fields rejected by the API; wire serialization now
uses aliases and omits unset nulls. Both the failed smoke and corrected run
remain in local evidence; this is not an accuracy result.

Sol and Astra with maximum reasoning and 32,768 output tokens were compared on
the same original 20 development examples. This experiment changes both API and
budget, so it cannot isolate the effect of reasoning continuity alone. The
final full-v3 selection is fixed from those development trials and earlier
pilots, before full-v3 inference. Product revision 7a51b9f includes the false-
clarification fix; full-v2 used 6f3baff and is retained as an intermediate result.
Production ambiguity handling is unchanged: genuinely missing business
policies still require clarification, and those cases remain benchmark errors.
