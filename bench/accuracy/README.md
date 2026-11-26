# Spider2 and BIRD execution accuracy

This experiment measures the real `ContextConcierge → agent_loop → ExplorerPort`
path. It is **not** a Codex MCP end-to-end measurement, Oracle dialect score,
long-term-memory evaluation, or proof of the semantic layer's causal benefit.
Each case starts with an isolated memory/store. There is no answer-key retrieval,
model fallback, multi-sample oracle selection, or user simulator.

## Protocol fixed before inference

- Spider **2.0-Lite SQLite**, all 135 local tasks, upstream
  `cafb867313aab4e674652054198f383cf4018943`. This is not Spider 1.0 or the full
  Spider2 cloud-database benchmark. The official execution-result grader is reused.
- BIRD **Mini-Dev SQLite 500**, annotations at
  `5b256b5a9c17478f06ed6912612e9a14d17065c8` (2026-10-25 corrections).
  This is not BIRD's hidden test set or its full development set.
- Fixed seed 20261126: 20 development questions per benchmark, then 115/480
  evaluation questions. Selection is before inference and is not based on
  difficulty or success. These are question-level splits, not unseen-database
  splits; public data may also have appeared in foundation-model training.
- Development examples may guide general changes and model selection. Evaluation
  examples must not be used for tuning. Never add their gold SQL to any model
  input. Report development and evaluation separately, including every attempt.
- Pass@1: one agent trajectory per question, up to 16 model calls including
  auxiliary tools, 8192 output tokens/call, high reasoning effort, 180 seconds
  per API call, 900 seconds/agent loop, 30 seconds/SQL, 100,000 result rows. Final runs allow one logged retry for timeout/connection/rate-limit failures
  using identical input; retries count toward the 16-call budget. Successful
  responses and SQL mistakes are never retried for answer selection. No excluded failed predictions or last-tool fallback.
- SQL queries execute in read-only SQLite connections with a query deadline.
  A final answer must contain one SELECT/WITH SQL block. Clarification and missing
  final SQL count as incorrect on these noninteractive benchmarks.
- Preserve question/DB/grader/gold hashes, product revision, runner/provider
  hashes, generated SQL, full transcripts, API usage, errors and elapsed time.
  Raw databases, credentials and provider error messages are never committed.
- BIRD inference manifest is an explicit allowlist of question/evidence fields;
  `SQL` is rejected. Gold is loaded only by `score`. The pinned upstream
  `calculate_ex` function is extracted unchanged and applied to SQLite row tuples:
  set equality, including its official disregard for row order and duplicates.
  No CSV string conversion or custom rounding is used for BIRD scoring.
- A broken gold/database pairing fails scoring. A missing record prevents final
  publication. A failed model/SQL prediction remains in the planned denominator.

## Data provenance and attribution

BIRD DBs were extracted without running the image's entrypoint from
`3036197201/bird-mini-dev-sqlite@sha256:f5e2746e284fdf4bc5d7a168a42d951bdf3d429d6db91b9928308e8261d0b75e`,
path `/bird/mini_dev/dev_databases`. All 11 DB SHA prefixes match the current
per-database Harbor tags. The California DB was also extracted from the current
Harbor image and its full hash matched. All 500 reference SQL queries executed
and returned nonempty results; this is a dataset health check, not model accuracy.

- [Spider2](https://github.com/xlang-ai/Spider2)
- [BIRD annotations and dataset card](https://huggingface.co/datasets/birdsql/bird_mini_dev/tree/5b256b5a9c17478f06ed6912612e9a14d17065c8)
- [BIRD official EX comparator](https://github.com/bird-bench/mini_dev/blob/1c49a375e76c471e218564da3f4617513d33e371/evaluation/evaluation_ex.py)

BIRD annotations and the derived `bird-manifest.json` retain the source dataset's
[CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/) license and attribution to the BIRD authors (Li et al., NeurIPS 2024,
*Can LLM Already Serve as A Database Interface? A BIG Bench for Large-Scale
Database Grounded Text-to-SQLs*). The manifest removes gold SQL, renames fields,
and adds fixed development/evaluation IDs. The official comparator source stays
external and is verified by hash. The Spider runner/provider were reused from
Lang2SQL's existing benchmark commit `24cff8cb1126fc017d1c68389b76213858e3a2e6`.

The Spider2 upstream MIT notice is retained in `SPIDER2-LICENSE.txt`.

The reused Spider manifest retains a historical `baseline_commit` field. Current
product identity is `settings.json.product_commit` / `frozen-final.json`, not that
legacy data-manifest annotation. The executed runner revision is `0260422`.
After measurement, unused legacy CLI entrypoints were removed; retained primitive
ASTs and the SQL instruction are identical (see `results/primitives-equivalence.json`).
As-executed bytes are retained in `results/source-final/`.

## Reproduce

Install `uv sync --extra benchmark`. Set a credential file with the key name in
`models.json`; never commit that file. Set the paths below to your local copies.
Run from the repository root. Use a fresh output directory for every attempt.

```sh
export PYTHONPATH=src:.
export LITELLM_LOCAL_MODEL_COST_MAP=True
# SPIDER, SPIDER_DB, BIRD_DB, QUESTIONS, EVALUATOR, CREDENTIALS, RUN are local paths.
uv run python -m bench.accuracy.evaluate run \
  --root "$SPIDER" --db-dir "$SPIDER_DB" \
  --manifest bench/accuracy/spider2-manifest.json \
  --profiles bench/accuracy/models.json --env-file "$CREDENTIALS" \
  --product-commit 4e1b48a --split development --output "$RUN"
uv run python -m bench.accuracy.evaluate score \
  --root "$SPIDER" --db-dir "$SPIDER_DB" --output "$RUN"

# BIRD uses the same run command with its manifest and DB directory; score:
uv run python -m bench.accuracy.evaluate score --db-dir "$BIRD_DB" \
  --questions "$QUESTIONS" --evaluator "$EVALUATOR" --output "$RUN"
```

Each benchmark uses concurrency 4. Spider and BIRD final runs overlap in wall
time (up to 8 requests total); latency is not reported as a model-speed benchmark.

Change `--split` to `holdout` only after selecting and freezing the final
configuration. Changing the model, prompt, budget or source creates a new
experiment; historical scores under different configurations are not paired
improvement measurements. `--split all` is for a new explicitly reported full
public-dev run, not a way to hide development-set selection.


## Accuracy improvement experiment

See [diagnostics](diagnostics/README.md) for all baseline errors and the new
protocol. Old holdout questions have now been inspected: subsequent replays are
**development-exposed public-set results**, not independent held-out accuracy.

New optional flags (baseline defaults remain unchanged):

```sh
# Choose the appropriate pinned schema map for the suite.
--schema-docs bench/accuracy/bird-schema.json
--schema-docs bench/accuracy/spider2-schema.json
# Number of additional measured SQL-review turns; 0 by default.
--review-rounds 1
```

Review uses the existing agent loop and the same total API/time budget. It does
not see gold, and the last answer is scored even if review makes it worse. A
clarifying/non-SQL answer is never overridden to force a prediction. Schema maps
contain official metadata/examples only; their exact file hashes are validated
before any model call and recorded in settings. Optional context changes are
reported as part of the treatment, not attributed solely to model or prompt.


An optional profile `"api": "responses"` (see `models-responses.json`) compares
Responses with Chat Completions. It retains provider-returned opaque reasoning
items across tool calls for the exact message prefix and tool definitions, with
`store=false`; it does not add provider types to the core domain or enable a
second retry loop. The cache is bounded by the case call budget and is never
shared between cases. Profile settings and returned/replayed item counts are
recorded. Higher reasoning effort is an experiment, not an assumed improvement.
The production frontend/provider default is unchanged.


Latest outcome: [investigation and incomplete follow-up](IMPROVEMENT_RESULTS.md).
The 95% target was not reached; full-v3 stopped on account credit exhaustion.
Structured account-quota errors now abort the batch without retrying every
remaining question. Ordinary transient rate limits keep the configured bounded
retry. An aborted/partial run cannot publish an accuracy score.


## ChatGPT-authenticated Codex MCP experiment

[Same-question comparison and limitations](CODEX_MCP_RESULTS.md).
The real production MCP path can be evaluated without loading an API key:

```sh
PYTHONPATH=src:. uv run python -m bench.accuracy.codex_mcp \
  --root "$SPIDER" --db-dir "$BIRD_DB" \
  --manifest bench/accuracy/bird-manifest.json \
  --schema-docs bench/accuracy/bird-schema.json \
  --split development --output "$RUN"
```

Use the matching Spider manifest/schema map and DB directory for Spider. Requires
Codex CLI ChatGPT login. A final SQL block counts as MCP success only if the same
SQL actually ran through the production `run_sql` tool. Missing definitions are
not silently seeded or bypassed. The default model is left to Codex; no resolved
model identifier is asserted. Internal API calls are not observable, so report
CLI invocations and captured usage separately from API-harness measurements.
