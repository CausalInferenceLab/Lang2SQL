# Spider 2.0-Lite SQLite baseline

Evaluate the existing Lang2SQL agent on a pinned public benchmark across API providers. This is evaluation infrastructure, not a Codex migration. Product source under `src/` is unchanged from `e0a7fb1`. The measured system is **Lang2SQL master + a selected API model**, not a claim about Lang2SQL's lift over a plain model.

## Fixed protocol

- Upstream: `xlang-ai/Spider2`, commit `cafb867313aab4e674652054198f383cf4018943`.
- Full population: **135 SQLite (`local*`) tasks**, 30 referenced databases. All 135 have official evaluation configuration and gold CSVs at this revision.
- `manifest-sqlite135.json` fixes the questions, order, DB hashes, document hashes, gold result/configuration hashes and evaluator hash. `manifest-pilot20.json` is its first 20 tasks after seeded shuffle (seed 20261114); selection does not use difficulty or scores.
- Each question gets a new context, in-memory store and session. No cross-question learning. Only the official question, its named external document, and the normal Lang2SQL schema/tools reach the model. No gold SQL, gold table names or gold results are provided.
- The existing `ContextConcierge → agent_loop → tools → SafetyPipeline → SqlAlchemyExplorer` path runs unchanged. The benchmark supplies a measured LLMPort and read-only SQLite connections with per-query deadlines. Existing tool row limits remain; final SQL execution is separate and does not silently truncate output.
- Default budgets: 8 **total** model calls (including auxiliary tool inference), 4,096 output tokens per call, 120-second API deadline, 480-second case deadline, 30-second SQL deadline, 100,000 final rows, concurrency 2. No automatic provider fallback or API retries. Fix these settings before a comparison.
- All profiles use the same final-answer instruction: one fenced SQL block. Missing/multiple blocks are failures. Never infer a final answer from the last exploratory SQL.
- Official `evaluate_single_exec_result_instance` is loaded directly from the hash-pinned upstream evaluator. CSV column/order/tolerance handling is the official implementation. Our wrapper includes **all manifest IDs** in the denominator, including API errors, no SQL and SQL failures. It does not use the upstream CLI's intersection-only denominator or its hardcoded 547 denominator.
- Missing keys are `blocked_missing_key`, not 0% model accuracy. Incomplete runs have no accuracy. Partial task failures within a completed run count as incorrect.
- Token usage is taken from provider responses. Missing usage is counted, not treated as known zero. Dollar cost remains null: no invented rates or claims that token counts equal billed cost. `seconds` is the sum of case latencies, `wall_seconds` is elapsed time per profile when available.
- Snapshot IDs are preferred. Provider aliases can change; returned response model names are recorded. This is pass@1 with one sample per task, not a statistical confidence claim.

## Setup

```bash
uv sync --extra benchmark
# Place benchmark data outside this source repository.
git clone https://github.com/xlang-ai/Spider2.git /absolute/path/Spider2
git -C /absolute/path/Spider2 checkout cafb867313aab4e674652054198f383cf4018943
uv run --with gdown gdown 1coEVsCZq-Xvj9p2TnhBFoFTsY-UoYGmG -O /absolute/path/sqlite.zip
```

Extract `.sqlite` files into one DB directory, keeping their original basenames. Do not commit DBs, archives, credentials or provider debug logs. The upstream archive also contains a DB not referenced by this SQLite subset. The question manifests retain upstream MIT attribution in `LICENSE-Spider2.txt`; follow upstream data terms for the downloaded databases.

```bash
uv run --extra benchmark python -m bench.spider2.evaluate prepare \
  --root /absolute/path/Spider2 --db-dir /absolute/path/dbs \
  --output /tmp/manifest.json --limit 20
```

Compare the generated manifest with the checked-in one. `--limit 0` selects all 135. The `run` command verifies hashes and refuses product changes relative to the pinned baseline.

## API/model matrix

`models.example.json` describes independent profiles: provider/model identifier, name, and API key **environment variable name**. LiteLLM translates requests to the actual provider, while Lang2SQL's existing message/tool/response conversion is reused. Provider-specific tool-call metadata is retained for follow-up turns (including Gemini thought signatures).

Run with `OKF_BUNDLE_DIR` unset so an unrelated local knowledge bundle cannot enter the baseline prompt.

Set the keys through your shell or a local `.env` file and use `--env-file /path/to/file`. Only variables referenced by profiles are used. Existing project configurations using `OPEN_AI_KEY` can reference that name in `api_key_env`; no key needs to be copied into the JSON. Unknown/missing keys never trigger another provider. Model availability must be checked against the relevant account; example model names are not a claim of live verification for every provider.

```bash
uv run --extra benchmark python -m bench.spider2.evaluate run \
  --root /absolute/path/Spider2 --db-dir /absolute/path/dbs \
  --manifest bench/spider2/manifest-pilot20.json \
  --profiles bench/spider2/models.example.json \
  --env-file /absolute/path/private.env \
  --output /absolute/path/runs/pilot20-unique-id

uv run --extra benchmark python -m bench.spider2.evaluate score \
  --root /absolute/path/Spider2 --output /absolute/path/runs/pilot20-unique-id
```

Use a new output directory for every run; overwriting or mixing outputs is rejected. Record files are written atomically per completed task, so interrupted runs retain evidence. A full run replaces the manifest with `manifest-sqlite135.json`; keep the same budgets and model profiles. No production application deployment changes are needed; the extra installs evaluation dependencies only.

## Results and limits

[Measured 135-task baseline results](RESULTS.md): GPT-4.1 mini 21/135 (15.56%), GPT-4.1 17/135 (12.59%). Anthropic/Gemini remain blocked on credentials.

Runs contain a protocol snapshot, per-model final SQL/CSV, per-task answer/transcript/API usage/timing/status, official per-task scores and a summary. API exception messages are not persisted because providers may echo credentials. Public result exports can omit bulky transcripts but should retain SQL, scores and the exact manifest/settings.

Report scores as **Spider 2.0-Lite SQLite subset**, with the exact denominator (20 pilot vs 135 full). Do not compare them directly to full 547-task Lite scores. This evaluates task completion, not federation isolation or 100-turn memory. Additional controlled ablations are needed to claim benefits from Lang2SQL context features.

## Sources

- https://github.com/xlang-ai/Spider2
- https://github.com/xlang-ai/Spider2/tree/main/spider2-lite/evaluation_suite
- https://docs.litellm.ai/docs/python_sdk
