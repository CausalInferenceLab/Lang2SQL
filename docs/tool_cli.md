# Direct tool CLI for coding agents

`lang2sql-tools` exposes the same five tools as the MCP adapter without MCP,
an internal LLM, or an API key. The original `lang2sql "question"` command is
unchanged. Both adapters share `tenancy/tool_runtime.py` and the existing tools.

```sh
uv sync --extra tools
export LANG2SQL_DB_URL=sqlite:////absolute/path/analytics.db

lang2sql-tools --user-id analyst --org-id demo list
lang2sql-tools --user-id analyst --org-id demo call explore_schema
lang2sql-tools --user-id analyst --org-id demo call run_sql \
  --args '{"sql":"SELECT COUNT(*) AS n FROM orders"}'
```

Use `.venv/bin/lang2sql-tools` or `uv run --extra tools lang2sql-tools` when the
virtualenv is not activated. Install the matching DB extra for other databases.
A configured DB is required even for `list`. No DB or LLM stub is used.

`list` returns names, descriptions and argument schemas as JSON. `call` returns
`call_id`, `content`, and `is_error`; SQL content remains the existing text table.
Tool errors exit 1. JSON/schema/config input errors exit 2. Success exits 0.
Argument-parser usage errors go to stderr. Normal results go to stdout as one JSON
value; logs go to stderr. Extra tool arguments (including identity overrides) are
rejected. Operator identity is configured before the subcommand with `--user-id`,
`--org-id`, optional `--team-id` and `--admin`, just as for the MCP server.

For arguments containing quotes or large text, `--args-file /path/to/input.json`
reads a UTF-8 JSON file; `--args-file -` reads stdin. It is exclusive with `--args`.

## Memory and batches

`--store /absolute/path/metadata.db` persists definitions/audits, not memory notes.
Each CLI invocation starts a new memory store. To remember and recall within the
same invocation, pass an array of calls to `batch`:

```sh
lang2sql-tools --user-id analyst --org-id demo batch --args '[
  {"name":"remember","arguments":{"text":"Exclude refunds"}},
  {"name":"recall_memory","arguments":{"query":"refunds"}}
]'
```

All schemas are validated before the first call. Calls then run sequentially and
stop at the first tool error. The JSON result includes `results`, `completed`, and
`requested`. Successful earlier writes are not rolled back. This is not a database
transaction or a persistent agent session. Notes disappear after the batch exits.
For lasting definitions, use `term_custom` and the same `--store` in later calls.

DB permissions, federation scope, and SQL safety use the same code as the MCP
adapter. See [MCP limitations](mcp.md). Oracle-specific support is in PR #257 and
must be merged before this branch can claim end-to-end Oracle compatibility;
this CLI does not add or duplicate dialect handling.
