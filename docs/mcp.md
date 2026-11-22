# Lang2SQL MCP for coding agents

Codex or Claude Code owns the conversation and tool-calling loop. This optional
stdio server forwards calls to Lang2SQL's existing tool registry; it does not run
`agent_loop`, instantiate an LLM provider, or require an OpenAI API key.
The CLI/Discord harness, core ports and SQL safety pipeline are unchanged.
The shared ambiguous-term guidance now requires clarification before querying. `HarnessContext` is reused with an inert session and an LLM that raises
if called. Concrete dependencies are assembled separately under `tenancy/tool_runtime.py`.

## Install and connect

From the checkout:

```sh
uv sync --extra mcp
# Add the matching existing DB extra for other databases, e.g.:
# uv sync --extra mcp --extra postgres
```

One server process uses one DB connection and one fixed identity. Set the existing
`LANG2SQL_DB_URL` (and optionally `LANG2SQL_DB_SCHEMA`) or the existing D1 variables.
The server fails at startup if no connection is configured; it does not use the
CLI's stub DB. Configure a read-only source DB account. Metadata changes still
write to Lang2SQL's separate SQLite store.

Replace the paths below with absolute paths. The SQLite source database must
already exist. These commands register a server in the client; installation of
Lang2SQL itself does not change client configuration.

Codex CLI:

```sh
codex mcp add lang2sql --env LANG2SQL_DB_URL=sqlite:////absolute/path/analytics.db -- \
  /absolute/path/Lang2SQL/.venv/bin/lang2sql-mcp \
  --user-id analyst --org-id demo --team-id finance \
  --store /absolute/path/lang2sql-metadata.db
```

Claude Code:

```sh
claude mcp add --transport stdio lang2sql \
  --env LANG2SQL_DB_URL=sqlite:////absolute/path/analytics.db -- \
  /absolute/path/Lang2SQL/.venv/bin/lang2sql-mcp \
  --user-id analyst --org-id demo --team-id finance \
  --store /absolute/path/lang2sql-metadata.db
```

CLI argument forms were checked against the locally installed clients' `mcp add
--help`. Protocol integration is tested using the official Python MCP client;
interactive Codex/Claude conversations are not an automated test requirement.

The parent directory for `--store` must exist. Without `--store`, metadata and audit
are in-memory. Pass `--admin` only when the local operator should be able to write
org-level definitions. User/org/team/admin values come from startup arguments,
not model-supplied tool arguments. This is a local, single-identity adapter, not
remote authentication or a multi-tenant HTTP server. Use a distinct metadata store
per DB to avoid mixing definitions between databases. Shared user-level definitions
retain the existing federation semantics across teams within an organization.

## Tools

| Name | Behavior |
|---|---|
| `explore_schema` | List tables or describe a table, including cached descriptions. |
| `resolve_business_terms` | Resolve exact canonical business terms in the current identity scope. Missing, empty or `inferred=true` definitions require user clarification. Returns exact definitions and a single-use check ID. |
| `run_sql` | Requires `definition_check_id` from the latest successful resolution, then delegates to existing SQL safety, execution and audit. |
| `term_custom` | Existing org/team/user definitions; `list=true` reads effective definitions; registration/removal retains existing permissions. |
| `remember` | Save a note in this server process for the configured user. |
| `recall_memory` | Call the existing memory service. The default inject-all retriever returns all notes, not semantic search. |

## Definition-first SQL (MCP only)

Before every SQL attempt, call `resolve_business_terms({"terms": ["net_revenue"]})`
with **all business terms from the user question** using their canonical names.
Business terms are domain metrics or concepts, not literal dates, numbers or SQL
identifiers. A calendar month is a filter; domain-specific fiscal-period rules
require a definition.
The response contains the effective confirmed definitions and `definition_check_id`.
If a term is absent, from another scope, or marked `inferred=true`, stop and ask
for its exact meaning and intended scope. Do not silently substitute a familiar
term or register an interpretation invented by the model. Only save definitions
explicitly supplied or confirmed by the user through `term_custom`.

After resolution, inspect schema with `explore_schema`, generate SQL from the
returned definitions, and call `run_sql` with `sql` and `definition_check_id`.
Use qualified table names returned by schema discovery. The server rejects SQL
without a valid check. Checks are single-use, process/identity-bound, and rejected
if any resolved definition changes. Failed resolution invalidates the prior check.
After an execution error or restart, resolve again before retrying. Registry calls
remain serialized, including definition writes and receipt validation.

This intentionally changes the MCP SQL contract: existing clients must include
the check ID. All MCP SQL requests require at least one confirmed business term;
there is no empty-terms bypass. Use `explore_schema` for metadata discovery.
`term_custom(list=true)` remains useful for browsing but does not authorize SQL.
Restart the MCP process after installing this change. The direct tool CLI retains
its original SQL interface; the new receipt guard is installed by the MCP server.

**Limits:** this gate checks the terms submitted by the agent. It cannot prove
that the model listed every term in the natural-language request, that SQL exactly
implements a definition, or that a model-supplied definition truly came from a
human. Existing `inferred=false` is a metadata flag, not a trusted approval record.
Authenticated human approval and semantic SQL verification remain separate work.
There is no internal LLM call, automatic approval, or new environment setting.

`remember` notes are process-local and disappear on restart even when `--store` is
set. Business definitions and audits persist with `--store`. No new memory
algorithm, alias gate, graph experiment or extraction model is included.

`ask_user`, `enrich_schema`, `org_setup`, `ingest_doc` and `confirm_ingest` are not
advertised. The external agent asks the user directly. LLM-driven enrichment and
document ingestion remain available in existing frontends.

## Protocol and limitations

- Tool schemas come from existing `ToolSpec`; the MCP boundary rejects extra
  arguments and the SDK validates declared types before dispatch.
- Existing `ToolResult.content` becomes MCP text content; `is_error` becomes
  `isError`. SQL output remains the existing text table (first 50 rows shown),
  not a new structured result or export API.
- SQL `NEEDS CONFIRMATION` is forwarded as existing text without executing the
  query. This adapter adds no approval bypass or automatic resume mechanism.
- Calls share one registry/store and are serialized in a worker thread. Existing
  synchronous SQLite KV operations do not block the transport event loop.
  Cancellation of a request does not guarantee cancellation of an executing DB
  query; the worker retains the store lock until completion.
- Logs use stderr; stdout carries only MCP messages. No HTTP server or listener
  is started. Environment credentials are not advertised as tool parameters.
- Existing DB adapter and SQL safety limitations remain; this PR does not add
  database-side timeout guarantees or a new SQL parser.

## Verification

```sh
uv run --extra mcp pytest -q
uv run --extra mcp python bench/ecommerce_demo.py
```

The stdio tests launch the actual server process and check handshake, tool listing,
schema discovery, SQL execution, blocked DDL, input errors, unavailable tools,
metadata permission checks, team scoping, persistence and process-local notes.

For direct command-line calls using the same runtime, see [tool CLI](tool_cli.md).
