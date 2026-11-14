# Lang2SQL MCP for coding agents

Codex or Claude Code owns the conversation and tool-calling loop. This optional
stdio server forwards calls to Lang2SQL's existing tool registry; it does not run
`agent_loop`, instantiate an LLM provider, or require an OpenAI API key.
The CLI/Discord harness, core ports, SQL safety pipeline and existing tools are
unchanged. `HarnessContext` is reused with an inert session and an LLM that raises
if called. Concrete dependencies are assembled separately under `tenancy/mcp.py`.

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
| `run_sql` | Existing SQL validation, execution, audit and text result formatting. |
| `term_custom` | Existing org/team/user definitions; `list=true` reads effective definitions; registration/removal retains existing permissions. |
| `remember` | Save a note in this server process for the configured user. |
| `recall_memory` | Call the existing memory service. The default inject-all retriever returns all notes, not semantic search. |

Example client request: “Use Lang2SQL to inspect the schema and business
definitions, then calculate total revenue.” The agent should call
`term_custom({"list": true})` explicitly: the old harness's automatic system-prompt
injection does not run here. Notes are observations, not approved instructions.

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
