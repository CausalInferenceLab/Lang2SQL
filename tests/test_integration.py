"""Week-2 integration: the ContextConcierge wires a working agent.

Exercises the assembled stack (tools + safety + scope resolver + explorer stub
+ sqlite) without network, using the offline FakeLLM. Locks in that the six V1
tools are registered and that run_sql/define_metric behave through the live
HarnessContext.
"""

from __future__ import annotations

import asyncio
import sqlite3
from contextlib import closing

import pytest

from lang2sql.adapters.db.d1_explorer import D1Explorer
from lang2sql.adapters.db.postgres_explorer import PostgresExplorer
from lang2sql.core.identity import Identity
from lang2sql.core.ports.explorer import Column, Table
from lang2sql.core.ports.safety import SafetyContext, Verdict
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.run_sql import RunSQL
from lang2sql.tools.semantic_federation import SemanticFederationTool


def _ctx():
    concierge = ContextConcierge()
    ident = Identity(user_id="u1", guild_id="g1", channel_id="c-mkt")
    return ident, asyncio.run(concierge.build_context(ident))


def test_v1_tools_registered():
    _, ctx = _ctx()
    names = {s.name for s in ctx.tools.specs()}
    assert names == {
        "run_sql",
        "explore_schema",
        "enrich_schema",
        "term_custom",
        "org_setup",
        "ask_user",
        "remember",
        "ingest_doc",
        "confirm_ingest",
    }


def test_run_sql_passes_gate_and_returns_rows():
    _, ctx = _ctx()
    res = asyncio.run(RunSQL().run({"sql": "SELECT * FROM orders", "limit": 10}, ctx))
    assert not res.is_error
    assert "row" in res.content.lower()


def test_run_sql_blocks_ddl():
    _, ctx = _ctx()
    res = asyncio.run(RunSQL().run({"sql": "DROP TABLE users"}, ctx))
    assert res.is_error and "BLOCKED" in res.content


def test_run_sql_tolerates_bad_limit():
    _, ctx = _ctx()
    res = asyncio.run(RunSQL().run({"sql": "SELECT 1", "limit": "demo"}, ctx))
    assert not res.is_error  # malformed limit must not crash the tool


def test_explore_schema_batch_preserves_column_details_and_partial_results():
    _, ctx = _ctx()

    async def explore(args):
        return await ctx.tools.dispatch("explore_schema", args, ctx, "schema")

    listed = asyncio.run(explore({}))
    single = asyncio.run(explore({"table": "orders"}))
    batch = asyncio.run(
        explore({"tables": ["public.orders", "missing", "orders", " users "]})
    )
    assert listed.content == "Tables:\n- public.orders\n- public.users"
    assert not single.is_error
    assert batch.is_error
    assert batch.content.startswith(single.content + "\n\n")
    assert batch.content.count("public.orders\n") == 1
    assert "missing: KeyError: 'unknown table: missing'" in batch.content
    assert "public.users\n" in batch.content


def test_explore_schema_d1_batch_reports_missing_table_and_preserves_columns():
    _, ctx = _ctx()
    with closing(sqlite3.connect(":memory:", check_same_thread=False)) as conn:
        conn.execute("CREATE TABLE orders (amount INTEGER NOT NULL)")
        conn.row_factory = sqlite3.Row

        def transport(sql, params):
            rows = [dict(row) for row in conn.execute(sql, params)]
            return {"success": True, "result": [{"results": rows}]}

        ctx.explorer = D1Explorer("acct", "db", transport=transport)
        result = asyncio.run(
            ctx.tools.dispatch(
                "explore_schema", {"tables": ["missing", "orders"]}, ctx, "schema"
            )
        )

    assert result.is_error
    assert "missing: KeyError: 'unknown table: missing'" in result.content
    assert "orders\n- amount: INTEGER NOT NULL" in result.content


def test_explore_schema_batch_preserves_qualified_names_and_rejects_ambiguity():
    class MultiSchemaExplorer(PostgresExplorer):
        tables = {
            "sales.orders": Table("orders", "sales", [Column("sold_at", "date")]),
            "archive.orders": Table(
                "orders", "archive", [Column("archived_at", "date")]
            ),
        }

        async def list_tables(self):
            return list(self.tables.values())

        async def describe_table(self, name):
            return self.tables["sales.orders" if name == "orders" else name]

    _, ctx = _ctx()
    ctx.explorer = MultiSchemaExplorer("postgresql://ignored")

    result = asyncio.run(
        ctx.tools.dispatch(
            "explore_schema",
            {"tables": ["sales.orders", "orders", "archive.orders"]},
            ctx,
            "schema",
        )
    )

    assert result.is_error
    assert result.content.split("\n\n") == [
        "sales.orders\n- sold_at: date",
        "orders: ambiguous table. Include its schema name.",
        "archive.orders\n- archived_at: date",
    ]


@pytest.mark.parametrize(
    "args",
    [
        {"tables": []},
        {"tables": "orders"},
        {"tables": ["orders", " "]},
        {"tables": ["orders"], "table": "users"},
    ],
)
def test_explore_schema_rejects_invalid_batch_input(args):
    _, ctx = _ctx()
    result = asyncio.run(ctx.tools.dispatch("explore_schema", args, ctx, "schema"))
    assert result.is_error


def test_term_custom_is_scope_local():
    from lang2sql.tools.semantic_federation import _render_effective

    ident, ctx = _ctx()
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "active_user", "definition": "30d login", "layer": "team"}, ctx
        )
    )
    rendered = _render_effective(
        ctx.store, ident.kv_scope, ident.effective_channel_id, ident.user_id
    )
    assert "active_user" in rendered
    # a different channel does not see this channel-level definition
    other_rendered = _render_effective(ctx.store, ident.kv_scope, "c-fin", "u1")
    assert "active_user" not in other_rendered


def test_term_custom_emits_audit_event():
    concierge = ContextConcierge()
    ident = Identity(user_id="u1", guild_id="g1", channel_id="c-mkt", is_admin=True)
    ctx = asyncio.run(concierge.build_context(ident))
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "revenue", "definition": "gross revenue", "layer": "org"}, ctx
        )
    )
    events = asyncio.run(ctx.audit.query(ident.user_id))
    assert any(
        e.action == "term_custom" and e.detail.get("term") == "revenue" for e in events
    )


def test_safety_pipeline_on_context():
    _, ctx = _ctx()
    assert ctx.safety.evaluate("SELECT 1", SafetyContext()).verdict == Verdict.PASS
