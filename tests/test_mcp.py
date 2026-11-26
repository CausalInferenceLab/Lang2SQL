"""Exercise the actual stdio protocol against the tool-only composition."""

import json
import os
import sqlite3
import sys
from contextlib import asynccontextmanager

import pytest

pytest.importorskip("mcp")
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import ExternalAgentLLM, build_tool_context


@asynccontextmanager
async def connected(db, store, *, team="finance"):
    env = dict(os.environ)
    env["LANG2SQL_DB_URL"] = f"sqlite:///{db}"
    env.pop("LANG2SQL_DB_SCHEMA", None)
    parameters = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "lang2sql.frontends.mcp.server",
            "--user-id",
            "analyst",
            "--org-id",
            "demo",
            "--team-id",
            team,
            "--store",
            str(store),
        ],
        env=env,
    )
    async with (
        stdio_client(parameters) as (reader, writer),
        ClientSession(reader, writer) as client,
    ):
        await client.initialize()
        yield client


async def checked_sql(client, sql):
    saved = await client.call_tool(
        "term_custom",
        {"term": "order_total", "definition": "SUM(orders.amount)", "layer": "team"},
    )
    assert not saved.isError
    check = await client.call_tool("resolve_business_terms", {"terms": ["order_total"]})
    token = json.loads(check.content[0].text)["definition_check_id"]
    return await client.call_tool("run_sql", {"sql": sql, "definition_check_id": token})


def fixture_db(tmp_path):
    db = tmp_path / "data.db"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE orders (amount INTEGER)")
        conn.executemany("INSERT INTO orders VALUES (?)", [(10,), (20,)])
    return db


@pytest.mark.asyncio
async def test_stdio_schema_sql_memory_and_errors(tmp_path):
    db = fixture_db(tmp_path)
    async with connected(db, tmp_path / "metadata.db") as client:
        tools = (await client.list_tools()).tools
        assert {t.name for t in tools} == {
            "context_candidates",
            "run_sql",
            "explore_schema",
            "term_custom",
            "remember",
            "recall_memory",
            "resolve_business_terms",
        }
        schema = await client.call_tool("explore_schema", {})
        assert not schema.isError and "orders" in schema.content[0].text
        result = await checked_sql(client, "SELECT SUM(amount) AS total FROM orders")
        assert not result.isError and "30" in result.content[0].text
        blocked = await checked_sql(client, "DROP TABLE orders")
        assert blocked.isError and "BLOCKED" in blocked.content[0].text
        malformed = await client.call_tool("run_sql", {"sql": 123})
        assert malformed.isError
        spoof = await client.call_tool("term_custom", {"list": True, "is_admin": True})
        assert spoof.isError
        unknown = await client.call_tool("org_setup", {})
        assert unknown.isError
        await client.call_tool("remember", {"text": "Refunds are excluded."})
        recalled = await client.call_tool("recall_memory", {"query": "refund"})
        assert "Refunds are excluded." in recalled.content[0].text
    with sqlite3.connect(db) as conn:
        assert conn.execute("SELECT SUM(amount) FROM orders").fetchone() == (30,)
    with sqlite3.connect(tmp_path / "metadata.db") as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM audit WHERE action='run_sql'"
            ).fetchone()[0]
            == 1
        )


@pytest.mark.asyncio
async def test_metadata_persists_and_team_scope_is_preserved(tmp_path):
    db = fixture_db(tmp_path)
    store = tmp_path / "metadata.db"
    async with connected(db, store) as client:
        saved = await client.call_tool(
            "term_custom",
            {"term": "net_revenue", "definition": "Exclude refunds", "layer": "team"},
        )
        assert not saved.isError
        denied = await client.call_tool(
            "term_custom",
            {"term": "net_revenue", "definition": "Not allowed", "layer": "org"},
        )
        assert denied.isError
        await client.call_tool("remember", {"text": "Persistent note"})
    async with connected(db, store) as client:
        terms = await client.call_tool("term_custom", {"list": True})
        assert "Exclude refunds" in terms.content[0].text
        notes = await client.call_tool("recall_memory", {"query": "note"})
        assert "Persistent note" in notes.content[0].text
    async with connected(db, store, team="sales") as client:
        terms = await client.call_tool("term_custom", {"list": True})
        assert "Exclude refunds" not in terms.content[0].text


def test_no_connection_does_not_silently_use_stub(monkeypatch):
    for key in [
        "LANG2SQL_DB_URL",
        "CLOUDFLARE_D1_ACCOUNT_ID",
        "CLOUDFLARE_D1_DATABASE_ID",
    ]:
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValueError, match="LANG2SQL_DB_URL"):
        build_tool_context(Identity(user_id="test"))


@pytest.mark.asyncio
async def test_internal_llm_is_disabled():
    with pytest.raises(RuntimeError, match="internal LLM"):
        await ExternalAgentLLM().complete([])


@pytest.mark.asyncio
async def test_missing_definition_never_reaches_database(tmp_path):
    db = fixture_db(tmp_path)
    store = tmp_path / "metadata.db"
    async with connected(db, store) as client:
        direct = await client.call_tool("run_sql", {"sql": "SELECT * FROM orders"})
        assert direct.isError
        check = await client.call_tool(
            "resolve_business_terms", {"terms": ["loyal_customer"]}
        )
        assert check.isError and "DEFINITION_REQUIRED" in check.content[0].text
        assert "context_candidates" in check.content[0].text
        assert "operator must approve" in check.content[0].text
        assert "term_custom" not in check.content[0].text
        guessed = await client.call_tool(
            "run_sql",
            {"sql": "SELECT * FROM orders", "definition_check_id": "fabricated"},
        )
        assert guessed.isError and "NOT executed" in guessed.content[0].text
        empty = await client.call_tool("resolve_business_terms", {"terms": []})
        assert (
            not empty.isError
        )  # No lookup needed for an explicitly specified literal query.
    with sqlite3.connect(store) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM audit WHERE action LIKE 'run_sql%'"
            ).fetchone()[0]
            == 0
        )


@pytest.mark.asyncio
async def test_check_scope_inferred_mutation_and_single_use(tmp_path):
    db = fixture_db(tmp_path)
    store = tmp_path / "metadata.db"
    async with connected(db, store) as client:

        async def save(definition, inferred=False):
            result = await client.call_tool(
                "term_custom",
                {
                    "term": "revenue",
                    "definition": definition,
                    "layer": "team",
                    "inferred": inferred,
                },
            )
            assert not result.isError

        async def resolve():
            return await client.call_tool(
                "resolve_business_terms", {"terms": ["revenue"]}
            )

        async def execute(token):
            return await client.call_tool(
                "run_sql",
                {"sql": "SELECT SUM(amount) FROM orders", "definition_check_id": token},
            )

        await save("SUM(amount)", True)
        assert (await resolve()).isError
        await save("SUM(amount)")
        token = json.loads((await resolve()).content[0].text)["definition_check_id"]
        await save("SUM(amount) - 5")
        assert (await execute(token)).isError
        await save("SUM(amount)")
        token = json.loads((await resolve()).content[0].text)["definition_check_id"]
        result = await execute(token)
        assert not result.isError and "30" in result.content[0].text
        assert (await execute(token)).isError
        token = json.loads((await resolve()).content[0].text)["definition_check_id"]
        assert (
            await client.call_tool(
                "resolve_business_terms", {"terms": ["revenue", "missing"]}
            )
        ).isError
        assert (await execute(token)).isError
    async with connected(db, store) as client:
        assert (
            await client.call_tool(
                "run_sql",
                {"sql": "SELECT * FROM orders", "definition_check_id": token},
            )
        ).isError
        assert not (
            await client.call_tool("resolve_business_terms", {"terms": ["revenue"]})
        ).isError
    async with connected(db, store, team="sales") as client:
        assert (
            await client.call_tool("resolve_business_terms", {"terms": ["revenue"]})
        ).isError


@pytest.mark.asyncio
async def test_memory_survives_stdio_restart_and_isolates_team(tmp_path):
    db = fixture_db(tmp_path)
    store = tmp_path / "metadata.db"
    async with connected(db, store, team="sales") as client:
        result = await client.call_tool(
            "remember", {"text": "October cutoff is October 31"}
        )
        assert not result.isError
    async with connected(db, store, team="sales") as client:
        result = await client.call_tool("recall_memory", {"query": "cutoff"})
        assert "October cutoff is October 31" in result.content[0].text
    async with connected(db, store, team="finance") as client:
        result = await client.call_tool("recall_memory", {"query": "cutoff"})
        assert "No remembered notes" in result.content[0].text


@pytest.mark.asyncio
async def test_literal_query_without_catalog_still_requires_single_use_receipt(
    tmp_path,
):
    db = fixture_db(tmp_path)
    async with connected(db, tmp_path / "metadata.db") as client:
        check = await client.call_tool("resolve_business_terms", {"terms": []})
        assert not check.isError
        token = json.loads(check.content[0].text)["definition_check_id"]
        args = {"sql": "SELECT COUNT(*) AS n FROM orders", "definition_check_id": token}
        result = await client.call_tool("run_sql", args)
        assert not result.isError and "2" in result.content[0].text
        assert (await client.call_tool("run_sql", args)).isError
        missing = await client.call_tool(
            "resolve_business_terms", {"terms": ["undefined_net_revenue"]}
        )
        assert missing.isError and "DEFINITION_REQUIRED" in missing.content[0].text


@pytest.mark.asyncio
async def test_missing_definitions_are_reported_together_and_revoke_receipt(tmp_path):
    async with connected(fixture_db(tmp_path), tmp_path / "metadata.db") as client:
        await client.call_tool("term_custom", {
            "term": "known", "definition": "SUM(orders.amount)", "layer": "team"
        })
        check = await client.call_tool("resolve_business_terms", {"terms": ["known"]})
        token = json.loads(check.content[0].text)["definition_check_id"]
        check = await client.call_tool("resolve_business_terms", {
            "terms": ["missing_a", "known", "missing_b"]
        })
        assert check.isError
        message = check.content[0].text
        assert '["missing_a", "missing_b"]' in message
        assert '"known"' not in message
        rejected = await client.call_tool("run_sql", {
            "sql": "SELECT COUNT(*) FROM orders", "definition_check_id": token
        })
        assert rejected.isError


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [-1, 0])
async def test_invalid_fetch_limit_rejected_before_database_access(tmp_path, limit):
    from unittest.mock import AsyncMock

    from lang2sql.tools.run_sql import RunSQL

    explorer = AsyncMock()
    ctx = build_tool_context(Identity(user_id="analyst", guild_id="demo"), store_path=":memory:", explorer=explorer)
    try:
        result = await RunSQL().run({"sql": "SELECT 1", "limit": limit}, ctx)
        assert result.is_error
        assert "positive maximum row count" in result.content
        explorer.execute.assert_not_called()
        assert RunSQL().spec.parameters["properties"]["limit"]["minimum"] == 1
    finally:
        ctx.store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("count,limit,truncated", [(3, 2, True), (2, 2, False), (1, 2, False), (0, 2, False), (65, 100, False)])
async def test_sql_result_reports_fetch_and_display_coverage(tmp_path, count, limit, truncated):
    db = fixture_db(tmp_path)
    with sqlite3.connect(db) as conn:
        conn.execute("DELETE FROM orders")
        conn.executemany("INSERT INTO orders VALUES (?)", [(n,) for n in range(count)])
    async with connected(db, tmp_path / "metadata.db") as client:
        check = await client.call_tool("resolve_business_terms", {"terms": []})
        result = await client.call_tool("run_sql", {
            "sql": "SELECT amount FROM orders ORDER BY amount LIMIT -1",
            "limit": limit,
            "definition_check_id": json.loads(check.content[0].text)["definition_check_id"],
        })
        assert not result.isError
        text = result.content[0].text
        assert f"fetch_truncated={str(truncated).lower()}" in text
        assert f"fetched_rows={min(count, limit)}" in text
        assert f"displayed_rows={min(count, limit, 50)}" in text
        assert f"display_truncated={str(min(count, limit) > 50).lower()}" in text
        assert "rows total" not in text
        assert "SQL: SELECT amount FROM orders ORDER BY amount LIMIT -1" in text
        if truncated:
            assert f"\n{limit}\n" not in text  # The lookahead row is not exposed.


@pytest.mark.asyncio
async def test_lookahead_does_not_increase_safety_sql_limit(tmp_path):
    async with connected(fixture_db(tmp_path), tmp_path / "metadata.db") as client:
        check = await client.call_tool("resolve_business_terms", {"terms": []})
        result = await client.call_tool("run_sql", {
            "sql": "SELECT amount FROM orders ORDER BY amount",
            "limit": 1,
            "definition_check_id": json.loads(check.content[0].text)["definition_check_id"],
        })
        assert not result.isError
        text = result.content[0].text
        assert "fetched_rows=1" in text and "sql_rewritten=true" in text
        assert "fetch_truncated=false" in text
        assert text.endswith("SQL: SELECT amount FROM orders ORDER BY amount\nLIMIT 1")
        assert "\n20\n" not in text


@pytest.mark.asyncio
async def test_duplicate_result_columns_are_not_silently_overwritten(tmp_path):
    async with connected(fixture_db(tmp_path), tmp_path / "metadata.db") as client:
        for suffix in ("", " WHERE 0"):
            result = await checked_sql(client, "SELECT amount AS value, amount + 1 AS value FROM orders" + suffix)
            assert result.isError
            assert "unique column aliases" in result.content[0].text
        result = await checked_sql(client, "SELECT amount AS original, amount + 1 AS incremented FROM orders")
        assert not result.isError
        assert "original | incremented" in result.content[0].text
        assert "10 | 11" in result.content[0].text

@pytest.mark.asyncio
@pytest.mark.parametrize(
    'sql, expected, error, fragment',
    [
        ('SELECT amount FROM orders', ['amount'], False, 'Output columns validated'),
        ('SELECT amount FROM orders', ['amount', 'growth'], True, 'missing'),
        ('SELECT amount, 1 AS extra FROM orders', ['amount'], True, 'unexpected'),
        ('SELECT amount FROM orders WHERE 0', ['amount'], False, 'Output columns validated'),
        ('SELECT amount FROM orders WHERE 0', ['growth'], True, 'missing'),
        ('SELECT amount, 1 AS extra FROM orders', ['extra', 'amount'], True, 'order'),
    ],
)
async def test_output_columns_contract_stdio(tmp_path, sql, expected, error, fragment):
    async with connected(fixture_db(tmp_path), tmp_path / 'metadata.db') as client:
        check = await client.call_tool('resolve_business_terms', {'terms': []})
        token = json.loads(check.content[0].text)['definition_check_id']
        result = await client.call_tool('run_sql', {
            'sql': sql, 'definition_check_id': token, 'expected_columns': expected,
        })
        assert result.isError == error
        assert fragment in result.content[0].text
        if error:
            assert 'Output column contract failed' in result.content[0].text

@pytest.mark.asyncio
async def test_output_contract_legacy_empty_result_is_not_verified():
    from lang2sql.tools.run_sql import RunSQL

    class LegacyExplorer:
        async def execute(self, sql, limit):
            return []

    ctx = build_tool_context(Identity(user_id='analyst'), explorer=LegacyExplorer())
    try:
        result = await RunSQL().run({'sql': 'SELECT 1 AS n', 'expected_columns': ['n']}, ctx)
        assert result.is_error and 'metadata unavailable' in result.content
        unchecked = await RunSQL().run({'sql': 'SELECT 1 AS n'}, ctx)
        assert not unchecked.is_error
    finally:
        ctx.store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize('expected', [None, [], ['n', 'n'], [''], [1], 'n', [{}]])
async def test_invalid_output_contract_never_executes(expected):
    from lang2sql.tools.run_sql import RunSQL

    class NeverExecute:
        async def execute(self, sql, limit):
            pytest.fail('Invalid contract reached database')

    ctx = build_tool_context(Identity(user_id='analyst'), explorer=NeverExecute())
    try:
        result = await RunSQL().run({'sql': 'SELECT 1 AS n', 'expected_columns': expected}, ctx)
        assert result.is_error and 'NOT executed' in result.content
    finally:
        ctx.store.close()
