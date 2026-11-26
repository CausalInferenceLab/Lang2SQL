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
            "calendar_interval_sql",
            "fixed_decimal_sql",
            "integer_ratio_sql",
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
        else:
            from bench.accuracy.codex_mcp import executed_sql

            events = [{'type': 'item.completed', 'item': {
                'type': 'mcp_tool_call', 'tool': 'run_sql', 'server': 'lang2sql',
                'status': 'completed', 'arguments': {'sql': sql},
                'result': {'content': [{'text': result.content[0].text}]},
            }}]
            assert executed_sql('```sql\n' + sql + '\n```', events) == sql + '\nLIMIT 1000'

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
        assert any(e.action == 'run_sql_contract_failed' for e in await ctx.store.query('analyst'))
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


@pytest.mark.asyncio
async def test_calendar_compiler_through_stdio_and_sql_guard(tmp_path):
    from lang2sql.tools.calendar_interval_sql import CALENDAR_CTES

    async with connected(fixture_db(tmp_path), tmp_path / "meta.db") as client:
        compiled = await client.call_tool("calendar_interval_sql", {"dialect": "sqlite"})
        assert not compiled.isError
        assert CALENDAR_CTES in compiled.content[0].text
        check = await client.call_tool("resolve_business_terms", {"terms": []})
        token = json.loads(check.content[0].text)["definition_check_id"]
        sql = (
            "WITH calendar_input(row_id, start_date, end_date) AS "
            "(VALUES (1, '2020-02-29', '2021-02-28')), " + CALENDAR_CTES
            + " SELECT years, months, days FROM calendar_interval"
        )
        result = await client.call_tool("run_sql", {"sql": sql, "definition_check_id": token})
        assert not result.isError
        assert "1 | 0 | 0" in result.content[0].text
        denied = await client.call_tool("run_sql", {"sql": sql})
        assert denied.isError


@pytest.mark.asyncio
async def test_fixed_decimal_compiler_stdio_execution(tmp_path):
    from lang2sql.tools.fixed_decimal_sql import fixed_decimal_ctes

    async with connected(fixture_db(tmp_path), tmp_path / "meta.db") as client:
        compiled = await client.call_tool("fixed_decimal_sql", {
            "dialect": "sqlite", "input_scale": 1, "output_scale": 1, "aggregate": "avg"
        })
        assert not compiled.isError
        ctes = fixed_decimal_ctes(1, 1, "avg")
        assert ctes in compiled.content[0].text
        check = await client.call_tool("resolve_business_terms", {"terms": []})
        token = json.loads(check.content[0].text)["definition_check_id"]
        sql = (
            "WITH decimal_input(group_id, scaled_value) AS (VALUES ('a', 11), ('a', 12)), "
            + ctes + " SELECT status, rounded_value FROM decimal_result"
        )
        result = await client.call_tool("run_sql", {"sql": sql, "definition_check_id": token})
        assert not result.isError
        assert "ok | 1.2" in result.content[0].text


@pytest.mark.asyncio
@pytest.mark.parametrize('sql, expected, limit, error, fragment', [
    ('SELECT SUM(amount) AS value FROM orders', 1, 1000, False, '1 row(s):'),
    ('SELECT amount AS value FROM orders LIMIT -1', 1, 1000, True, 'expected=1; observed=2'),
    ('SELECT amount AS value FROM orders WHERE 0', 0, 1000, False, '(0 rows)'),
    ('SELECT amount AS value FROM orders LIMIT -1', 1, 1, True, 'observed=at least 2'),
    ('SELECT amount AS value FROM orders LIMIT -1', 2, 2, False, '2 row(s):'),
    ('WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM n WHERE value<60) SELECT value FROM n LIMIT -1', 60, 100, False, 'displayed_rows=50'),
    ('WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value+1 FROM n WHERE value<60) SELECT value FROM n LIMIT -1', 50, 50, True, 'observed=at least 51'),
])
async def test_output_cardinality_stdio(tmp_path, sql, expected, limit, error, fragment):
    from bench.accuracy.codex_mcp import executed_sql

    async with connected(fixture_db(tmp_path), tmp_path / 'metadata.db') as client:
        check = await client.call_tool('resolve_business_terms', {'terms': []})
        result = await client.call_tool('run_sql', {
            'definition_check_id': json.loads(check.content[0].text)['definition_check_id'],
            'sql': sql, 'expected_columns': ['value'], 'expected_rows': expected, 'limit': limit,
        })
        text = result.content[0].text
        assert result.isError == error
        assert fragment in text
        events = [{'type': 'item.completed', 'item': {
            'type': 'mcp_tool_call', 'tool': 'run_sql', 'server': 'lang2sql',
            'status': 'completed', 'arguments': {'sql': sql},
            'result': {'content': [{'text': text}]},
        }}]
        if error:
            assert text.startswith('Output row count contract failed:')
            with pytest.raises(ValueError, match='not executed through'):
                executed_sql('```sql\n' + sql + '\n```', events)
        else:
            assert 'Output columns validated' in text
            assert 'Output row count validated (executed SQL only).' in text
            assert executed_sql('```sql\n' + sql + '\n```', events) == text.rsplit('\nSQL: ', 1)[1]


@pytest.mark.asyncio
@pytest.mark.parametrize('args', [
    {'expected_rows': None}, {'expected_rows': True}, {'expected_rows': False},
    {'expected_rows': -1}, {'expected_rows': 1.0}, {'expected_rows': '1'},
    {'expected_rows': []}, {'expected_rows': 2, 'limit': 1},
])
async def test_invalid_cardinality_never_executes(args):
    from lang2sql.tools.run_sql import RunSQL

    class NeverExecute:
        async def execute(self, sql, limit):
            pytest.fail('Invalid cardinality contract reached database')

    ctx = build_tool_context(Identity(user_id='analyst'), explorer=NeverExecute())
    try:
        result = await RunSQL().run({'sql': 'SELECT 1 AS n', **args}, ctx)
        assert result.is_error and 'NOT executed' in result.content
    finally:
        ctx.store.close()


@pytest.mark.asyncio
async def test_cardinality_failure_is_audited_without_changing_sql():
    from lang2sql.tools.run_sql import RunSQL

    class TwoRows:
        async def execute(self, sql, limit):
            assert sql == 'SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT -1'
            return [{'n': 1}, {'n': 2}]

    ctx = build_tool_context(Identity(user_id='analyst'), explorer=TwoRows())
    try:
        result = await RunSQL().run({
            'sql': 'SELECT 1 AS n UNION ALL SELECT 2 AS n LIMIT -1', 'expected_rows': 1,
        }, ctx)
        assert result.is_error
        assert '2 row(s):' in result.content  # No truncation to the declared one-row target.
        events = await ctx.store.query('analyst')
        assert any(e.action == 'run_sql_contract_failed' and 'expected=1' in e.detail['contract_error'] for e in events)
    finally:
        ctx.store.close()


@pytest.mark.asyncio
async def test_integer_ratio_compiler_executes_via_stdio(tmp_path):
    from lang2sql.tools.integer_ratio_sql import integer_ratio_ctes

    async with connected(fixture_db(tmp_path), tmp_path / "metadata.db") as client:
        compiled = await client.call_tool("integer_ratio_sql", {"dialect": "sqlite", "multiplier": 100})
        assert not compiled.isError
        ctes = integer_ratio_ctes(100)
        assert ctes in compiled.content[0].text
        check = await client.call_tool("resolve_business_terms", {"terms": []})
        result = await client.call_tool("run_sql", {
            "definition_check_id": json.loads(check.content[0].text)["definition_check_id"],
            "sql": "WITH ratio_input AS (SELECT 1 AS group_id, 1.0 AS numerator, 3.0 AS denominator), "
                   + ctes + " SELECT status, ratio_value FROM ratio_result",
        })
        assert not result.isError
        assert "ok | 33.333333333333336" in result.content[0].text
