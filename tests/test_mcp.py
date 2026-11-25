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
        guessed = await client.call_tool(
            "run_sql",
            {"sql": "SELECT * FROM orders", "definition_check_id": "fabricated"},
        )
        assert guessed.isError and "NOT executed" in guessed.content[0].text
        empty = await client.call_tool("resolve_business_terms", {"terms": []})
        assert empty.isError
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
