"""DB explorer adapters: factory routing, SQLAlchemy (real sqlite), D1 (mocked).

The SQLAlchemy explorer is exercised against a real on-disk SQLite DB (a
SQLAlchemy dialect that ships with Python), proving introspection + execute end
to end. The D1 explorer uses an injected transport so its request/response
handling is tested with no network.
"""

from __future__ import annotations

import asyncio
import sqlite3

import pytest

from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context

from lang2sql.adapters.db import (
    D1Explorer,
    SqlAlchemyExplorer,
    build_explorer,
    explorer_from_env,
)

# --- factory routing -------------------------------------------------------


def test_factory_routes_d1():
    exp = build_explorer("d1://acct123/db456")
    assert isinstance(exp, D1Explorer)
    assert exp.account_id == "acct123" and exp.database_id == "db456"


def test_factory_routes_sqlalchemy_without_connecting():
    # No psycopg installed here; routing must not import a driver or connect.
    exp = build_explorer("postgresql+psycopg://u:p@h/db")
    assert isinstance(exp, SqlAlchemyExplorer)
    assert exp._engine is None  # lazy: not built yet


def test_factory_rejects_empty_and_schemeless():
    for bad in ["", "   ", "just-a-name"]:
        with pytest.raises(ValueError):
            build_explorer(bad)


def test_factory_bad_d1_url():
    with pytest.raises(ValueError):
        build_explorer("d1://account-only")


def test_explorer_from_env(monkeypatch):
    monkeypatch.delenv("LANG2SQL_DB_URL", raising=False)
    monkeypatch.delenv("CLOUDFLARE_D1_ACCOUNT_ID", raising=False)
    monkeypatch.delenv("CLOUDFLARE_D1_DATABASE_ID", raising=False)
    assert explorer_from_env() is None
    monkeypatch.setenv("LANG2SQL_DB_URL", "duckdb:///:memory:")
    assert isinstance(explorer_from_env(), SqlAlchemyExplorer)
    monkeypatch.delenv("LANG2SQL_DB_URL")
    monkeypatch.setenv("CLOUDFLARE_D1_ACCOUNT_ID", "a")
    monkeypatch.setenv("CLOUDFLARE_D1_DATABASE_ID", "b")
    assert isinstance(explorer_from_env(), D1Explorer)


# --- SQLAlchemy explorer against real SQLite -------------------------------


def _seed_sqlite(path: str) -> None:
    from sqlalchemy import create_engine, text

    eng = create_engine(f"sqlite:///{path}")
    with eng.begin() as conn:
        conn.execute(
            text("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)")
        )
        conn.execute(
            text("INSERT INTO users (id, email) VALUES (1, 'a@x.com'), (2, 'b@x.com')")
        )


def test_sqlalchemy_explorer_introspect_and_execute(tmp_path):
    db = tmp_path / "demo.db"
    _seed_sqlite(str(db))
    exp = SqlAlchemyExplorer(f"sqlite:///{db}")

    tables = asyncio.run(exp.list_tables())
    assert "users" in {t.name for t in tables}

    desc = asyncio.run(exp.describe_table("users"))
    cols = {c.name: c for c in desc.columns}
    assert set(cols) == {"id", "email"}
    assert cols["email"].nullable is False

    rows = asyncio.run(exp.execute("SELECT email FROM users ORDER BY id"))
    assert rows == [{"email": "a@x.com"}, {"email": "b@x.com"}]

    sample = asyncio.run(exp.sample_rows("users", limit=1))
    assert len(sample) == 1


def test_sqlalchemy_explorer_describes_names_in_its_configured_schema(tmp_path):
    db = tmp_path / "demo.db"
    _seed_sqlite(str(db))
    exp = SqlAlchemyExplorer(f"sqlite:///{db}", schema="main")

    qualified = asyncio.run(exp.describe_table("main.users"))
    bare = asyncio.run(exp.describe_table("users"))

    assert qualified == bare
    assert qualified.qualified == "main.users"
    assert [column.name for column in qualified.columns] == ["id", "email"]


# --- D1 explorer with mocked HTTP transport --------------------------------


def _d1_transport(sql, params):
    """Fake the D1 HTTP API: shape responses by the SQL it receives."""
    s = sql.lower()
    if "sqlite_master" in s:
        results = [{"name": "orders"}, {"name": "users"}]
    elif "pragma table_info" in s:
        results = [
            {
                "cid": 0,
                "name": "id",
                "type": "INTEGER",
                "notnull": 1,
                "dflt_value": None,
                "pk": 1,
            },
            {
                "cid": 1,
                "name": "amount",
                "type": "REAL",
                "notnull": 0,
                "dflt_value": None,
                "pk": 0,
            },
        ]
    else:
        results = [{"id": 1, "amount": 9.5}]
    return {
        "success": True,
        "result": [{"results": results, "success": True}],
        "errors": [],
    }


def test_d1_list_describe_execute():
    exp = D1Explorer("acct", "db", token="t", transport=_d1_transport)

    tables = asyncio.run(exp.list_tables())
    assert {t.name for t in tables} == {"orders", "users"}

    desc = asyncio.run(exp.describe_table("orders"))
    cols = {c.name: c for c in desc.columns}
    assert cols["id"].nullable is False and cols["amount"].nullable is True

    rows = asyncio.run(exp.execute("SELECT * FROM orders"))
    assert rows == [{"id": 1, "amount": 9.5}]


def test_d1_raises_on_api_error():
    def failing(sql, params):
        return {"success": False, "errors": [{"message": "bad token"}], "result": []}

    exp = D1Explorer("acct", "db", token="t", transport=failing)
    with pytest.raises(RuntimeError, match="D1 query failed"):
        asyncio.run(exp.execute("SELECT 1"))


def test_d1_rejects_unsafe_identifier():
    exp = D1Explorer("acct", "db", token="t", transport=_d1_transport)
    with pytest.raises(ValueError, match="unsafe table identifier"):
        asyncio.run(exp.describe_table("users; DROP TABLE x"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "sql",
    [
        "SELECT '(' AS label LIMIT 1;",
        "SELECT 1 AS value; -- sample",
        "SELECT ';' AS separator LIMIT 1",
        """SELECT 'it''s -- ; /* ( LIMIT 2' AS "a"";b"; /* tail */""",
        "SELECT 3 AS [);]; -- (",
        "SELECT 2 AS `(;)` -- LIMIT 1",
        "SELECT ':paid' AS status",
    ],
)
async def test_run_sql_preserves_sqlite_results(tmp_path, sql):
    db = tmp_path / "data.db"
    with sqlite3.connect(db) as conn:
        cursor = conn.execute(sql)
        header = cursor.description[0][0]
        (value,) = cursor.fetchone()
    ctx = build_tool_context(
        Identity(user_id="test"), explorer=SqlAlchemyExplorer(f"sqlite:///{db}")
    )
    result = await ctx.tools.dispatch("run_sql", {"sql": sql}, ctx, "query")
    assert not result.is_error
    assert result.content.startswith(f"1 row(s):\n{header}\n---\n{value}\nSQL:")


@pytest.mark.asyncio
async def test_run_sql_caps_rows_and_blocks_before_db(tmp_path, monkeypatch):
    db = SqlAlchemyExplorer(f"sqlite:///{tmp_path / 'data.db'}")
    ctx = build_tool_context(Identity(user_id="test"), explorer=db)
    sql = "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n; -- tail"
    result = await ctx.tools.dispatch("run_sql", {"sql": sql, "limit": 1}, ctx, "cap")
    capped = "SELECT 1 AS n UNION ALL SELECT 2 ORDER BY n\nLIMIT 1; -- tail"
    assert (
        result.content
        == f"1 row(s) fetched (row limit of 1 reached):\nn\n---\n1\nSQL: {capped}"
    )

    async def unexpected_execute(*args):
        pytest.fail("blocked SQL reached the DB")

    monkeypatch.setattr(db, "execute", unexpected_execute)
    for sql in ("SELECT ';'; /* split */ SELECT 2", "DELETE FROM orders"):
        result = await ctx.tools.dispatch("run_sql", {"sql": sql}, ctx, "blocked")
        assert result.is_error and "BLOCKED" in result.content


@pytest.mark.asyncio
@pytest.mark.parametrize("suffix", ["", " LIMIT 1200"])
async def test_fetched_rows_report_driver_cap_and_actual_sql(tmp_path, suffix):
    path = tmp_path / "amounts.db"
    with sqlite3.connect(path) as connection:
        connection.execute("CREATE TABLE amounts(n INTEGER)")
        connection.executemany(
            "INSERT INTO amounts VALUES (?)", ((n,) for n in range(1500))
        )
    ctx = build_tool_context(
        Identity(user_id="test"), explorer=SqlAlchemyExplorer(f"sqlite:///{path}")
    )
    sql = "SELECT n FROM amounts" + suffix
    result = await ctx.tools.dispatch("run_sql", {"sql": sql}, ctx, "rows")
    assert not result.is_error
    assert "1000 row(s) fetched (row limit of 1000 reached)" in result.content
    assert (
        "showing the first 50 rows" in result.content
        and "rows total" not in result.content
    )
    assert result.content.endswith("SQL: " + (sql if suffix else sql + "\nLIMIT 1000"))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("dialect", "sql", "message"),
    [
        ("postgresql", "EXPLAIN ANALYZE SELECT 1", "underlying SELECT"),
        ("mysql", "SELECT 1", "unavailable for this dialect"),
    ],
)
async def test_review_never_falls_back_to_executing_sql(
    monkeypatch, dialect, sql, message
):
    explorer = SqlAlchemyExplorer(f"{dialect}://")

    async def unexpected(*args, **kwargs):
        pytest.fail("review attempted to execute the draft")

    monkeypatch.setattr(explorer, "execute", unexpected)
    ctx = build_tool_context(Identity(user_id="test"), explorer=explorer)
    result = await ctx.tools.dispatch(
        "review_sql",
        {"sql": sql, "context": "Return SQL for the constant one."},
        ctx,
        "review",
    )
    assert not result.is_error and message in result.content
