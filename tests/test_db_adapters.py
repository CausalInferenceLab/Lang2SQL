"""DB explorer adapters: factory routing, SQLAlchemy (real sqlite), D1 (mocked).

The SQLAlchemy explorer is exercised against a real on-disk SQLite DB (a
SQLAlchemy dialect that ships with Python), proving introspection + execute end
to end. The D1 explorer uses an injected transport so its request/response
handling is tested with no network.
"""

from __future__ import annotations

import asyncio

import pytest

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


def test_run_sql_enforces_safety_deadline_and_reuses_connection(tmp_path):
    from lang2sql.core.identity import Identity
    from lang2sql.safety.pipeline import SafetyPipeline
    from lang2sql.tenancy.tool_runtime import build_tool_context
    from lang2sql.tools.run_sql import RunSQL

    class ShortDeadline:
        def evaluate(self, sql, context):
            context.timeout_seconds = 0.01
            return SafetyPipeline().evaluate(sql, context)

    explorer = SqlAlchemyExplorer(f"sqlite:///{tmp_path / 'timeout.db'}")
    ctx = build_tool_context(Identity(user_id="test"), store_path=":memory:", explorer=explorer)
    ctx.safety = ShortDeadline()
    query = "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000000) SELECT SUM(x) AS total FROM n"
    try:
        with pytest.raises(TimeoutError, match="deadline"):
            asyncio.run(RunSQL().run({"sql": query}, ctx))
        # The timed-out progress handler must not leak into the pooled connection.
        assert asyncio.run(explorer.execute(query.replace("2000000", "20000"))) == [{"total": 200010000}]
        result = asyncio.run(RunSQL().run({"sql": "SELECT 42 AS value"}, ctx))
        assert not result.is_error and "42" in result.content
        assert "query_timeout_enforced=true" in result.content
        assert "query_timeout_scope=sqlite_vm" in result.content
    finally:
        ctx.store.close()
        explorer._get_engine().dispose()


def test_sqlite_deadline_covers_fetch_and_preserves_other_errors(tmp_path):
    from sqlalchemy.exc import OperationalError

    explorer = SqlAlchemyExplorer(f"sqlite:///{tmp_path / 'fetch.db'}")
    query = "WITH RECURSIVE n(x) AS (VALUES(1) UNION ALL SELECT x+1 FROM n WHERE x<2000000) SELECT x FROM n"
    try:
        assert asyncio.run(explorer.execute_with_timeout(query, 1, 0.01)) == [{"x": 1}]
        with pytest.raises(TimeoutError, match="deadline"):
            asyncio.run(explorer.execute_with_timeout(query, 2000000, 0.01))
        with pytest.raises(OperationalError, match="syntax"):
            asyncio.run(explorer.execute_with_timeout("SELECT FROM", 1, 1))
        assert asyncio.run(explorer.execute(query.replace("2000000", "20000"), 20000))[-1] == {"x": 20000}
    finally:
        explorer._get_engine().dispose()


def test_legacy_explorer_reports_timeout_not_enforced():
    from lang2sql.adapters.db.postgres_explorer import PostgresExplorer
    from lang2sql.core.identity import Identity
    from lang2sql.tenancy.tool_runtime import build_tool_context
    from lang2sql.tools.run_sql import RunSQL

    ctx = build_tool_context(Identity(user_id="test"), store_path=":memory:", explorer=PostgresExplorer("stub"))
    try:
        result = asyncio.run(RunSQL().run({"sql": "SELECT 1"}, ctx))
        assert not result.is_error
        assert "query_timeout_enforced=false" in result.content
        assert "query_timeout_scope=unsupported" in result.content
    finally:
        ctx.store.close()


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
async def test_view_discovery_and_qualified_columns(tmp_path):
    from sqlalchemy import create_engine, text

    db = tmp_path / "views.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (amount INTEGER)"))
        conn.execute(text("CREATE VIEW net_sales AS SELECT amount FROM orders"))
    exp = SqlAlchemyExplorer(f"sqlite:///{db}", schema="main")
    objects = {t.name: t for t in await exp.list_tables()}
    assert objects["orders"].kind == "table"
    assert objects["net_sales"].kind == "view"
    view = await exp.describe_table("main.net_sales")
    assert view.kind == "view" and [c.name for c in view.columns] == ["amount"]


@pytest.mark.asyncio
async def test_composite_keys_reach_schema_tool_without_guessing(tmp_path):
    from sqlalchemy import create_engine, text

    from lang2sql.core.identity import Identity
    from lang2sql.tenancy.concierge import ContextConcierge
    from lang2sql.tools.explore_schema import ExploreSchema

    db = tmp_path / "keys.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as conn:
        conn.execute(
            text(
                "CREATE TABLE parent (tenant TEXT, id INTEGER, PRIMARY KEY (tenant, id))"
            )
        )
        conn.execute(
            text(
                "CREATE TABLE child (tenant TEXT, parent_id INTEGER, FOREIGN KEY (tenant, parent_id) REFERENCES parent (tenant, id))"
            )
        )
        conn.execute(text("CREATE VIEW child_view AS SELECT * FROM child"))
    exp = SqlAlchemyExplorer(f"sqlite:///{db}")
    assert (await exp.describe_table("parent")).primary_key == ["tenant", "id"]
    child = await exp.describe_table("child")
    assert child.foreign_keys[0].columns == ["tenant", "parent_id"]
    assert child.foreign_keys[0].referred_columns == ["tenant", "id"]
    assert (await exp.describe_table("child_view")).foreign_keys == []
    concierge = ContextConcierge(explorer=exp)
    try:
        ctx = await concierge.build_context(Identity(user_id="test"))
        result = await ExploreSchema().run({"table": "child"}, ctx)
        assert (
            "FOREIGN KEY (tenant, parent_id) REFERENCES parent (tenant, id)"
            in result.content
        )
        result = await ExploreSchema().run({"table": "parent"}, ctx)
        assert "PRIMARY KEY (tenant, id)" in result.content
    finally:
        concierge.store.close()
        engine.dispose()
        exp._engine.dispose()
