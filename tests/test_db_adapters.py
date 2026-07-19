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
