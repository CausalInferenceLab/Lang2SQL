from __future__ import annotations

import asyncio

import pytest

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.core.identity import Identity
from lang2sql.core.ports.explorer import QueryTimedOutError
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.tenancy.concierge import ContextConcierge

duckdb = pytest.importorskip("duckdb")


def _seed_duckdb(path: str) -> None:
    connection = duckdb.connect(path)
    try:
        connection.execute(
            "CREATE TABLE regions (region_id INTEGER PRIMARY KEY, name VARCHAR NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE orders ("
            "order_id INTEGER PRIMARY KEY, "
            "region_id INTEGER REFERENCES regions(region_id), "
            "amount DECIMAL(12,2) NOT NULL, status VARCHAR NOT NULL)"
        )
        connection.execute("INSERT INTO regions VALUES (1, 'North'), (2, 'South')")
        connection.execute(
            "INSERT INTO orders VALUES "
            "(1, 1, 10, 'paid'), (2, 1, 20, 'paid'), (3, 1, 30, 'pending'), "
            "(4, 2, 40, 'paid'), (5, 2, 50, 'pending'), (6, NULL, 60, 'paid')"
        )
        connection.commit()
    finally:
        connection.close()


def test_discord_activation_failure_releases_inspection_connection(
    tmp_path, monkeypatch
) -> None:
    path = tmp_path / "activation-race.duckdb"
    _seed_duckdb(str(path))
    concierge = ContextConcierge()
    handlers = CommandHandlers(concierge)
    identity = Identity(
        user_id="steward", guild_id="g1", channel_id="c1", is_admin=True
    )

    def fail_activation(**_kwargs) -> None:
        raise RuntimeError("forced activation race")

    monkeypatch.setattr(concierge, "activate_connection", fail_activation)
    result = asyncio.run(
        handlers.register_db_for_guild(identity, "duckdb", {"path": str(path)})
    )
    assert "원자적으로 활성화하지 못했습니다" in result.text

    # The failed Discord setup must not leave a read-only DuckDB configuration
    # pinned in-process; an operator can immediately reopen the file read-write.
    connection = duckdb.connect(str(path))
    connection.close()


def test_duckdb_file_adapter_is_read_only_hardened_and_bound(tmp_path) -> None:
    path = tmp_path / "warehouse.duckdb"
    _seed_duckdb(str(path))
    explorer = SqlAlchemyExplorer(f"duckdb:///{path}")

    assert explorer.governed_execution_supported() is True
    tables = asyncio.run(explorer.list_tables())
    assert {item.name for item in tables} == {"orders", "regions"}
    described = asyncio.run(explorer.describe_table("orders"))
    assert {item.name for item in described.columns} >= {"amount", "status"}
    metadata = asyncio.run(explorer.catalog_metadata())
    assert metadata["tables"]["orders"]["primary_key"] == ["order_id"]
    assert metadata["tables"]["orders"]["foreign_keys"][0]["columns"] == ["region_id"]

    rows = asyncio.run(
        explorer.execute(
            'SELECT amount FROM "orders" WHERE status = :p0 ORDER BY amount',
            parameters={"p0": "paid"},
        )
    )
    assert [str(item["amount"]) for item in rows] == [
        "10.00",
        "20.00",
        "40.00",
        "60.00",
    ]
    settings = asyncio.run(
        explorer.execute(
            "SELECT current_setting('enable_external_access') AS external_access"
        )
    )
    assert settings == [{"external_access": False}]
    with pytest.raises(Exception):
        asyncio.run(explorer.execute("CREATE TABLE forbidden_write (id INTEGER)"))


def test_duckdb_timeout_interrupts_and_connection_is_reusable(tmp_path) -> None:
    path = tmp_path / "timeout.duckdb"
    _seed_duckdb(str(path))
    explorer = SqlAlchemyExplorer(f"duckdb:///{path}")

    with pytest.raises(QueryTimedOutError):
        asyncio.run(
            explorer.execute(
                "SELECT SUM(a.i * b.i) FROM range(1000000) a(i), "
                "range(1000000) b(i)",
                timeout_seconds=0.01,
            )
        )
    assert asyncio.run(explorer.execute("SELECT 42 AS answer")) == [{"answer": 42}]


def test_cancelled_duckdb_query_has_no_orphan_error_and_is_reusable(tmp_path) -> None:
    path = tmp_path / "cancel.duckdb"
    _seed_duckdb(str(path))
    explorer = SqlAlchemyExplorer(f"duckdb:///{path}")

    async def scenario() -> tuple[list[dict], list[dict[str, object]]]:
        loop = asyncio.get_running_loop()
        orphan_errors: list[dict[str, object]] = []
        prior_handler = loop.get_exception_handler()
        loop.set_exception_handler(lambda _loop, context: orphan_errors.append(context))
        try:
            task = asyncio.create_task(
                explorer.execute(
                    "SELECT SUM(a.i * b.i) FROM range(1000000) a(i), "
                    "range(1000000) b(i)",
                    timeout_seconds=30,
                )
            )
            await asyncio.sleep(0.01)
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            rows = await explorer.execute("SELECT 42 AS answer")
            await asyncio.sleep(0)
            return rows, orphan_errors
        finally:
            loop.set_exception_handler(prior_handler)

    rows, orphan_errors = asyncio.run(scenario())
    assert rows == [{"answer": 42}]
    assert orphan_errors == []


def test_duckdb_in_memory_and_missing_files_never_enable_governed_execution(
    tmp_path,
) -> None:
    memory = SqlAlchemyExplorer("duckdb:///:memory:")
    assert memory.governed_execution_supported() is False
    missing = SqlAlchemyExplorer(f"duckdb:///{tmp_path / 'missing.duckdb'}")
    assert missing.governed_execution_supported() is False
    with pytest.raises(FileNotFoundError):
        asyncio.run(missing.list_tables())
