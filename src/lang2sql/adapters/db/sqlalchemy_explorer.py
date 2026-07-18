"""Generic SQLAlchemy explorer — one adapter, many engines.

A single :class:`ExplorerPort` implementation that connects to anything
SQLAlchemy speaks (PostgreSQL, MySQL, Snowflake, BigQuery, DuckDB, SQLite, …)
purely from a connection URL. This is the "사용성" win: adding a new warehouse is
``pip install <driver>`` + a DSN, not a new adapter class.

The engine is created lazily on first use so constructing the explorer (and
routing to it in the factory) never imports a driver that isn't installed.
Blocking DB calls run in a worker thread to keep the async event loop free.
"""

from __future__ import annotations

import asyncio
from typing import Any

from ...core.ports.explorer import Column, Table


class SqlAlchemyExplorer:
    """ExplorerPort over a SQLAlchemy Engine, built from a connection URL."""

    def __init__(self, url: str, *, schema: str | None = None) -> None:
        self.url = url
        self._schema = schema
        self._engine: Any = None  # created lazily

    def _get_engine(self) -> Any:
        if self._engine is None:
            from sqlalchemy import create_engine  # imported here = lazy driver load

            self._engine = create_engine(self.url)
        return self._engine

    # --- ExplorerPort ----------------------------------------------------

    async def list_tables(self) -> list[Table]:
        return await asyncio.to_thread(self._list_tables_sync)

    async def describe_table(self, name: str) -> Table:
        return await asyncio.to_thread(self._describe_table_sync, name)

    async def sample_rows(self, name: str, limit: int = 5) -> list[dict]:
        # Bind the limit; quote the identifier via the dialect's preparer.
        eng = self._get_engine()
        qname = eng.dialect.identifier_preparer.quote(name)
        return await self.execute(f"SELECT * FROM {qname}", limit=limit)

    async def execute(self, sql: str, limit: int = 1000) -> list[dict]:
        return await asyncio.to_thread(self._execute_sync, sql, int(limit))

    # --- sync workers ----------------------------------------------------

    def _list_tables_sync(self) -> list[Table]:
        from sqlalchemy import inspect

        engine = self._get_engine()
        engine.dispose()  # flush stale pool connections so schema changes are visible
        insp = inspect(engine)
        default = insp.default_schema_name
        effective = self._schema or default
        # Omit schema when it's the connection default so SQL stays unqualified.
        display_schema = (
            "" if (not self._schema or self._schema == default) else effective
        )
        return [
            Table(name=t, schema=display_schema)
            for t in insp.get_table_names(schema=self._schema)
        ]

    def _describe_table_sync(self, name: str) -> Table:
        from sqlalchemy import inspect

        insp = inspect(self._get_engine())
        cols = [
            Column(
                name=c["name"],
                type=str(c["type"]),
                nullable=bool(c.get("nullable", True)),
                description=c.get("comment") or "",
            )
            for c in insp.get_columns(name, schema=self._schema)
        ]
        return Table(name=name, schema=self._schema or "", columns=cols)

    def _execute_sync(self, sql: str, limit: int) -> list[dict]:
        from sqlalchemy import text

        with self._get_engine().connect() as conn:
            result = conn.execute(text(sql))
            if not result.returns_rows:
                return []
            rows = result.mappings().fetchmany(limit)
            return [dict(r) for r in rows]
