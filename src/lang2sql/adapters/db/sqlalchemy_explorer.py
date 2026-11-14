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

from ...core.ports.explorer import Column, DataAccessError, Table

_ORACLE_ACCESS_ERRORS = frozenset({942, 1031})


class SqlAlchemyExplorer:
    """ExplorerPort over a SQLAlchemy Engine, built from a connection URL."""

    def __init__(self, url: str, *, schema: str | None = None) -> None:
        self.url = url
        self._schema = schema
        self._engine: Any = None  # created lazily

    @property
    def dialect(self) -> str:
        """SQL dialect hint without importing a driver or opening a connection."""
        from sqlalchemy.engine import make_url

        return make_url(self.url).get_backend_name()

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
        return await asyncio.to_thread(self._sample_rows_sync, name, limit)

    def _sample_rows_sync(self, name: str, limit: int) -> list[dict]:
        preparer = self._get_engine().dialect.identifier_preparer
        qname = preparer.quote(name)
        if self._schema:
            qname = f"{preparer.quote_schema(self._schema)}.{qname}"
        return self._execute_sync(f"SELECT * FROM {qname}", limit)

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

        if self._schema and name.startswith(self._schema + "."):
            name = name[len(self._schema) + 1 :]
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
        from sqlalchemy.exc import DBAPIError

        with self._get_engine().connect() as conn:
            try:
                result = conn.execute(text(sql))
            except DBAPIError as exc:
                code = (
                    getattr(exc.orig.args[0], "code", None)
                    if exc.orig is not None and exc.orig.args
                    else None
                )
                if self.dialect == "oracle" and code in _ORACLE_ACCESS_ERRORS:
                    # ORA-00942 deliberately does not distinguish a missing table
                    # from one hidden by privileges. Do not assert either cause.
                    raise DataAccessError(
                        f"ORA-{code:05d}: requested table is unavailable to this account "
                        "(missing table or insufficient privileges)"
                    ) from exc
                raise
            if not result.returns_rows:
                return []
            rows = result.mappings().fetchmany(limit)
            return [dict(r) for r in rows]
