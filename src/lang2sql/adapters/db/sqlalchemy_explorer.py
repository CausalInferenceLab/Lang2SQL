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
import math
import sqlite3
import threading
from typing import Any

from ...core.ports.explorer import (
    Column,
    QueryCancelledError,
    QueryTimedOutError,
    QueryTimeoutUnsupportedError,
    Table,
)


class _SQLiteCancellationController:
    """Coordinate a per-request timer/cancel signal with one SQLite connection."""

    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._connection: sqlite3.Connection | None = None
        self._timer: threading.Timer | None = None
        self._reason = ""
        self._finished = False

    @property
    def reason(self) -> str:
        with self._lock:
            return self._reason

    def register(self, connection: sqlite3.Connection) -> None:
        with self._lock:
            if self._finished:
                raise QueryCancelledError("query cancelled before execution")
            self._connection = connection
            if self._reason:
                connection.interrupt()
                raise QueryCancelledError("query cancelled before execution")
            timer = threading.Timer(self._timeout_seconds, self._timeout)
            timer.daemon = True
            self._timer = timer
            timer.start()

    def cancel(self) -> None:
        self._interrupt("cancelled")

    def _timeout(self) -> None:
        self._interrupt("timeout")

    def _interrupt(self, reason: str) -> None:
        connection: sqlite3.Connection | None
        with self._lock:
            if self._finished or self._reason:
                return
            self._reason = reason
            connection = self._connection
        if connection is not None:
            connection.interrupt()

    def finish(self) -> None:
        timer: threading.Timer | None
        with self._lock:
            self._finished = True
            self._connection = None
            timer = self._timer
            self._timer = None
        if timer is not None:
            timer.cancel()
            if timer is not threading.current_thread():
                timer.join(timeout=0.25)


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

    async def execute(
        self,
        sql: str,
        limit: int = 1000,
        *,
        timeout_seconds: float = 30.0,
    ) -> list[dict]:
        timeout_seconds = float(timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a finite positive number")
        from sqlalchemy.engine import make_url

        if make_url(self.url).get_backend_name() != "sqlite":
            # A generic SQLAlchemy option cannot prove server-side statement
            # cancellation. Each dialect needs a verified implementation.
            raise QueryTimeoutUnsupportedError(
                "statement timeout is currently verified only for SQLite"
            )
        controller = _SQLiteCancellationController(timeout_seconds)
        async def worker_outcome() -> tuple[list[dict] | None, Exception | None]:
            try:
                return (
                    await asyncio.to_thread(
                        self._execute_sync, sql, int(limit), controller
                    ),
                    None,
                )
            except Exception as exc:
                # Shield logs an inner task exception as soon as its outer
                # waiter is cancelled. Return typed failures as values so the
                # cancellation cleanup path can consume them without orphan
                # warnings, then re-raise below on the normal path.
                return None, exc

        worker = asyncio.create_task(worker_outcome())
        try:
            rows, error = await asyncio.shield(worker)
            if error is not None:
                raise error
            assert rows is not None
            return rows
        except asyncio.CancelledError as cancelled:
            controller.cancel()
            current = asyncio.current_task()
            if current is not None:
                cancelling = getattr(current, "cancelling", lambda: 0)
                uncancel = getattr(current, "uncancel", lambda: None)
                while cancelling():
                    uncancel()
            while True:
                try:
                    await asyncio.shield(worker)
                    break
                except asyncio.CancelledError:
                    # A second caller cancellation must not orphan the DB
                    # worker. Clear it temporarily, finish cleanup, then
                    # re-raise the original cancellation below.
                    controller.cancel()
                    if current is not None:
                        cancelling = getattr(current, "cancelling", lambda: 0)
                        uncancel = getattr(current, "uncancel", lambda: None)
                        while cancelling():
                            uncancel()
                    continue
            raise cancelled

    async def catalog_metadata(self) -> dict[str, Any]:
        """Return declared PK/FK/unique facts for semantic onboarding.

        This is an optional concrete capability rather than a new core port:
        older/custom explorers remain valid and simply produce a catalog with
        no automatically trusted relationships.
        """

        return await asyncio.to_thread(self._catalog_metadata_sync)

    def quote_identifier(self, name: str) -> str:
        """Quote one DB-provided identifier using the active SQL dialect."""

        return self._get_engine().dialect.identifier_preparer.quote(name)

    def governed_execution_supported(self) -> bool:
        from sqlalchemy.engine import make_url

        return make_url(self.url).get_backend_name() == "sqlite"

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

    def _catalog_metadata_sync(self) -> dict[str, Any]:
        from sqlalchemy import inspect

        insp = inspect(self._get_engine())
        tables: dict[str, Any] = {}
        for name in insp.get_table_names(schema=self._schema):
            pk = insp.get_pk_constraint(name, schema=self._schema) or {}
            foreign_keys = insp.get_foreign_keys(name, schema=self._schema) or []
            try:
                unique_constraints = (
                    insp.get_unique_constraints(name, schema=self._schema) or []
                )
            except NotImplementedError:
                unique_constraints = []
            tables[name] = {
                "primary_key": list(pk.get("constrained_columns") or []),
                "foreign_keys": [
                    {
                        "columns": list(item.get("constrained_columns") or []),
                        "referred_schema": item.get("referred_schema") or "",
                        "referred_table": item.get("referred_table") or "",
                        "referred_columns": list(item.get("referred_columns") or []),
                    }
                    for item in foreign_keys
                ],
                "unique": [
                    list(item.get("column_names") or []) for item in unique_constraints
                ],
            }
        return {"tables": tables}

    def _execute_sync(
        self,
        sql: str,
        limit: int,
        controller: _SQLiteCancellationController,
    ) -> list[dict]:
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        try:
            with self._get_engine().connect() as conn:
                raw = conn.connection.driver_connection
                if not isinstance(raw, sqlite3.Connection):
                    raise QueryTimeoutUnsupportedError(
                        "SQLite driver does not expose interrupt()"
                    )
                controller.register(raw)
                result = None
                try:
                    result = conn.execute(text(sql))
                    if not result.returns_rows:
                        return []
                    rows = result.mappings().fetchmany(limit)
                    return [dict(row) for row in rows]
                finally:
                    if result is not None:
                        result.close()
        except DBAPIError as exc:
            original = getattr(exc, "orig", None)
            if getattr(original, "sqlite_errorcode", None) == 9:
                if controller.reason == "timeout":
                    raise QueryTimedOutError("SQLite statement deadline exceeded") from None
                if controller.reason == "cancelled":
                    raise QueryCancelledError("SQLite statement cancelled") from None
            raise
        finally:
            controller.finish()
