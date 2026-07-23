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
import threading
from pathlib import Path
from typing import Any

from ...core.ports.explorer import (
    Column,
    QueryCancelledError,
    QueryTimedOutError,
    QueryTimeoutUnsupportedError,
    Table,
)


class _InterruptCancellationController:
    """Coordinate a deadline with a DBAPI connection exposing interrupt()."""

    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds
        self._lock = threading.Lock()
        self._connection: Any | None = None
        self._timer: threading.Timer | None = None
        self._reason = ""
        self._finished = False

    @property
    def reason(self) -> str:
        with self._lock:
            return self._reason

    def register(self, connection: Any) -> None:
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
        connection: Any | None
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
        self._engine_lock = threading.RLock()

    def _get_engine(self) -> Any:
        with self._engine_lock:
            if self._engine is not None:
                return self._engine
            import sqlite3

            from sqlalchemy import create_engine  # imported here = lazy driver load
            from sqlalchemy.engine import make_url

            url = make_url(self.url)
            backend = url.get_backend_name()
            if backend in {"sqlite", "duckdb"} and url.database not in {
                None,
                "",
                ":memory:",
            }:
                database = Path(str(url.database))
                if not database.is_file():
                    # A typo must not create and then trust an empty database.
                    raise FileNotFoundError(
                        f"{backend} database file does not exist: {database}"
                    )
                if backend == "sqlite":
                    sqlite_uri = f"file:{database.as_posix()}?mode=ro"
                    # A governed analytics connection must never create or
                    # mutate the user's SQLite file, even if a future caller
                    # accidentally bypasses the SELECT-only safety layer.
                    self._engine = create_engine(
                        "sqlite://",
                        creator=lambda: sqlite3.connect(
                            sqlite_uri,
                            uri=True,
                            check_same_thread=False,
                        ),
                    )
                else:
                    self._engine = create_engine(
                        self.url,
                        connect_args={
                            "read_only": True,
                            "config": {
                                "enable_external_access": "false",
                                "autoinstall_known_extensions": "false",
                                "autoload_known_extensions": "false",
                                "allow_community_extensions": "false",
                                "lock_configuration": "true",
                            },
                        },
                    )
            else:
                self._engine = create_engine(self.url)
            return self._engine

    def close(self) -> None:
        """Dispose pooled connections and make a later use start fresh."""

        with self._engine_lock:
            engine = self._engine
            self._engine = None
        if engine is not None:
            engine.dispose()

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
        parameters: dict[str, object] | None = None,
    ) -> list[dict]:
        timeout_seconds = float(timeout_seconds)
        if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be a finite positive number")
        from sqlalchemy.engine import make_url

        backend = make_url(self.url).get_backend_name()
        if backend not in {"sqlite", "duckdb"}:
            # A generic SQLAlchemy option cannot prove server-side statement
            # cancellation. Each dialect needs a verified implementation.
            raise QueryTimeoutUnsupportedError(
                "statement timeout is currently verified only for SQLite and DuckDB"
            )
        if backend == "duckdb" and make_url(self.url).database == ":memory:":
            raise QueryTimeoutUnsupportedError(
                "governed DuckDB execution requires an existing file-backed database"
            )
        controller = _InterruptCancellationController(timeout_seconds)

        async def worker_outcome() -> tuple[list[dict] | None, Exception | None]:
            try:
                return (
                    await asyncio.to_thread(
                        self._execute_sync,
                        sql,
                        int(limit),
                        controller,
                        parameters or {},
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

        url = make_url(self.url)
        if url.get_backend_name() == "sqlite":
            return bool(
                url.database in {None, "", ":memory:"}
                or Path(str(url.database)).is_file()
            )
        return bool(
            url.get_backend_name() == "duckdb"
            and url.database not in {None, "", ":memory:"}
            and Path(str(url.database)).is_file()
        )

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
        from sqlalchemy.engine import make_url

        engine = self._get_engine()
        insp = inspect(engine)
        if make_url(self.url).get_backend_name() == "duckdb":
            return self._duckdb_catalog_metadata_sync(insp)
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

    def _duckdb_catalog_metadata_sync(self, inspector: Any) -> dict[str, Any]:
        """Read DuckDB's native constraint catalog.

        ``duckdb-engine`` currently omits declared primary keys during
        SQLAlchemy reflection.  Semantic onboarding must not silently lose
        those identity facts, so this adapter uses DuckDB's documented catalog
        table function while preserving the same backend-neutral result shape.
        """

        from sqlalchemy import text

        default_schema = inspector.default_schema_name or "main"
        effective_schema = self._schema or default_schema
        table_names = inspector.get_table_names(schema=self._schema)
        tables: dict[str, Any] = {
            name: {"primary_key": [], "foreign_keys": [], "unique": []}
            for name in table_names
        }
        statement = text(
            "SELECT table_name, constraint_type, constraint_column_names, "
            "referenced_table, referenced_column_names "
            "FROM duckdb_constraints() "
            "WHERE schema_name = :schema_name "
            "ORDER BY table_name, constraint_index"
        )
        with self._get_engine().connect() as connection:
            rows = connection.execute(
                statement, {"schema_name": effective_schema}
            ).mappings()
            for row in rows:
                table_name = str(row["table_name"])
                target = tables.get(table_name)
                if target is None:
                    # Respect the inspector's selected table/schema boundary.
                    continue
                columns = list(row["constraint_column_names"] or [])
                constraint_type = str(row["constraint_type"])
                if constraint_type == "PRIMARY KEY":
                    target["primary_key"] = columns
                elif constraint_type == "UNIQUE":
                    target["unique"].append(columns)
                elif constraint_type == "FOREIGN KEY":
                    target["foreign_keys"].append(
                        {
                            "columns": columns,
                            # DuckDB currently exposes the referenced table but
                            # not a distinct referenced-schema field here.
                            "referred_schema": "",
                            "referred_table": str(row["referenced_table"] or ""),
                            "referred_columns": list(
                                row["referenced_column_names"] or []
                            ),
                        }
                    )
        return {"tables": tables}

    def _execute_sync(
        self,
        sql: str,
        limit: int,
        controller: _InterruptCancellationController,
        parameters: dict[str, object],
    ) -> list[dict]:
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        try:
            with self._get_engine().connect() as conn:
                raw = conn.connection.driver_connection
                if not callable(getattr(raw, "interrupt", None)):
                    raise QueryTimeoutUnsupportedError(
                        "database driver does not expose interrupt()"
                    )
                controller.register(raw)
                result = None
                try:
                    result = conn.execute(text(sql), parameters)
                    if not result.returns_rows:
                        return []
                    rows = result.mappings().fetchmany(limit)
                    return [dict(row) for row in rows]
                finally:
                    if result is not None:
                        result.close()
        except DBAPIError as exc:
            original = getattr(exc, "orig", None)
            if controller.reason == "timeout":
                raise QueryTimedOutError(
                    "database statement deadline exceeded"
                ) from None
            if controller.reason == "cancelled":
                raise QueryCancelledError("database statement cancelled") from None
            if getattr(original, "sqlite_errorcode", None) == 9:
                raise QueryCancelledError("SQLite statement interrupted") from None
            raise
        finally:
            controller.finish()
