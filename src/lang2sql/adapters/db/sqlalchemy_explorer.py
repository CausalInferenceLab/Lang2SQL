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
import logging
import math
import time
from collections import Counter
from contextlib import contextmanager
from typing import Any

from ...core.ports.explorer import Column, DataAccessError, ForeignKey, QueryRows, Table

logger = logging.getLogger(__name__)

_ORACLE_ACCESS_ERRORS = frozenset({942, 1031})
_SQLITE_PROGRESS_OPCODES = 1000


@contextmanager
def _sqlite_deadline(conn, timeout_seconds):
    """Bound SQLite VM execution and remove the handler before returning to the pool.

    This does not bound connection acquisition, lock waits, or blocking user
    functions that do not yield to the SQLite VM.
    """
    if timeout_seconds is None:
        yield
        return
    if not math.isfinite(timeout_seconds) or timeout_seconds <= 0:
        raise ValueError("SQL deadline must be a finite positive number")
    from sqlalchemy.exc import DBAPIError

    raw = conn.connection.driver_connection
    deadline = time.monotonic() + timeout_seconds
    expired = False

    def check_deadline():
        nonlocal expired
        expired = time.monotonic() >= deadline
        return int(expired)

    raw.set_progress_handler(check_deadline, _SQLITE_PROGRESS_OPCODES)
    try:
        yield
    except DBAPIError as exc:
        if expired:
            raise TimeoutError(f"SQLite query exceeded execution deadline ({timeout_seconds}s)") from exc
        raise
    finally:
        raw.set_progress_handler(None, 0)


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

    async def oracle_metric(self, contract, *, stamp=None, sql=None, params=None):
        from .oracle_view import inspect_or_query

        if self.dialect != "oracle":
            raise ValueError("The confirmed view requires an Oracle connection")
        return await asyncio.to_thread(
            lambda: inspect_or_query(self._get_engine(), contract, stamp, sql, params)
        )

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

    @property
    def supports_query_timeout(self) -> bool:
        return self.dialect == "sqlite"

    @property
    def query_timeout_scope(self) -> str:
        return "sqlite_vm" if self.supports_query_timeout else "unsupported"

    async def execute_with_timeout(self, sql: str, limit: int, timeout_seconds: float) -> list[dict]:
        if not self.supports_query_timeout:
            raise NotImplementedError("Native query deadlines are currently supported for SQLite only")
        return await asyncio.to_thread(self._execute_sync, sql, int(limit), timeout_seconds)

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
            Table(name=name, schema=display_schema, kind=kind)
            for kind, names in (
                ("table", insp.get_table_names(schema=self._schema)),
                ("view", insp.get_view_names(schema=self._schema)),
            )
            for name in sorted(names)
        ]

    def _describe_table_sync(self, name: str) -> Table:
        from sqlalchemy import inspect

        insp = inspect(self._get_engine())
        if self._schema and name.upper().startswith(self._schema.upper() + "."):
            name = name[len(self._schema) + 1 :]
        if self.dialect == "oracle":
            name = self._get_engine().dialect.normalize_name(name)
        kind = "view" if name in insp.get_view_names(schema=self._schema) else "table"
        cols = [
            Column(
                name=c["name"],
                type=str(c["type"]),
                nullable=bool(c.get("nullable", True)),
                description=c.get("comment") or "",
            )
            for c in insp.get_columns(name, schema=self._schema)
        ]
        primary_key, foreign_keys = [], []
        if kind == "table":
            try:
                primary_key = (
                    insp.get_pk_constraint(name, schema=self._schema).get(
                        "constrained_columns"
                    )
                    or []
                )
                foreign_keys = [
                    ForeignKey(
                        columns=fk["constrained_columns"],
                        referred_table=fk["referred_table"],
                        referred_columns=fk["referred_columns"],
                        referred_schema=fk.get("referred_schema") or "",
                    )
                    for fk in insp.get_foreign_keys(name, schema=self._schema)
                ]
            except NotImplementedError as exc:
                logger.warning(
                    "constraint reflection unsupported table=%s dialect=%s error=%s: %s",
                    name,
                    self.dialect,
                    type(exc).__name__,
                    exc,
                )
        return Table(
            name=name,
            schema=self._schema or "",
            columns=cols,
            kind=kind,
            primary_key=primary_key,
            foreign_keys=foreign_keys,
        )

    def _execute_sync(self, sql: str, limit: int, timeout_seconds=None) -> list[dict]:
        from sqlalchemy import text
        from sqlalchemy.exc import DBAPIError

        with self._get_engine().connect() as conn, _sqlite_deadline(conn, timeout_seconds):
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
            duplicates = [name for name, count in Counter(result.keys()).items() if count > 1]
            if duplicates:
                raise ValueError(
                    "Duplicate result columns would discard values; use unique column aliases: "
                    + ", ".join(duplicates)
                )
            rows = result.mappings().fetchmany(limit)
            return QueryRows((dict(r) for r in rows), columns=result.keys())
