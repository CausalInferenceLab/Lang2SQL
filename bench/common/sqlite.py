"""Read-only SQLite connections for schema tools and offline scoring."""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

from sqlalchemy import create_engine, event
from sqlalchemy.pool import NullPool

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer


def set_deadline(connection: sqlite3.Connection, deadline: float) -> None:
    connection.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)


def read_connection(db: Path, deadline: float) -> sqlite3.Connection:
    connection = sqlite3.connect(
        db.resolve().as_uri() + "?mode=ro",
        uri=True,
        check_same_thread=False,
        timeout=max(0.0, deadline - time.monotonic()),
    )

    def authorize(action, *_):
        if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH):
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    try:
        connection.execute("PRAGMA query_only=ON")
        connection.set_authorizer(authorize)
        set_deadline(connection, deadline)
        return connection
    except BaseException:
        connection.close()
        raise


class ReadOnlyExplorer(SqlAlchemyExplorer):
    def __init__(self, db: Path, timeout: float):
        super().__init__("sqlite://")
        self._engine = create_engine(
            "sqlite://",
            poolclass=NullPool,
            creator=lambda: read_connection(db, time.monotonic() + timeout),
        )

        @event.listens_for(self._engine, "before_cursor_execute")
        def deadline(connection, cursor, statement, parameters, context, executemany):
            set_deadline(cursor.connection, time.monotonic() + timeout)

    def close(self) -> None:
        self._engine.dispose()
