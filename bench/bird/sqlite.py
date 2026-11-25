"""Shared read-only SQLite connection for inference and EX."""

import sqlite3
import time
from pathlib import Path


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
