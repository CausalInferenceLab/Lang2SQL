"""build_explorer — turn a connection string into the right ExplorerPort.

The setup workflow (or an environment variable) supplies one URL and this
factory routes it. Cloudflare D1 has its own HTTP adapter; everything else with
a normal SQLAlchemy URL goes through the generic SQLAlchemy explorer.

    d1://<account_id>/<database_id>      → D1Explorer        (token from env)
    postgresql+psycopg://user:…/db       → SqlAlchemyExplorer
    bigquery://project/dataset           → SqlAlchemyExplorer
    snowflake://user:…@account/db        → SqlAlchemyExplorer
    mysql+pymysql://…  /  duckdb:///…    → SqlAlchemyExplorer
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit

from ...core.ports.explorer import ExplorerPort
from .d1_explorer import D1Explorer
from .sqlalchemy_explorer import SqlAlchemyExplorer


def build_explorer(
    connection: str,
    *,
    schema: str | None = None,
    extras: dict | None = None,
) -> ExplorerPort:
    """Route a connection string to the matching explorer adapter.

    ``schema`` is forwarded to the SQLAlchemy explorer (ignored by D1, which is
    schema-less SQLite). ``extras`` carries per-adapter secrets that don't
    belong in the URL — currently ``d1_token`` for the D1 HTTP API. Raises
    ``ValueError`` on an empty/unparseable string.
    """
    if not connection or not connection.strip():
        raise ValueError("empty connection string")

    scheme = urlsplit(connection).scheme.lower()
    if not scheme:
        raise ValueError(f"connection string has no scheme: {connection!r}")

    extras = extras or {}

    if scheme == "d1":
        parts = urlsplit(connection)
        account_id = parts.netloc
        database_id = parts.path.lstrip("/")
        if not account_id or not database_id:
            raise ValueError("d1 URL must be d1://<account_id>/<database_id>")
        return D1Explorer(
            account_id=account_id,
            database_id=database_id,
            token=extras.get("d1_token"),
        )

    # Normalize bare postgresql:// → postgresql+psycopg:// (psycopg3 is installed).
    if scheme == "postgresql":
        connection = "postgresql+psycopg" + connection[len("postgresql") :]

    # Anything else is assumed to be a SQLAlchemy URL (driver loaded lazily).
    return SqlAlchemyExplorer(connection, schema=schema)


def explorer_from_env() -> ExplorerPort | None:
    """Build an explorer from environment, or ``None`` if nothing is configured.

    Precedence: an explicit ``LANG2SQL_DB_URL`` wins; otherwise a pair of
    ``CLOUDFLARE_D1_ACCOUNT_ID`` + ``CLOUDFLARE_D1_DATABASE_ID`` selects D1.
    """
    url = os.environ.get("LANG2SQL_DB_URL")
    if url:
        schema = os.environ.get("LANG2SQL_DB_SCHEMA") or None
        return build_explorer(url, schema=schema)

    account = os.environ.get("CLOUDFLARE_D1_ACCOUNT_ID")
    database = os.environ.get("CLOUDFLARE_D1_DATABASE_ID")
    if account and database:
        return build_explorer(f"d1://{account}/{database}")

    return None
