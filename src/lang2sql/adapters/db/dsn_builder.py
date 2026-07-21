"""Form fields → DSN assembly.

The setup wizard collects credentials field-by-field so non-developers never
see a DSN string. Each ``build_*`` here turns those fields into the canonical
SQLAlchemy/D1 URL that :func:`build_explorer` already understands. Splitting
this off keeps the wizard's UI layer (Discord modals) thin and lets us unit-
test the assembly without a Discord runtime.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable
from urllib.parse import quote_plus, urlsplit


@dataclass
class ConnectionSpec:
    """The wizard's output: a DSN + any out-of-band secrets the adapter needs."""

    dsn: str
    extras: dict[str, str]


# Supported DB types in the wizard. Order matters — surfaces in the dropdown.
SUPPORTED_DB_TYPES: tuple[str, ...] = (
    "sqlite",
    "postgresql",
    "mysql",
    "snowflake",
    "bigquery",
    "duckdb",
    "d1",
)


def _quote(s: str) -> str:
    return quote_plus(s, safe="")


def build_postgresql(
    *, host: str, port: str, database: str, user: str, password: str
) -> ConnectionSpec:
    # User may paste a full URL (e.g. "host/db?sslmode=require") into the host field.
    # Extract just the hostname to avoid corrupting the assembled DSN.
    parsed = urlsplit("//" + host)
    clean_host = parsed.hostname or host
    p = int(port) if port else 5432
    suffix = "?sslmode=require" if clean_host.endswith(".neon.tech") else ""
    dsn = f"postgresql+psycopg://{_quote(user)}:{_quote(password)}@{clean_host}:{p}/{database}{suffix}"
    return ConnectionSpec(dsn=dsn, extras={})


def build_sqlite(*, path: str) -> ConnectionSpec:
    return ConnectionSpec(dsn=f"sqlite:///{path}", extras={})


def build_mysql(
    *, host: str, port: str, database: str, user: str, password: str
) -> ConnectionSpec:
    p = int(port) if port else 3306
    dsn = f"mysql+pymysql://{_quote(user)}:{_quote(password)}@{host}:{p}/{database}"
    return ConnectionSpec(dsn=dsn, extras={})


def build_snowflake(
    *, account: str, user: str, password: str, database: str, warehouse: str
) -> ConnectionSpec:
    dsn = (
        f"snowflake://{_quote(user)}:{_quote(password)}@{account}"
        f"/{database}?warehouse={_quote(warehouse)}"
    )
    return ConnectionSpec(dsn=dsn, extras={})


def build_bigquery(*, project: str, dataset: str) -> ConnectionSpec:
    # Auth via Application Default Credentials (gcloud) — credentials are not
    # in the DSN. We document this in the wizard's success message.
    dsn = f"bigquery://{project}/{dataset}"
    return ConnectionSpec(dsn=dsn, extras={})


def build_duckdb(*, path: str) -> ConnectionSpec:
    return ConnectionSpec(dsn=f"duckdb:///{path}", extras={})


def build_d1(*, account_id: str, database_id: str, api_token: str) -> ConnectionSpec:
    # The token doesn't go in the URL — it's an out-of-band header.
    return ConnectionSpec(
        dsn=f"d1://{account_id}/{database_id}",
        extras={"d1_token": api_token},
    )


# Field schemas surfaced by the Discord Modal layer. Each entry is
# (label, placeholder, required, masked).
FIELD_SCHEMA: dict[str, list[tuple[str, str, bool, bool]]] = {
    "sqlite": [
        ("path", "/data/demo.db", True, False),
    ],
    "postgresql": [
        ("host", "db.example.com", True, False),
        ("port", "5432", False, False),
        ("database", "analytics", True, False),
        ("user", "readonly_user", True, False),
        ("password", "•••••", True, True),
    ],
    "mysql": [
        ("host", "db.example.com", True, False),
        ("port", "3306", False, False),
        ("database", "analytics", True, False),
        ("user", "readonly_user", True, False),
        ("password", "•••••", True, True),
    ],
    "snowflake": [
        ("account", "abc12345.us-east-1", True, False),
        ("user", "readonly_user", True, False),
        ("password", "•••••", True, True),
        ("database", "ANALYTICS", True, False),
        ("warehouse", "COMPUTE_WH", True, False),
    ],
    "bigquery": [
        ("project", "my-gcp-project", True, False),
        ("dataset", "analytics", True, False),
    ],
    "duckdb": [
        ("path", "/data/warehouse.duckdb", True, False),
    ],
    "d1": [
        ("account_id", "Cloudflare account ID", True, False),
        ("database_id", "D1 database ID", True, False),
        ("api_token", "Cloudflare API token", True, True),
    ],
}


_BUILDERS: dict[str, Callable[..., ConnectionSpec]] = {
    "sqlite": build_sqlite,
    "postgresql": build_postgresql,
    "mysql": build_mysql,
    "snowflake": build_snowflake,
    "bigquery": build_bigquery,
    "duckdb": build_duckdb,
    "d1": build_d1,
}


def assemble(db_type: str, fields: dict[str, str]) -> ConnectionSpec:
    """Dispatch by ``db_type`` to the matching builder.

    The wizard hands raw modal inputs in ``fields``; this is the one entry
    point so the UI layer stays dialect-agnostic.
    """
    builder = _BUILDERS.get(db_type)
    if builder is None:
        raise ValueError(f"unsupported db type: {db_type!r}")
    # Filter to the expected kwargs (modal can hand stray keys safely).
    expected = {name for name, *_ in FIELD_SCHEMA[db_type]}
    cleaned = {k: (v or "").strip() for k, v in fields.items() if k in expected}
    missing = [
        n for n, _, req, _ in FIELD_SCHEMA[db_type] if req and not cleaned.get(n)
    ]
    if missing:
        raise ValueError(f"missing required fields: {', '.join(missing)}")
    return builder(**cleaned)
