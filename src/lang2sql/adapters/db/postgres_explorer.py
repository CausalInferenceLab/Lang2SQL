"""PostgresExplorer — V1 stub; real psycopg-backed explorer lands in v1.5.

psycopg is not stdlib, so V1 does not connect to anything. The ctor stores the
DSN for the future real impl, and the read methods return small canned schema
data so the agent loop and schema-aware tools can be exercised end-to-end.
"""

from __future__ import annotations

from ...core.ports.explorer import Column, QueryTimeoutUnsupportedError, Table

# Canned catalog — two tables a demo guild would plausibly have.
_TABLES: dict[str, Table] = {
    "public.orders": Table(
        name="orders",
        schema="public",
        description="One row per customer order.",
        columns=[
            Column("id", "integer", nullable=False, description="Primary key."),
            Column("amount", "numeric", nullable=False, description="Order total."),
            Column(
                "status", "text", description="pending | paid | shipped | cancelled."
            ),
            Column("created_at", "timestamptz", nullable=False),
        ],
    ),
    "public.users": Table(
        name="users",
        schema="public",
        description="Registered users.",
        columns=[
            Column("id", "integer", nullable=False, description="Primary key."),
            Column("email", "text", nullable=False),
            Column("created_at", "timestamptz", nullable=False),
        ],
    ),
}

_SAMPLES: dict[str, list[dict]] = {
    "public.orders": [
        {
            "id": 1,
            "amount": 49.90,
            "status": "paid",
            "created_at": "2026-05-01T10:00:00Z",
        },
        {
            "id": 2,
            "amount": 12.00,
            "status": "pending",
            "created_at": "2026-05-02T14:30:00Z",
        },
    ],
    "public.users": [
        {"id": 1, "email": "alice@example.com", "created_at": "2026-04-20T08:00:00Z"},
        {"id": 2, "email": "bob@example.com", "created_at": "2026-04-21T09:15:00Z"},
    ],
}


def _resolve_key(name: str) -> str:
    """Map a bare or qualified table name onto a canned-catalog key."""
    if name in _TABLES:
        return name
    return f"public.{name}"


class PostgresExplorer:
    """Read-only schema introspection — canned data in V1."""

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn  # stored for the v1.5 psycopg impl; no connection in V1.

    async def list_tables(self) -> list[Table]:
        return list(_TABLES.values())

    async def describe_table(self, name: str) -> Table:
        key = _resolve_key(name)
        table = _TABLES.get(key)
        if table is None:
            raise KeyError(f"unknown table: {name}")
        return table

    async def sample_rows(self, name: str, limit: int = 5) -> list[dict]:
        key = _resolve_key(name)
        return list(_SAMPLES.get(key, []))[:limit]

    async def execute(
        self,
        sql: str,
        limit: int = 1000,
        *,
        timeout_seconds: float = 30.0,
    ) -> list[dict]:
        raise QueryTimeoutUnsupportedError(
            "PostgresExplorer is a metadata stub without statement cancellation"
        )

    def governed_execution_supported(self) -> bool:
        return False

    async def catalog_metadata(self) -> dict:
        """Declared facts for the canned fixture; no sample inference."""

        return {
            "tables": {
                "orders": {
                    "primary_key": ["id"],
                    "foreign_keys": [],
                    "unique": [],
                },
                "users": {
                    "primary_key": ["id"],
                    "foreign_keys": [],
                    "unique": [],
                },
            }
        }

    def quote_identifier(self, name: str) -> str:
        if not name.replace("_", "").isalnum():
            raise ValueError(f"unsafe identifier: {name!r}")
        return f'"{name}"'
