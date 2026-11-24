"""DB explorer port — read-only schema introspection.

The agent uses this to discover tables/columns before writing SQL. V1 backs it
with a PostgreSQL adapter; the contract is dialect-neutral so BigQuery et al.
slot in later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable


class DataAccessError(RuntimeError):
    """Requested data is unavailable to this DB identity; do not probe alternatives."""


@dataclass
class Column:
    name: str
    type: str
    nullable: bool = True
    description: str = ""  # may be auto-enriched (v1.5 metadata layer)


@dataclass
class Table:
    name: str
    schema: str = "public"
    columns: list[Column] = field(default_factory=list)
    description: str = ""

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name


@runtime_checkable
class ExplorerPort(Protocol):
    """Introspect a connected database, read-only.

    Adapters may expose a non-I/O ``dialect`` string (e.g. ``oracle``) for
    prompt and row-limit syntax. Legacy implementations default to LIMIT SQL.
    """

    async def list_tables(self) -> list[Table]:
        """Tables visible to the connection (columns may be unpopulated)."""
        ...

    async def describe_table(self, name: str) -> Table:
        """Full column detail for one table."""
        ...

    async def sample_rows(self, name: str, limit: int = 5) -> list[dict]:
        """A few rows to give the model a feel for the data."""
        ...

    async def execute(self, sql: str, limit: int = 1000) -> list[dict]:
        """Run a read-only query (already cleared by the safety pipeline) and
        return up to ``limit`` rows. The ``run_sql`` tool calls this only after
        a PASS verdict; the adapter must never see un-gated SQL."""
        ...
