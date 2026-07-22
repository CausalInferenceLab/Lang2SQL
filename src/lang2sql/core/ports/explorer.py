"""DB explorer port — read-only schema introspection.

The agent uses this to discover tables/columns before writing SQL. V1 backs it
with a PostgreSQL adapter; the contract is dialect-neutral so BigQuery et al.
slot in later.
"""

from __future__ import annotations

from dataclasses import dataclass, field
import inspect
from typing import Protocol, runtime_checkable


class QueryTimedOutError(TimeoutError):
    """The adapter stopped a statement after its verified deadline."""


class QueryTimeoutUnsupportedError(RuntimeError):
    """The adapter cannot prove statement cancellation for this dialect."""


class QueryCancelledError(RuntimeError):
    """Internal worker signal after a caller cancellation interrupted SQL."""


def accepts_statement_timeout(explorer: object) -> bool:
    """Fail closed for legacy adapters that predate the timeout contract."""

    execute = getattr(explorer, "execute", None)
    if execute is None:
        return False
    try:
        signature = inspect.signature(execute)
    except (TypeError, ValueError):
        return False
    parameter = signature.parameters.get("timeout_seconds")
    if parameter is not None and parameter.kind in {
        inspect.Parameter.KEYWORD_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    }:
        return True
    return any(
        item.kind == inspect.Parameter.VAR_KEYWORD
        for item in signature.parameters.values()
    )


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
    """Introspect a connected database, read-only."""

    async def list_tables(self) -> list[Table]:
        """Tables visible to the connection (columns may be unpopulated)."""
        ...

    async def describe_table(self, name: str) -> Table:
        """Full column detail for one table."""
        ...

    async def sample_rows(self, name: str, limit: int = 5) -> list[dict]:
        """A few rows to give the model a feel for the data."""
        ...

    async def execute(
        self,
        sql: str,
        limit: int = 1000,
        *,
        timeout_seconds: float = 30.0,
    ) -> list[dict]:
        """Run a read-only query (already cleared by the safety pipeline) and
        return up to ``limit`` rows. The ``run_sql`` tool calls this only after
        a PASS verdict; the adapter must never see un-gated SQL."""
        ...
