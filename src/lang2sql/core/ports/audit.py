"""Audit port — append-only record of what the agent did.

Backs ``/audit me``. V1 is a plain append-only SQLite table (no hash chain —
that's V2). Every SQL execution, definition change, and ingestion lands here.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable


@dataclass
class AuditEvent:
    actor: str  # user_id
    action: str  # "run_sql" | "define_metric" | "ingest" | ...
    scope: str  # session/scope key
    detail: dict[str, Any] = field(default_factory=dict)
    ts: float = 0.0  # epoch seconds; filled by the store if 0


@runtime_checkable
class AuditPort(Protocol):
    async def record(self, event: AuditEvent) -> None: ...

    async def query(self, actor: str, limit: int = 20) -> list[AuditEvent]:
        """Recent events for one actor, newest first."""
        ...
