from __future__ import annotations

from typing import Any, Optional

from ...core.base import BaseComponent
from ...core.exceptions import ComponentError
from ...core.hooks import TraceHook
from ...core.ports import DBPort


class SQLExecutor(BaseComponent):
    """Executes a SQL string and returns rows as a list of dicts."""

    def __init__(
        self,
        *,
        db: DBPort,
        name: Optional[str] = None,
        hook: Optional[TraceHook] = None,
    ) -> None:
        super().__init__(name=name or "SQLExecutor", hook=hook)
        self._db = db

    def _run(self, sql: str) -> list[dict[str, Any]]:
        if not sql or not sql.strip():
            raise ComponentError(self.name, "sql must not be empty.")
        return self._db.execute(sql)
