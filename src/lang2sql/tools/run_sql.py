"""RunSQL tool — execute read-only SQL against the database."""

from __future__ import annotations

import json
from typing import Any

from ..core.ports import DBExplorerPort
from ..harness.tool import ToolSpec
from ..harness.types import ToolResult

_MAX_ROWS = 100


class RunSQL:
    """Execute read-only SQL against the database."""

    def __init__(self, explorer: DBExplorerPort) -> None:
        self._explorer = explorer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="run_sql",
            description=(
                "Execute a read-only SQL query against the database. "
                "Write operations (INSERT, UPDATE, DELETE, DROP, etc.) "
                "are blocked. Results are limited to 100 rows."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to execute",
                    },
                },
                "required": ["sql"],
            },
        )

    def execute(self, sql: str, **_: Any) -> ToolResult:
        try:
            rows = self._explorer.execute_read_only(sql)
            truncated = len(rows) > _MAX_ROWS
            if truncated:
                rows = rows[:_MAX_ROWS]

            content = json.dumps(rows, ensure_ascii=False, default=str)
            if truncated:
                content += f"\n\n... truncated to {_MAX_ROWS} rows"

            return ToolResult(
                tool_call_id="",
                content=content,
                data={"rows": rows, "truncated": truncated},
            )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
