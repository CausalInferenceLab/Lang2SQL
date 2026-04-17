"""ExploreSchema tool — wraps DBExplorerPort for schema discovery."""

from __future__ import annotations

from typing import Any

from ..core.ports import DBExplorerPort
from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class ExploreSchema:
    """Explore database schema — list tables, get DDL, sample data."""

    def __init__(self, explorer: DBExplorerPort) -> None:
        self._explorer = explorer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="explore_schema",
            description=(
                "Explore database schema. "
                "Actions: 'list_tables' to see all tables, "
                "'get_ddl' to see table definition, "
                "'sample_data' to see example rows."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "action": {
                        "type": "string",
                        "enum": ["list_tables", "get_ddl", "sample_data"],
                    },
                    "table_name": {
                        "type": "string",
                        "description": "Required for get_ddl and sample_data",
                    },
                    "limit": {
                        "type": "integer",
                        "default": 5,
                        "description": "Number of sample rows",
                    },
                },
                "required": ["action"],
            },
        )

    def execute(
        self,
        action: str,
        table_name: str = "",
        limit: int = 5,
        **_: Any,
    ) -> ToolResult:
        try:
            if action == "list_tables":
                return self._list_tables()
            elif action == "get_ddl":
                return self._get_ddl(table_name)
            elif action == "sample_data":
                return self._sample_data(table_name, limit)
            else:
                return ToolResult(
                    tool_call_id="",
                    content=f"Unknown action: {action!r}. "
                    f"Use list_tables, get_ddl, or sample_data.",
                    is_error=True,
                )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )

    # ------------------------------------------------------------------ #
    #  Action handlers                                                     #
    # ------------------------------------------------------------------ #

    def _list_tables(self) -> ToolResult:
        tables = self._explorer.list_tables()
        if not tables:
            content = "No tables found in database."
        else:
            lines = [f"Found {len(tables)} table(s):", ""]
            for t in tables:
                lines.append(f"  - {t}")
            content = "\n".join(lines)
        return ToolResult(tool_call_id="", content=content, data=tables)

    def _get_ddl(self, table_name: str) -> ToolResult:
        if not table_name:
            return ToolResult(
                tool_call_id="",
                content="table_name is required for get_ddl action.",
                is_error=True,
            )
        ddl = self._explorer.get_ddl(table_name)
        content = f"DDL for `{table_name}`:\n\n```sql\n{ddl}\n```"
        return ToolResult(tool_call_id="", content=content)

    def _sample_data(self, table_name: str, limit: int) -> ToolResult:
        if not table_name:
            return ToolResult(
                tool_call_id="",
                content="table_name is required for sample_data action.",
                is_error=True,
            )
        rows = self._explorer.sample_data(table_name, limit=limit)
        if not rows:
            return ToolResult(
                tool_call_id="",
                content=f"Table `{table_name}` is empty.",
                data=[],
            )
        content = _format_rows_as_table(table_name, rows, limit)
        return ToolResult(tool_call_id="", content=content, data=rows)


def _format_rows_as_table(
    table_name: str, rows: list[dict[str, Any]], limit: int
) -> str:
    """Format rows as a markdown table."""
    cols = list(rows[0].keys())
    # Header
    header = "| " + " | ".join(cols) + " |"
    sep = "| " + " | ".join("---" for _ in cols) + " |"
    # Rows
    body_lines: list[str] = []
    for row in rows:
        vals = [str(row.get(c, "")) for c in cols]
        body_lines.append("| " + " | ".join(vals) + " |")

    parts = [
        f"Sample data from `{table_name}` (limit {limit}):",
        "",
        header,
        sep,
        *body_lines,
    ]
    return "\n".join(parts)
