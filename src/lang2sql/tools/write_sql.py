"""WriteSQLTool — generate SQL using the semantic layer."""

from __future__ import annotations

from typing import Any

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult
from ..semantic.sql_composer import SQLComposer


class WriteSQLTool:
    """Generate SQL using the semantic layer definitions."""

    def __init__(self, composer: SQLComposer) -> None:
        self._composer = composer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="write_sql",
            description=(
                "Generate a SQL query using the semantic layer. "
                "Specify a metric name and optional dimension names. "
                "The SQL will automatically include correct JOINs, "
                "WHERE filters, and GROUP BY based on the semantic "
                "layer definitions."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "metric_name": {"type": "string"},
                    "dimension_names": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "filters": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Additional WHERE conditions",
                    },
                    "order_by": {"type": "string"},
                    "limit": {"type": "integer"},
                },
                "required": ["metric_name"],
            },
        )

    def execute(
        self,
        metric_name: str,
        dimension_names: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
        **_: Any,
    ) -> ToolResult:
        try:
            sql = self._composer.compose(
                metric_name=metric_name,
                dimension_names=dimension_names,
                filters=filters,
                order_by=order_by,
                limit=limit,
            )
            content = f"```sql\n{sql}\n```"
            return ToolResult(
                tool_call_id="",
                content=content,
                data={"sql": sql},
            )
        except KeyError as exc:
            return ToolResult(
                tool_call_id="",
                content=f"Semantic layer lookup failed: {exc}",
                is_error=True,
            )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
