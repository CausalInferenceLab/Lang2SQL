"""DefineMetric tool — define or update semantic layer metrics."""

from __future__ import annotations

from typing import Any

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class DefineMetric:
    """Define or update a metric in the semantic layer."""

    def __init__(self, layer: Any) -> None:
        self._layer = layer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="define_metric",
            description=(
                "Define or update a metric in the semantic layer. "
                "A metric is a measurable business value like revenue "
                "or order count."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": (
                            "Metric identifier (e.g., 'revenue')"
                        ),
                    },
                    "display_name": {
                        "type": "string",
                        "description": (
                            "Human-readable name (e.g., '매출')"
                        ),
                    },
                    "expression": {
                        "type": "string",
                        "description": (
                            "SQL aggregate expression "
                            "(e.g., 'SUM(orders.amount)')"
                        ),
                    },
                    "table": {
                        "type": "string",
                        "description": "Primary table for this metric",
                    },
                    "description": {"type": "string"},
                    "filters": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Default WHERE conditions",
                    },
                },
                "required": ["name", "expression", "table"],
            },
        )

    def execute(
        self,
        name: str,
        expression: str,
        table: str,
        display_name: str = "",
        description: str = "",
        filters: list[str] | None = None,
        **_: Any,
    ) -> ToolResult:
        from ..semantic.types import Metric

        try:
            existing = self._layer.get_metric(name)
            if existing:
                self._layer.update_metric(
                    name,
                    expression=expression,
                    table=table,
                    display_name=display_name or existing.display_name,
                    description=description or existing.description,
                    filters=(
                        filters if filters is not None else existing.filters
                    ),
                )
                return ToolResult(
                    tool_call_id="",
                    content=f"Updated metric '{name}': {expression}",
                )
            else:
                m = Metric(
                    name=name,
                    display_name=display_name or name,
                    expression=expression,
                    table=table,
                    description=description,
                    filters=filters or [],
                )
                self._layer.add_metric(m)
                return ToolResult(
                    tool_call_id="",
                    content=f"Defined new metric '{name}': {expression}",
                )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
