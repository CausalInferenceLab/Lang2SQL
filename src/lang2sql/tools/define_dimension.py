"""DefineDimension tool — define or update semantic layer dimensions."""

from __future__ import annotations

from typing import Any, Literal

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class DefineDimension:
    """Define or update a dimension in the semantic layer."""

    def __init__(self, layer: Any) -> None:
        self._layer = layer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="define_dimension",
            description=(
                "Define or update a dimension in the semantic layer. "
                "A dimension is an axis for grouping or filtering data "
                "like time period, region, or category."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": (
                            "Dimension identifier (e.g., 'order_month')"
                        ),
                    },
                    "display_name": {
                        "type": "string",
                        "description": (
                            "Human-readable name (e.g., '주문월')"
                        ),
                    },
                    "expression": {
                        "type": "string",
                        "description": (
                            "SQL expression "
                            "(e.g., \"DATE_TRUNC('month', orders.order_date)\")"
                        ),
                    },
                    "table": {
                        "type": "string",
                        "description": "Primary table for this dimension",
                    },
                    "type": {
                        "type": "string",
                        "enum": ["time", "categorical", "geographic"],
                        "description": "Dimension type (default: categorical)",
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
        type: Literal["time", "categorical", "geographic"] = "categorical",
        **_: Any,
    ) -> ToolResult:
        from ..semantic.types import Dimension

        try:
            existing = self._layer.get_dimension(name)
            if existing:
                self._layer.update_dimension(
                    name,
                    expression=expression,
                    table=table,
                    display_name=display_name or existing.display_name,
                    type=type,
                )
                return ToolResult(
                    tool_call_id="",
                    content=f"Updated dimension '{name}': {expression}",
                )
            else:
                d = Dimension(
                    name=name,
                    display_name=display_name or name,
                    expression=expression,
                    table=table,
                    type=type,
                )
                self._layer.add_dimension(d)
                return ToolResult(
                    tool_call_id="",
                    content=f"Defined new dimension '{name}': {expression}",
                )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
