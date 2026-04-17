"""Visualize tool — create visualization specs for the TUI."""

from __future__ import annotations

from typing import Any

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class Visualize:
    """Create a visualization from query results.

    Returns a spec dict in ``data`` that the TUI reads to render the
    actual chart.
    """

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="visualize",
            description=(
                "Create a visualization from data. Automatically selects "
                "the best chart type based on data shape, or use a "
                "specific chart_type."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "chart_type": {
                        "type": "string",
                        "enum": [
                            "line",
                            "bar",
                            "horizontal_bar",
                            "scatter",
                            "histogram",
                            "table",
                            "auto",
                        ],
                        "default": "auto",
                    },
                    "x_column": {
                        "type": "string",
                        "description": "Column for x-axis",
                    },
                    "y_column": {
                        "type": "string",
                        "description": "Column for y-axis",
                    },
                },
                "required": ["title"],
            },
        )

    def execute(
        self,
        title: str,
        chart_type: str = "auto",
        x_column: str = "",
        y_column: str = "",
        **_: Any,
    ) -> ToolResult:
        viz_spec = {
            "title": title,
            "chart_type": chart_type,
            "x_column": x_column,
            "y_column": y_column,
        }
        return ToolResult(
            tool_call_id="",
            content=f"Visualization spec created: {chart_type} chart — {title}",
            data=viz_spec,
        )
