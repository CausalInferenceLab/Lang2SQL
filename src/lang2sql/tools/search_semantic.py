"""SearchSemantic tool — search the semantic layer by keyword."""

from __future__ import annotations

from typing import Any

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult
from ..semantic.layer import SemanticLayer


class SearchSemantic:
    """Search the semantic layer for metrics, dimensions, and business rules."""

    def __init__(self, layer: SemanticLayer) -> None:
        self._layer = layer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="search_semantic",
            description=(
                "Search the semantic layer for metrics, dimensions, "
                "and business rules by keyword. Returns matching "
                "definitions with their SQL expressions."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "query": {
                        "type": "string",
                        "description": "Search keyword",
                    },
                },
                "required": ["query"],
            },
        )

    def execute(self, query: str, **_: Any) -> ToolResult:
        try:
            results = self._layer.search(query)
            content = _format_results(query, results)
            return ToolResult(
                tool_call_id="",
                content=content,
                data=results,
            )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )


def _format_results(query: str, results: dict[str, list]) -> str:
    """Format search results as readable text."""
    metrics = results.get("metrics", [])
    dimensions = results.get("dimensions", [])
    rules = results.get("rules", [])

    total = len(metrics) + len(dimensions) + len(rules)
    if total == 0:
        return f"No results found for '{query}'."

    lines = [f"Found {total} result(s) for '{query}':", ""]

    if metrics:
        lines.append("### Metrics")
        for m in metrics:
            lines.append(
                f"- **{m.display_name}** (`{m.name}`): "
                f"`{m.expression}` on `{m.table}`"
            )
            if m.description:
                lines.append(f"  {m.description}")
            if m.filters:
                lines.append(f"  Filters: {', '.join(m.filters)}")
        lines.append("")

    if dimensions:
        lines.append("### Dimensions")
        for d in dimensions:
            lines.append(
                f"- **{d.display_name}** (`{d.name}`, {d.type}): "
                f"`{d.expression}` on `{d.table}`"
            )
        lines.append("")

    if rules:
        lines.append("### Business Rules")
        for r in rules:
            line = f"- **{r.name}**: {r.rule}"
            if r.sql_condition:
                line += f" → `{r.sql_condition}`"
            if r.applies_to:
                line += f"  (applies to: {', '.join(r.applies_to)})"
            lines.append(line)

    return "\n".join(lines)
