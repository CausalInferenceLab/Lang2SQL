"""Chart rendering widget and utility function."""

from __future__ import annotations

from typing import Any

try:
    from textual.widgets import Static

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False


def render_chart_text(
    rows: list[dict[str, Any]],
    chart_type: str,
    title: str,
    x_col: str = "",
    y_col: str = "",
) -> str:
    """Render a chart and return terminal text.

    This function does not require Textual and can be used standalone.
    """
    from ...viz.selector import select_chart_type
    from ...viz.terminal import (
        _fallback_table,
        render_bar_chart,
        render_line_chart,
        render_scatter,
        render_stat_card,
    )

    if chart_type == "auto":
        chart_type = select_chart_type(rows, x_column=x_col, y_column=y_col)

    if not rows:
        return "(no data to visualize)"

    if chart_type == "line":
        return render_line_chart(rows, x_col, y_col, title)
    if chart_type in ("bar", "horizontal_bar"):
        return render_bar_chart(
            rows, x_col, y_col, title, horizontal=(chart_type == "horizontal_bar")
        )
    if chart_type == "scatter":
        return render_scatter(rows, x_col, y_col, title)
    if chart_type == "stat_card":
        first_val = list(rows[0].values())[0] if rows else ""
        return render_stat_card(first_val, title)
    return _fallback_table(rows, title)


if _HAS_TEXTUAL:

    class ChartPanel(Static):
        """Renders terminal charts inline."""

        def render_chart(
            self,
            rows: list[dict[str, Any]],
            chart_type: str,
            title: str,
            x_col: str = "",
            y_col: str = "",
        ) -> str:
            """Render a chart, update this widget, and return the text."""
            text = render_chart_text(rows, chart_type, title, x_col, y_col)
            self.update(text)
            return text
