"""HTML chart export using Plotly (optional dependency)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import plotly.graph_objects as go  # type: ignore[import]

    _HAS_PLOTLY = True
except ImportError:
    go = None  # type: ignore[assignment]
    _HAS_PLOTLY = False


def is_available() -> bool:
    """Check if plotly is installed."""
    return _HAS_PLOTLY


def export_html(
    rows: list[dict[str, Any]],
    chart_type: str,
    x_col: str,
    y_col: str,
    title: str = "",
    output_path: str = "chart.html",
) -> str:
    """Export an interactive HTML chart using Plotly.

    Returns the output file path.
    """
    if not _HAS_PLOTLY:
        raise ImportError("plotly is required for HTML export: pip install plotly")

    x_vals = [r.get(x_col) for r in rows]
    y_vals = [r.get(y_col) for r in rows]

    fig: Any
    if chart_type == "line":
        fig = go.Figure(go.Scatter(x=x_vals, y=y_vals, mode="lines+markers", name=y_col))
    elif chart_type in ("bar", "horizontal_bar"):
        orientation = "h" if chart_type == "horizontal_bar" else "v"
        if orientation == "h":
            fig = go.Figure(go.Bar(x=y_vals, y=x_vals, orientation="h", name=y_col))
        else:
            fig = go.Figure(go.Bar(x=x_vals, y=y_vals, name=y_col))
    elif chart_type == "scatter":
        fig = go.Figure(go.Scatter(x=x_vals, y=y_vals, mode="markers", name=f"{x_col} vs {y_col}"))
    elif chart_type == "histogram":
        fig = go.Figure(go.Histogram(x=y_vals, name=y_col))
    else:
        # Default: table
        fig = go.Figure(go.Table(
            header=dict(values=list(rows[0].keys()) if rows else []),
            cells=dict(values=[[r.get(k) for r in rows] for k in (rows[0].keys() if rows else [])]),
        ))

    fig.update_layout(title=title)
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    fig.write_html(output_path)
    return output_path
