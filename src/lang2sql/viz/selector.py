"""Intent-based chart type selection from data shape."""

from __future__ import annotations

from typing import Any


def select_chart_type(
    rows: list[dict[str, Any]],
    *,
    x_column: str = "",
    y_column: str = "",
    hint: str = "auto",
) -> str:
    """Select the best chart type based on data characteristics.

    Parameters
    ----------
    rows : data rows from SQL execution
    x_column : suggested x-axis column
    y_column : suggested y-axis column
    hint : "auto" for automatic selection, or explicit type

    Returns one of: "line", "bar", "horizontal_bar", "scatter",
    "histogram", "table", "stat_card"
    """
    if hint != "auto":
        return hint

    if not rows:
        return "table"

    # Single value → stat card
    if len(rows) == 1 and len(rows[0]) == 1:
        return "stat_card"

    # Detect column types from first row
    columns = list(rows[0].keys())
    col_types = _classify_columns(rows, columns)

    # If x_column specified, use it as guide
    x_col = x_column or (columns[0] if columns else "")
    y_col = y_column or (columns[1] if len(columns) > 1 else "")

    x_type = col_types.get(x_col, "unknown")
    y_type = col_types.get(y_col, "unknown")

    # Time x-axis → line chart (trend)
    if x_type == "time" and y_type == "numeric":
        return "line"

    # Categorical x + numeric y → bar chart
    if x_type == "categorical" and y_type == "numeric":
        if len(rows) > 10:
            return "horizontal_bar"
        return "bar"

    # Two numeric columns → scatter
    if x_type == "numeric" and y_type == "numeric":
        return "scatter"

    # Single numeric column → histogram
    if len(columns) == 1 and col_types.get(columns[0]) == "numeric":
        return "histogram"

    # Default → table
    return "table"


def _classify_columns(
    rows: list[dict[str, Any]],
    columns: list[str],
) -> dict[str, str]:
    """Classify each column as 'numeric', 'time', 'categorical', or 'unknown'."""
    result: dict[str, str] = {}
    sample = rows[:20]  # check first 20 rows

    for col in columns:
        values = [r.get(col) for r in sample if r.get(col) is not None]
        if not values:
            result[col] = "unknown"
            continue

        # Check numeric
        if all(isinstance(v, (int, float)) for v in values):
            result[col] = "numeric"
            continue

        # Check time-like strings
        str_values = [str(v) for v in values]
        if _looks_like_time(str_values):
            result[col] = "time"
            continue

        # Otherwise categorical
        result[col] = "categorical"

    return result


_TIME_PATTERNS = [
    # YYYY-MM-DD, YYYY-MM, YYYY
    r"\d{4}-\d{2}-\d{2}",
    r"\d{4}-\d{2}",
    r"\d{4}",
    # Q1 2024, 2024 Q1
    r"Q[1-4]\s*\d{4}",
    r"\d{4}\s*Q[1-4]",
]


def _looks_like_time(values: list[str]) -> bool:
    """Heuristic: do these string values look like time/date values?"""
    import re

    patterns = [re.compile(p) for p in _TIME_PATTERNS]
    matches = 0
    for v in values[:10]:
        if any(p.fullmatch(v.strip()) for p in patterns):
            matches += 1
    return matches >= len(values[:10]) * 0.7
