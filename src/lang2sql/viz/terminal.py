"""Terminal chart rendering using plotext (optional dependency)."""

from __future__ import annotations

from typing import Any

try:
    import plotext as plt

    _HAS_PLOTEXT = True
except ImportError:
    plt = None  # type: ignore[assignment]
    _HAS_PLOTEXT = False


def is_available() -> bool:
    """Check if plotext is installed."""
    return _HAS_PLOTEXT


def render_line_chart(
    rows: list[dict[str, Any]],
    x_col: str,
    y_col: str,
    title: str = "",
) -> str:
    """Render a line chart as terminal text."""
    if not _HAS_PLOTEXT:
        return _fallback_table(rows, title)

    x_vals = [r[x_col] for r in rows]
    y_vals = [float(r[y_col]) for r in rows]

    plt.clear_figure()
    plt.plot(y_vals, label=y_col)
    plt.xticks(range(len(x_vals)), [str(v) for v in x_vals])
    if title:
        plt.title(title)
    return plt.build()


def render_bar_chart(
    rows: list[dict[str, Any]],
    x_col: str,
    y_col: str,
    title: str = "",
    horizontal: bool = False,
) -> str:
    """Render a bar chart as terminal text."""
    if not _HAS_PLOTEXT:
        return _fallback_table(rows, title)

    labels = [str(r[x_col]) for r in rows]
    values = [float(r[y_col]) for r in rows]

    plt.clear_figure()
    if horizontal:
        plt.simple_bar(labels, values, title=title or y_col, width=60)
    else:
        plt.simple_bar(labels, values, title=title or y_col, width=60)
    return plt.build()


def render_scatter(
    rows: list[dict[str, Any]],
    x_col: str,
    y_col: str,
    title: str = "",
) -> str:
    """Render a scatter plot as terminal text."""
    if not _HAS_PLOTEXT:
        return _fallback_table(rows, title)

    x_vals = [float(r[x_col]) for r in rows]
    y_vals = [float(r[y_col]) for r in rows]

    plt.clear_figure()
    plt.scatter(x_vals, y_vals, label=f"{x_col} vs {y_col}")
    if title:
        plt.title(title)
    return plt.build()


def render_stat_card(value: Any, label: str = "") -> str:
    """Render a single-value stat card."""
    formatted = f"{value:,.2f}" if isinstance(value, float) else str(value)
    border = "─" * (len(formatted) + 4)
    return f"┌{border}┐\n│  {formatted}  │\n└{border}┘\n{label}"


def _fallback_table(rows: list[dict[str, Any]], title: str = "") -> str:
    """Simple text table when plotext is not available."""
    if not rows:
        return "(no data)"
    cols = list(rows[0].keys())
    header = " | ".join(f"{c:>12}" for c in cols)
    sep = "-+-".join("-" * 12 for _ in cols)
    lines = [title, header, sep] if title else [header, sep]
    for r in rows[:20]:
        lines.append(" | ".join(f"{str(r.get(c, '')):>12}" for c in cols))
    if len(rows) > 20:
        lines.append(f"... ({len(rows)} total rows)")
    return "\n".join(lines)
