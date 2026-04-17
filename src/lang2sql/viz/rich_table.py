"""Rich-based table rendering for terminal output."""

from __future__ import annotations

from typing import Any

try:
    from rich.console import Console
    from rich.table import Table

    _HAS_RICH = True
except ImportError:
    _HAS_RICH = False


def is_available() -> bool:
    """Check if rich is installed."""
    return _HAS_RICH


def render_table(
    rows: list[dict[str, Any]],
    title: str = "",
    max_rows: int = 50,
    max_col_width: int = 40,
) -> str:
    """Render data as a rich-formatted table string."""
    if not _HAS_RICH:
        return _plain_table(rows, title, max_rows)

    if not rows:
        return "(no data)"

    table = Table(title=title or None, show_lines=False)
    cols = list(rows[0].keys())

    for col in cols:
        table.add_column(col, overflow="ellipsis", max_width=max_col_width)

    for row in rows[:max_rows]:
        table.add_row(*(str(row.get(c, "")) for c in cols))

    if len(rows) > max_rows:
        table.caption = f"... {len(rows)} total rows (showing {max_rows})"

    console = Console(width=120, record=True)
    console.print(table)
    return console.export_text()


def _plain_table(
    rows: list[dict[str, Any]],
    title: str = "",
    max_rows: int = 50,
) -> str:
    """Fallback plain text table."""
    if not rows:
        return "(no data)"
    cols = list(rows[0].keys())
    header = " | ".join(f"{c:>15}" for c in cols)
    sep = "-+-".join("-" * 15 for _ in cols)
    lines = []
    if title:
        lines.append(title)
    lines.extend([header, sep])
    for r in rows[:max_rows]:
        lines.append(" | ".join(f"{str(r.get(c, '')):>15}" for c in cols))
    if len(rows) > max_rows:
        lines.append(f"... ({len(rows)} total rows)")
    return "\n".join(lines)
