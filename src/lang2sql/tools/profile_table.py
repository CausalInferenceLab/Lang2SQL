"""ProfileTable tool — profile a database table's columns."""

from __future__ import annotations

import re
from typing import Any

from ..core.ports import DBExplorerPort
from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class ProfileTable:
    """Profile a database table — null rates, distinct counts, value ranges."""

    def __init__(self, explorer: DBExplorerPort) -> None:
        self._explorer = explorer

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="profile_table",
            description=(
                "Profile a database table. Shows row count, "
                "null rates, distinct counts, and min/max values "
                "for each column."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "The table to profile",
                    },
                },
                "required": ["table_name"],
            },
        )

    def execute(self, table_name: str, **_: Any) -> ToolResult:
        try:
            columns = self._extract_columns(table_name)
            if not columns:
                return ToolResult(
                    tool_call_id="",
                    content=f"Could not determine columns for `{table_name}`.",
                    is_error=True,
                )

            sql = self._build_profile_query(table_name, columns)
            rows = self._explorer.execute_read_only(sql)
            if not rows:
                return ToolResult(
                    tool_call_id="",
                    content=f"Table `{table_name}` returned no profile data.",
                    data={},
                )

            profile = rows[0]
            content = self._format_profile(table_name, columns, profile)
            return ToolResult(
                tool_call_id="", content=content, data=profile,
            )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )

    # ------------------------------------------------------------------ #
    #  Internals                                                           #
    # ------------------------------------------------------------------ #

    def _extract_columns(self, table_name: str) -> list[str]:
        """Extract column names from DDL."""
        ddl = self._explorer.get_ddl(table_name)
        # Try to get column names from sample_data first (more reliable).
        sample = self._explorer.sample_data(table_name, limit=1)
        if sample:
            return list(sample[0].keys())
        # Fallback: parse column names from DDL (CREATE TABLE ... (...))
        match = re.search(r"\((.+)\)", ddl, re.DOTALL)
        if not match:
            return []
        body = match.group(1)
        cols: list[str] = []
        for line in body.split(","):
            token = line.strip().split()[0].strip('"').strip("`").strip("'")
            # Skip constraints
            if token.upper() in (
                "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT", "INDEX",
            ):
                continue
            if token:
                cols.append(token)
        return cols

    def _build_profile_query(
        self, table_name: str, columns: list[str]
    ) -> str:
        """Build a single SQL query that profiles all columns."""
        parts: list[str] = ["COUNT(*) AS total_rows"]
        for col in columns:
            q = f'"{col}"'
            parts.append(f"COUNT(DISTINCT {q}) AS \"{col}__distinct\"")
            parts.append(
                f"SUM(CASE WHEN {q} IS NULL THEN 1 ELSE 0 END) "
                f"AS \"{col}__nulls\""
            )
            parts.append(f"MIN({q}) AS \"{col}__min\"")
            parts.append(f"MAX({q}) AS \"{col}__max\"")
        select = ",\n       ".join(parts)
        return f"SELECT {select}\nFROM \"{table_name}\""

    def _format_profile(
        self,
        table_name: str,
        columns: list[str],
        profile: dict[str, Any],
    ) -> str:
        """Format profile results as readable markdown."""
        total = profile.get("total_rows", "?")
        lines = [
            f"## Profile: `{table_name}`",
            f"**Total rows:** {total}",
            "",
            "| Column | Distinct | Nulls | Null % | Min | Max |",
            "| --- | --- | --- | --- | --- | --- |",
        ]
        for col in columns:
            distinct = profile.get(f"{col}__distinct", "?")
            nulls = profile.get(f"{col}__nulls", "?")
            mn = profile.get(f"{col}__min", "?")
            mx = profile.get(f"{col}__max", "?")
            if isinstance(total, int) and isinstance(nulls, int) and total > 0:
                pct = f"{nulls / total * 100:.1f}%"
            else:
                pct = "?"
            lines.append(
                f"| {col} | {distinct} | {nulls} | {pct} | {mn} | {mx} |"
            )
        return "\n".join(lines)
