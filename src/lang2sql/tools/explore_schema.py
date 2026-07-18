"""explore_schema — let the agent discover tables/columns before writing SQL.

Read-only introspection through the :class:`ExplorerPort`. With no ``table``
arg it lists tables; with one it returns full column detail.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ..core.ports.explorer import Column, Table
from ..core.types import ToolResult, ToolSpec

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

_KV_PREFIX = "enriched_desc"


def _apply_enrichment_cache(table: Table, ctx: "HarnessContext") -> Table:
    """Overlay KV-cached descriptions onto columns that lack one."""
    if ctx.store is None:
        return table
    scope = ctx.identity.kv_scope
    enriched_cols: list[Column] = []
    for col in table.columns:
        if col.description:
            enriched_cols.append(col)
            continue
        cached = ctx.store.kv_get(scope, f"{_KV_PREFIX}:{table.name}:{col.name}")
        enriched_cols.append(replace(col, description=cached or ""))
    return replace(table, columns=enriched_cols)


class ExploreSchema:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="explore_schema",
            description="List tables, or describe one table's columns. Call before writing SQL.",
            parameters={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "table name to describe; omit to list all tables",
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        if ctx.explorer is None:
            return ToolResult(
                call_id="", content="no DB connected (use /connect)", is_error=True
            )

        table = (args.get("table") or "").strip()
        if not table:
            tables = await ctx.explorer.list_tables()
            names = "\n".join(f"- {t.qualified}" for t in tables) or "(no tables)"
            return ToolResult(call_id="", content="Tables:\n" + names)

        t = await ctx.explorer.describe_table(table)
        t = _apply_enrichment_cache(t, ctx)
        cols = (
            "\n".join(
                f"- {c.name}: {c.type}{'' if c.nullable else ' NOT NULL'}"
                f"{(' — ' + c.description) if c.description else ''}"
                for c in t.columns
            )
            or "(no columns)"
        )
        return ToolResult(call_id="", content=f"{t.qualified}\n{cols}")
