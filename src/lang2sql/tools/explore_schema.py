"""explore_schema — let the agent discover tables/columns before writing SQL.

Read-only introspection through the :class:`ExplorerPort`. List available
tables, or describe the columns of one or more selected tables.
"""

from __future__ import annotations

from dataclasses import replace
from typing import TYPE_CHECKING, Any

from ..core.ports.explorer import Column, ExplorerPort, Table
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


def _format_columns(table: Table, ctx: "HarnessContext") -> str:
    table = _apply_enrichment_cache(table, ctx)
    cols = (
        "\n".join(
            f"- {c.name}: {c.type}{'' if c.nullable else ' NOT NULL'}"
            f"{(' — ' + c.description) if c.description else ''}"
            for c in table.columns
        )
        or "(no columns)"
    )
    return f"{table.qualified}\n{cols}"


async def _list_tables(explorer: ExplorerPort, ctx: "HarnessContext") -> list[Table]:
    if ctx.known_tables is not None:
        return ctx.known_tables
    return await explorer.list_tables()


async def _describe_tables(
    names: list[str], explorer: ExplorerPort, ctx: "HarnessContext"
) -> ToolResult:
    catalog = await _list_tables(explorer, ctx)
    qualified_names = {table.qualified: table for table in catalog}
    bare_names: dict[str, Table | None] = {}
    for candidate in catalog:
        bare_names[candidate.name] = (
            candidate if candidate.name not in bare_names else None
        )

    sections: list[str] = []
    seen: set[str] = set()
    has_error = False
    for name in dict.fromkeys(name.strip() for name in names):
        table = qualified_names.get(name) or bare_names.get(name)
        if table is None and name in bare_names:
            sections.append(f"{name}: ambiguous table. Include its schema name.")
            has_error = True
            continue

        if name in seen or (table is not None and table.qualified in seen):
            continue
        try:
            described = await explorer.describe_table(name)
            if described.qualified not in seen:
                sections.append(_format_columns(described, ctx))
            seen.update((name, described.qualified))
        except Exception as exc:
            sections.append(f"{name}: {type(exc).__name__}: {exc}")
            has_error = True
    return ToolResult(call_id="", content="\n\n".join(sections), is_error=has_error)


class ExploreSchema:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="explore_schema",
            description=(
                "List tables or describe their columns before writing SQL. "
                "Request multiple relevant tables together with 'tables'."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "table": {
                        "type": "string",
                        "description": "one table name; use either 'table' or 'tables'",
                    },
                    "tables": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                        "description": (
                            "names of relevant tables; omit both arguments to list tables"
                        ),
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        explorer = ctx.explorer
        if explorer is None:
            return ToolResult(
                call_id="", content="no DB connected (use /connect)", is_error=True
            )

        if "tables" in args:
            if "table" in args:
                return ToolResult(
                    call_id="", content="Use either 'table' or 'tables'.", is_error=True
                )
            names = args["tables"]
            if (
                not isinstance(names, list)
                or not names
                or any(not isinstance(name, str) or not name.strip() for name in names)
            ):
                return ToolResult(
                    call_id="",
                    content="'tables' must be a non-empty list of table names.",
                    is_error=True,
                )
            return await _describe_tables(names, explorer, ctx)

        table = (args.get("table") or "").strip()
        if not table:
            tables = await _list_tables(explorer, ctx)
            listed = "\n".join(f"- {t.qualified}" for t in tables) or "(no tables)"
            return ToolResult(call_id="", content="Tables:\n" + listed)

        described = await explorer.describe_table(table)
        return ToolResult(call_id="", content=_format_columns(described, ctx))
