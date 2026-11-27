"""save_column_notes — store user-confirmed column descriptions for explore_schema.

Writes the same ``enriched_desc:{table}:{column}`` keys that ``enrich_schema``
produces and ``explore_schema`` already overlays, but without an internal LLM:
the external agent drafts descriptions, the user confirms, this tool saves.
DB comments still win; notes only fill columns whose comment is empty.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from ..core.ports.audit import AuditEvent
from ..core.types import ToolResult, ToolSpec
from .explore_schema import _KV_PREFIX

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

logger = logging.getLogger(__name__)


class SaveColumnNotes:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="save_column_notes",
            description=(
                "Save column descriptions the user confirmed, for one table. "
                "explore_schema shows them for columns without a DB comment. "
                "Organization-wide metadata: requires an admin connection."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "table": {"type": "string", "description": "table name"},
                    "notes": {
                        "type": "object",
                        "description": "column name -> confirmed description",
                        "additionalProperties": {"type": "string"},
                    },
                },
                "required": ["table", "notes"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: HarnessContext) -> ToolResult:
        if not ctx.identity.is_admin:
            logger.warning("save_column_notes denied scope=%s actor=%s reason=not_admin", ctx.identity.kv_scope, ctx.identity.user_id)
            return ToolResult(
                call_id="",
                is_error=True,
                content="Column notes are organization metadata; this connection is not an admin.",
            )
        if ctx.store is None or ctx.explorer is None:
            return ToolResult(
                call_id="", is_error=True, content="No metadata store or DB connected."
            )
        notes = args.get("notes")
        if not isinstance(notes, dict) or not notes:
            return ToolResult(call_id="", is_error=True, content="notes must be a non-empty object")

        table = await ctx.explorer.describe_table(str(args.get("table", "")).strip())
        columns = {c.name.casefold(): c.name for c in table.columns}
        unknown = [name for name in notes if str(name).casefold() not in columns]
        if not columns or unknown:
            return ToolResult(
                call_id="",
                is_error=True,
                content=f"Unknown columns for {table.qualified}: {', '.join(map(str, unknown)) or '(table not found)'}",
            )

        saved = {}
        for name, text in notes.items():
            column = columns[str(name).casefold()]
            description = str(text).strip()
            if not description:
                return ToolResult(call_id="", is_error=True, content=f"Empty description for {column}")
            ctx.store.kv_set(
                ctx.identity.kv_scope, f"{_KV_PREFIX}:{table.name}:{column}", description
            )
            saved[column] = description

        logger.info(
            "save_column_notes saved scope=%s table=%s columns=%d",
            ctx.identity.kv_scope,
            table.name,
            len(saved),
        )
        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="save_column_notes",
                    scope=ctx.identity.kv_scope,
                    detail={"table": table.name, "columns": sorted(saved)},
                )
            )
        lines = "\n".join(f"- {c}: {d}" for c, d in saved.items())
        return ToolResult(call_id="", content=f"Saved {len(saved)} note(s) for {table.qualified}:\n{lines}")
