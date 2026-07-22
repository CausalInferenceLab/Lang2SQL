"""Assemble the system prompt for one turn.

Per v4.1 §2.1 the prompt injects: (1) the agent's role/rules, (2) the effective
semantic layer for the current scope, (3) recalled facts, (4) DB schema. V1
keeps each section simple; later versions enrich them without changing the
loop. Sections are suppressed when empty.
"""

from __future__ import annotations

import json

from .context import HarnessContext
from ..semantic.shortlist import prompt_table_section

_RAW_BASE = """\
You are Lang2SQL, a read-only data analytics agent.

Rules:
- Only ever read data. Never modify the database.
- When you need data, call the run_sql tool with a single SELECT/WITH query.
- Discover schema with explore_schema before guessing table or column names.
- Prefer definitions from the semantic layer below over your own assumptions.
- Answer concisely. Show only the final successful SQL you ran, not intermediate attempts.
"""

_GOVERNED_BASE = """\
You are Lang2SQL, a read-only data analytics agent using a governed semantic catalog.

Rules:
- Never write or return SQL yourself. Call semantic_query with catalog IDs only.
- Never call or imitate run_sql; it is intentionally unavailable in governed mode.
- Every data request must call semantic_query or ask_user; never answer from memory.
- Policy-blocked columns cannot be registered or exposed through another tool.
- Copy metric and dimension phrases exactly from the user's question.
- A new question phrase mapped to an existing catalog ID is representable by
  the one-time semantic review flow. Mapping novelty alone is not an unresolved
  obligation; keep the exact phrase in its metric or dimension slot.
- A phrase that only identifies the same source table or dataset already
  encoded by the selected catalog IDs is source context, not an unresolved
  obligation. This never applies to source choices, filters, locations, times,
  groupings, comparisons, modifiers, units, conversions, or operators.
- Select the requested aggregate explicitly; do not reuse another phrase's aggregate.
- List every requested filter, time rule, comparison, business modifier, unit,
  or operator the selected typed slots cannot represent in unresolved_obligations.
- Preserve every requested grouping, time basis, filter, business modifier, and unit.
- If the catalog or tool cannot represent one of those obligations, ask for clarification.
- Unknown IDs, blocked columns, and unsafe joins must stay blocked. Never invent a fallback.
- Answer concisely and explain any one-time semantic confirmation in plain language.
"""


async def build_system_prompt(ctx: HarnessContext) -> str:
    tool_names = {item.name for item in ctx.tools.specs()}
    base = _GOVERNED_BASE if "semantic_query" in tool_names else _RAW_BASE
    parts: list[str] = [base]

    if "semantic_query" in tool_names:
        if ctx.semantic_table_ids:
            parts.append(prompt_table_section(ctx.semantic_table_ids))
    elif ctx.explorer is not None:
        tables = await ctx.explorer.list_tables()
        if tables:
            scope = ctx.identity.kv_scope if ctx.store else None
            has_enrichment = bool(
                scope and ctx.store and ctx.store.kv_get(scope, "schema_relationships")
            )

            if has_enrichment and scope and ctx.store:
                schema_lines: list[str] = []
                for tbl in tables:
                    try:
                        described = await ctx.explorer.describe_table(tbl.name)
                    except Exception:
                        schema_lines.append(f"- {tbl.qualified}")
                        continue
                    col_lines = []
                    for col in described.columns:
                        desc = (
                            col.description
                            or ctx.store.kv_get(
                                scope, f"enriched_desc:{tbl.name}:{col.name}"
                            )
                            or ""
                        )
                        col_lines.append(f"  - {col.name}{': ' + desc if desc else ''}")
                    schema_lines.append(f"- {tbl.qualified}\n" + "\n".join(col_lines))
                parts.append(
                    "## Known tables (with column descriptions)\n"
                    + "\n".join(schema_lines)
                )
            else:
                names = ", ".join(t.qualified for t in tables)
                parts.append("## Known tables\n" + names)

    if ctx.store is not None and "semantic_query" not in tool_names:
        scope = ctx.identity.kv_scope
        raw = ctx.store.kv_get(scope, "schema_relationships")
        if raw:
            try:
                rels = json.loads(raw)
                if rels:
                    rel_text = "\n".join(f"- {r}" for r in rels)
                    parts.append(
                        "## Table relationships (use these for JOINs)\n" + rel_text
                    )
            except (ValueError, TypeError):
                pass

        from ..tools.semantic_federation import build_prompt_section

        user_id = ctx.identity.user_id or "unknown"
        channel_id = ctx.identity.effective_channel_id
        semfed_section = build_prompt_section(ctx.store, scope, channel_id, user_id)
        if semfed_section:
            parts.append(semfed_section)

    return "\n\n".join(parts)
