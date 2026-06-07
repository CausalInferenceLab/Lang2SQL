"""Assemble the system prompt for one turn.

Per v4.1 §2.1 the prompt injects: (1) the agent's role/rules, (2) the effective
semantic layer for the current scope, (3) recalled facts, (4) DB schema. V1
keeps each section simple; later versions enrich them without changing the
loop. Sections are suppressed when empty.
"""

from __future__ import annotations

import json

from .context import HarnessContext

_BASE = """\
You are Lang2SQL, a read-only data analytics agent.

Rules:
- Only ever read data. Never modify the database.
- When you need data, call the run_sql tool with a single SELECT/WITH query.
- Discover schema with explore_schema before guessing table or column names.
- Prefer definitions from the semantic layer below over your own assumptions.
- Answer concisely. Show only the final successful SQL you ran, not intermediate attempts.
"""


async def build_system_prompt(ctx: HarnessContext) -> str:
    parts: list[str] = [_BASE]

    if ctx.scope_resolver is not None:
        layer = await ctx.scope_resolver.effective_layer(ctx.identity)
        rendered = layer.render() if layer is not None else ""
        if rendered:
            parts.append("## Semantic layer (effective for this scope)\n" + rendered)

    if ctx.explorer is not None:
        tables = await ctx.explorer.list_tables()
        if tables:
            scope = (ctx.identity.guild_id or f"dm:{ctx.identity.user_id}") if ctx.store else None
            has_enrichment = bool(
                scope and ctx.store and
                ctx.store.kv_get(scope, "schema_relationships")
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
                        desc = col.description or ctx.store.kv_get(scope, f"enriched_desc:{tbl.name}:{col.name}") or ""
                        col_lines.append(f"  - {col.name}{': ' + desc if desc else ''}")
                    schema_lines.append(f"- {tbl.qualified}\n" + "\n".join(col_lines))
                parts.append("## Known tables (with column descriptions)\n" + "\n".join(schema_lines))
            else:
                names = ", ".join(t.qualified for t in tables)
                parts.append("## Known tables\n" + names)

    if ctx.store is not None:
        scope = ctx.identity.guild_id or f"dm:{ctx.identity.user_id}"
        raw = ctx.store.kv_get(scope, "schema_relationships")
        if raw:
            try:
                rels = json.loads(raw)
                if rels:
                    rel_text = "\n".join(f"- {r}" for r in rels)
                    parts.append("## Table relationships (use these for JOINs)\n" + rel_text)
            except (ValueError, TypeError):
                pass

        from ..tools.semantic_federation import build_prompt_section
        user_id = ctx.identity.user_id or "unknown"
        channel_id = ctx.identity.thread_id or ctx.identity.channel_id or ""
        semfed_section = build_prompt_section(ctx.store, scope, channel_id, user_id)
        if semfed_section:
            parts.append(semfed_section)

    return "\n\n".join(parts)
