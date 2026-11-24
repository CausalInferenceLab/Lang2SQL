"""Assemble the system prompt for one turn.

Per v4.1 §2.1 the prompt injects: (1) the agent's role/rules, (2) the effective
semantic layer for the current scope, (3) recalled facts, (4) DB schema. V1
keeps each section simple; later versions enrich them without changing the
loop. Sections are suppressed when empty.
"""

from __future__ import annotations

import json
from dataclasses import dataclass

from .context import HarnessContext

_BASE = """\
You are Lang2SQL, a read-only data analytics agent.

Rules:
- Only ever read data. Never modify the database.
- Review SQL drafts with review_sql(sql, context). Use the original request,
  schema and business definitions to explain how the query meets the request
  or where it needs revision. Use the feedback to choose your next step.
- Before writing a database query, use explore_schema(tables=[...]) for
  relevant tables whose columns are missing from your context. Choose table
  names from Known tables. A list of table names does not provide columns.
- Prefer definitions from the semantic layer below over your own assumptions.
- If the user wants SQL, return the query in the requested format. If they want
  data, call run_sql with the final query and answer from its returned rows.

Clarification rule:
- If the user's query contains business terms (e.g. '활성고객', '월매출', '신규유저') that are NOT defined in the semantic layer and whose meaning is ambiguous, ask ONE concise clarifying question BEFORE running any SQL. Do not guess.
- After answering with SQL results, if you had to infer a term's meaning yourself, suggest the user save it: "이 정의를 저장하려면 `/term_custom`으로 등록해두세요. 다음 질문부터 자동 적용됩니다."
"""


@dataclass
class SystemPrompt:
    content: str
    tables_with_columns: set[str]


async def build_system_prompt(ctx: HarnessContext) -> SystemPrompt:
    parts: list[str] = [_BASE]
    tables_with_columns: set[str] = set()

    if ctx.explorer is not None:
        tables = ctx.known_tables
        if tables is None:
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
                        described = await ctx.explorer.describe_table(tbl.qualified)
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
                    if col_lines:
                        tables_with_columns.add(tbl.qualified)
                parts.append(
                    "## Known tables (with column descriptions)\n"
                    + "\n".join(schema_lines)
                )
            else:
                names = ", ".join(t.qualified for t in tables)
                parts.append("## Known tables\n" + names)

    if ctx.store is not None:
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

    return SystemPrompt("\n\n".join(parts), tables_with_columns)
