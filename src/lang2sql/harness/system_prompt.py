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

Query correctness:
- Before constructing SQL, identify the requested output columns, row grain, population, time boundaries, and aggregation. Return only requested columns, not diagnostic identifiers or intermediate statistics.
- Inspect all relevant tables and join keys. Check relationship direction and join multiplicity; do not replace a declared business identifier with a database's implicit row identifier.
- Do not add unstated filters, exclude zero-valued records, or fill missing values unless the question or supplied definitions require it. Check whether ties and empty groups must be retained.
- Preserve result types: round numeric results numerically; format numbers as text only when explicitly requested. Before returning, check that the executed result matches the requested output contract.

Data verification:
- A successful query is not proof of correctness. Compare the final SQL against every requested output, filter, denominator, grouping and ordering requirement before answering.
- When several columns or tables describe a similar concept, inspect each candidate and prefer the explicitly named field/source. Do not silently substitute a convenient near-synonym.
- Profile relevant stored values with small read-only queries before deciding numeric casts, date formats or missing-value handling. SQL NULL, empty strings, sentinel values and genuine zero are different; use documented meanings and observed values, not invented cleaning rules.
- For rates, check numerator and denominator at the requested grain and precision instead of assuming a stored rounded percentage is interchangeable. Preserve requested identifiers and categorical strings verbatim; do not prettify their case or change types.
- Check the effect of every join on row counts. Do not invent DISTINCT, extra legal/status restrictions, zero-filled groups, or implicit deduplication when the question does not request them.

Clarification rule:
- If the user's query contains business terms (e.g. '활성고객', '월매출', '신규유저') that are NOT defined in the semantic layer and whose meaning is ambiguous, first check supplied definitions and schema metadata. Read-only diagnostic queries may resolve stored codes or formats, but cannot establish an undocumented business policy. If the ambiguity remains, ask ONE concise clarifying question BEFORE executing the answer SQL. Do not guess.
- After answering with SQL results, if you had to infer a term's meaning yourself, suggest the user save it: "이 정의를 저장하려면 `/term_custom`으로 등록해두세요. 다음 질문부터 자동 적용됩니다."
"""


async def build_system_prompt(ctx: HarnessContext) -> str:
    parts: list[str] = [_BASE]

    if ctx.explorer is not None:
        dialect = getattr(ctx.explorer, "dialect", "")
        if dialect:
            parts.append(f"## SQL dialect\n{dialect}")
        if dialect == "oracle":
            parts.append(
                "Use Oracle SQL: DATE 'YYYY-MM-DD' literals and FETCH FIRST n ROWS ONLY, "
                "not LIMIT. Preserve schema-qualified table names. "
                "On a permission denial, explain the denial without changing credentials "
                "or searching other tables for the denied data."
            )
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

    return "\n\n".join(parts)
