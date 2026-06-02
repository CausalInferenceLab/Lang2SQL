"""Assemble the system prompt for one turn.

Per v4.1 §2.1 the prompt injects: (1) the agent's role/rules, (2) the effective
semantic layer for the current scope, (3) recalled facts, (4) DB schema. V1
keeps each section simple; later versions enrich them without changing the
loop. Sections are suppressed when empty.
"""

from __future__ import annotations

from .context import HarnessContext

_BASE = """\
You are Lang2SQL, a read-only data analytics agent.

Rules:
- Only ever read data. Never modify the database.
- When you need data, call the run_sql tool with a single SELECT/WITH query.
- Discover schema with explore_schema before guessing table or column names.
- Prefer definitions from the semantic layer below over your own assumptions.
- Answer concisely; show the SQL you ran.

Working with messy schemas (cryptic column names / no descriptions / dirty enums):
- After explore_schema, if a column's purpose is unclear (e.g. `amt`, `st`,
  `e_at`), call run_sql with a small `SELECT DISTINCT` or `LIMIT 5` to see the
  actual values. That tells you what a status enum or date field really holds.
- Once you have inferred a column's meaning or a value set, persist it for
  future turns: call `define_metric` to record a usable mapping (e.g. a metric
  whose definition is the SQL expression you'll keep reusing). Future questions
  in this scope will see it in the semantic layer above and won't have to
  re-guess.
- If business meaning is ambiguous (currency unit, what "active" means), ask
  the user with ask_user instead of inventing an answer.
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
            names = ", ".join(t.qualified for t in tables)
            parts.append("## Known tables\n" + names)

    return "\n\n".join(parts)
