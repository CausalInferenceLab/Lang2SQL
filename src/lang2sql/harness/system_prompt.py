"""Dynamic system prompt builder for the agent loop."""

from __future__ import annotations

from pathlib import Path
from typing import Any

_PROMPTS_DIR = Path(__file__).parent / "prompts"


def _load_template(name: str) -> str:
    path = _PROMPTS_DIR / name
    return path.read_text(encoding="utf-8")


def _format_semantic_context(semantic_layer: dict[str, Any]) -> str:
    """Format the semantic layer for the system prompt."""
    if not semantic_layer:
        return "No semantic layer defined yet. Help the user build one in setup mode."

    parts: list[str] = ["### Semantic Layer"]

    metrics = semantic_layer.get("metrics", {})
    if metrics:
        parts.append("\n**Metrics:**")
        for name, m in metrics.items():
            display = m.get("display_name", name)
            expr = m.get("expression", "")
            filters = m.get("filters", [])
            desc = m.get("description", "")
            line = f"- **{display}** (`{name}`): `{expr}`"
            if filters:
                line += f"  filters: {', '.join(filters)}"
            if desc:
                line += f"  — {desc}"
            parts.append(line)

    dimensions = semantic_layer.get("dimensions", {})
    if dimensions:
        parts.append("\n**Dimensions:**")
        for name, d in dimensions.items():
            display = d.get("display_name", name)
            expr = d.get("expression", "")
            dim_type = d.get("type", "")
            parts.append(f"- **{display}** (`{name}`): `{expr}` [{dim_type}]")

    relationships = semantic_layer.get("relationships", [])
    if relationships:
        parts.append("\n**Relationships:**")
        for r in relationships:
            parts.append(
                f"- {r['from_table']} {r.get('join_type', '→')} {r['to_table']} "
                f"ON {r.get('on_clause', '?')}"
            )

    rules = semantic_layer.get("business_rules", [])
    if rules:
        parts.append("\n**Business Rules:**")
        for r in rules:
            parts.append(f"- {r.get('name', '?')}: {r.get('rule', '')}")

    return "\n".join(parts)


def _format_schema_context(schema_cache: dict[str, str]) -> str:
    """Format cached DDL for the system prompt."""
    if not schema_cache:
        return "No schema cached yet. Use `explore_schema` to discover tables."

    parts = ["### Database Schema (cached)"]
    for table_name, ddl in schema_cache.items():
        parts.append(f"\n```sql\n-- {table_name}\n{ddl}\n```")
    return "\n".join(parts)


def build_system_prompt(
    *,
    semantic_layer: dict[str, Any] | None = None,
    schema_cache: dict[str, str] | None = None,
    mode: str = "query",
    dialect: str = "sqlite",
    tool_descriptions: str = "",
) -> str:
    """Assemble the full system prompt for the agent loop.

    Combines:
    1. Base agent instructions (agent_system.md)
    2. Planning mode instructions (planning.md) — always included
    3. Semantic layer context
    4. Schema context
    5. Tool descriptions (auto-generated from ToolRegistry)
    """
    template = _load_template("agent_system.md")
    planning = _load_template("planning.md")

    semantic_ctx = _format_semantic_context(semantic_layer or {})
    schema_ctx = _format_schema_context(schema_cache or {})

    prompt = template.format(
        semantic_layer_context=semantic_ctx,
        schema_context=schema_ctx,
        mode=mode,
        dialect=dialect,
    )

    prompt += "\n\n" + planning

    if tool_descriptions:
        prompt += "\n\n## Available Tools\n\n" + tool_descriptions

    return prompt
