from __future__ import annotations

import re
from pathlib import Path
from typing import Optional

from ...core.base import BaseComponent
from ...core.catalog import CatalogEntry
from ...core.exceptions import ComponentError
from ...core.hooks import TraceHook
from ...core.ports import LLMPort

_PROMPT_DIR = Path(__file__).parent / "prompts"

_SUPPORTED_DIALECTS = {"default", "sqlite", "postgresql", "mysql", "bigquery", "duckdb"}


def _load_prompt(dialect: str) -> str:
    path = _PROMPT_DIR / f"{dialect}.md"
    if not path.exists():
        raise ValueError(
            f"Unsupported dialect: {dialect!r}. "
            f"Available: {sorted(_SUPPORTED_DIALECTS)}"
        )
    return path.read_text(encoding="utf-8").strip()


class SQLGenerator(BaseComponent):
    """Generates a SQL string from a natural language query and schema context.

    System prompt priority (highest to lowest):
    1. ``system_prompt`` — explicit string passed by the caller
    2. ``db_dialect``    — loads the matching ``prompts/{dialect}.md``
    3. default           — loads ``prompts/default.md``
    """

    def __init__(
        self,
        *,
        llm: LLMPort,
        db_dialect: Optional[str] = None,
        system_prompt: Optional[str] = None,
        name: Optional[str] = None,
        hook: Optional[TraceHook] = None,
    ) -> None:
        super().__init__(name=name or "SQLGenerator", hook=hook)
        self._llm = llm

        if system_prompt is not None:
            self._system_prompt = system_prompt
        elif db_dialect is not None:
            self._system_prompt = _load_prompt(db_dialect)
        else:
            self._system_prompt = _load_prompt("default")

    def _run(self, query: str, schemas: list[CatalogEntry]) -> str:
        context = self._build_context(schemas)
        messages = [
            {"role": "system", "content": self._system_prompt},
            {"role": "user", "content": f"Schemas:\n{context}\n\nQuestion: {query}"},
        ]
        response = self._llm.invoke(messages)
        sql = self._extract_sql(response)
        if not sql:
            raise ComponentError(
                self.name,
                "LLM response did not contain a ```sql ... ``` code block.",
            )
        return sql

    def _build_context(self, schemas: list[CatalogEntry]) -> str:
        parts: list[str] = []
        for entry in schemas:
            name = entry.get("name", "(unnamed)")
            description = entry.get("description", "")
            columns = entry.get("columns", {})

            lines = [f"Table: {name}"]
            if description:
                lines.append(f"  Description: {description}")
            if columns:
                lines.append("  Columns:")
                for col, col_desc in columns.items():
                    lines.append(f"    - {col}: {col_desc}")
            parts.append("\n".join(lines))
        return "\n\n".join(parts)

    @staticmethod
    def _extract_sql(text: str) -> str:
        match = re.search(r"```sql\s*(.*?)```", text, re.DOTALL | re.IGNORECASE)
        if not match:
            return ""
        sql = match.group(1).strip()
        sql = sql.rstrip(";").rstrip()
        return sql
