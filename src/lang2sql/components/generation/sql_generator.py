from __future__ import annotations

import re
from typing import Optional

from ...core.base import BaseComponent
from ...core.catalog import CatalogEntry
from ...core.exceptions import ComponentError
from ...core.hooks import TraceHook
from ...core.ports import LLMPort

_DEFAULT_SYSTEM_PROMPT = (
    "You are a SQL expert. Given a natural language question and relevant table schemas, "
    "write a single SQL query that answers the question. "
    "Return ONLY the SQL query inside a ```sql ... ``` code block. "
    "Do not include any explanation."
)


class SQLGenerator(BaseComponent):
    """Generates a SQL string from a natural language query and schema context."""

    def __init__(
        self,
        *,
        llm: LLMPort,
        system_prompt: str = _DEFAULT_SYSTEM_PROMPT,
        name: Optional[str] = None,
        hook: Optional[TraceHook] = None,
    ) -> None:
        super().__init__(name=name or "SQLGenerator", hook=hook)
        self._llm = llm
        self._system_prompt = system_prompt

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
