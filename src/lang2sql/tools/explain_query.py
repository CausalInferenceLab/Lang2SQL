"""ExplainQuery tool — LLM-powered SQL query explainer."""

from __future__ import annotations

from typing import Any

from ..core.ports import LLMPort
from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class ExplainQuery:
    """Explain a SQL query in natural language using an LLM."""

    def __init__(self, llm: LLMPort) -> None:
        self._llm = llm

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="explain_query",
            description=(
                "Explain what a SQL query does in plain language, "
                "including assumptions and potential issues."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "The SQL query to explain",
                    },
                    "question": {
                        "type": "string",
                        "description": (
                            "The original user question (for context)"
                        ),
                    },
                },
                "required": ["sql"],
            },
        )

    def execute(self, sql: str, question: str = "", **_: Any) -> ToolResult:
        prompt = (
            "Explain this SQL query in plain language. "
            "What does it calculate? What assumptions does it make? "
            "Are there any potential issues?\n\n"
            f"SQL:\n```sql\n{sql}\n```"
        )
        if question:
            prompt += f"\n\nOriginal question: {question}"

        try:
            explanation = self._llm.invoke([
                {
                    "role": "system",
                    "content": (
                        "You are a SQL expert. "
                        "Explain queries concisely in Korean."
                    ),
                },
                {"role": "user", "content": prompt},
            ])
            return ToolResult(tool_call_id="", content=explanation)
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
