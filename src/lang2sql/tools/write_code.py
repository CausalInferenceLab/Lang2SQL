"""WriteCode tool — LLM-powered Python code generation."""

from __future__ import annotations

from typing import Any

from ..core.ports import LLMPort
from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class WriteCode:
    """Generate Python analysis code.

    The agent uses this to create Python code for complex analyses
    that go beyond SQL (statistics, ML, data transformations).
    The code is returned as a string for review before execution.
    """

    def __init__(self, llm: LLMPort) -> None:
        self._llm = llm

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="write_code",
            description=(
                "Generate Python analysis code. Use this for complex "
                "analyses that require statistics (scipy), ML (sklearn), "
                "or data transformations (pandas). The generated code "
                "will be shown to the user before execution."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "task": {
                        "type": "string",
                        "description": "What the code should do",
                    },
                    "data_description": {
                        "type": "string",
                        "description": (
                            "Description of the input data "
                            "(columns, types)"
                        ),
                    },
                    "libraries": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Python libraries to use "
                            "(e.g., pandas, scipy, sklearn)"
                        ),
                    },
                },
                "required": ["task"],
            },
        )

    def execute(
        self,
        task: str,
        data_description: str = "",
        libraries: list[str] | None = None,
        **_: Any,
    ) -> ToolResult:
        prompt = (
            "Generate Python analysis code for this task:\n\n"
            f"Task: {task}\n"
        )
        if data_description:
            prompt += f"\nInput data: {data_description}"
        if libraries:
            prompt += f"\nUse libraries: {', '.join(libraries)}"
        prompt += (
            "\n\nRequirements:\n"
            "- Read data from a variable called `df` "
            "(pandas DataFrame, already loaded)\n"
            "- Store the main result in a variable called `result`\n"
            "- Print a summary at the end\n"
            "- Include comments explaining key steps\n"
            "- Handle edge cases (empty data, missing values)\n"
        )

        try:
            code = self._llm.invoke([
                {
                    "role": "system",
                    "content": (
                        "You are a Python data analysis expert. "
                        "Generate clean, well-commented code. "
                        "Output ONLY the Python code, no markdown fences."
                    ),
                },
                {"role": "user", "content": prompt},
            ])
            # Strip any markdown code fences if present
            code = code.strip()
            if code.startswith("```"):
                lines = code.split("\n")
                if lines[-1].startswith("```"):
                    code = "\n".join(lines[1:-1])
                else:
                    code = "\n".join(lines[1:])
            return ToolResult(
                tool_call_id="",
                content=code,
                data={"code": code, "task": task},
            )
        except Exception as exc:
            return ToolResult(
                tool_call_id="",
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
