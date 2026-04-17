"""AskUser tool — harness-intercepted tool for clarifying questions."""

from __future__ import annotations

from ..harness.tool import ToolSpec
from ..harness.types import ToolResult


class AskUser:
    """Ask the user a clarifying question.

    The harness loop intercepts this tool and yields a ``UserPromptEvent``
    instead of executing it.
    """

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="ask_user",
            description=(
                "Ask the user a clarifying question when you need more "
                "information. Use this when: the meaning of a business term "
                "is unclear, you need to confirm assumptions about data, "
                "or the user's intent is ambiguous."
            ),
            input_schema={
                "type": "object",
                "properties": {
                    "question": {
                        "type": "string",
                        "description": "The question to ask the user",
                    },
                    "options": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of suggested answers",
                    },
                },
                "required": ["question"],
            },
        )

    def execute(self, **kwargs: object) -> ToolResult:
        return ToolResult(
            tool_call_id="", content="(handled by harness loop)"
        )
