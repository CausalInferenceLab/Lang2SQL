"""ask_user — request a clarification from the human.

The harness treats a successful clarification as a turn boundary. Any other
calls in that completion are cancelled, and only a new user turn can resume.

"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core.types import ToolResult, ToolSpec

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


class AskUser:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="ask_user",
            description="Ask the user a clarifying question when the request is ambiguous.",
            parameters={
                "type": "object",
                "properties": {"question": {"type": "string"}},
                "required": ["question"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        question = (args.get("question") or "").strip()
        if not question:
            return ToolResult(call_id="", content="no question provided", is_error=True)
        return ToolResult(
            call_id="", content=f"❓ Awaiting user: {question}", stop_turn=True
        )
