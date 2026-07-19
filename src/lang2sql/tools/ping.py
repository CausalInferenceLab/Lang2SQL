"""ping — a trivial ctx-aware tool used to validate the tool dispatch path.

Stands in for real tools (run_sql, explore_schema, …) in Week 1: it proves the
ToolPort contract, the registry dispatch, and the loop's tool-result handling
all line up. Removed once the real tools land.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core.types import ToolResult, ToolSpec

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


class Ping:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="ping",
            description="Health-check tool. Echoes back a message.",
            parameters={
                "type": "object",
                "properties": {
                    "message": {"type": "string", "description": "text to echo"}
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        msg = args.get("message", "")
        return ToolResult(
            call_id="", content=f"pong: {msg!r} (user={ctx.identity.user_id})"
        )
