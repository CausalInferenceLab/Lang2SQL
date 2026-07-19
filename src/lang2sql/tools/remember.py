"""remember — persist a user fact via the memory service (★②).

V1 is the manual ``/remember`` path: the model (or a slash command) records a
fact verbatim. Recall/extraction strategies evolve behind MemoryService without
this tool changing. The service is injected at assembly time.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core.ports.audit import AuditEvent
from ..core.types import ToolResult, ToolSpec
from ..memory.service import MemoryService

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


class Remember:
    def __init__(self, memory: MemoryService) -> None:
        self._memory = memory

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="remember",
            description="Persist a fact about the user/data so it informs future turns.",
            parameters={
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        text = (args.get("text") or "").strip()
        if not text:
            return ToolResult(call_id="", content="nothing to remember", is_error=True)

        fact = await self._memory.remember(ctx.identity.user_id, text)
        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="remember",
                    scope=ctx.identity.session_key(),
                    detail={"fact_id": fact.id},
                )
            )
        return ToolResult(call_id="", content=f"🧠 Remembered: {text}")
