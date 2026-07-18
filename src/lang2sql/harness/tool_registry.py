"""Tool registry — name→tool dispatch and spec catalog for the loop."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core.ports.tool import ToolPort
from ..core.types import ToolResult, ToolSpec

if TYPE_CHECKING:
    from .context import HarnessContext


class ToolRegistry:
    def __init__(self, tools: list[ToolPort] | None = None) -> None:
        self._tools: dict[str, ToolPort] = {}
        for tool in tools or []:
            self.register(tool)

    def register(self, tool: ToolPort) -> None:
        self._tools[tool.spec.name] = tool

    def specs(self) -> list[ToolSpec]:
        return [t.spec for t in self._tools.values()]

    async def dispatch(
        self, name: str, args: dict[str, Any], ctx: "HarnessContext", call_id: str
    ) -> ToolResult:
        tool = self._tools.get(name)
        if tool is None:
            return ToolResult(
                call_id=call_id, content=f"unknown tool: {name}", is_error=True
            )
        try:
            result = await tool.run(args, ctx)
            result.call_id = call_id  # tools don't know their call id; stamp it here
            return result
        except Exception as exc:  # tools must never crash the loop
            return ToolResult(
                call_id=call_id, content=f"{type(exc).__name__}: {exc}", is_error=True
            )
