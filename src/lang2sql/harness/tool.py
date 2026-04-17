"""Tool protocol and registry for the harness agent loop."""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable

from .types import ToolResult


# ---------------------------------------------------------------------------
# ToolSpec — declarative tool metadata
# ---------------------------------------------------------------------------

@dataclass
class ToolSpec:
    """Describes a tool's name, purpose, and expected parameters.

    ``input_schema`` must be a valid JSON Schema object that specifies the
    tool's parameters (``type: "object"`` with ``properties``).
    """

    name: str
    description: str
    input_schema: dict[str, Any]


# ---------------------------------------------------------------------------
# Tool Protocol
# ---------------------------------------------------------------------------

@runtime_checkable
class Tool(Protocol):
    """Interface that every harness tool must satisfy."""

    @property
    def spec(self) -> ToolSpec: ...

    def execute(self, **kwargs: Any) -> ToolResult: ...


# ---------------------------------------------------------------------------
# ToolRegistry
# ---------------------------------------------------------------------------

class ToolRegistry:
    """Registry of available tools.

    Handles registration, lookup, and LLM schema generation.
    """

    def __init__(self) -> None:
        self._tools: dict[str, Tool] = {}

    # -- mutation -----------------------------------------------------------

    def register(self, tool: Tool) -> None:
        """Register a tool. Raises ``ValueError`` if name already registered."""
        name = tool.spec.name
        if name in self._tools:
            raise ValueError(f"Tool already registered: {name!r}")
        self._tools[name] = tool

    # -- lookup -------------------------------------------------------------

    def get(self, name: str) -> Tool:
        """Get tool by name. Raises ``KeyError`` if not found."""
        try:
            return self._tools[name]
        except KeyError:
            raise KeyError(f"Unknown tool: {name!r}") from None

    def list_tools(self) -> list[ToolSpec]:
        """List all registered tool specs."""
        return [t.spec for t in self._tools.values()]

    def __len__(self) -> int:
        return len(self._tools)

    def __contains__(self, name: str) -> bool:
        return name in self._tools

    # -- LLM integration ----------------------------------------------------

    def tool_specs_for_llm(self) -> list[dict[str, Any]]:
        """Generate tool definitions in the format expected by LLM APIs.

        Returns a list of dicts with ``name``, ``description``, and
        ``input_schema`` keys.  This format is compatible with both the
        OpenAI and Anthropic tool-calling APIs.
        """
        return [
            {
                "name": s.name,
                "description": s.description,
                "input_schema": s.input_schema,
            }
            for s in self.list_tools()
        ]

    # -- execution ----------------------------------------------------------

    async def execute(
        self,
        name: str,
        arguments: dict[str, Any],
        *,
        tool_call_id: str = "",
    ) -> ToolResult:
        """Execute a tool by name.

        Synchronous ``tool.execute()`` calls are wrapped with
        ``asyncio.to_thread()`` so they never block the event loop.

        On any exception the method returns a ``ToolResult`` with
        ``is_error=True`` instead of propagating.
        """
        try:
            tool = self.get(name)
            if inspect.iscoroutinefunction(tool.execute):
                result = await tool.execute(**arguments)
            else:
                result = await asyncio.to_thread(tool.execute, **arguments)

            # Ensure tool_call_id is set on the result
            if tool_call_id and not result.tool_call_id:
                result.tool_call_id = tool_call_id

            return result
        except Exception as exc:  # noqa: BLE001
            return ToolResult(
                tool_call_id=tool_call_id,
                content=f"[{type(exc).__name__}] {exc}",
                is_error=True,
            )
