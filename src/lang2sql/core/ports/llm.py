"""LLM port — the one outbound call the agent loop makes per turn.

Production wires CodexLLM behind this port. Because the loop
depends only on this Protocol, swapping in Anthropic/NIM later (v1.5+) is an
adapter add with zero loop changes.
"""

from __future__ import annotations

from typing import Protocol, Sequence, runtime_checkable

from ..types import Completion, Message, ToolSpec


@runtime_checkable
class LLMPort(Protocol):
    """Tool-calling chat completion."""

    async def complete(
        self,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> Completion:
        """Run one completion. May return tool calls, a final answer, or both."""
        ...
