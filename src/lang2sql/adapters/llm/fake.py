"""FakeLLM — a scripted LLMPort for skeleton validation (no network).

It drives one full tool cycle so the agent loop can be exercised end-to-end
before the real OpenAI adapter exists: on the first turn it requests the first
available tool; once it sees a tool result it produces a final answer quoting
that result. Replaced by ``adapters/llm/openai_.py`` in Week 2.
"""

from __future__ import annotations

import json
from typing import Sequence

from ...core.types import Completion, Message, Role, ToolCall, ToolSpec


class FakeLLM:
    """Deterministic stand-in implementing :class:`LLMPort`."""

    def __init__(self) -> None:
        self._counter = 0

    async def complete(
        self,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> Completion:
        last = messages[-1] if messages else None

        # Saw a tool result → wrap up.
        if last is not None and last.role == Role.TOOL:
            return Completion(
                content=f"Done. Tool reported: {last.content}",
                finish_reason="stop",
            )

        # First pass with a tool available → call it.
        if tools:
            self._counter += 1
            spec = tools[0]
            return Completion(
                tool_calls=[
                    ToolCall(
                        id=f"call_{self._counter}",
                        name=spec.name,
                        arguments=json.loads(_demo_args(spec)),
                    )
                ],
                finish_reason="tool_calls",
            )

        # No tools at all → just answer.
        return Completion(
            content="(no tools available) Hello from FakeLLM.", finish_reason="stop"
        )


def _demo_args(spec: ToolSpec) -> str:
    """Best-effort sample args from a tool's JSON-Schema, as a JSON string."""
    props = (spec.parameters or {}).get("properties", {})
    sample = {name: "demo" for name in props}
    return json.dumps(sample)
