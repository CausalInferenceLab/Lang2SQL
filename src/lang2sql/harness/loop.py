"""agent_loop — the turn engine.

One call per user message: build the system prompt, then iterate
LLM → tool calls → LLM until the model returns a final answer (no tool calls)
or ``max_turns`` is hit. Tool failures come back as tool messages so the model
can recover rather than crashing the loop.
"""

from __future__ import annotations

from dataclasses import replace

from ..core.types import Message, Role
from .context import HarnessContext
from .system_prompt import build_system_prompt


async def agent_loop(ctx: HarnessContext, user_text: str) -> str:
    """Run one user turn to completion; return the final assistant text."""
    ctx.session.add(Message(role=Role.USER, content=user_text))

    if ctx.explorer is not None:
        ctx = replace(ctx, known_tables=await ctx.explorer.list_tables())
    system = await build_system_prompt(ctx)
    specs = ctx.tools.specs()

    for _ in range(ctx.max_turns):
        messages = [Message(role=Role.SYSTEM, content=system), *ctx.session.history()]
        completion = await ctx.llm.complete(messages, specs)

        assistant = Message(
            role=Role.ASSISTANT,
            content=completion.content,
            tool_calls=completion.tool_calls,
        )
        ctx.session.add(assistant)

        if not completion.tool_calls:
            return completion.content

        for call in completion.tool_calls:
            result = await ctx.tools.dispatch(call.name, call.arguments, ctx, call.id)
            ctx.session.add(
                Message(
                    role=Role.TOOL,
                    content=result.content,
                    tool_call_id=result.call_id,
                    name=call.name,
                )
            )

    return "(reached max turns without a final answer)"
