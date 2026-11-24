"""agent_loop — the turn engine.

One call per user message: build the system prompt, then iterate
LLM → tool calls → LLM until the model returns a final answer (no tool calls)
or ``max_turns`` is hit. Tool failures come back as tool messages so the model
can recover rather than crashing the loop.
"""

from __future__ import annotations

import logging

from ..core.types import Message, Role, ToolCall

from .context import HarnessContext
from .system_prompt import build_system_prompt

logger = logging.getLogger(__name__)


async def agent_loop(ctx: HarnessContext, user_text: str) -> str:
    """Run one user turn to completion; return the final assistant text."""
    ctx.session.add(Message(role=Role.USER, content=user_text))

    system = await build_system_prompt(ctx)
    specs = ctx.tools.specs()

    for turn in range(ctx.max_turns):
        logger.info(
            "llm request scope=%s turn=%d", ctx.identity.session_key(), turn + 1
        )
        messages = [Message(role=Role.SYSTEM, content=system), *ctx.session.history()]
        completion = await ctx.llm.complete(messages, specs)
        logger.info(
            "llm response scope=%s turn=%d tool_calls=%d",
            ctx.identity.session_key(),
            turn + 1,
            len(completion.tool_calls),
        )

        assistant = Message(
            role=Role.ASSISTANT,
            content=completion.content,
            tool_calls=completion.tool_calls,
        )
        ctx.session.add(assistant)

        if not completion.tool_calls:
            return completion.content

        # A clarification in the same batch must stop SQL even if SQL came first.
        clarification = next(
            (
                call
                for call in completion.tool_calls
                if call.name == "ask_user"
                and str(call.arguments.get("question") or "").strip()
            ),
            None,
        )
        if clarification is not None:
            result = await ctx.tools.dispatch(
                clarification.name, clarification.arguments, ctx, clarification.id
            )
            if result.stop_turn:
                return _finish_tool_turn(
                    ctx, completion.tool_calls, clarification.id, result.content
                )

        for index, call in enumerate(completion.tool_calls):
            result = await ctx.tools.dispatch(call.name, call.arguments, ctx, call.id)
            if result.stop_turn:
                return _finish_tool_turn(
                    ctx, completion.tool_calls[index:], call.id, result.content
                )
            ctx.session.add(
                Message(
                    role=Role.TOOL,
                    content=result.content,
                    tool_call_id=result.call_id,
                    name=call.name,
                )
            )

    return "(reached max turns without a final answer)"


def _finish_tool_turn(
    ctx: HarnessContext, pending: list[ToolCall], call_id: str, content: str
) -> str:
    """Resolve every outstanding tool ID before returning control to the user."""
    for call in pending:
        ctx.session.add(
            Message(
                role=Role.TOOL,
                name=call.name,
                tool_call_id=call.id,
                content=(
                    content
                    if call.id == call_id
                    else "Not executed: this turn has stopped."
                ),
            )
        )
    ctx.session.add(Message(role=Role.ASSISTANT, content=content))
    logger.info(
        "tool stopped turn scope=%s call_id=%s", ctx.identity.session_key(), call_id
    )
    return content
