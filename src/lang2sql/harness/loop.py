"""agent_loop — the turn engine.

One call per user message: build the system prompt, then iterate
LLM → tool calls → LLM until the model returns a final answer (no tool calls)
or ``max_turns`` is hit. Tool failures come back as tool messages so the model
can recover rather than crashing the loop.
"""

from __future__ import annotations

from ..core.types import Message, Role
from .context import HarnessContext
from .system_prompt import build_system_prompt


async def agent_loop(ctx: HarnessContext, user_text: str) -> str:
    """Run one user turn to completion; return the final assistant text."""
    ctx.semantic_result_ready = False
    ctx.semantic_result_message = ""
    ctx.semantic_result_headers = ()
    ctx.semantic_result_rows = []
    ctx.semantic_result_stamp = ()
    if ctx.semantic_query is not None:
        attention = ctx.semantic_query.bind_question(user_text)
        _ = ctx.semantic_query.spec
        ctx.semantic_attention_state = (
            "candidate_schema_too_large"
            if ctx.semantic_query.schema_blocker
            else attention.state
        )
        ctx.semantic_attention_message = (
            ctx.semantic_query.schema_blocker
            if ctx.semantic_query.schema_blocker
            else attention.message
        )
        ctx.semantic_table_ids = attention.table_ids
    ctx.session.add(Message(role=Role.USER, content=user_text))
    if ctx.semantic_attention_state and ctx.semantic_attention_state != "ready":
        return (
            "NEEDS CLARIFICATION (semantic_candidate_scope): "
            + ctx.semantic_attention_message
        )

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

        ask_calls = [
            call for call in completion.tool_calls if call.name == "ask_user"
        ]
        if ask_calls:
            # Clarification is a suspension boundary.  A model must never ask
            # the human and then keep acting in the same user turn, nor hide a
            # sibling side effect beside the question.
            if len(completion.tool_calls) != 1 or len(ask_calls) != 1:
                content = (
                    "BLOCKED (ask_user_must_be_single_call): clarification "
                    "cannot be combined with another tool call."
                )
                for emitted_call in completion.tool_calls:
                    ctx.session.add(
                        Message(
                            role=Role.TOOL,
                            content=content,
                            tool_call_id=emitted_call.id,
                            name=emitted_call.name,
                        )
                    )
                return content
            call = ask_calls[0]
            result = await ctx.tools.dispatch(
                call.name, call.arguments, ctx, call.id
            )
            ctx.session.add(
                Message(
                    role=Role.TOOL,
                    content=result.content,
                    tool_call_id=result.call_id,
                    name=call.name,
                )
            )
            return result.content

        semantic_calls = [
            call for call in completion.tool_calls if call.name == "semantic_query"
        ]
        if semantic_calls:
            # Governed query output is authoritative. Never feed DB-derived
            # labels back into another model turn, and never execute sibling
            # tool calls that could turn a label into an indirect side effect.
            if len(completion.tool_calls) != 1 or len(semantic_calls) != 1:
                content = (
                    "BLOCKED (semantic_query_must_be_single_call): governed data "
                    "queries cannot be combined with another tool call."
                )
                for emitted_call in completion.tool_calls:
                    ctx.session.add(
                        Message(
                            role=Role.TOOL,
                            content=content,
                            tool_call_id=emitted_call.id,
                            name=emitted_call.name,
                        )
                    )
                return content
            call = semantic_calls[0]
            result = await ctx.tools.dispatch(
                call.name, call.arguments, ctx, call.id
            )
            ctx.session.add(
                Message(
                    role=Role.TOOL,
                    content=result.content,
                    tool_call_id=result.call_id,
                    name=call.name,
                )
            )
            return result.content

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
