"""Core agentic loop — async generator inspired by Claude Code's query.ts."""

from __future__ import annotations

import logging
from typing import Any, AsyncGenerator

from ..core.hooks import Event, NullHook, TraceHook, ms, now, summarize
from ..core.ports import ToolCallLLMPort
from .session import Session
from .system_prompt import build_system_prompt
from .tool import ToolRegistry
from .types import (
    AgentEvent,
    AssistantEvent,
    DataEvent,
    ErrorEvent,
    Message,
    PlanApprovalEvent,
    ToolCall,
    ToolCallEvent,
    ToolResult,
    ToolResultEvent,
    UserPromptEvent,
)

logger = logging.getLogger(__name__)

# Tools that pause the loop and yield control to the caller
_INTERACTIVE_TOOLS = frozenset({"ask_user", "show_plan"})


async def agent_loop(
    question: str,
    *,
    llm: ToolCallLLMPort,
    tools: ToolRegistry,
    session: Session,
    hook: TraceHook = NullHook(),
    max_turns: int = 20,
) -> AsyncGenerator[AgentEvent, str | None]:
    """Run the agentic ReAct loop.

    Yields ``AgentEvent`` objects as the agent progresses.  The caller
    can ``send()`` user responses back when the loop yields
    ``UserPromptEvent`` or ``PlanApprovalEvent``.

    Parameters
    ----------
    question:
        The user's natural-language question for this turn.
    llm:
        A tool-calling LLM backend.
    tools:
        Registry of available tools.
    session:
        Persistent session state (conversation, semantic layer, etc.).
    hook:
        Observability hook.
    max_turns:
        Safety limit to prevent infinite loops.
    """
    # --- 1. Push the user question into the conversation --------------------
    session.push_user(question)

    # --- 2. Build the system prompt -----------------------------------------
    tool_descs = "\n".join(
        f"- **{s['name']}**: {s['description']}"
        for s in tools.tool_specs_for_llm()
    )
    system_prompt = build_system_prompt(
        semantic_layer=session.semantic_layer,
        schema_cache=session.schema_cache,
        mode=session.mode,
        dialect=session.db_dialect or "sqlite",
        tool_descriptions=tool_descs,
    )

    # --- 3. Main ReAct loop -------------------------------------------------
    turn = 0
    while turn < max_turns:
        turn += 1
        t0 = now()

        hook.on_event(Event(
            name="harness.turn",
            component="agent_loop",
            phase="start",
            ts=t0,
            data={"turn": turn},
        ))

        # 3a. Build messages for the LLM
        messages = session.build_messages(system_prompt)

        # 3b. Call the LLM with tool definitions
        try:
            response = llm.invoke_with_tools(
                messages,
                tools.tool_specs_for_llm(),
            )
        except Exception as exc:
            hook.on_event(Event(
                name="harness.llm_error",
                component="agent_loop",
                phase="error",
                ts=now(),
                error=str(exc),
            ))
            yield ErrorEvent(error=str(exc), recoverable=False)
            return

        content: str | None = response.get("content")
        tool_calls_raw: list[dict[str, Any]] | None = response.get("tool_calls")

        # 3c. Record the assistant message
        parsed_tool_calls: list[ToolCall] | None = None
        if tool_calls_raw:
            parsed_tool_calls = [
                ToolCall(
                    id=tc.get("id", f"tc_{turn}_{i}"),
                    name=tc["name"],
                    arguments=tc.get("arguments", {}),
                )
                for i, tc in enumerate(tool_calls_raw)
            ]
        session.push_assistant(content, parsed_tool_calls)

        # 3d. If the LLM returned tool calls, execute them
        if parsed_tool_calls:
            for tc in parsed_tool_calls:
                # --- Interactive tools: pause loop, yield to caller ---
                if tc.name == "ask_user":
                    q = tc.arguments.get("question", "")
                    opts = tc.arguments.get("options")
                    event = UserPromptEvent(question=q, options=opts)
                    user_answer = yield event  # type: ignore[misc]
                    answer_text = user_answer if user_answer else ""
                    session.push_tool_result(
                        tc.id,
                        ToolResult(
                            tool_call_id=tc.id,
                            content=answer_text,
                        ),
                    )
                    hook.on_event(Event(
                        name="harness.ask_user",
                        component="ask_user",
                        phase="end",
                        ts=now(),
                        data={"question": q, "answer": answer_text},
                    ))
                    continue

                if tc.name == "show_plan":
                    plan_text = tc.arguments.get("plan", "")
                    steps = tc.arguments.get("steps", [])
                    event = PlanApprovalEvent(plan=plan_text, steps=steps)
                    approval = yield event  # type: ignore[misc]
                    approved = bool(approval) if approval is not None else True
                    session.push_tool_result(
                        tc.id,
                        ToolResult(
                            tool_call_id=tc.id,
                            content="approved" if approved else "rejected",
                        ),
                    )
                    hook.on_event(Event(
                        name="harness.plan_approval",
                        component="show_plan",
                        phase="end",
                        ts=now(),
                        data={"approved": approved},
                    ))
                    continue

                # --- Regular tools: execute and yield events ---
                yield ToolCallEvent(tool_call=tc)

                t_tool = now()
                result = await tools.execute(
                    tc.name,
                    tc.arguments,
                    tool_call_id=tc.id,
                )
                hook.on_event(Event(
                    name="harness.tool",
                    component=tc.name,
                    phase="error" if result.is_error else "end",
                    ts=now(),
                    duration_ms=ms(t_tool, now()),
                    input_summary=summarize(tc.arguments),
                    output_summary=summarize(result.content),
                    error=result.content if result.is_error else None,
                ))

                yield ToolResultEvent(result=result)

                # Push result into session for next LLM turn
                session.push_tool_result(tc.id, result)

                # If this was run_sql and it succeeded, emit DataEvent
                if tc.name == "run_sql" and not result.is_error and result.data:
                    rows = result.data if isinstance(result.data, list) else []
                    sql = tc.arguments.get("sql", session.last_sql or "")
                    session.last_sql = sql
                    session.last_result = rows
                    yield DataEvent(
                        rows=rows,
                        sql=sql,
                        row_count=len(rows),
                        truncated=len(rows) >= 100,
                    )

            # After processing all tool calls, continue the loop so the
            # LLM can see the results and decide what to do next.
            hook.on_event(Event(
                name="harness.turn",
                component="agent_loop",
                phase="end",
                ts=now(),
                duration_ms=ms(t0, now()),
                data={"turn": turn, "had_tool_calls": True},
            ))
            continue

        # 3e. Text-only response — the agent's final answer for this turn
        if content:
            yield AssistantEvent(content=content)

        hook.on_event(Event(
            name="harness.turn",
            component="agent_loop",
            phase="end",
            ts=now(),
            duration_ms=ms(t0, now()),
            data={"turn": turn, "had_tool_calls": False},
        ))
        break  # Text response = turn complete, wait for next user input

    else:
        # max_turns exceeded
        yield ErrorEvent(
            error=f"Agent loop exceeded maximum turns ({max_turns})",
            recoverable=True,
        )
