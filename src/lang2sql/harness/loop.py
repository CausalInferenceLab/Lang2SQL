"""agent_loop — the turn engine.

One call per user message: build the system prompt, then iterate
LLM → tool calls → LLM until the model returns a final answer (no tool calls)
or ``max_turns`` is hit. Tool failures come back as tool messages so the model
can recover rather than crashing the loop.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Callable

from ..core.ports.llm import RequiredToolLLMPort
from ..core.types import Message, Role, ToolResult, ToolSpec
from .context import HarnessContext
from .final_sql import check_final_sql
from .sql_candidate import is_database_query, sql_from_answer
from .system_prompt import build_system_prompt

_SCHEMA_TOOL = "explore_schema"
_SQL_WITHHELD = (
    "SQL withheld because no table columns were provided to the model. "
    "Use column information from explore_schema before rewriting the query."
)
_NEEDS_SCHEMA = (
    "Call explore_schema with a non-empty tables list to obtain the missing columns."
)
_REWRITE_SQL = (
    "A previous SQL query was withheld. Use the columns in the schema results to "
    "rewrite the query before running it or returning it as the final answer."
)


def _recovery_specs(specs: list[ToolSpec], schema: ToolSpec) -> list[ToolSpec]:
    tables_parameter = schema.parameters["properties"]["tables"]
    schema_for_recovery = replace(
        schema,
        description="Return the columns for one or more relevant tables.",
        parameters={
            "type": "object",
            "properties": {
                "tables": {
                    **tables_parameter,
                    "description": "Names of tables needed to rewrite the query.",
                }
            },
            "required": ["tables"],
            "additionalProperties": False,
        },
    )
    return [schema_for_recovery if spec is schema else spec for spec in specs]


async def agent_loop(
    ctx: HarnessContext,
    user_text: str,
    *,
    extract_final_sql: Callable[[str], str] | None = None,
) -> str:
    """Run one user turn.

    Evaluators can supply their final SQL parser to apply the same output
    contract before scoring. The parser raises ValueError for other answers.
    """
    ctx.session.add(Message(role=Role.USER, content=user_text))

    if ctx.explorer is not None:
        ctx = replace(ctx, known_tables=await ctx.explorer.list_tables())
    system = await build_system_prompt(ctx)
    specs = ctx.tools.specs()
    schema_spec = next((spec for spec in specs if spec.name == _SCHEMA_TOOL), None)
    tables_with_columns = system.tables_with_columns
    sql_withheld = False
    history = (
        ctx.session.history()
    )  # Includes transient validation feedback for this turn.
    failure: str | None = None
    revision_requested = False

    for _ in range(ctx.max_turns):
        # Every tool call in this response was authored with the same context.
        model_has_columns = bool(tables_with_columns)
        needs_recovery = sql_withheld and not model_has_columns
        prompt = system.content
        if sql_withheld:
            prompt += "\n\n" + (
                f"{_SQL_WITHHELD} {_NEEDS_SCHEMA}" if needs_recovery else _REWRITE_SQL
            )
        messages = [Message(role=Role.SYSTEM, content=prompt), *history]

        try:
            if needs_recovery:
                if schema_spec is None or ctx.explorer is None:
                    failure = "schema lookup is unavailable"
                    break
                if not isinstance(ctx.llm, RequiredToolLLMPort):
                    failure = "the model adapter cannot require schema lookup"
                    break
                completion = await ctx.llm.complete_with_required_tool(
                    messages, _recovery_specs(specs, schema_spec), _SCHEMA_TOOL
                )
            else:
                completion = await ctx.llm.complete(messages, specs)
        except Exception as exc:
            if not sql_withheld and not revision_requested:
                raise
            step = "schema lookup" if needs_recovery else "SQL rewrite"
            failure = f"model request failed during {step}: {type(exc).__name__}"
            break

        assistant = Message(
            role=Role.ASSISTANT,
            content="" if needs_recovery else completion.content,
            tool_calls=completion.tool_calls,
        )
        if not completion.tool_calls:
            if needs_recovery:
                continue
            candidate = sql_from_answer(completion.content, extract_final_sql)
            if not model_has_columns and is_database_query(candidate):
                sql_withheld = True
                history.append(assistant)
                continue
            if (sql_withheld or revision_requested) and (
                completion.finish_reason == "length"
                or not completion.content.strip()
                or (
                    (revision_requested or extract_final_sql is not None)
                    and candidate is None
                )
            ):
                continue
            if candidate is not None:
                error = await check_final_sql(ctx, candidate)
                if error is not None:
                    if revision_requested:
                        failure = f"revised SQL failed validation: {error}"
                        break
                    repair_feedback = (
                        f"Final SQL failed validation: {error}\n"
                        "Use explore_schema or run_sql if needed, then submit one corrected query."
                    )
                    revision_requested = True
                    history.extend(
                        [assistant, Message(role=Role.USER, content=repair_feedback)]
                    )
                    continue
            ctx.session.add(assistant)
            return completion.content

        ctx.session.add(assistant)
        history.append(assistant)
        for call in completion.tool_calls:
            if needs_recovery and (
                call.name != _SCHEMA_TOOL
                or not isinstance(call.arguments, dict)
                or call.arguments.keys() != {"tables"}
            ):
                result = ToolResult(
                    call_id=call.id, content=_NEEDS_SCHEMA, is_error=True
                )
            elif (
                not model_has_columns
                and call.name == "run_sql"
                and ctx.explorer is not None
                and isinstance(call.arguments, dict)
                and is_database_query(call.arguments.get("sql"))
            ):
                sql_withheld = True
                # Keep the candidate in the tool call, outside the persisted reply.
                assistant.content = ""
                result = ToolResult(
                    call_id=call.id, content=_SQL_WITHHELD, is_error=True
                )
            else:
                result = await ctx.tools.dispatch(
                    call.name, call.arguments, ctx, call.id
                )
            tables_with_columns.update(result.tables_with_columns)
            tool_message = Message(
                role=Role.TOOL,
                content=result.content,
                tool_call_id=result.call_id,
                name=call.name,
            )
            ctx.session.add(tool_message)
            history.append(tool_message)

    if revision_requested and failure is None:
        failure = "call budget exhausted before receiving a validated revision"
    if not sql_withheld and failure is None:
        return "(reached max turns without a final answer)"
    if failure is None:
        failure = (
            "reached max turns before receiving a rewritten final answer"
            if tables_with_columns
            else "no table columns obtained before reaching max turns"
        )
    answer = f"(SQL withheld: {failure})"
    ctx.session.add(Message(role=Role.ASSISTANT, content=answer))
    return answer
