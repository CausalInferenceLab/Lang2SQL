"""agent_loop — the turn engine.

One call per user message: build the system prompt, then iterate
LLM → tool calls → LLM until the model returns a final answer (no tool calls)
or ``max_turns`` is hit. Tool failures come back as tool messages so the model
can recover rather than crashing the loop.
"""

from __future__ import annotations

from dataclasses import replace
from typing import Callable

from ..core.types import Message, Role, ToolResult, ToolSpec
from .call_budget import SchemaLookupUnavailable, TurnModel
from .context import HarnessContext
from .final_sql import SQLSubmission, check_submission, read_submission
from .semantic_review import question_with_clarifications
from .sql_candidate import is_database_query
from .system_prompt import SUBMIT_SQL, build_system_prompt

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

    model = TurnModel(ctx.llm, ctx.max_turns)
    tables = await ctx.explorer.list_tables() if ctx.explorer is not None else None
    ctx = replace(ctx, llm=model, known_tables=tables)
    system = await build_system_prompt(ctx, structured_sql=extract_final_sql is None)
    specs = ctx.tools.specs()
    schema_spec = next((spec for spec in specs if spec.name == _SCHEMA_TOOL), None)
    tables_with_columns = system.tables_with_columns
    sql_withheld = False
    # Rejected drafts/feedback belong to this turn only.
    history = ctx.session.history()
    review_context = (
        [system.schema_and_definitions] if system.schema_and_definitions else []
    )
    failure: str | None = None
    revision_requested = False
    review_question, definitions = question_with_clarifications(history)
    review_context.extend(definitions)

    submission: SQLSubmission | None = None
    submission_required = False

    while model.remaining > 0:
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
                completion = await model.require_tool(
                    messages, _recovery_specs(specs, schema_spec), _SCHEMA_TOOL
                )
            else:
                completion = await ctx.llm.complete(messages, specs)
        except SchemaLookupUnavailable as exc:
            failure = str(exc)
            break
        except Exception as exc:
            if not (sql_withheld or revision_requested or submission_required):
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
            try:
                proposed = read_submission(completion.content, extract_final_sql)
            except ValueError as exc:
                submission_required = True
                history.extend(
                    [
                        assistant,
                        Message(role=Role.USER, content=str(exc) + ". " + SUBMIT_SQL),
                    ]
                )
                continue
            if completion.finish_reason == "length" and (
                proposed is not None
                or submission is not None
                or sql_withheld
                or submission_required
            ):
                submission_required = True
                history.append(
                    Message(
                        role=Role.USER,
                        content=(
                            "The response was truncated. Submit the complete query in the requested format."
                        ),
                    )
                )
                continue
            if proposed is not None:
                if submission is not None:
                    # Rewriting SQL cannot change what this request should return.
                    proposed.output = submission.output
                submission = proposed
            candidate = proposed.candidate_sql if proposed else None
            if not model_has_columns and is_database_query(candidate):
                sql_withheld = True
                history.append(assistant)
                continue
            if proposed is None and (
                submission_required
                or submission is not None
                or (sql_withheld and extract_final_sql is not None)
            ):
                if extract_final_sql is None:
                    history.extend(
                        [assistant, Message(role=Role.USER, content=SUBMIT_SQL)]
                    )
                continue
            if proposed is not None:
                check = await check_submission(
                    ctx, review_question, review_context, proposed
                )
                if check.action == "accept":
                    assistant.content = proposed.answer()
                if check.action == "withhold":
                    failure = check.detail
                    break
                if check.action == "clarify":
                    assistant.content = f"❓ Awaiting user: {check.detail}"
                    assistant.name = "clarify_definition"
                if check.action == "revise":
                    if revision_requested:
                        failure = f"revised SQL failed validation: {check.detail}"
                        break
                    repair_feedback = (
                        f"Final SQL failed validation: {check.detail}\n"
                        "Use explore_schema or run_sql if needed, then submit one corrected query."
                    )
                    revision_requested = True
                    history.extend(
                        [assistant, Message(role=Role.USER, content=repair_feedback)]
                    )
                    continue
            ctx.session.add(assistant)
            return assistant.content

        ctx.session.add(assistant)
        history.append(assistant)
        clarification = None
        for call in completion.tool_calls:
            if call.name == "run_sql" and extract_final_sql is None:
                submission_required = True
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
            if result.tables_with_columns or (
                call.name in {"term_custom", "recall_memory", "enrich_schema"}
                and not result.is_error
            ):
                review_context.append(result.content)
            if (
                call.name == "ask_user"
                and not result.is_error
                and extract_final_sql is None
                and (submission_required or submission is not None)
            ):
                clarification = result.content
            tables_with_columns.update(result.tables_with_columns)
            tool_message = Message(
                role=Role.TOOL,
                content=result.content,
                tool_call_id=result.call_id,
                name=call.name,
            )
            ctx.session.add(tool_message)
            history.append(tool_message)

        if clarification is not None:
            ctx.session.add(
                Message(
                    role=Role.ASSISTANT,
                    content=clarification,
                    name="clarify_definition",
                )
            )
            return clarification

    if submission_required and not (failure or revision_requested or sql_withheld):
        failure = "call budget exhausted before receiving a final SQL submission"
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
    # A diagnostic can quote model text. Keep fenced SQL out of a non-submission.
    reason = failure.replace("```", "`")
    answer = f"(SQL withheld: {reason})"
    ctx.session.add(Message(role=Role.ASSISTANT, content=answer))
    return answer
