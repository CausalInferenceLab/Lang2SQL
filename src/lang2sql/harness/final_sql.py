"""Check a submitted query on the same connection used by run_sql."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Callable, Literal

from ..core.ports.audit import AuditEvent
from ..core.ports.safety import Verdict
from ..core.types import ToolResult
from ..safety.pipeline import query_context
from ..safety.tokens import tokens
from ..tools.run_sql import SQLExecution, execute_read_query
from .call_budget import CallBudgetExhausted
from .context import HarnessContext
from .semantic_review import InvalidReview, review_sql
from .sql_candidate import sql_from_answer


async def check_final_sql(ctx: HarnessContext, candidate: str) -> str | None:
    """Return actionable feedback, or None when the DB accepts the query."""
    if ctx.explorer is None or ctx.safety is None:
        return "Final SQL validation requires a DB connection and safety pipeline."

    policy = query_context(ctx.explorer)
    decision = ctx.safety.evaluate(candidate, policy)
    checked_sql = None
    error = None
    if decision.verdict is Verdict.BLOCK:
        error = f"BLOCKED by {decision.layer}: {decision.reason}"
    elif decision.verdict is Verdict.CONFIRM:
        error = f"Needs confirmation: {decision.confirm_prompt}"
    elif decision.verdict is Verdict.REWRITE and decision.sql != candidate:
        error = f"The safety policy requires a revised submission: {decision.sql}"
    else:
        # The protective row cap is for the DB check only. SQLite prepares a
        # plan without scanning the result set. Other explorers use a bounded read.
        checked_sql = decision.sql
        if policy.extras["dialect"] == "sqlite":
            first = next(tokens(checked_sql, "sqlite"))
            if first[0].upper() != "EXPLAIN":
                checked_sql = "EXPLAIN QUERY PLAN " + checked_sql
        try:
            await ctx.explorer.execute(checked_sql, limit=1)
        except Exception as exc:
            # SQLAlchemy's orig is the driver's error without the statement echo.
            error = f"{type(exc).__name__}: {getattr(exc, 'orig', exc)}"

    if ctx.audit is not None:
        await ctx.audit.record(
            AuditEvent(
                actor=ctx.identity.user_id,
                scope=ctx.identity.session_key(),
                action="check_final_sql",
                detail={
                    "candidate_sql": candidate,
                    "checked_sql": checked_sql,
                    "error": error,
                },
            )
        )
    return error


@dataclass
class SQLSubmission:
    """One proposed final query, owned by a single agent_loop invocation."""

    candidate_sql: str
    output: Literal["sql", "result"]
    sql_answer: str
    execution: SQLExecution | None = None

    def answer(self) -> str:
        if self.output == "sql":
            return self.sql_answer
        if self.execution is None:
            raise RuntimeError("result submission has no execution")
        return self.execution.render(self.candidate_sql)


def read_submission(
    content: str,
    extract_final_sql: Callable[[str], str] | None,
) -> SQLSubmission | None:
    text = content.strip()
    if text.startswith("```json") and text.endswith("```"):
        text = text.removeprefix("```json").removesuffix("```").strip()
    if extract_final_sql is None and text.startswith("{"):
        try:
            obj = json.loads(text)
        except ValueError as error:
            raise ValueError("invalid submission JSON") from error
        if "sql" not in obj and "output" not in obj:
            return None
        sql = obj.get("sql")
        output = obj.get("output", "result")
        if (
            not isinstance(sql, str)
            or not sql.strip()
            or output not in ("sql", "result")
        ):
            raise ValueError("expected sql text and output of sql or result")
        return SQLSubmission(sql.strip(), output, sql.strip())

    # A caller-supplied extractor owns the output format (e.g. BIRD's fenced SQL).
    # Also preserve standalone SQL answers from existing model integrations.
    sql = sql_from_answer(content, extract_final_sql)
    return SQLSubmission(sql, "sql", content) if sql is not None else None


@dataclass(frozen=True)
class SubmissionCheck:
    action: Literal["accept", "revise", "clarify", "withhold"]
    detail: str = ""


async def check_submission(
    ctx: HarnessContext, question: str, context: list[str], submission: SQLSubmission
) -> SubmissionCheck:
    candidate = submission.candidate_sql
    error = await check_final_sql(ctx, candidate)
    if error is not None:
        return SubmissionCheck("revise", error)

    try:
        review = await review_sql(ctx.llm, question, context, candidate)
    except CallBudgetExhausted:
        return SubmissionCheck(
            "withhold", "call budget exhausted before semantic review"
        )
    except InvalidReview as exc:
        return SubmissionCheck("withhold", f"incomplete semantic review: {exc}")
    except Exception as exc:
        return SubmissionCheck(
            "withhold",
            f"model request failed during semantic review: {type(exc).__name__}",
        )

    if ctx.audit is not None:
        await ctx.audit.record(
            AuditEvent(
                actor=ctx.identity.user_id,
                scope=ctx.identity.session_key(),
                action="review_sql",
                detail={
                    "candidate_sql": candidate,
                    "status": review.status,
                    **review.details,
                },
            )
        )
    if review.status == "accept":
        if submission.output == "result":
            try:
                result = await execute_read_query(ctx, candidate)
            except Exception as exc:
                return SubmissionCheck(
                    "revise", f"Execution failed: {getattr(exc, 'orig', exc)}"
                )
            if isinstance(result, ToolResult):
                return SubmissionCheck("revise", result.content)
            submission.execution = result
        return SubmissionCheck("accept")
    if review.status == "clarify":
        return SubmissionCheck("clarify", review.details["question"])
    return SubmissionCheck(
        "revise",
        json.dumps({"status": review.status, **review.details}, ensure_ascii=False),
    )
