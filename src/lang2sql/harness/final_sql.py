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


@dataclass(frozen=True)
class SubmissionCheck:
    action: Literal["accept", "revise", "clarify", "withhold"]
    detail: str = ""


async def check_final_sql(
    ctx: HarnessContext, candidate: str
) -> SubmissionCheck | None:
    """Audit the DB check and return a repairable or terminal failure."""
    checked_sql = None
    failure = None
    if (
        ctx.explorer is None
        or ctx.safety is None
        or not getattr(ctx.explorer, "supports_query_validation", True)
    ):
        failure = SubmissionCheck(
            "withhold",
            "Final SQL validation requires a connected DB and safety pipeline.",
        )
    else:
        policy = query_context(ctx.explorer)
        decision = ctx.safety.evaluate(candidate, policy)
        if decision.verdict is Verdict.BLOCK:
            failure = SubmissionCheck(
                "revise", f"BLOCKED by {decision.layer}: {decision.reason}"
            )
        elif decision.verdict is Verdict.CONFIRM:
            failure = SubmissionCheck(
                "revise", f"Needs confirmation: {decision.confirm_prompt}"
            )
        elif decision.verdict is Verdict.REWRITE and decision.sql != candidate:
            failure = SubmissionCheck(
                "revise",
                f"The safety policy requires a revised submission: {decision.sql}",
            )
        else:
            checked_sql = decision.sql
            if policy.extras["dialect"] == "sqlite":
                first = next(tokens(checked_sql, "sqlite"))
                if first[0].upper() != "EXPLAIN":
                    checked_sql = "EXPLAIN QUERY PLAN " + checked_sql
            try:
                await ctx.explorer.execute(checked_sql, limit=1)
            except Exception as exc:
                failure = SubmissionCheck(
                    "revise", f"{type(exc).__name__}: {getattr(exc, 'orig', exc)}"
                )

    if ctx.audit is not None:
        await ctx.audit.record(
            AuditEvent(
                actor=ctx.identity.user_id,
                scope=ctx.identity.session_key(),
                action="check_final_sql",
                detail={
                    "candidate_sql": candidate,
                    "checked_sql": checked_sql,
                    "error": failure.detail if failure else None,
                },
            )
        )
    return failure


@dataclass
class SQLSubmission:
    """One proposed final query, owned by a single agent_loop invocation."""

    candidate_sql: str
    output: Literal["sql", "result"] | None
    sql_answer: str
    execution: SQLExecution | None = None

    def answer(self) -> str:
        if self.output == "sql":
            return self.sql_answer
        if self.execution is None:
            raise RuntimeError("result submission has no execution")
        return self.execution.render()


def read_submission(
    content: str,
    extract_final_sql: Callable[[str], str] | None,
) -> SQLSubmission | None:
    if extract_final_sql is not None:
        sql = extract_final_sql(content)
        if not sql.strip():
            raise ValueError("expected a complete final SQL query")
        return SQLSubmission(sql, "sql", content)

    text = content.strip()
    if text.lower().startswith("```json"):
        if not text.endswith("```"):
            raise ValueError("incomplete JSON submission")
        text = text[len("```json") : -len("```")].strip()
    if text.startswith("{"):
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

    # A draft by itself does not specify the output the user requested.
    draft = sql_from_answer(content)
    return SQLSubmission(draft, None, draft) if draft is not None else None


async def check_submission(
    ctx: HarnessContext, question: str, context: list[str], submission: SQLSubmission
) -> SubmissionCheck:
    candidate = submission.candidate_sql
    failure = await check_final_sql(ctx, candidate)
    if failure is not None:
        return failure

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
