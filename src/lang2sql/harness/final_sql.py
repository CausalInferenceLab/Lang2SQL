"""Check a submitted query on the same connection used by run_sql."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

from ..core.ports.audit import AuditEvent
from ..core.ports.safety import Verdict
from ..safety.pipeline import query_context
from ..safety.tokens import tokens
from .call_budget import CallBudgetExhausted
from .context import HarnessContext
from .semantic_review import InvalidReview, review_sql


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


@dataclass(frozen=True)
class SubmissionCheck:
    action: Literal["accept", "revise", "clarify", "withhold"]
    detail: str = ""


async def check_submission(
    ctx: HarnessContext, question: str, context: list[str], candidate: str
) -> SubmissionCheck:
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
        return SubmissionCheck("accept")
    if review.status == "clarify":
        return SubmissionCheck("clarify", review.details["question"])
    return SubmissionCheck(
        "revise",
        json.dumps({"status": review.status, **review.details}, ensure_ascii=False),
    )
