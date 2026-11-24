"""Review caller-supplied SQL using code checks and a non-executing query plan."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..core.ports.audit import AuditEvent
from ..core.ports.safety import Verdict
from ..core.types import ToolResult, ToolSpec
from ..safety.pipeline import query_context
from ..safety.tokens import tokens

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


class ReviewSQL:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="review_sql",
            description=(
                "Review a read-only SQL draft. Compare the SQL with the user's "
                "request as you fill in context. The tool checks the read-only "
                "policy and, where supported, asks the DB to plan the query. "
                "Read the feedback to decide whether to revise, ask a question, "
                "return the SQL, or call run_sql for results."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "minLength": 1,
                        "description": "The SQL draft to review.",
                    },
                    "context": {
                        "type": "string",
                        "minLength": 1,
                        "description": (
                            "Summarize the request, relevant schema or definitions, "
                            "and how the draft's joins, filters and aggregation match "
                            "them. Include any mismatch or uncertainty."
                        ),
                    },
                },
                "required": ["sql", "context"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        sql = args.get("sql")
        context = args.get("context")
        if (
            not isinstance(sql, str)
            or not sql.strip()
            or not isinstance(context, str)
            or not context.strip()
        ):
            return ToolResult(
                call_id="",
                content="Provide SQL and review context as non-empty strings.",
                is_error=True,
            )

        feedback, is_error = await _check_sql(ctx, sql)
        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    scope=ctx.identity.session_key(),
                    action="review_sql",
                    detail={"sql": sql, "feedback": feedback, "is_error": is_error},
                )
            )
        return ToolResult(
            call_id="",
            is_error=is_error,
            content=json.dumps(
                {"sql": sql, "context": context, "feedback": feedback},
                ensure_ascii=False,
            ),
        )


async def _check_sql(ctx: "HarnessContext", sql: str) -> tuple[str, bool]:
    if ctx.safety is None:
        return "Safety check unavailable.", True
    policy = query_context(ctx.explorer)
    decision = ctx.safety.evaluate(sql, policy)
    if decision.verdict is Verdict.BLOCK:
        return f"Blocked by {decision.layer}: {decision.reason}", True
    if decision.verdict is Verdict.CONFIRM:
        return f"Needs confirmation: {decision.confirm_prompt}", True
    if decision.verdict is Verdict.REWRITE and decision.sql != sql:
        return f"The safety policy requests this revision: {decision.sql}", True

    if ctx.explorer is None or getattr(ctx.explorer, "uses_sample_data", False):
        return (
            "Read-only policy passed. A live DB is required to check the query plan.",
            True,
        )
    dialect = policy.extras["dialect"]
    prefix = {"sqlite": "EXPLAIN QUERY PLAN", "postgresql": "EXPLAIN"}.get(dialect)
    if prefix is None:
        return (
            "Read-only policy passed. Plan checking is unavailable for this dialect.",
            False,
        )
    # Only the tool supplies EXPLAIN options. A caller may submit EXPLAIN ANALYZE,
    # which would execute its query if forwarded verbatim to PostgreSQL.
    first = next(tokens(decision.sql, dialect))
    if first[0].upper() not in {"SELECT", "WITH"}:
        return (
            "Read-only policy passed. To check a plan, submit the underlying SELECT or WITH query.",
            False,
        )
    try:
        await ctx.explorer.execute(f"{prefix} {decision.sql}", limit=1)
    except Exception as exc:
        return f"DB plan error: {getattr(exc, 'orig', exc)}", True
    return "Read-only policy and DB query plan checks passed.", False
