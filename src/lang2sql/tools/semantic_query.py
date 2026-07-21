"""Typed semantic query tool.

The model selects catalog IDs only.  This tool owns deterministic compilation,
the existing safety pipeline, and execution; raw SQL is deliberately absent
from its input contract.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core.ports.audit import AuditEvent
from ..core.ports.safety import SafetyContext, Verdict
from ..core.types import Role, ToolResult, ToolSpec
from ..semantic.catalog import SemanticCatalog
from ..semantic.service import SemanticService, review_scope_key

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


class SemanticQuery:
    def __init__(self, service: SemanticService, catalog: SemanticCatalog) -> None:
        self._service = service
        self._catalog = catalog

    @property
    def spec(self) -> ToolSpec:
        metric_lines = [
            (
                f"{item.id} = {item.label}; allowed="
                f"{','.join(value.value for value in item.allowed_aggregates)}; "
                f"reviewed={item.reviewed_bindings or '{}'}"
            )
            for item in self._catalog.metrics
            if item.state.value != "rejected"
        ]
        dimension_lines = [
            f"{item.id} = {item.label}" for item in self._catalog.dimensions
        ]
        return ToolSpec(
            name="semantic_query",
            description=(
                "Run a governed aggregate query by selecting catalog IDs. Never "
                "write SQL. Copy metric_phrase and every dimension phrase exactly "
                "from the user's question; the service verifies and persists each "
                "phrase-to-column binding. Put every requested filter, time rule, "
                "comparison, business modifier, or conversion that this schema "
                "cannot represent in unresolved_obligations. Never drop one.\n"
                "Metrics:\n- "
                + "\n- ".join(metric_lines or ["(none)"])
                + "\nDimensions:\n- "
                + "\n- ".join(dimension_lines or ["(none)"])
            ),
            parameters={
                "type": "object",
                "properties": {
                    "metric_id": {
                        "type": "string",
                        "enum": [
                            item.id
                            for item in self._catalog.metrics
                            if item.state.value != "rejected"
                        ],
                    },
                    "metric_phrase": {
                        "type": "string",
                        "description": "Exact words in the question naming the metric.",
                    },
                    "aggregate": {
                        "type": "string",
                        "enum": ["sum", "avg", "min", "max", "count"],
                        "description": (
                            "Aggregate requested by the question. It is reviewed "
                            "and stored per metric phrase, never guessed by SQL."
                        ),
                    },
                    "dimensions": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "dimension_id": {
                                    "type": "string",
                                    "enum": [
                                        item.id for item in self._catalog.dimensions
                                    ],
                                },
                                "phrase": {
                                    "type": "string",
                                    "description": (
                                        "Exact words in the question naming this "
                                        "grouping dimension."
                                    ),
                                },
                            },
                            "required": ["dimension_id", "phrase"],
                            "additionalProperties": False,
                        },
                        "default": [],
                    },
                    "unresolved_obligations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": (
                            "Requested constraints not represented by metric and "
                            "dimensions. Use [] only when none remain."
                        ),
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": 1000,
                        "default": 100,
                    },
                },
                "required": [
                    "metric_id",
                    "metric_phrase",
                    "aggregate",
                    "dimensions",
                    "unresolved_obligations",
                ],
                "additionalProperties": False,
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        if ctx.explorer is None or ctx.store is None:
            return ToolResult(
                call_id="",
                content="BLOCKED: governed query context is unavailable",
                is_error=True,
            )
        if ctx.safety is None:
            return ToolResult(
                call_id="",
                content="BLOCKED: safety pipeline is unavailable",
                is_error=True,
            )

        question = str(args.get("_reviewed_question") or _latest_user_question(ctx))
        raw_dimensions = args.get("dimensions") or []
        if not isinstance(raw_dimensions, list):
            return ToolResult(
                call_id="",
                content="BLOCKED: dimensions must be a list",
                is_error=True,
            )
        dimension_bindings: list[dict[str, str]] = []
        for item in raw_dimensions:
            if not isinstance(item, dict):
                return ToolResult(
                    call_id="",
                    content="BLOCKED: every dimension must be an object",
                    is_error=True,
                )
            dimension_bindings.append(
                {
                    "dimension_id": str(item.get("dimension_id", "")),
                    "phrase": str(item.get("phrase", "")),
                }
            )
        raw_obligations = args.get("unresolved_obligations")
        if not isinstance(raw_obligations, list):
            return ToolResult(
                call_id="",
                content="BLOCKED: unresolved_obligations must be a list",
                is_error=True,
            )
        try:
            limit = int(args.get("limit", 100))
        except (TypeError, ValueError):
            limit = 100

        outcome = self._service.prepare_query(
            scope=ctx.identity.kv_scope,
            review_scope=_review_scope(ctx),
            requester_id=ctx.identity.user_id,
            explorer=ctx.explorer,
            question=question,
            metric_id=str(args.get("metric_id", "")),
            metric_phrase=str(args.get("metric_phrase", "")),
            aggregate=str(args.get("aggregate", "")),
            dimension_bindings=dimension_bindings,
            unresolved_obligations=[str(item) for item in raw_obligations],
            limit=limit,
        )
        if outcome.status == "clarification":
            return ToolResult(
                call_id="", content=f"NEEDS CLARIFICATION: {outcome.message}"
            )
        if outcome.status != "ready":
            return ToolResult(
                call_id="",
                content=f"BLOCKED ({outcome.blocker}): {outcome.message}",
                is_error=True,
            )

        safety = ctx.safety.evaluate(
            outcome.sql, SafetyContext(row_limit=max(1, min(limit, 1000)))
        )
        if safety.verdict != Verdict.PASS:
            return ToolResult(
                call_id="",
                content=f"BLOCKED by {safety.layer}: {safety.reason}",
                is_error=True,
            )
        try:
            rows = await ctx.explorer.execute(safety.sql, max(1, min(limit, 1000)))
        except Exception as exc:
            if ctx.audit is not None:
                await ctx.audit.record(
                    AuditEvent(
                        actor=ctx.identity.user_id,
                        action="semantic_query_failed",
                        scope=ctx.identity.session_key(),
                        detail={
                            "metric_id": outcome.metric_id,
                            "aggregate": outcome.aggregate,
                            "dimension_ids": outcome.dimension_ids,
                            "sql": safety.sql,
                            "error_type": type(exc).__name__,
                        },
                    )
                )
            return ToolResult(
                call_id="",
                content=(
                    "BLOCKED (query_execution_failed): DB가 검토된 질의를 "
                    "실행하지 못했습니다. SQL과 드라이버 상세는 audit에만 남깁니다."
                ),
                is_error=True,
            )
        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="semantic_query",
                    scope=ctx.identity.session_key(),
                    detail={
                        "metric_id": outcome.metric_id,
                        "aggregate": outcome.aggregate,
                        "dimension_ids": outcome.dimension_ids,
                        "sql": safety.sql,
                    },
                )
            )
        return ToolResult(
            call_id="",
            content=_render_rows(outcome.message, rows),
        )


def _latest_user_question(ctx: "HarnessContext") -> str:
    for message in reversed(ctx.session.history()):
        if message.role == Role.USER:
            return message.content
    return ""


def _review_scope(ctx: "HarnessContext") -> str:
    """Keep concurrent users' pending confirmations from overwriting each other."""

    return review_scope_key(ctx.identity.session_key(), ctx.identity.user_id)


def _render_rows(message: str, rows: list[dict]) -> str:
    # SQL remains in the audit record; Discord gets only status and result data.
    lines = [f"READY: {message}"]
    if not rows:
        return "\n".join([*lines, "Result: (0 rows)"])
    headers = list(rows[0])
    lines.append("Result:")
    lines.append(" | ".join(headers))
    lines.append(" | ".join("---" for _ in headers))
    for row in rows[:50]:
        lines.append(" | ".join(str(row.get(header, "")) for header in headers))
    if len(rows) > 50:
        lines.append(f"… ({len(rows)} rows total)")
    return "\n".join(lines)
