"""run_sql — execute a read-only query, but only after the safety gate (★①).

The tool never touches the DB directly: it pushes the model's SQL through the
:class:`SafetyPipelinePort` first. BLOCK/CONFIRM short-circuit with an
explanation the model can act on; PASS/REWRITE proceed to
``explorer.execute`` and the (possibly rewritten) SQL is what runs.
"""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from ..core.ports.audit import AuditEvent
from ..core.ports.explorer import DataAccessError, QueryTimeoutExplorerPort
from ..core.ports.safety import SafetyContext, Verdict
from ..core.types import Role, ToolResult, ToolSpec
from .bound_metric import query_metric
from .semantic_federation import _load_all, _resolve_entry

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

logger = logging.getLogger(__name__)
_DISPLAY_ROW_LIMIT = 50


class RunSQL:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="run_sql",
            description=(
                "Run a single read-only SQL query (SELECT/WITH only) and return "
                "rows. Queries are checked by a safety gate before execution."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "sql": {
                        "type": "string",
                        "description": "a single SELECT or WITH query",
                    },
                    "expected_columns": {
                        "type": "array",
                        "items": {"type": "string", "minLength": 1},
                        "minItems": 1,
                        "uniqueItems": True,
                        "description": "Optional exact, ordered output column labels derived from the user request. Missing, extra or reordered columns return an error. This verifies projection only, not semantic correctness. SQL mode only.",
                    },
                    "metric_term": {
                        "type": "string",
                        "description": "Exact confirmed Oracle metric. Provide its date interval instead of SQL.",
                    },
                    "period_start": {"type": "string", "format": "date"},
                    "period_end": {
                        "type": "string",
                        "format": "date",
                        "description": "Exclusive end date",
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "description": "Positive maximum number of rows to fetch (default 1000); unlike SQLite SQL LIMIT, -1 is not supported.",
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: HarnessContext) -> ToolResult:
        expected = args.get("expected_columns")
        if "expected_columns" in args and (
            not isinstance(expected, list) or not expected
            or any(not isinstance(c, str) or not c for c in expected)
            or len(set(expected)) != len(expected)
        ):
            logger.warning("invalid output columns scope=%s", ctx.identity.session_key())
            return ToolResult(call_id="", is_error=True, content="Invalid expected_columns: supply unique nonempty column labels. SQL was NOT executed.")
        metric = args.get("metric_term")
        question = next(
            (
                m.content.casefold()
                for m in reversed(ctx.session.history())
                if m.role == Role.USER
            ),
            "",
        )
        named = []
        if ctx.store and (metric or question):
            for entries in _load_all(ctx.store, ctx.identity.kv_scope).values():
                entry = _resolve_entry(
                    entries, ctx.identity.effective_channel_id, ctx.identity.user_id
                )
                if (
                    entry
                    and entry.oracle_view
                    and (
                        entry.term.casefold() == str(metric).casefold()
                        or any(
                            re.search(
                                r"(?<![a-z0-9_])"
                                + re.escape(label.casefold())
                                + r"(?![a-z0-9_])",
                                question,
                            )
                            for label in [entry.term, *entry.synonyms]
                            if label
                        )
                    )
                ):
                    named.append(entry)
        if named or metric:
            if len(named) != 1 or (
                metric and named[0].term.casefold() != str(metric).casefold()
            ):
                return ToolResult(
                    call_id="",
                    is_error=True,
                    content="Resolve one confirmed Oracle metric in this scope before running SQL.",
                )
            return await query_metric(ctx, named[0], args)
        if any(k in args for k in ("period_start", "period_end")):
            return ToolResult(
                call_id="",
                is_error=True,
                content="A bound metric is required for period parameters.",
            )
        sql = (args.get("sql") or "").strip()
        try:
            limit = int(args.get("limit", 1000))
        except (TypeError, ValueError) as exc:
            logger.warning(
                "invalid row limit scope=%s error=%s: %s",
                ctx.identity.session_key(),
                type(exc).__name__,
                exc,
            )
            limit = 1000  # tolerate a malformed limit from the model

        if limit <= 0:
            logger.warning("invalid nonpositive row limit scope=%s", ctx.identity.session_key())
            return ToolResult(
                call_id="", is_error=True,
                content="Invalid limit: supply a positive maximum row count. SQL was NOT executed.",
            )

        if ctx.safety is None:
            return ToolResult(
                call_id="",
                content="run_sql unavailable: no safety pipeline wired",
                is_error=True,
            )
        if ctx.explorer is None:
            return ToolResult(
                call_id="",
                content="run_sql unavailable: no DB connected (use /connect)",
                is_error=True,
            )

        safety_context = SafetyContext(
            row_limit=limit,
            extras={"dialect": getattr(ctx.explorer, "dialect", "")},
        )
        decision = ctx.safety.evaluate(sql, safety_context)
        if decision.verdict == Verdict.BLOCK:
            return ToolResult(
                call_id="",
                content=f"BLOCKED by {decision.layer}: {decision.reason}",
                is_error=True,
            )
        if decision.verdict == Verdict.CONFIRM:
            return ToolResult(
                call_id="", content=f"NEEDS CONFIRMATION: {decision.confirm_prompt}"
            )

        scope = ctx.identity.session_key()
        timeout_enforced = (
            isinstance(ctx.explorer, QueryTimeoutExplorerPort)
            and ctx.explorer.supports_query_timeout
        )
        logger.info("run_sql started scope=%s actor=%s", scope, ctx.identity.user_id)
        try:
            # One lookahead row distinguishes an exact-size result from a
            # result truncated by the adapter fetch budget. SQL safety is unchanged.
            if timeout_enforced:
                rows = await ctx.explorer.execute_with_timeout(
                    decision.sql, limit + 1, safety_context.timeout_seconds
                )
            else:
                logger.warning("native query timeout unsupported scope=%s", scope)
                rows = await ctx.explorer.execute(decision.sql, limit + 1)
        except Exception as exc:
            logger.warning(
                "run_sql failed scope=%s error=%s: %s", scope, type(exc).__name__, exc
            )
            if ctx.audit is not None:
                await ctx.audit.record(
                    AuditEvent(
                        actor=ctx.identity.user_id,
                        action="run_sql_failed",
                        scope=scope,
                        detail={"sql": decision.sql, "error_type": type(exc).__name__},
                    )
                )
            if isinstance(exc, DataAccessError):
                return ToolResult(
                    call_id="",
                    is_error=True,
                    stop_turn=True,
                    content="요청한 데이터에 접근할 수 없습니다. 현재 DB 계정에 조회 권한이 없거나 테이블이 존재하지 않습니다. DB 관리자에게 확인해 주세요.",
                )
            raise
        actual_columns = getattr(rows, "columns", None)
        if actual_columns is None and rows:
            actual_columns = tuple(rows[0])
        contract_error = None
        if expected is not None:
            if actual_columns is None:
                contract_error = "column metadata unavailable for this empty result"
            elif list(actual_columns) != expected:
                missing = [c for c in expected if c not in actual_columns]
                unexpected = [c for c in actual_columns if c not in expected]
                contract_error = f"missing={missing!r}; unexpected={unexpected!r}; expected order={expected!r}; actual order={list(actual_columns)!r}"
            if contract_error:
                logger.warning("run_sql output contract failed scope=%s reason=%s", scope, contract_error)
            else:
                logger.info("run_sql output columns validated scope=%s", scope)
        fetch_truncated = len(rows) > limit
        rows = rows[:limit]
        logger.info("run_sql completed scope=%s rows=%d fetch_truncated=%s", scope, len(rows), fetch_truncated)

        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="run_sql_contract_failed" if contract_error else "run_sql",
                    scope=ctx.identity.session_key(),
                    detail={"sql": decision.sql, **({"contract_error": contract_error} if contract_error else {})},
                )
            )

        rendered = _render_rows(
            decision.sql, rows, fetch_truncated=fetch_truncated,
            sql_rewritten=decision.sql != sql,
            query_timeout_enforced=timeout_enforced,
            query_timeout_scope=ctx.explorer.query_timeout_scope if timeout_enforced else "unsupported",
        )
        if contract_error:
            return ToolResult(call_id="", is_error=True, content=f"Output column contract failed: {contract_error}. SQL executed; revise the projection or verify the contract against the request.\n{rendered}")
        if expected is not None:
            # Preserve the row-count header and SQL footer consumed by provenance checks.
            header, remainder = rendered.split("\n", 1)
            rendered = header + "\nOutput columns validated (projection only).\n" + remainder
        return ToolResult(call_id="", content=rendered)


def _render_rows(sql: str, rows: list[dict], *, fetch_truncated=False, sql_rewritten=False, query_timeout_enforced=False, query_timeout_scope="unsupported") -> str:
    coverage = (
        f"Result coverage: fetched_rows={len(rows)}; "
        f"displayed_rows={min(len(rows), _DISPLAY_ROW_LIMIT)}; "
        f"fetch_truncated={str(fetch_truncated).lower()}; "
        f"display_truncated={str(len(rows) > _DISPLAY_ROW_LIMIT).lower()}; "
        f"sql_rewritten={str(sql_rewritten).lower()}; "
        f"query_timeout_enforced={str(query_timeout_enforced).lower()}; "
        f"query_timeout_scope={query_timeout_scope}. "
        "Counts refer to the executed SQL, not the source table size."
    )
    if not rows:
        return f"(0 rows)\n{coverage}\nSQL: {sql}"
    headers = list(rows[0].keys())
    observed_types = {
        h: sorted({type(r.get(h)).__name__ for r in rows[:_DISPLAY_ROW_LIMIT]}) for h in headers
    }
    type_note = "Observed value types (displayed rows): " + "; ".join(
        f"{h}={','.join(kinds)}" for h, kinds in observed_types.items()
    )
    lines = [" | ".join(headers), " | ".join("---" for _ in headers)]
    for r in rows[:_DISPLAY_ROW_LIMIT]:
        lines.append(" | ".join(str(r.get(h, "")) for h in headers))
    return (
        f"{len(rows)} row(s):\n{coverage}\n{type_note}\n"
        + "\n".join(lines)
        + f"\nSQL: {sql}"
    )
