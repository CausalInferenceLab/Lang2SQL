"""run_sql — execute a read-only query, but only after the safety gate (★①).

The tool never touches the DB directly: it pushes the model's SQL through the
:class:`SafetyPipelinePort` first. BLOCK/CONFIRM short-circuit with an
explanation the model can act on; PASS/REWRITE proceed to
``explorer.execute`` and the (possibly rewritten) SQL is what runs.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any


from ..core.ports.audit import AuditEvent
from ..core.ports.explorer import DataAccessError
from ..core.ports.safety import SafetyContext, Verdict
from ..core.types import ToolResult, ToolSpec

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

logger = logging.getLogger(__name__)


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
                    "limit": {
                        "type": "integer",
                        "description": "max rows (default 1000)",
                    },
                },
                "required": ["sql"],
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        sql = (args.get("sql") or "").strip()
        try:
            limit = int(args.get("limit") or 1000)
        except (TypeError, ValueError) as exc:
            logger.warning(
                "invalid row limit scope=%s error=%s: %s",
                ctx.identity.session_key(),
                type(exc).__name__,
                exc,
            )
            limit = 1000  # tolerate a malformed limit from the model

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

        decision = ctx.safety.evaluate(
            sql,
            SafetyContext(
                row_limit=limit,
                extras={"dialect": getattr(ctx.explorer, "dialect", "")},
            ),
        )
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
        logger.info("run_sql started scope=%s actor=%s", scope, ctx.identity.user_id)
        try:
            rows = await ctx.explorer.execute(decision.sql, limit)
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
        logger.info("run_sql completed scope=%s rows=%d", scope, len(rows))

        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="run_sql",
                    scope=ctx.identity.session_key(),
                    detail={"sql": decision.sql},
                )
            )

        return ToolResult(call_id="", content=_render_rows(decision.sql, rows))


def _render_rows(sql: str, rows: list[dict]) -> str:
    if not rows:
        return f"(0 rows)\nSQL: {sql}"
    headers = list(rows[0].keys())
    lines = [" | ".join(headers), " | ".join("---" for _ in headers)]
    for r in rows[:50]:
        lines.append(" | ".join(str(r.get(h, "")) for h in headers))
    suffix = f"\n… ({len(rows)} rows total)" if len(rows) > 50 else ""
    return f"{len(rows)} row(s):\n" + "\n".join(lines) + suffix + f"\nSQL: {sql}"
