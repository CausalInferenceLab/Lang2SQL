"""RowLimitLayer — automatically appends LIMIT to queries that lack one.

Returns PASS (not REWRITE) so the rewritten SQL flows into the next layer
(TimeoutLayer) and ctx.timeout_seconds still gets configured.
"""

from __future__ import annotations

from ...core.ports.safety import SafetyContext, SafetyDecision, Verdict
from ..tokens import tokens


class RowLimitLayer:
    """Appends LIMIT when absent; leaves explicit LIMITs untouched."""

    @property
    def name(self) -> str:
        return "row_limit"

    def check(self, sql: str, ctx: SafetyContext) -> SafetyDecision:
        depth = end = 0
        for token in tokens(sql, ctx.extras.get("dialect", "")):
            value = token[0]
            if value == "(":
                depth += 1
            elif value == ")":
                depth -= 1
            elif depth == 0 and value.upper() == "LIMIT":
                return SafetyDecision(verdict=Verdict.PASS, sql=sql, layer=self.name)
            if value != ";":
                end = token.end()

        limit = ctx.row_limit if ctx.row_limit > 0 else 1000
        rewritten = sql[:end] + f"\nLIMIT {limit}" + sql[end:]
        return SafetyDecision(verdict=Verdict.PASS, sql=rewritten, layer=self.name)
