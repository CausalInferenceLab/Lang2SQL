"""RowLimitLayer — appends a dialect-appropriate outer row limit when absent.

Returns PASS (not REWRITE) so the rewritten SQL flows into the next layer
(TimeoutLayer) and ctx.timeout_seconds still gets configured.
"""

from __future__ import annotations

import re

from ...core.ports.safety import SafetyContext, SafetyDecision, Verdict

_LIMIT_RE = re.compile(r"\bLIMIT\s+\d+", re.IGNORECASE)
_FETCH_RE = re.compile(
    r"\bFETCH\s+(?:FIRST|NEXT)\s+(?:\d+\s+)?ROWS?\s+(?:ONLY|WITH\s+TIES)", re.IGNORECASE
)


def _has_top_level_limit(sql: str, pattern: re.Pattern = _LIMIT_RE) -> bool:
    """True if the requested row-limit clause exists at parenthesis depth 0."""
    # Ignore quoted text/comments so examples such as 'FETCH FIRST 1 ROWS ONLY'
    # cannot disable the cap. Parentheses in literals do not affect nesting.
    tokens = re.findall(
        r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|[()]|[^()'\"/-]+|.",
        sql,
        re.DOTALL,
    )
    depth = 0
    top_level = []
    for token in tokens:
        if token.startswith(("'", '"', "--", "/*")):
            top_level.append(" ")
        elif token == "(":
            depth += 1
        elif token == ")":
            depth -= 1
        elif depth == 0:
            top_level.append(token)
    return bool(pattern.search("".join(top_level)))


class RowLimitLayer:
    """Appends LIMIT or Oracle FETCH; leaves explicit outer limits untouched."""

    @property
    def name(self) -> str:
        return "row_limit"

    def check(self, sql: str, ctx: SafetyContext) -> SafetyDecision:
        oracle = ctx.extras.get("dialect") == "oracle"
        if oracle:
            sql = sql.rstrip().rstrip(";")
        pattern = _FETCH_RE if oracle else _LIMIT_RE
        if _has_top_level_limit(sql, pattern):
            return SafetyDecision(verdict=Verdict.PASS, sql=sql, layer=self.name)
        limit = ctx.row_limit if ctx.row_limit and ctx.row_limit > 0 else 1000
        clause = f"FETCH FIRST {limit} ROWS ONLY" if oracle else f"LIMIT {limit}"
        rewritten = sql.rstrip().rstrip(";") + f"\n{clause}"
        return SafetyDecision(verdict=Verdict.PASS, sql=rewritten, layer=self.name)
