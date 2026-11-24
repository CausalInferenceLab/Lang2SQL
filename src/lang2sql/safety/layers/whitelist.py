"""Whitelist layer — V1 gate that only lets read-only statements through.

Fail-closed by design: anything we can't confidently classify as a single
read-only ``SELECT`` / ``WITH`` (or an ``EXPLAIN`` of one) is BLOCKED. This is
deliberately blunt string parsing — the precise AST validation that catches
schema-qualified bypasses and the like is V1.5 work (no sqlglot here).
"""

from __future__ import annotations

import re

from ...core.ports.safety import (
    SafetyContext,
    SafetyDecision,
    Verdict,
)
from ..tokens import tokens

# Statement keywords that mutate state. If any of these appears as a *statement*
# keyword anywhere (including inside a CTE body), we block fail-closed.
_DML_DDL = frozenset(
    {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "ALTER",
        "CREATE",
        "TRUNCATE",
        "GRANT",
        "COPY",
        "INTO",
        "MERGE",
    }
)

# Statements we allow to *start* a query.
_ALLOWED_START = ("SELECT", "WITH")

_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


class WhitelistLayer:
    """SELECT/WITH-only gate. Implements ``SafetyLayerPort``."""

    @property
    def name(self) -> str:
        return "whitelist"

    def check(self, sql: str, ctx: SafetyContext) -> SafetyDecision:
        try:
            # Keep a placeholder for each literal/identifier so that a quoted
            # leading keyword cannot become the start of a statement.
            parts = [
                "?" if token.lastgroup == "quoted" else token[0]
                for token in tokens(sql, ctx.extras.get("dialect", ""))
            ]
        except ValueError:
            return SafetyDecision(Verdict.BLOCK, sql, "parse_error", self.name)
        if parts and parts[-1] == ";":
            parts.pop()
        if not parts:
            return SafetyDecision(Verdict.BLOCK, sql, "parse_error", self.name)
        if ";" in parts:
            return SafetyDecision(Verdict.BLOCK, sql, "multi_statement", self.name)
        statement = " ".join(parts)

        # An EXPLAIN wrapper is allowed only when it fronts a read-only query.
        # Strip a leading EXPLAIN (and EXPLAIN ANALYZE / EXPLAIN VERBOSE etc.)
        # then re-check the underlying statement's leading keyword.
        body = statement
        explain = re.match(r"(?is)^EXPLAIN\b(.*)$", body)
        if explain is not None:
            body = explain.group(1).strip()
            # ``EXPLAIN`` may carry options like ANALYZE / VERBOSE / a paren
            # option list before the real statement. Drop leading option words
            # and any "( ... )" option block so we reach the real keyword.
            paren = re.match(r"(?s)^\((?:[^()]|\([^()]*\))*\)\s*(.*)$", body)
            if paren is not None:
                body = paren.group(1).strip()
            else:
                # Consume leading bare option words (ANALYZE, VERBOSE, ...).
                while True:
                    m = re.match(
                        r"(?i)^(ANALYZE|VERBOSE|COSTS|BUFFERS|QUERY\s+PLAN)\b\s*(.*)$",
                        body,
                    )
                    if m is None:
                        break
                    body = m.group(2).strip()
            if not body:
                return SafetyDecision(
                    verdict=Verdict.BLOCK,
                    sql=sql,
                    reason="parse_error",
                    layer=self.name,
                )

        # The (possibly EXPLAIN-unwrapped) statement must start with an allowed
        # read-only keyword.
        first_word_m = _WORD.match(body)
        if first_word_m is None or first_word_m.group(0).upper() not in _ALLOWED_START:
            return SafetyDecision(
                verdict=Verdict.BLOCK,
                sql=sql,
                reason="not_select",
                layer=self.name,
            )

        # Fail-closed keyword scan: a mutating keyword appearing anywhere as a
        # standalone word blocks (catches ``WITH x AS (INSERT ...) SELECT``).
        for token in _WORD.findall(body):
            if token.upper() in _DML_DDL:
                return SafetyDecision(
                    verdict=Verdict.BLOCK,
                    sql=sql,
                    reason=f"dml_keyword:{token.upper()}",
                    layer=self.name,
                )

        return SafetyDecision(verdict=Verdict.PASS, sql=sql, layer=self.name)
