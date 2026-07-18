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
    }
)

# Statements we allow to *start* a query.
_ALLOWED_START = ("SELECT", "WITH")

# A line comment runs to end-of-line; a block comment is /* ... */ (non-greedy,
# spanning newlines).
_LINE_COMMENT = re.compile(r"--[^\n]*")
_BLOCK_COMMENT = re.compile(r"/\*.*?\*/", re.DOTALL)

# Word-boundary keyword match, case-insensitive. Used to scan for mutating
# keywords as standalone words (so "created_at" never matches "CREATE").
_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9]*")


def _strip_comments(sql: str) -> str:
    """Remove line and block comments, then collapse surrounding whitespace."""
    no_block = _BLOCK_COMMENT.sub(" ", sql)
    no_line = _LINE_COMMENT.sub(" ", no_block)
    return no_line.strip()


def _split_statements(sql: str) -> list[str]:
    """Split on ``;`` into non-empty trimmed statements.

    String/identifier-quote awareness is V1.5 AST work; for V1 a bare ``;``
    separator is enough to detect multi-statement payloads fail-closed.
    """
    parts = [p.strip() for p in sql.split(";")]
    return [p for p in parts if p]


class WhitelistLayer:
    """SELECT/WITH-only gate. Implements ``SafetyLayerPort``."""

    @property
    def name(self) -> str:
        return "whitelist"

    def check(self, sql: str, ctx: SafetyContext) -> SafetyDecision:
        cleaned = _strip_comments(sql)

        # Empty / blank (or comment-only) input → cannot parse anything.
        if not cleaned:
            return SafetyDecision(
                verdict=Verdict.BLOCK,
                sql=sql,
                reason="parse_error",
                layer=self.name,
            )

        statements = _split_statements(cleaned)

        # Multi-statement payloads (e.g. ``; DELETE FROM t; --``) are blocked.
        if len(statements) != 1:
            return SafetyDecision(
                verdict=Verdict.BLOCK,
                sql=sql,
                reason="multi_statement",
                layer=self.name,
            )

        statement = statements[0]

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
                        r"(?i)^(ANALYZE|VERBOSE|COSTS|BUFFERS)\b\s*(.*)$", body
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
