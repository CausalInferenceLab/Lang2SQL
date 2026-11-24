"""Identify SQL submissions at the harness boundary."""

from __future__ import annotations

import re
from typing import Callable

_SQL_FENCE = re.compile(r"```sql[ \t]*\r?\n(.*?)```", re.IGNORECASE | re.DOTALL)
_SQL_LABEL = re.compile(
    r"^(?:\*\*)?(?:final\s+sql|최종\s*sql|sql)(?:\*\*)?\s*:(?:\*\*)?\s*",
    re.IGNORECASE,
)
_SENTENCE_END = re.compile(r"(?:[!?。！？]|(?<!\d)\.)$")
_COMMENTS_AND_QUOTES = re.compile(
    r"--[^\n]*|/\*.*?\*/|'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|\[[^\]]*\]",
    re.DOTALL,
)
_READ_QUERY = re.compile(
    r"^;?\s*(?:EXPLAIN\s+(?:\([^)]*\)\s*"
    r"|(?:(?:ANALYZE|VERBOSE|COSTS|BUFFERS|QUERY\s+PLAN)\s+)*))?"
    r"(?:SELECT\b|WITH\s+(?:RECURSIVE\s+)?[\w$]+\s*(?:\([^)]*\)\s*)?AS\s*"
    r"(?:(?:NOT\s+)?MATERIALIZED\s*)?\()",
    re.IGNORECASE,
)
_QUERY_TOKENS = re.compile(
    r"\b(?:SELECT|IS\s+(?:NOT\s+)?DISTINCT\s+FROM|FROM)\b|[()]",
    re.IGNORECASE,
)
_TABLE_SOURCE = re.compile(
    r"(?:\s*\()*\s*(?!SELECT\b|WITH\b|VALUES\b)\w", re.IGNORECASE
)


def is_database_query(sql: object) -> bool:
    """Recognize a table source in a read query without resolving SQL names."""
    if not isinstance(sql, str):
        return False

    def mask(match: re.Match[str]) -> str:
        token = match[0]
        if token.startswith(("--", "/*", "'")):
            return " "
        return " identifier "

    bare = _COMMENTS_AND_QUOTES.sub(mask, sql).strip()
    if not _READ_QUERY.match(bare):
        return False

    # SQL expressions such as EXTRACT(... FROM ...) share the FROM keyword.
    # A FROM clause belongs to a SELECT at the same parenthesis depth.
    depth = 0
    select_depths: set[int] = set()
    for token in _QUERY_TOKENS.finditer(bare):
        word = token[0].upper()
        if word == "(":
            depth += 1
        elif word == ")":
            select_depths.discard(depth)
            depth -= 1
        elif word == "SELECT":
            select_depths.add(depth)
        elif (
            word == "FROM"
            and depth in select_depths
            and _TABLE_SOURCE.match(bare, token.end())
        ):
            return True
    return False


def sql_from_answer(
    answer: str, extract_final_sql: Callable[[str], str] | None
) -> str | None:
    """Extract a candidate from a standalone, optionally labeled SQL answer."""
    if extract_final_sql is not None:
        try:
            return extract_final_sql(answer)
        except ValueError:
            return None

    answer = _SQL_LABEL.sub("", answer.strip(), count=1)
    if block := _SQL_FENCE.fullmatch(answer):
        return block[1].strip() if "```" not in block[1] else None
    if "```" in answer or _SENTENCE_END.search(answer):
        return None
    return answer
