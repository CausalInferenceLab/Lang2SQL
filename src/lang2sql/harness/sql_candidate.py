"""Identify SQL submissions at the harness boundary."""

from __future__ import annotations

import re

_SQL_FENCE = re.compile(r"```sql[ \t]*\r?\n(.*?)```", re.IGNORECASE | re.DOTALL)
_SQL_LABEL = re.compile(
    r"^[ \t]*(?:#{1,6}[ \t]+)?(?:\*\*)?(?:final\s+sql|최종\s*sql|sql)(?:\*\*)?\s*:(?:\*\*)?\s*",
    re.IGNORECASE | re.MULTILINE,
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


def _query_shape(sql: str) -> str:
    def mask(match: re.Match[str]) -> str:
        token = match[0]
        if token.startswith(("--", "/*", "'")):
            return " "
        return " identifier "

    return _COMMENTS_AND_QUOTES.sub(mask, sql).strip()


def is_database_query(sql: object) -> bool:
    """Recognize a table source in a read query without resolving SQL names."""
    if not isinstance(sql, str):
        return False
    bare = _query_shape(sql)
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


def sql_from_answer(answer: str) -> str | None:
    """Recognize draft SQL and reject malformed submissions marked as final."""
    answer = answer.strip()
    bare = _query_shape(answer)
    if _READ_QUERY.match(bare) and not _SENTENCE_END.search(bare):
        return answer

    # A label inside a fenced example is part of the example.
    label = next(
        (
            m
            for m in _SQL_LABEL.finditer(answer)
            if answer[: m.start()].count("```") % 2 == 0
        ),
        None,
    )
    if label:
        answer = answer[label.end() :].strip()
    if label or re.match(r"^```sql\b", answer, re.IGNORECASE):
        if block := _SQL_FENCE.fullmatch(answer):
            sql = block[1].strip()
            if sql and "```" not in sql:
                return sql
        elif answer and "```" not in answer:
            return answer
        raise ValueError("expected one complete final SQL submission")

    if _SENTENCE_END.search(bare):
        return None
    if re.match(
        r"^(?:INSERT\s+INTO|UPDATE\s+\S+\s+SET|DELETE\s+FROM|DROP\s+TABLE|"
        r"CREATE\s+TABLE|ALTER\s+TABLE|EXPLAIN)\b",
        bare,
        re.IGNORECASE,
    ):
        return answer
    return None
