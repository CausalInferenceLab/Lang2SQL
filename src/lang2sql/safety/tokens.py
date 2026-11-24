"""Lexical boundaries shared by the statement gate and row cap."""

from __future__ import annotations

import re
from collections.abc import Iterator

# Match quoted spans atomically. A stray opening delimiter means the span
# was unterminated. Comments/whitespace are trivia, but offsets always refer
# to the original SQL so a rewrite never needs to reconstruct a literal.
_COMMON = r"""
    (?P<trivia>\s+|--[^\n]*|/\*.*?\*/)
  | (?P<quoted>'(?:''|[^'])*'|"(?:""|[^"])*"|`(?:``|[^`])*`{bracket})
  | (?P<invalid>/\*|['"`]{unclosed_bracket})
  | (?P<word>[\w$]+)
  | (?P<symbol>.)
"""
_FLAGS = re.DOTALL | re.VERBOSE
_SQLITE = re.compile(
    _COMMON.format(bracket=r"|\[[^\]]*\]", unclosed_bracket=r"|\["), _FLAGS
)
_STANDARD = re.compile(_COMMON.format(bracket="", unclosed_bracket=""), _FLAGS)


def tokens(sql: str, dialect: str = "") -> Iterator[re.Match[str]]:
    sqlite = dialect == "sqlite"
    for token in (_SQLITE if sqlite else _STANDARD).finditer(sql):
        value = token[0]
        if token.lastgroup == "invalid":
            raise ValueError("unterminated SQL token")
        if not sqlite:
            # These forms change the meaning of delimiters across engines.
            # Only classify SQL whose quoting/comment rules we can determine.
            if (
                (token.lastgroup == "quoted" and re.search(r"""\\['"`]""", value))
                or value.startswith(("/*!", "/*M!"))
                or (value.startswith("/*") and "/*" in value[2:])
                or (
                    value.startswith("--") and len(value) > 2 and not value[2].isspace()
                )
                or (token.lastgroup == "word" and re.search(r"\$\w*\$", value))
                or value == "#"
            ):
                raise ValueError("unsupported SQL delimiter")
        if token.lastgroup != "trivia":
            yield token
