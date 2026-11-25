"""Parse the single fenced read-only SQL submission used by the benchmarks."""

import re


class SubmissionError(ValueError):
    def __init__(self, reason: str):
        self.reason = reason
        super().__init__(reason)


def final_sql(answer: str) -> str:
    blocks = re.findall(r"```sql\s*\n?(.*?)```", answer, re.IGNORECASE | re.DOTALL)
    if not blocks:
        raise SubmissionError("missing_sql")
    if len(blocks) != 1:
        raise SubmissionError("multiple_sql_blocks")
    sql = blocks[0].strip()
    if not re.match(r"^(SELECT|WITH)\b", sql, re.IGNORECASE):
        raise SubmissionError("invalid_final_sql")
    return sql
