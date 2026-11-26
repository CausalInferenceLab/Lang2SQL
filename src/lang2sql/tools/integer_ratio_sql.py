"""Compile integer ratios with exact scaling followed by one REAL division."""

from __future__ import annotations

from ..core.types import ToolResult, ToolSpec
from .fixed_decimal_sql import _MAX_EXACT_INTEGER


def integer_ratio_ctes(multiplier: int) -> str:
    """Consume ratio_input(group_id, numerator, denominator), without aggregation."""
    if type(multiplier) is not int or not 1 <= multiplier <= _MAX_EXACT_INTEGER:
        raise ValueError(f"multiplier must be an integer from 1 to {_MAX_EXACT_INTEGER}")
    bound = _MAX_EXACT_INTEGER // multiplier
    return f"""ratio_checked AS (
  SELECT group_id, numerator, denominator,
    CASE WHEN numerator IS NULL OR denominator IS NULL THEN 'null_input'
      WHEN typeof(numerator) NOT IN ('integer', 'real')
        OR typeof(denominator) NOT IN ('integer', 'real') THEN 'invalid_input'
      WHEN numerator NOT BETWEEN {-bound} AND {bound}
        OR denominator NOT BETWEEN {-_MAX_EXACT_INTEGER} AND {_MAX_EXACT_INTEGER} THEN 'overflow'
      WHEN numerator <> CAST(numerator AS INTEGER)
        OR denominator <> CAST(denominator AS INTEGER) THEN 'invalid_input'
      WHEN denominator = 0 THEN 'zero_denominator'
      ELSE 'ok' END AS status
  FROM ratio_input
), ratio_result AS (
  SELECT group_id, numerator, denominator, status,
    CASE WHEN status = 'ok' THEN
      (CAST(numerator AS INTEGER) * {multiplier}) * 1.0 / CAST(denominator AS INTEGER)
    END AS ratio_value
  FROM ratio_checked
)"""


class IntegerRatioSQL:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="integer_ratio_sql",
            description=(
                "Generate tested SQLite SQL for a ratio or percentage of exact integer quantities. "
                "Scale the numerator exactly before one REAL division, avoiding integer division "
                "and an extra rounding step from divide-then-scale. Accepts integral REAL values "
                "within safe bounds; rejects fractional/text inputs rather than rounding them. "
                "Returns CTEs to execute through run_sql; no business meaning or precision inferred."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "dialect": {"type": "string", "enum": ["sqlite"]},
                    "multiplier": {
                        "type": "integer", "minimum": 1, "maximum": _MAX_EXACT_INTEGER,
                        "description": "Explicit requested scale: 1 for a fraction, 100 for percent. No default.",
                    },
                },
                "required": ["dialect", "multiplier"],
            },
        )

    async def run(self, args, ctx) -> ToolResult:
        if set(args) != {"dialect", "multiplier"} or args["dialect"] != "sqlite":
            raise ValueError("Require dialect=sqlite and multiplier")
        ctes = integer_ratio_ctes(args["multiplier"])
        return ToolResult(call_id="", content=(
            "SQLite only. Supply WITH ratio_input AS (your SELECT exposing group_id, numerator, "
            "denominator), append these CTEs, and SELECT from ratio_result. Reserve ratio_* names. "
            "One output row per input row, including failures; no grouping, filtering or deduplication. "
            "Only exact integer quantities are supported, including REAL values that already equal "
            "integers. Do not round or cast away fractional source data to satisfy this contract. "
            "Inspect status: null_input, invalid_input, overflow and zero_denominator retain their "
            "row with NULL ratio_value. For ok rows, integer scaling is exact within the shared "
            "binary64 exact-integer bound; only the final division rounds to REAL. This is not an "
            "exact decimal representation or a decimal-place rounding policy. Use the requested "
            "numerator, denominator and units; this tool cannot validate those choices.\n\n" + ctes
        ))
