"""Compile exact fixed-scale aggregation to ordinary SQLite integer arithmetic."""

from __future__ import annotations

from ..core.types import ToolResult, ToolSpec

# TOTAL is used only to bound absolute sums. Every integer up to this boundary
# is exactly representable, and the actual signed sum uses INTEGER arithmetic.
_MAX_EXACT_INTEGER = 2**53 - 1
_MAX_SCALE = 6


def fixed_decimal_ctes(input_scale: int, output_scale: int, aggregate: str) -> str:
    """Consume decimal_input(group_id, scaled_value); do not infer input precision."""
    if any(
        type(x) is not int or not 0 <= x <= _MAX_SCALE
        for x in (input_scale, output_scale)
    ):
        raise ValueError(
            f"input_scale/output_scale must be integers from 0 to {_MAX_SCALE}"
        )
    if aggregate not in ("avg", "sum"):
        raise ValueError("aggregate must be avg or sum")
    factor = 10**output_scale
    denominator = (
        f"n * {10**input_scale}" if aggregate == "avg" else str(10**input_scale)
    )
    bound = _MAX_EXACT_INTEGER // factor
    return f"""decimal_bounds AS (
  SELECT group_id, scaled_value,
    COUNT(scaled_value) OVER g AS n,
    SUM(CASE WHEN scaled_value IS NOT NULL AND typeof(scaled_value) <> 'integer' THEN 1 ELSE 0 END) OVER g AS invalid_count,
    SUM(CASE WHEN typeof(scaled_value) = 'integer' AND scaled_value NOT BETWEEN {-bound} AND {bound} THEN 1 ELSE 0 END) OVER g AS oversized_count,
    TOTAL(CASE WHEN typeof(scaled_value) = 'integer' AND scaled_value BETWEEN {-bound} AND {bound} THEN ABS(scaled_value) ELSE 0 END) OVER g AS abs_total
  FROM decimal_input
  WINDOW g AS (PARTITION BY group_id)
), decimal_totals AS (
  SELECT group_id, n, invalid_count,
    CASE WHEN invalid_count > 0 THEN 'invalid_input'
      WHEN n = 0 THEN 'empty'
      WHEN oversized_count > 0 OR abs_total > {bound}
        OR n > {_MAX_EXACT_INTEGER // 10**input_scale} THEN 'overflow'
      ELSE 'ok' END AS status,
    SUM(CASE WHEN invalid_count = 0 AND oversized_count = 0
      AND abs_total <= {bound} AND n <= {_MAX_EXACT_INTEGER // 10**input_scale}
      THEN scaled_value END) AS scaled_sum
  FROM decimal_bounds
  GROUP BY group_id
), decimal_fraction AS (
  SELECT group_id, n, invalid_count, status,
    scaled_sum * {factor} AS numerator, {denominator} AS denominator
  FROM decimal_totals
), decimal_rounded AS (
  SELECT group_id, n, invalid_count, status,
    CASE WHEN status = 'ok' THEN numerator / denominator
      + CASE WHEN ABS(numerator % denominator) >= (denominator + 1) / 2
        THEN CASE WHEN numerator < 0 THEN -1 ELSE 1 END ELSE 0 END END AS rounded_units
  FROM decimal_fraction
), decimal_result AS (
  SELECT *, rounded_units * 1.0 / {factor} AS rounded_value
  FROM decimal_rounded
)"""


class FixedDecimalSQL:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="fixed_decimal_sql",
            description=(
                "Generate tested SQLite SQL for rounded decimal AVG/SUM using integer arithmetic, "
                "avoiding binary floating-point midpoint errors in ROUND(AVG(...)). "
                "Requires explicitly scaled integer inputs and known decimal precision. "
                "Returns CTEs; execute through run_sql. Does not infer monetary units or round source data."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "dialect": {"type": "string", "enum": ["sqlite"]},
                    "input_scale": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": _MAX_SCALE,
                    },
                    "output_scale": {
                        "type": "integer",
                        "minimum": 0,
                        "maximum": _MAX_SCALE,
                    },
                    "aggregate": {"type": "string", "enum": ["avg", "sum"]},
                },
                "required": ["dialect", "input_scale", "output_scale", "aggregate"],
            },
        )

    async def run(self, args, ctx) -> ToolResult:
        if (
            set(args) != {"dialect", "input_scale", "output_scale", "aggregate"}
            or args["dialect"] != "sqlite"
        ):
            raise ValueError(
                "Require dialect=sqlite, input_scale, output_scale and aggregate"
            )
        ctes = fixed_decimal_ctes(
            args["input_scale"], args["output_scale"], args["aggregate"]
        )
        return ToolResult(
            call_id="",
            content=(
                "SQLite only. Supply WITH decimal_input AS (your SELECT exposing group_id, "
                "scaled_value), append these CTEs, then SELECT from decimal_result. Reserve "
                "decimal_* CTE names. NULL inputs are excluded like AVG/SUM.\n"
                "scaled_value MUST be an INTEGER representing value * 10^input_scale. "
                "Confirm source precision before conversion: for known fixed-scale numeric data, "
                "CAST(ROUND(value * 10^input_scale) AS INTEGER) removes representation noise. "
                "Do not choose a scale that rounds away meaningful source digits. No precision "
                "or currency conversion is inferred by this tool.\n"
                "Inspect status and invalid_count: noninteger non-NULL input makes the entire "
                "group invalid_input; unsafe magnitude makes it overflow; all NULL makes it empty. "
                "Those groups retain their row with NULL results. No partial aggregates.\n"
                "Rounding is half away from zero, performed once after aggregation using exact "
                "integer quotient/remainder. rounded_units is the exact integer at output_scale; "
                "rounded_value is its REAL representation for normal SQL display. "
                "This changes neither the source values nor the DB connection; run_sql remains required.\n\n"
                + ctes
            ),
        )
