"""Compile calendar duration components to portable SQLite CTEs, without I/O."""

from __future__ import annotations

from ..core.types import ToolResult, ToolSpec


def _clamped_anchor(months: str) -> str:
    """Shift from a month start, then clamp the original day to that month."""
    month = f"date(lo, 'start of month', printf('+%d months', {months}))"
    last_day = (
        f"CASE WHEN strftime('%m', {month}) = '12' THEN 31 "
        f"ELSE CAST(strftime('%d', date({month}, '+1 month', '-1 day')) AS INTEGER) END"
    )
    return (
        f"date({month}, printf('+%d days', "
        f"MIN(CAST(strftime('%d', lo) AS INTEGER), {last_day}) - 1))"
    )


# The caller supplies calendar_input(row_id, start_date, end_date). Keep each
# input row, including invalid dates, so missing data cannot silently vanish.
CALENDAR_CTES = f"""calendar_validated AS (
  SELECT row_id,
    CASE WHEN typeof(start_date) = 'text' AND length(start_date) = 10
      AND start_date BETWEEN '0001-01-01' AND '9999-12-31'
      AND date(start_date, '+0 days') = start_date THEN start_date END AS s,
    CASE WHEN typeof(end_date) = 'text' AND length(end_date) = 10
      AND end_date BETWEEN '0001-01-01' AND '9999-12-31'
      AND date(end_date, '+0 days') = end_date THEN end_date END AS e
  FROM calendar_input
), calendar_ordered AS (
  SELECT row_id, MIN(s, e) AS lo, MAX(s, e) AS hi,
    CASE WHEN s IS NULL OR e IS NULL THEN NULL
      WHEN e < s THEN -1 ELSE 1 END AS direction
  FROM calendar_validated
), calendar_months AS (
  SELECT *, (CAST(strftime('%Y', hi) AS INTEGER) - CAST(strftime('%Y', lo) AS INTEGER)) * 12
    + CAST(strftime('%m', hi) AS INTEGER) - CAST(strftime('%m', lo) AS INTEGER) AS m
  FROM calendar_ordered
), calendar_complete_months AS (
  SELECT *, m - CASE WHEN {_clamped_anchor("m")} > hi THEN 1 ELSE 0 END AS whole_months
  FROM calendar_months
), calendar_interval AS (
  SELECT row_id, direction,
    whole_months / 12 AS years, whole_months % 12 AS months,
    CAST(julianday(hi) - julianday({_clamped_anchor("whole_months")}) AS INTEGER) AS days
  FROM calendar_complete_months
)"""


class CalendarIntervalSQL:
    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="calendar_interval_sql",
            description=(
                "Generate tested SQLite SQL for elapsed calendar durations: whole years, "
                "remaining months and remaining days, with month-end clamping and leap years. "
                "Use for ages/tenures/calendar spans, not independent year/month/day field "
                "subtraction, boundary counts or total elapsed days. Returns CTEs only; "
                "execute the composed query through run_sql. Does not choose business rules."
            ),
            parameters={
                "type": "object",
                "properties": {"dialect": {"type": "string", "enum": ["sqlite"]}},
                "required": ["dialect"],
            },
        )

    async def run(self, args, ctx) -> ToolResult:
        if args != {"dialect": "sqlite"}:
            raise ValueError("calendar_interval_sql requires dialect='sqlite' only")
        return ToolResult(
            call_id="",
            content=(
                "SQLite only. Start with WITH calendar_input AS (your SELECT exposing "
                "row_id, start_date, end_date), then append these CTEs, then SELECT from "
                "calendar_interval. Reserve the calendar_* CTE names.\n"
                "Inputs: strict YYYY-MM-DD dates, years 0001..9999; NULL, invalid dates "
                "and timestamps yield NULL components and direction, preserving the row.\n"
                "Output: nonnegative years, remaining months (0..11), remaining days; "
                "direction=-1 for reversed inputs, otherwise +1. Dates are ordered first: "
                "this is an absolute calendar span, not a signed SQL AGE implementation. "
                "Take the maximum whole months with the original day clamped to the target "
                "month end; divide into years/months; days are the remaining elapsed days. "
                "Rounding, fractional-year conversion and aggregation remain explicit "
                "caller choices. No DB read, UDF or SQL execution occurs in this tool.\n\n"
                + CALENDAR_CTES
            ),
        )
