"""Dialect-neutral physical type checks for semantic predicates.

The model chooses only typed semantic values.  Both the planning service and
the deterministic compiler call these checks so a caller cannot bypass the
human-feedback path by constructing a ``SemanticPlan`` directly.
"""

from __future__ import annotations

import re

from .catalog import DimensionSpec
from .plan import FilterOperator, FilterPredicate, LiteralKind, TimeWindow


def filter_compatibility_error(
    dimension: DimensionSpec, predicate: FilterPredicate
) -> str:
    if predicate.dimension_id != dimension.id:
        return "filter_dimension_mismatch"
    if predicate.operator not in {FilterOperator.EQ, FilterOperator.IN}:
        return "unsupported_filter_operator"
    if dimension.kind in {"time", "calendar"}:
        return "temporal_filter_requires_time_window"

    kinds = {item.kind for item in predicate.values}
    expected = set(allowed_filter_literal_kinds(dimension))
    if not expected:
        return "filter_type_not_supported"
    if not kinds.issubset(expected):
        return "filter_literal_type_mismatch"
    return ""


def allowed_filter_literal_kinds(
    dimension: DimensionSpec,
) -> tuple[LiteralKind, ...]:
    """Expose the exact server-owned literal contract to bounded API clients."""

    if dimension.kind in {"time", "calendar"}:
        return ()
    data_type = dimension.data_type.lower()
    if dimension.kind == "boolean" or "bool" in data_type:
        return (LiteralKind.BOOLEAN,)
    elif re.search(r"\b(int|integer|bigint|smallint|tinyint)\b", data_type):
        return (LiteralKind.INTEGER,)
    elif re.search(r"\b(decimal|numeric|real|float|double)\b", data_type):
        return (LiteralKind.INTEGER, LiteralKind.DECIMAL)
    elif re.search(r"\b(char|varchar|text|string)\b", data_type):
        return (LiteralKind.STRING,)
    return ()


def time_window_compatibility_error(
    dimension: DimensionSpec, window: TimeWindow
) -> str:
    if window.dimension_id != dimension.id:
        return "time_dimension_mismatch"
    data_type = dimension.data_type.lower()
    # Native DATE has deterministic day boundaries. Timestamp, calendar
    # integers, and string dates remain blocked until timezone/format metadata
    # is explicitly reviewed.
    if dimension.kind != "time" or not re.search(r"\bdate\b", data_type):
        return "time_axis_not_reviewed"
    if window.start.kind != LiteralKind.DATE:
        return "time_literal_type_mismatch"
    return ""
