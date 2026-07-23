"""Deterministic compiler from validated semantic plans to bound SQL."""

from __future__ import annotations

from ..core.ports.explorer import ExplorerPort
from .catalog import (
    Aggregate,
    DimensionDisclosureTier,
    DimensionReviewPolicy,
    JoinSpec,
    MetricExpressionKind,
    SemanticCatalog,
)
from .plan import (
    BaseMeasure,
    BoundParameter,
    DerivedMeasure,
    FilterOperator,
    FilterPredicate,
    LiteralKind,
    PreparedSql,
    SemanticPlan,
    TimeWindow,
)
from .policy import (
    dimension_is_released,
    has_controlled_dimension,
    public_data_scope_confirmed,
)
from .type_compatibility import (
    filter_compatibility_error,
    time_window_compatibility_error,
)

RELEASE_GROUP_SIZE_KEY = "__semantic_group_size"
RELEASE_CATEGORY_COUNT_KEY = "__semantic_category_count"
METRIC_CONTRIBUTOR_COUNT_KEY = "__semantic_metric_contributors"
DIMENSION_OUTPUT_PREFIX = "__l2s_dimension_"
METRIC_OUTPUT_KEY = "__l2s_metric"


_COMPARISON_SQL = {
    FilterOperator.EQ: "=",
    FilterOperator.NE: "<>",
    FilterOperator.LT: "<",
    FilterOperator.LTE: "<=",
    FilterOperator.GT: ">",
    FilterOperator.GTE: ">=",
}


def compile_semantic_plan(
    *,
    catalog: SemanticCatalog,
    explorer: ExplorerPort,
    plan: SemanticPlan,
    paths: list[list[JoinSpec]],
) -> PreparedSql:
    """Compile one server-validated plan without interpolating literal values."""

    if isinstance(plan.measure, DerivedMeasure):
        # The IR can represent a reviewed derived metric, but compilation stays
        # fail-closed until grain, unit, DAG, and contributor policies are wired.
        raise ValueError("derived metric compilation is not enabled")
    if not isinstance(plan.measure, BaseMeasure):
        raise ValueError("unsupported semantic measure")
    return _compile_base_measure(
        catalog=catalog,
        explorer=explorer,
        metric_id=plan.measure.metric_id,
        aggregate=plan.measure.aggregate,
        dimension_ids=[item.dimension_id for item in plan.dimensions],
        paths=paths,
        limit=plan.limit,
        filters=plan.filters,
        time_window=plan.time_window,
        plan_hash=plan.plan_hash,
    )


def compile_legacy_aggregate_sql(
    *,
    catalog: SemanticCatalog,
    explorer: ExplorerPort,
    metric_id: str,
    aggregate: Aggregate,
    dimension_ids: list[str],
    paths: list[list[JoinSpec]],
    limit: int,
) -> str:
    """Compatibility seam for callers that have not adopted SemanticPlan yet."""

    return _compile_base_measure(
        catalog=catalog,
        explorer=explorer,
        metric_id=metric_id,
        aggregate=aggregate,
        dimension_ids=dimension_ids,
        paths=paths,
        limit=limit,
        filters=(),
        time_window=None,
        plan_hash="0" * 64,
    ).sql


def _compile_base_measure(
    *,
    catalog: SemanticCatalog,
    explorer: ExplorerPort,
    metric_id: str,
    aggregate: Aggregate,
    dimension_ids: list[str],
    paths: list[list[JoinSpec]],
    limit: int,
    filters: tuple[FilterPredicate, ...],
    time_window: TimeWindow | None,
    plan_hash: str,
) -> PreparedSql:
    metric = catalog.metric(metric_id)
    if metric is None or aggregate not in metric.allowed_aggregates:
        raise ValueError("reviewed metric aggregate required")
    dimensions = [catalog.dimension(item) for item in dimension_ids]
    if any(item is None for item in dimensions):
        raise ValueError("known dimensions required")
    if any(
        item is not None and not dimension_is_released(catalog, item)
        for item in dimensions
    ):
        raise ValueError("released dimensions required")
    known_dimensions = [item for item in dimensions if item is not None]
    controlled_dimension = has_controlled_dimension(known_dimensions)
    metric_guard_required = (
        not public_data_scope_confirmed(catalog) or controlled_dimension
    )
    if aggregate in {Aggregate.MIN, Aggregate.MAX} and metric_guard_required:
        raise ValueError("controlled metrics cannot compile MIN/MAX")

    referenced_dimension_ids = {
        *dimension_ids,
        *(item.dimension_id for item in filters),
        *([time_window.dimension_id] if time_window is not None else []),
    }
    referenced_dimensions = {
        item: catalog.dimension(item) for item in referenced_dimension_ids
    }
    if any(item is None for item in referenced_dimensions.values()):
        raise ValueError("filter and time dimensions must exist in the catalog")
    for dimension_id in {
        *(item.dimension_id for item in filters),
        *([time_window.dimension_id] if time_window is not None else []),
    }:
        dimension = referenced_dimensions[dimension_id]
        assert dimension is not None
        if not dimension_is_released(catalog, dimension):
            raise ValueError("filter and time dimensions must be released")
        if (
            dimension.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED
            or not public_data_scope_confirmed(catalog)
        ):
            raise ValueError("controlled dimensions cannot be used as row filters")
    for predicate in filters:
        dimension = referenced_dimensions[predicate.dimension_id]
        assert dimension is not None
        compatibility_error = filter_compatibility_error(dimension, predicate)
        if compatibility_error:
            raise ValueError(compatibility_error)
    if time_window is not None:
        dimension = referenced_dimensions[time_window.dimension_id]
        assert dimension is not None
        compatibility_error = time_window_compatibility_error(dimension, time_window)
        if compatibility_error:
            raise ValueError(compatibility_error)

    ordered_tables = [metric.table_id]
    ordered_joins: list[JoinSpec] = []
    seen_joins: set[str] = set()
    for path in paths:
        for join in path:
            if join.id not in seen_joins:
                ordered_joins.append(join)
                seen_joins.add(join.id)
            if join.parent_table_id not in ordered_tables:
                ordered_tables.append(join.parent_table_id)
    required_tables = {
        item.table_id for item in referenced_dimensions.values() if item is not None
    }
    if not required_tables.issubset(set(ordered_tables)):
        raise ValueError("compiler did not receive every required safe join path")
    aliases = {
        table_id: f"t{index + 1}" for index, table_id in enumerate(ordered_tables)
    }

    select_parts: list[str] = []
    group_parts: list[str] = []
    for index, dimension in enumerate(dimensions):
        assert dimension is not None
        expression = _column_expression(
            explorer, aliases, dimension.table_id, dimension.column
        )
        select_parts.append(
            f"{expression} AS {_quote(explorer, f'{DIMENSION_OUTPUT_PREFIX}{index}')}"
        )
        group_parts.append(expression)

    if metric.expression_kind == MetricExpressionKind.SOURCE_ROWS:
        if not metric.source_record_count or aggregate != Aggregate.COUNT:
            raise ValueError("source rows require the dedicated COUNT expression")
        metric_expression = "*"
    elif metric.expression_kind == MetricExpressionKind.COLUMN:
        if not metric.column:
            raise ValueError("column metric requires a physical column")
        metric_expression = _column_expression(
            explorer, aliases, metric.table_id, metric.column
        )
    else:
        raise ValueError("unsupported metric expression kind")
    select_parts.append(
        f"{aggregate.value.upper()}({metric_expression}) AS "
        f"{_quote(explorer, METRIC_OUTPUT_KEY)}"
    )
    contributor_expression = (
        "*"
        if metric.expression_kind == MetricExpressionKind.SOURCE_ROWS
        else metric_expression
    )
    if metric_guard_required:
        contributor_guard = (
            f"MIN(COUNT({contributor_expression})) OVER ()"
            if group_parts
            else f"COUNT({contributor_expression})"
        )
        select_parts.append(
            f"{contributor_guard} AS "
            f"{_quote(explorer, METRIC_CONTRIBUTOR_COUNT_KEY)}"
        )
    released_dimensions = [
        item
        for item in dimensions
        if item is not None
        and item.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
    ]
    if released_dimensions:
        if any(
            item.disclosure_tier == DimensionDisclosureTier.CONTROLLED_GROUPED
            for item in released_dimensions
        ):
            select_parts.append(
                f"MIN(COUNT({contributor_expression})) OVER () AS "
                f"{_quote(explorer, RELEASE_GROUP_SIZE_KEY)}"
            )
        select_parts.append(
            f"COUNT(*) OVER () AS {_quote(explorer, RELEASE_CATEGORY_COUNT_KEY)}"
        )

    source_table = catalog.table(metric.table_id)
    if source_table is None:
        raise ValueError("metric source table missing")
    lines = [
        "SELECT",
        "  " + ",\n  ".join(select_parts),
        (
            f"FROM {_qualified_table(explorer, source_table.schema, source_table.name)} "
            f"{_quote(explorer, aliases[source_table.id])}"
        ),
    ]
    for join in ordered_joins:
        parent = catalog.table(join.parent_table_id)
        if parent is None:
            raise ValueError("join parent table missing")
        child_alias = _quote(explorer, aliases[join.child_table_id])
        parent_alias = _quote(explorer, aliases[join.parent_table_id])
        lines.extend(
            [
                (
                    f"LEFT JOIN {_qualified_table(explorer, parent.schema, parent.name)} "
                    f"{parent_alias}"
                ),
                (
                    f"  ON {child_alias}.{_quote(explorer, join.child_column)} = "
                    f"{parent_alias}.{_quote(explorer, join.parent_column)}"
                ),
            ]
        )

    where_parts: list[str] = []
    parameters: list[BoundParameter] = []
    for predicate in filters:
        dimension = referenced_dimensions[predicate.dimension_id]
        assert dimension is not None
        expression = _column_expression(
            explorer, aliases, dimension.table_id, dimension.column
        )
        placeholders: list[str] = []
        for literal in predicate.values:
            name = f"p{len(parameters)}"
            parameters.append(BoundParameter(name, literal))
            placeholders.append(f":{name}")
        if predicate.operator == FilterOperator.IN:
            where_parts.append(f"{expression} IN ({', '.join(placeholders)})")
        else:
            operator = _COMPARISON_SQL.get(predicate.operator)
            if operator is None:
                raise ValueError("unsupported filter operator")
            if predicate.values[
                0
            ].kind == LiteralKind.STRING and predicate.operator not in {
                FilterOperator.EQ,
                FilterOperator.NE,
            }:
                raise ValueError("ordered comparisons are not allowed for strings")
            where_parts.append(f"{expression} {operator} {placeholders[0]}")

    if time_window is not None:
        dimension = referenced_dimensions[time_window.dimension_id]
        assert dimension is not None
        expression = _column_expression(
            explorer, aliases, dimension.table_id, dimension.column
        )
        start_name = f"p{len(parameters)}"
        parameters.append(BoundParameter(start_name, time_window.start))
        end_name = f"p{len(parameters)}"
        parameters.append(BoundParameter(end_name, time_window.end))
        where_parts.extend(
            [f"{expression} >= :{start_name}", f"{expression} < :{end_name}"]
        )
    if where_parts:
        lines.append("WHERE " + " AND ".join(where_parts))
    if group_parts:
        lines.append("GROUP BY " + ", ".join(group_parts))
    lines.append(f"LIMIT {limit}")
    return PreparedSql(
        sql="\n".join(lines),
        parameters=tuple(parameters),
        plan_hash=plan_hash,
    )


def _column_expression(
    explorer: ExplorerPort,
    aliases: dict[str, str],
    table_id: str,
    column: str,
) -> str:
    if table_id not in aliases:
        raise ValueError("semantic field table is outside the safe join graph")
    return f"{_quote(explorer, aliases[table_id])}.{_quote(explorer, column)}"


def _qualified_table(explorer: ExplorerPort, schema: str, table: str) -> str:
    if schema:
        return f"{_quote(explorer, schema)}.{_quote(explorer, table)}"
    return _quote(explorer, table)


def _quote(explorer: ExplorerPort, name: str) -> str:
    quote = getattr(explorer, "quote_identifier", None)
    if quote is not None:
        return str(quote(name))
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier from catalog: {name!r}")
    return f'"{name}"'
