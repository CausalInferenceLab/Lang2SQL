from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from lang2sql.semantic.catalog import Aggregate
from lang2sql.semantic.plan import (
    BaseMeasure,
    BinaryMetricNode,
    BoundParameter,
    DerivedMeasure,
    DerivedMetricDefinition,
    DerivedOperator,
    DimensionSelection,
    FilterOperator,
    FilterPredicate,
    LiteralKind,
    MetricAggregateNode,
    PlanReady,
    PreparedSql,
    ScalarLiteral,
    SemanticPlan,
    SemanticStateStamp,
    TimeWindow,
)


def _stamp() -> SemanticStateStamp:
    return SemanticStateStamp(
        source_id="source",
        connection_generation=2,
        catalog_fingerprint="catalog",
        catalog_review_revision=3,
        catalog_version=4,
        classification_policy_version=5,
    )


def _plan(*, filters=(), time_window=None, measure=None) -> SemanticPlan:
    return SemanticPlan(
        question_sha256="a" * 64,
        stamp=_stamp(),
        measure=measure or BaseMeasure("metric:orders.amount", Aggregate.SUM),
        metric_phrase="amount",
        dimensions=(DimensionSelection("dimension:regions.name", "region"),),
        filters=filters,
        time_window=time_window,
        limit=100,
    )


def test_semantic_plan_is_immutable_and_hashes_question_source_and_values() -> None:
    predicate = FilterPredicate(
        dimension_id="dimension:orders.status",
        dimension_phrase="status",
        operator=FilterOperator.EQ,
        values=(ScalarLiteral(LiteralKind.STRING, "paid"),),
        value_phrases=("paid",),
    )
    plan = _plan(filters=(predicate,))
    same = _plan(filters=(predicate,))
    changed = _plan(
        filters=(
            FilterPredicate(
                dimension_id="dimension:orders.status",
                dimension_phrase="status",
                operator=FilterOperator.EQ,
                values=(ScalarLiteral(LiteralKind.STRING, "refunded"),),
                value_phrases=("refunded",),
            ),
        )
    )

    assert plan.plan_hash == same.plan_hash
    assert plan.plan_hash != changed.plan_hash
    with pytest.raises(FrozenInstanceError):
        plan.limit = 1  # type: ignore[misc]


def test_and_filter_order_is_canonical_but_output_dimension_order_is_not() -> None:
    first = FilterPredicate(
        "dimension:orders.status",
        "status",
        FilterOperator.EQ,
        (ScalarLiteral(LiteralKind.STRING, "paid"),),
        ("paid",),
    )
    second = FilterPredicate(
        "dimension:orders.channel",
        "channel",
        FilterOperator.EQ,
        (ScalarLiteral(LiteralKind.STRING, "web"),),
        ("web",),
    )
    assert (
        _plan(filters=(first, second)).plan_hash
        == _plan(filters=(second, first)).plan_hash
    )

    original = _plan()
    with_two_dimensions = SemanticPlan(
        question_sha256=original.question_sha256,
        stamp=original.stamp,
        measure=original.measure,
        metric_phrase=original.metric_phrase,
        dimensions=(
            *original.dimensions,
            DimensionSelection("dimension:orders.channel", "channel"),
        ),
        limit=original.limit,
    )
    reversed_dimensions = SemanticPlan(
        question_sha256=with_two_dimensions.question_sha256,
        stamp=with_two_dimensions.stamp,
        measure=with_two_dimensions.measure,
        metric_phrase=with_two_dimensions.metric_phrase,
        dimensions=tuple(reversed(with_two_dimensions.dimensions)),
        limit=with_two_dimensions.limit,
    )
    assert with_two_dimensions.plan_hash != reversed_dimensions.plan_hash


def test_prepared_sql_requires_exact_compiler_owned_bound_parameters() -> None:
    plan = _plan()
    prepared = PreparedSql(
        sql='SELECT * FROM "orders" WHERE "status" = :p0',
        parameters=(BoundParameter("p0", ScalarLiteral(LiteralKind.STRING, "paid")),),
        plan_hash=plan.plan_hash,
    )
    assert prepared.parameter_mapping() == {"p0": "paid"}
    assert "paid" not in repr(prepared)
    assert "paid" not in str(prepared.audit_detail())
    assert PlanReady(plan, prepared).status == "ready"

    with pytest.raises(ValueError, match="placeholders"):
        PreparedSql(
            sql='SELECT * FROM "orders" WHERE "status" = :p0',
            parameters=(),
            plan_hash=plan.plan_hash,
        )
    with pytest.raises(ValueError, match="pN"):
        BoundParameter("status", ScalarLiteral(LiteralKind.STRING, "paid"))


def test_time_window_is_explicit_utc_half_open_only() -> None:
    window = TimeWindow(
        dimension_id="dimension:orders.created_at",
        dimension_phrase="created at",
        start=ScalarLiteral(LiteralKind.DATE, "2025-01-01"),
        end=ScalarLiteral(LiteralKind.DATE, "2025-02-01"),
        start_phrase="2025-01-01",
        end_phrase="2025-02-01",
    )
    assert _plan(time_window=window).time_window == window
    with pytest.raises(ValueError, match="UTC"):
        TimeWindow(
            dimension_id=window.dimension_id,
            dimension_phrase=window.dimension_phrase,
            start=window.start,
            end=window.end,
            start_phrase=window.start_phrase,
            end_phrase=window.end_phrase,
            timezone="Asia/Seoul",
        )
    with pytest.raises(ValueError, match="precede"):
        TimeWindow(
            dimension_id=window.dimension_id,
            dimension_phrase=window.dimension_phrase,
            start=window.end,
            end=window.start,
            start_phrase=window.end_phrase,
            end_phrase=window.start_phrase,
        )
    with pytest.raises(ValueError, match="explicit UTC"):
        ScalarLiteral(LiteralKind.TIMESTAMP, "2025-01-01T00:00:00")
    with pytest.raises(ValueError, match="explicit UTC"):
        ScalarLiteral(LiteralKind.TIMESTAMP, "2025-01-01T09:00:00+09:00")


def test_literals_validate_before_execution_and_filter_dimensions_do_not_conflict() -> (
    None
):
    with pytest.raises(ValueError, match="invalid integer"):
        ScalarLiteral(LiteralKind.INTEGER, "1.5")
    with pytest.raises(ValueError, match="finite"):
        ScalarLiteral(LiteralKind.DECIMAL, "NaN")
    with pytest.raises(ValueError, match="invalid date"):
        ScalarLiteral(LiteralKind.DATE, "2025-02-30")

    status_paid = FilterPredicate(
        "dimension:orders.status",
        "status",
        FilterOperator.EQ,
        (ScalarLiteral(LiteralKind.STRING, "paid"),),
        ("paid",),
    )
    status_pending = FilterPredicate(
        "dimension:orders.status",
        "status",
        FilterOperator.EQ,
        (ScalarLiteral(LiteralKind.STRING, "pending"),),
        ("pending",),
    )
    with pytest.raises(ValueError, match="one predicate"):
        _plan(filters=(status_paid, status_pending))


def test_derived_metric_contract_requires_topological_dag_and_reviewed_reference() -> (
    None
):
    definition = DerivedMetricDefinition(
        id="derived:conversion_rate",
        label="conversion rate",
        nodes=(
            MetricAggregateNode(
                "orders", "metric:orders.source_record_count", Aggregate.COUNT
            ),
            MetricAggregateNode(
                "visits", "metric:visits.source_record_count", Aggregate.COUNT
            ),
            BinaryMetricNode("ratio", DerivedOperator.DIVIDE, "orders", "visits"),
        ),
        root_node_id="ratio",
        grain_dimension_ids=(),
        unit="ratio",
    )
    assert definition.root_node_id == "ratio"
    assert _plan(measure=DerivedMeasure(definition.id)).measure.kind.value == "derived"

    with pytest.raises(ValueError, match="topologically"):
        DerivedMetricDefinition(
            id="derived:bad",
            label="bad",
            nodes=(BinaryMetricNode("ratio", DerivedOperator.DIVIDE, "a", "b"),),
            root_node_id="ratio",
            grain_dimension_ids=(),
            unit="ratio",
        )
