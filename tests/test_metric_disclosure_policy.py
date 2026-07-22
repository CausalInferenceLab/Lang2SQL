"""Regression matrix for aggregate contributor disclosure policy."""

from __future__ import annotations

import asyncio

import pytest
from sqlalchemy import create_engine, text

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.semantic.catalog import Aggregate
from lang2sql.semantic.service import (
    SemanticService,
    StewardAssertion,
    _compile_sql,
    _unique_safe_path,
    decode_semantic_query_rows,
    enforce_metric_disclosure_output,
    enforce_released_dimension_output,
)


def _onboard(path: str) -> tuple[SqlAlchemyExplorer, SemanticService]:
    explorer = SqlAlchemyExplorer(f"sqlite:///{path}")
    service = SemanticService(SqliteStore())
    asyncio.run(service.onboard("g1", explorer))
    return explorer, service


def _compile(
    service: SemanticService,
    explorer: SqlAlchemyExplorer,
    metric_id: str,
    aggregate: Aggregate,
    dimension_ids: list[str] | None = None,
    *,
    limit: int = 100,
) -> str:
    return _compile_sql(
        catalog=service.load("g1"),
        explorer=explorer,
        metric_id=metric_id,
        aggregate=aggregate,
        dimension_ids=dimension_ids or [],
        paths=[[] for _ in dimension_ids or []],
        limit=limit,
    )


def _execute_and_enforce(
    service: SemanticService,
    explorer: SqlAlchemyExplorer,
    metric_id: str,
    aggregate: Aggregate,
    sql: str,
    dimension_ids: list[str] | None = None,
):
    dimensions = dimension_ids or []
    rows = asyncio.run(explorer.execute(sql))
    return enforce_metric_disclosure_output(
        service.load("g1"), metric_id, aggregate.value, dimensions, rows
    )


def test_nonpublic_ungrouped_four_blocks_and_five_passes_with_guard_removed(
    tmp_path,
):
    db = tmp_path / "four-five.sqlite"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE four_rows (id INTEGER PRIMARY KEY, amount NUMERIC)")
        )
        connection.execute(
            text("CREATE TABLE five_rows (id INTEGER PRIMARY KEY, amount NUMERIC)")
        )
        connection.execute(
            text("INSERT INTO four_rows VALUES (1,1),(2,2),(3,3),(4,4)")
        )
        connection.execute(
            text("INSERT INTO five_rows VALUES (1,1),(2,2),(3,3),(4,4),(5,5)")
        )
    explorer, service = _onboard(str(db))

    four_id = "metric:four_rows.amount"
    four_sql = _compile(service, explorer, four_id, Aggregate.SUM)
    assert "__semantic_metric_contributors" in four_sql
    _rows, blocker = _execute_and_enforce(
        service, explorer, four_id, Aggregate.SUM, four_sql
    )
    assert blocker == "metric_contributor_count_too_small"

    five_id = "metric:five_rows.amount"
    five_sql = _compile(service, explorer, five_id, Aggregate.SUM)
    rows, blocker = _execute_and_enforce(
        service, explorer, five_id, Aggregate.SUM, five_sql
    )
    assert blocker == ""
    assert rows == [{"__l2s_metric": 15}]
    decoded, blocker = decode_semantic_query_rows(
        service.load("g1"), [], rows
    )
    assert blocker == ""
    assert decoded == [{"metric_value": 15}]


@pytest.mark.parametrize("aggregate", [Aggregate.SUM, Aggregate.AVG])
def test_nullable_column_aggregates_count_only_non_null_contributors(
    tmp_path, aggregate: Aggregate
):
    db = tmp_path / f"nullable-{aggregate.value}.sqlite"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE facts (id INTEGER PRIMARY KEY, amount NUMERIC)")
        )
        connection.execute(
            text("INSERT INTO facts VALUES (1,1),(2,2),(3,3),(4,4),(5,NULL)")
        )
    explorer, service = _onboard(str(db))
    metric_id = "metric:facts.amount"

    sql = _compile(service, explorer, metric_id, aggregate)
    _rows, blocker = _execute_and_enforce(
        service, explorer, metric_id, aggregate, sql
    )

    assert blocker == "metric_contributor_count_too_small"


def test_source_record_count_uses_all_rows_while_empty_and_null_only_metrics_block(
    tmp_path,
):
    db = tmp_path / "source-and-empty.sqlite"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE source_rows (id INTEGER PRIMARY KEY, amount NUMERIC)")
        )
        connection.execute(
            text(
                "INSERT INTO source_rows VALUES "
                "(1,NULL),(2,NULL),(3,NULL),(4,NULL),(5,NULL)"
            )
        )
        connection.execute(
            text("CREATE TABLE empty_rows (id INTEGER PRIMARY KEY, amount NUMERIC)")
        )
    explorer, service = _onboard(str(db))

    source_id = "metric:source_rows.source_record_count"
    source_sql = _compile(service, explorer, source_id, Aggregate.COUNT)
    rows, blocker = _execute_and_enforce(
        service, explorer, source_id, Aggregate.COUNT, source_sql
    )
    assert blocker == ""
    assert rows == [{"__l2s_metric": 5}]

    null_metric_id = "metric:source_rows.amount"
    null_sql = _compile(service, explorer, null_metric_id, Aggregate.SUM)
    _rows, blocker = _execute_and_enforce(
        service, explorer, null_metric_id, Aggregate.SUM, null_sql
    )
    assert blocker == "metric_contributor_count_too_small"

    empty_id = "metric:empty_rows.source_record_count"
    empty_sql = _compile(service, explorer, empty_id, Aggregate.COUNT)
    _rows, blocker = _execute_and_enforce(
        service, explorer, empty_id, Aggregate.COUNT, empty_sql
    )
    assert blocker == "metric_contributor_count_too_small"


def test_public_scope_allows_extremes_but_controlled_dimension_keeps_guard(
    tmp_path,
):
    db = tmp_path / "public-controlled.sqlite"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE facts (id INTEGER PRIMARY KEY, carrier VARCHAR(40), amount NUMERIC)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO facts VALUES "
                "(1,'a',1),(2,'a',2),(3,'a',3),(4,'a',4),"
                "(5,'b',5),(6,'b',6),(7,'b',7),(8,'b',8),(9,'b',9)"
            )
        )
    explorer, service = _onboard(str(db))
    metric_id = "metric:facts.amount"
    dimension_id = "dimension:facts.carrier"

    with pytest.raises(ValueError, match="controlled metrics cannot compile MIN/MAX"):
        _compile(service, explorer, metric_id, Aggregate.MIN)

    public_assertion = StewardAssertion(
        scope="g1",
        reviewer_id="steward",
        authorized=True,
        public_data_confirmed=True,
    )
    assert service.confirm_public_data_scope(
        "g1", public_assertion
    ).status == "confirmed"
    public_min = _compile(service, explorer, metric_id, Aggregate.MIN)
    assert "__semantic_metric_contributors" not in public_min

    controlled_assertion = StewardAssertion(
        scope="g1", reviewer_id="steward", authorized=True
    )
    released = service.release_dimension(
        "g1", dimension_id, controlled_assertion
    )
    assert released.status == "confirmed"
    grouped_sum = _compile(
        service,
        explorer,
        metric_id,
        Aggregate.SUM,
        [dimension_id],
        limit=1,
    )
    assert "MIN(COUNT(" in grouped_sum
    rows, blocker = _execute_and_enforce(
        service,
        explorer,
        metric_id,
        Aggregate.SUM,
        grouped_sum,
        [dimension_id],
    )
    assert blocker == "metric_contributor_count_too_small"

    with pytest.raises(ValueError, match="controlled metrics cannot compile MIN/MAX"):
        _compile(
            service,
            explorer,
            metric_id,
            Aggregate.MAX,
            [dimension_id],
        )


def test_public_grouped_dimension_keeps_category_and_label_guards(tmp_path):
    db = tmp_path / "public-grouped.sqlite"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE facts (id INTEGER PRIMARY KEY, carrier VARCHAR(40), amount NUMERIC)"
            )
        )
        connection.execute(text("INSERT INTO facts VALUES (1,'a',10)"))
    explorer, service = _onboard(str(db))
    assertion = StewardAssertion(
        scope="g1",
        reviewer_id="steward",
        authorized=True,
        public_data_confirmed=True,
    )
    assert service.confirm_public_data_scope("g1", assertion).status == "confirmed"
    assert service.release_dimension(
        "g1",
        "dimension:facts.carrier",
        assertion,
        disclosure_tier="public_grouped",
    ).status == "confirmed"

    sql = _compile(
        service,
        explorer,
        "metric:facts.amount",
        Aggregate.SUM,
        ["dimension:facts.carrier"],
    )
    rows, blocker = _execute_and_enforce(
        service,
        explorer,
        "metric:facts.amount",
        Aggregate.SUM,
        sql,
        ["dimension:facts.carrier"],
    )
    assert blocker == ""
    rows, blocker = enforce_released_dimension_output(
        service.load("g1"), ["dimension:facts.carrier"], rows
    )
    assert blocker == ""
    assert rows == [{"__l2s_dimension_0": "a", "__l2s_metric": 10}]


def test_left_join_preserves_nullable_orphan_and_multihop_metric_rows(tmp_path):
    db = tmp_path / "left-join-coverage.sqlite"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE regions (code TEXT PRIMARY KEY, carrier VARCHAR(40))")
        )
        connection.execute(
            text(
                "CREATE TABLE customers ("
                "id INTEGER PRIMARY KEY, "
                "region_code TEXT REFERENCES regions(code))"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE orders ("
                "id INTEGER PRIMARY KEY, "
                "customer_id INTEGER REFERENCES customers(id), amount NUMERIC)"
            )
        )
        connection.execute(text("INSERT INTO regions VALUES ('known','north')"))
        connection.execute(
            text(
                "INSERT INTO customers VALUES "
                "(1,'known'),(2,NULL),(3,'missing')"
            )
        )
        rows = []
        row_id = 1
        for customer_id, amount in ((1, 1), (2, 2), (3, 3), (None, 4), (999, 5)):
            for _index in range(5):
                rows.append(
                    {"id": row_id, "customer_id": customer_id, "amount": amount}
                )
                row_id += 1
        connection.execute(
            text(
                "INSERT INTO orders (id, customer_id, amount) "
                "VALUES (:id, :customer_id, :amount)"
            ),
            rows,
        )

    explorer, service = _onboard(str(db))
    public_assertion = StewardAssertion(
        scope="g1",
        reviewer_id="steward",
        authorized=True,
        public_data_confirmed=True,
    )
    assert service.confirm_public_data_scope(
        "g1", public_assertion
    ).status == "confirmed"
    dimension_id = "dimension:regions.carrier"
    assert service.release_dimension(
        "g1",
        dimension_id,
        public_assertion,
        disclosure_tier="public_grouped",
    ).status == "confirmed"
    catalog = service.load("g1")
    path, error = _unique_safe_path(catalog, "orders", "regions")
    assert error == "" and len(path) == 2

    metric_id = "metric:orders.amount"
    total_sql = _compile(
        service, explorer, metric_id, Aggregate.SUM
    )
    total_rows, blocker = _execute_and_enforce(
        service, explorer, metric_id, Aggregate.SUM, total_sql
    )
    assert blocker == ""
    total = total_rows[0]["__l2s_metric"]

    grouped_sql = _compile_sql(
        catalog=catalog,
        explorer=explorer,
        metric_id=metric_id,
        aggregate=Aggregate.SUM,
        dimension_ids=[dimension_id],
        paths=[path],
        limit=100,
    )
    assert grouped_sql.count("LEFT JOIN") == 2
    grouped_rows, blocker = _execute_and_enforce(
        service,
        explorer,
        metric_id,
        Aggregate.SUM,
        grouped_sql,
        [dimension_id],
    )
    assert blocker == ""
    grouped_rows, blocker = enforce_released_dimension_output(
        catalog, [dimension_id], grouped_rows
    )
    assert blocker == ""
    grouped_rows, blocker = decode_semantic_query_rows(
        catalog, [dimension_id], grouped_rows
    )
    assert blocker == ""
    assert sum(row["metric_value"] for row in grouped_rows) == total
    assert {row["regions.carrier"] for row in grouped_rows} == {"north", None}
