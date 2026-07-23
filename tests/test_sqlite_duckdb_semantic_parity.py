from __future__ import annotations

import asyncio
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, text

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.semantic.catalog import DimensionDisclosureTier
from lang2sql.semantic.service import (
    SemanticService,
    StewardAssertion,
    decode_semantic_query_rows,
    enforce_metric_disclosure_output,
    enforce_released_dimension_output,
)

duckdb = pytest.importorskip("duckdb")


QUESTION = (
    "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
)


def _seed_sqlite(path: str) -> SqlAlchemyExplorer:
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
                "amount DECIMAL(12,2) NOT NULL, status VARCHAR NOT NULL, "
                "ordered_on DATE NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO orders VALUES "
                "(1, 10, 'paid', '2025-01-01'), "
                "(2, 20, 'paid', '2025-01-31'), "
                "(3, 40, 'paid', '2025-02-01'), "
                "(4, 80, 'pending', '2025-01-15')"
            )
        )
    return SqlAlchemyExplorer(f"sqlite:///{path}")


def _seed_duckdb(path: str) -> SqlAlchemyExplorer:
    connection = duckdb.connect(path)
    try:
        connection.execute(
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
            "amount DECIMAL(12,2) NOT NULL, status VARCHAR NOT NULL, "
            "ordered_on DATE NOT NULL)"
        )
        connection.execute(
            "INSERT INTO orders VALUES "
            "(1, 10, 'paid', '2025-01-01'), "
            "(2, 20, 'paid', '2025-01-31'), "
            "(3, 40, 'paid', '2025-02-01'), "
            "(4, 80, 'pending', '2025-01-15')"
        )
    finally:
        connection.close()
    return SqlAlchemyExplorer(f"duckdb:///{path}")


def _query_args(explorer: SqlAlchemyExplorer) -> dict[str, object]:
    return {
        "scope": "scope",
        "review_scope": "review:user",
        "requester_id": "user",
        "explorer": explorer,
        "question": QUESTION,
        "metric_id": "metric:orders.amount",
        "metric_phrase": "amount",
        "aggregate": "sum",
        "dimension_bindings": [],
        "filter_bindings": [
            {
                "dimension_id": "dimension:orders.status",
                "dimension_phrase": "status",
                "operator": "eq",
                "operator_phrase": "is",
                "values": [{"kind": "string", "value": "paid", "phrase": "paid"}],
            }
        ],
        "time_window_binding": {
            "dimension_id": "dimension:orders.ordered_on",
            "dimension_phrase": "ordered on",
            "range_phrase": "from 2025-01-01 to 2025-02-01",
            "start": {
                "kind": "date",
                "value": "2025-01-01",
                "phrase": "2025-01-01",
            },
            "end": {
                "kind": "date",
                "value": "2025-02-01",
                "phrase": "2025-02-01",
            },
        },
        "unresolved_obligations": [],
        "limit": 100,
    }


def _run(explorer: SqlAlchemyExplorer):
    service = SemanticService(SqliteStore())
    summary = asyncio.run(service.onboard("scope", explorer))
    assertion = StewardAssertion(
        scope="scope",
        reviewer_id="steward",
        authorized=True,
        public_data_confirmed=True,
    )
    assert service.confirm_public_data_scope("scope", assertion).status == "confirmed"
    for dimension_id in (
        "dimension:orders.status",
        "dimension:orders.ordered_on",
    ):
        assert (
            service.release_dimension(
                "scope",
                dimension_id,
                assertion,
                DimensionDisclosureTier.PUBLIC_GROUPED.value,
            ).status
            == "confirmed"
        )

    args = _query_args(explorer)
    outcome = service.prepare_query(**args)
    for _ in range(5):
        if outcome.status == "ready":
            break
        assert outcome.status == "clarification"
        pending = service.pending_review("review:user")
        assert pending is not None
        choice = (
            pending.proposed_aggregate
            if pending.review_kind == "metric" and pending.aggregate_pending
            else "confirm"
        )
        assert (
            service.confirm_pending(
                "scope", "review:user", choice, reviewer_id="user"
            ).status
            == "confirmed"
        )
        outcome = service.prepare_query(**args)
    assert outcome.status == "ready"
    assert outcome.prepared is not None
    assert "paid" not in outcome.prepared.sql
    assert "2025-01-01" not in outcome.prepared.sql
    rows = asyncio.run(
        explorer.execute(
            outcome.prepared.sql,
            parameters=outcome.prepared.parameter_mapping(),
        )
    )
    catalog = service.load("scope")
    assert catalog is not None
    rows, blocker = enforce_metric_disclosure_output(
        catalog, outcome.metric_id, outcome.aggregate, outcome.dimension_ids, rows
    )
    assert blocker == ""
    rows, blocker = enforce_released_dimension_output(
        catalog, outcome.dimension_ids, rows
    )
    assert blocker == ""
    rows, blocker = decode_semantic_query_rows(catalog, outcome.dimension_ids, rows)
    assert blocker == ""
    shape = {
        "tables": sorted(item.name for item in summary.catalog.tables),
        "metrics": sorted(
            (item.column, item.expression_kind.value)
            for item in summary.catalog.metrics
        ),
        "dimensions": sorted(
            (item.column, item.kind) for item in summary.catalog.dimensions
        ),
        "blocked": sorted(summary.catalog.blocked_columns),
    }
    return shape, outcome.prepared.audit_detail()["parameter_kinds"], rows


def test_sqlite_and_duckdb_share_semantic_shape_and_bound_result(tmp_path) -> None:
    sqlite_shape, sqlite_kinds, sqlite_rows = _run(
        _seed_sqlite(str(tmp_path / "orders.sqlite"))
    )
    duck_shape, duck_kinds, duck_rows = _run(
        _seed_duckdb(str(tmp_path / "orders.duckdb"))
    )

    assert sqlite_shape == duck_shape
    assert (
        sqlite_kinds
        == duck_kinds
        == {
            "p0": "string",
            "p1": "date",
            "p2": "date",
        }
    )
    assert Decimal(str(sqlite_rows[0]["metric_value"])) == Decimal("30")
    assert Decimal(str(duck_rows[0]["metric_value"])) == Decimal("30")
