"""First-connect semantic vertical slice on an unseen three-table schema."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import re

import pytest

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.types import Completion, Message, Role, ToolCall, ToolResult
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.semantic.catalog import (
    CATALOG_KEY,
    Aggregate,
    DimensionSpec,
    JoinSpec,
    MetricExpressionKind,
    MetricSpec,
    SemanticCatalog,
    TableSpec,
)
from lang2sql.semantic.onboarding import _build_declared_joins
from lang2sql.semantic.service import (
    SemanticService,
    StewardAssertion,
    decode_semantic_query_rows,
    enforce_metric_disclosure_output,
    enforce_released_dimension_output,
    review_scope_key,
)
from lang2sql.semantic.shortlist import build_attention_envelope
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.semantic_query import SemanticQuery


def _seed_multitable(path: str) -> None:
    from sqlalchemy import create_engine, text

    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE regions ("
                "region_code TEXT PRIMARY KEY, region_name TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE customers ("
                "customer_id INTEGER PRIMARY KEY, "
                "region_code TEXT NOT NULL REFERENCES regions(region_code), "
                "email TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE orders ("
                "order_id INTEGER PRIMARY KEY, "
                "customer_id INTEGER NOT NULL REFERENCES customers(customer_id), "
                "amount NUMERIC NOT NULL, weight_kg NUMERIC NOT NULL, "
                "status TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO regions VALUES ('NE', 'North East'), ('SW', 'South West')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO customers VALUES "
                "(1, 'NE', 'a@example.com'), "
                "(2, 'NE', 'b@example.com'), "
                "(3, 'SW', 'c@example.com')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO orders VALUES "
                "(1, 1, 100, 1000, 'paid'), "
                "(2, 2, 60, 500, 'paid'), "
                "(3, 3, 200, 750, 'paid')"
            )
        )


def test_declared_join_requires_exact_existing_columns_and_deduplicates() -> None:
    tables = [
        TableSpec(id="child", name="child"),
        TableSpec(id="parent", name="parent"),
    ]
    edge = {
        "columns": ["parent_id"],
        "referred_table": "parent",
        "referred_columns": ["id"],
    }
    joins = _build_declared_joins(
        tables,
        {"tables": {"child": {"foreign_keys": [edge, dict(edge)]}}},
        {"child": {"parent_id", "amount"}, "parent": {"id", "name"}},
    )
    assert len(joins) == 1
    assert joins[0].child_column == "parent_id"
    assert joins[0].parent_column == "id"


@pytest.mark.parametrize(
    "foreign_key",
    [
        {"columns": [], "referred_table": "parent", "referred_columns": ["id"]},
        {
            "columns": ["parent_id"],
            "referred_table": "parent",
            "referred_columns": [],
        },
        {
            "columns": [""],
            "referred_table": "parent",
            "referred_columns": ["id"],
        },
        {
            "columns": ["parent_id"],
            "referred_table": "parent",
            "referred_columns": ["   "],
        },
        {
            "columns": [None],
            "referred_table": "parent",
            "referred_columns": ["id"],
        },
        {
            "columns": ["missing"],
            "referred_table": "parent",
            "referred_columns": ["id"],
        },
        {
            "columns": ["parent_id"],
            "referred_table": "parent",
            "referred_columns": ["missing"],
        },
        {
            "columns": ["parent_id"],
            "referred_table": "missing",
            "referred_columns": ["id"],
        },
        {
            "columns": "parent_id",
            "referred_table": "parent",
            "referred_columns": ["id"],
        },
        {
            "columns": ["parent_id"],
            "referred_table": "parent",
            "referred_columns": "id",
        },
        {
            "columns": ["parent_id", "other"],
            "referred_table": "parent",
            "referred_columns": ["id", "other_id"],
        },
    ],
)
def test_malformed_or_unverifiable_declared_join_is_omitted(foreign_key) -> None:
    joins = _build_declared_joins(
        [TableSpec(id="child", name="child"), TableSpec(id="parent", name="parent")],
        {"tables": {"child": {"foreign_keys": [foreign_key]}}},
        {
            "child": {"parent_id", "other"},
            "parent": {"id", "other_id"},
        },
    )
    assert joins == []


def test_malformed_fk_blocks_downstream_while_valid_fk_still_compiles(tmp_path) -> None:
    from sqlalchemy import create_engine, text

    database = tmp_path / "mixed-fk.sqlite"
    with create_engine(f"sqlite:///{database}").begin() as connection:
        connection.execute(
            text("CREATE TABLE Country (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        )
        connection.execute(
            text("CREATE TABLE League (id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
        )
        connection.execute(
            text(
                "CREATE TABLE Match (id INTEGER PRIMARY KEY, country_id INTEGER, "
                "league_id INTEGER, home_team_goal INTEGER NOT NULL)"
            )
        )

    class MixedMetadataExplorer(_CountingSqlAlchemyExplorer):
        async def catalog_metadata(self):
            return {
                "tables": {
                    "Country": {"primary_key": ["id"], "foreign_keys": []},
                    "League": {"primary_key": ["id"], "foreign_keys": []},
                    "Match": {
                        "primary_key": ["id"],
                        "foreign_keys": [
                            {
                                "columns": ["league_id"],
                                "referred_table": "League",
                                "referred_columns": [],
                            },
                            {
                                "columns": ["country_id"],
                                "referred_table": "Country",
                                "referred_columns": ["id"],
                            },
                        ],
                    },
                }
            }

    explorer = MixedMetadataExplorer(f"sqlite:///{database}")
    service = SemanticService(SqliteStore())
    summary = asyncio.run(service.onboard("g1", explorer))
    assert len(summary.catalog.joins) == 1
    _release_all_dimensions(service)

    malformed = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        requester_id="u1",
        explorer=explorer,
        question="total home team goal by league name",
        metric_id="metric:Match.home_team_goal",
        metric_phrase="home team goal",
        aggregate="sum",
        dimension_bindings=[
            {"dimension_id": "dimension:League.name", "phrase": "league name"}
        ],
        unresolved_obligations=[],
        limit=100,
    )
    assert malformed.status == "blocked"
    assert malformed.blocker == "safe_join_path_missing"
    assert malformed.sql == ""
    assert service.pending_review("review:u1") is None
    assert explorer.execute_calls == 0

    valid, _stages = _review_until_ready(
        service,
        {
            "scope": "g1",
            "review_scope": "review:u1",
            "explorer": explorer,
            "question": "total home team goal by country name",
            "metric_id": "metric:Match.home_team_goal",
            "metric_phrase": "home team goal",
            "aggregate": "sum",
            "dimension_bindings": [
                {
                    "dimension_id": "dimension:Country.name",
                    "phrase": "country name",
                }
            ],
            "unresolved_obligations": [],
            "limit": 100,
        },
    )
    assert valid.status == "ready"
    assert "JOIN" in valid.sql
    assert explorer.execute_calls == 0


def _release_all_dimensions(service: SemanticService, scope: str = "g1") -> None:
    """Simulate an explicit steward pass for tests focused on later query stages."""

    for candidate in service.release_candidates(scope):
        assert (
            service.release_dimension(
                scope,
                candidate.id,
                StewardAssertion(
                    scope=scope,
                    reviewer_id="test-steward",
                    authorized=True,
                ),
            ).status
            == "confirmed"
        )


def _onboard(path: str, *, release_dimensions: bool = True):
    explorer = SqlAlchemyExplorer(f"sqlite:///{path}")
    store = SqliteStore()
    service = SemanticService(store)
    summary = asyncio.run(service.onboard("g1", explorer))
    if release_dimensions:
        _release_all_dimensions(service)
    return explorer, store, service, summary


def _review_until_ready(
    service: SemanticService, args: dict
) -> tuple[object, list[str]]:
    """Drive each independent review stage without merging its decisions."""

    stages: list[str] = []
    outcome = service.prepare_query(**args)
    for _ in range(8):
        if outcome.status == "ready":
            return outcome, stages
        assert outcome.status == "clarification"
        pending = service.pending_review(str(args["review_scope"]))
        assert pending is not None, outcome
        stages.append(pending.review_kind)
        choice = (
            pending.proposed_aggregate
            if pending.review_kind == "metric" and pending.aggregate_pending
            else "confirm"
        )
        confirmed = service.confirm_pending(
            str(args["scope"]), str(args["review_scope"]), choice
        )
        assert confirmed.status == "confirmed", confirmed
        outcome = service.prepare_query(**args)
    raise AssertionError("semantic review did not converge")


def test_first_connect_accepts_structure_without_sampling_or_manual_cards(tmp_path):
    db = tmp_path / "unseen.db"
    _seed_multitable(str(db))
    explorer, _store, _service, summary = _onboard(str(db), release_dimensions=False)

    assert summary.table_count == 3
    assert summary.declared_join_count == 2
    assert summary.blocked_column_count == 6
    assert "customers.email" in summary.catalog.blocked_columns
    assert summary.confirmed_metric_count == 3  # source record counts from PKs
    assert summary.pending_metric_count == 2  # amount and weight_kg, lazily reviewed
    assert summary.catalog.metric("metric:orders.amount") is not None
    assert summary.catalog.dimension("dimension:regions.region_name") is not None

    # The PII-safe scan reads catalog metadata only.  No row/value sampler is
    # needed to construct the result.
    assert explorer._engine is not None


def test_first_connect_blocks_free_text_and_credential_like_dimensions(tmp_path):
    from sqlalchemy import create_engine, text

    db = tmp_path / "sensitive-text.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE users ("
                "id INTEGER PRIMARY KEY, email TEXT, notes TEXT, "
                "password_hash VARCHAR(255), api_token VARCHAR(255))"
            )
        )
        connection.execute(
            text(
                "INSERT INTO users VALUES "
                "(1, 'a@example.com', 'Call Jane at 010-1234-5678', 'hash', 'token')"
            )
        )
    _explorer, _store, _service, summary = _onboard(str(db), release_dimensions=False)
    assert {
        "users.email",
        "users.notes",
        "users.password_hash",
        "users.api_token",
    }.issubset(summary.catalog.blocked_columns)
    assert summary.catalog.dimension("dimension:users.notes") is None
    assert summary.catalog.dimension("dimension:users.password_hash") is None
    assert summary.catalog.dimension("dimension:users.api_token") is None


@pytest.mark.parametrize(
    ("table", "metric", "dimension"),
    [
        ("shipments", "freight_cost", "carrier"),
        ("generation_readings", "megawatt_hours", "plant_code"),
        ("enrollments", "tuition_amount", "campus"),
        ("service_cases", "resolution_minutes", "priority"),
        ("tide_observations", "water_level", "station_code"),
    ],
)
def test_first_connect_pipeline_is_not_tied_to_one_domain(
    tmp_path, table: str, metric: str, dimension: str
):
    """One generic path works across unrelated, previously unseen schemas."""

    from sqlalchemy import create_engine, text

    db = tmp_path / f"{table}.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text(
                f"CREATE TABLE {table} ("
                f"id INTEGER PRIMARY KEY, {dimension} VARCHAR(40) NOT NULL, "
                f"{metric} NUMERIC NOT NULL)"
            )
        )
        connection.execute(
            text(
                f"INSERT INTO {table} (id, {dimension}, {metric}) "
                f"VALUES (:id, :dimension, :metric)"
            ),
            [
                {"id": 1, "dimension": "alpha", "metric": 10},
                {"id": 2, "dimension": "alpha", "metric": 30},
                {"id": 3, "dimension": "beta", "metric": 20},
                {"id": 4, "dimension": "alpha", "metric": 0},
                {"id": 5, "dimension": "alpha", "metric": 0},
                {"id": 6, "dimension": "alpha", "metric": 0},
                {"id": 7, "dimension": "beta", "metric": 0},
                {"id": 8, "dimension": "beta", "metric": 0},
                {"id": 9, "dimension": "beta", "metric": 0},
                {"id": 10, "dimension": "beta", "metric": 0},
            ],
        )

    explorer, _store, service, summary = _onboard(str(db))
    metric_id = f"metric:{table}.{metric}"
    dimension_id = f"dimension:{table}.{dimension}"
    assert summary.catalog.metric(metric_id) is not None
    assert summary.catalog.dimension(dimension_id) is not None

    metric_phrase = metric.replace("_", " ")
    dimension_phrase = dimension.replace("_", " ")
    question = f"sum {metric_phrase} by {dimension_phrase}"
    args = dict(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question=question,
        metric_id=metric_id,
        metric_phrase=metric_phrase,
        aggregate="sum",
        dimension_bindings=[{"dimension_id": dimension_id, "phrase": dimension_phrase}],
        unresolved_obligations=[],
        limit=100,
    )
    ready, stages = _review_until_ready(service, args)
    assert stages == ["metric", "dimension"]
    assert ready.status == "ready"
    rows = asyncio.run(explorer.execute(ready.sql))
    rows, blocker = enforce_metric_disclosure_output(
        service.load("g1"), metric_id, "sum", [dimension_id], rows
    )
    assert blocker == ""
    rows, blocker = enforce_released_dimension_output(
        service.load("g1"), [dimension_id], rows
    )
    assert blocker == ""
    rows, blocker = decode_semantic_query_rows(service.load("g1"), [dimension_id], rows)
    assert blocker == ""
    assert rows == [
        {f"{table}.{dimension}": "alpha", "metric_value": 40},
        {f"{table}.{dimension}": "beta", "metric_value": 20},
    ]


def test_metric_review_then_unique_two_hop_query_executes_exact_result(tmp_path):
    db = tmp_path / "join.db"
    _seed_multitable(str(db))
    from sqlalchemy import create_engine, text

    with create_engine(f"sqlite:///{db}").begin() as connection:
        connection.execute(
            text(
                "INSERT INTO orders VALUES "
                "(4, 1, 0, 0, 'paid'), (5, 1, 0, 0, 'paid'), "
                "(6, 1, 0, 0, 'paid'), (7, 3, 0, 0, 'paid'), "
                "(8, 3, 0, 0, 'paid'), (9, 3, 0, 0, 'paid'), "
                "(10, 3, 0, 0, 'paid')"
            )
        )
    explorer, store, service, summary = _onboard(str(db))
    metric_id = "metric:orders.amount"
    dimension_id = "dimension:regions.region_name"

    first = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="Amount by region name",
        metric_id=metric_id,
        metric_phrase="Amount",
        aggregate="sum",
        dimension_bindings=[{"dimension_id": dimension_id, "phrase": "region name"}],
        unresolved_obligations=[],
        limit=100,
    )
    assert first.status == "clarification"
    assert first.sql == ""

    reviewed = service.confirm_pending("g1", "review:u1", Aggregate.SUM.value)
    assert reviewed.status == "confirmed"

    dimension_stage = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="Amount by region name",
        metric_id=metric_id,
        metric_phrase="Amount",
        aggregate="sum",
        dimension_bindings=[{"dimension_id": dimension_id, "phrase": "region name"}],
        unresolved_obligations=[],
        limit=100,
    )
    assert dimension_stage.status == "clarification"
    assert service.pending_review("review:u1").review_kind == "dimension"
    assert service.confirm_pending("g1", "review:u1", "confirm").status == "confirmed"

    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content="Amount by region name"))
    active_catalog = service.load("g1") or summary.catalog
    tool = SemanticQuery(
        service,
        active_catalog,
        build_attention_envelope(active_catalog, "Amount by region name"),
    )
    context = HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry([tool]),
        session=session,
        explorer=explorer,
        safety=SafetyPipeline(),
        audit=store,
        store=store,
    )
    result = asyncio.run(
        tool.run(
            {
                "metric_id": metric_id,
                "metric_phrase": "Amount",
                "aggregate": "sum",
                "dimensions": [{"dimension_id": dimension_id, "phrase": "region name"}],
                "unresolved_obligations": [],
                "limit": 100,
            },
            context,
        )
    )

    assert result.is_error is False
    assert result.content.startswith("READY:")
    audit_events = asyncio.run(store.query("u1"))
    executed_sql = str(audit_events[-1].detail["sql"])
    assert "JOIN customers" in executed_sql
    assert "JOIN regions" in executed_sql
    assert "SQL:" not in result.content
    assert "North East" not in result.content
    assert context.semantic_result_headers == (
        "regions.region_name",
        "metric_value",
    )
    assert context.semantic_result_rows == [
        ("North East", 160),
        ("South West", 200),
    ]


def test_multiple_aggregates_can_be_reviewed_for_the_same_metric_phrase(tmp_path):
    db = tmp_path / "aggregates.db"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))
    dimension = {
        "dimension_id": "dimension:regions.region_name",
        "phrase": "region name",
    }

    total = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert total.status == "clarification"
    service.confirm_pending("g1", "review:u1", "sum")
    dimension_stage = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert dimension_stage.status == "clarification"
    assert service.confirm_pending("g1", "review:u1", "confirm").status == "confirmed"

    average = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="average amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="avg",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert average.status == "clarification"
    assert average.sql == ""
    service.confirm_pending("g1", "review:u1", "avg")

    average_ready = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="average amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="avg",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    total_ready = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert average_ready.status == "ready"
    assert "AVG(" in average_ready.sql
    assert total_ready.status == "ready"
    assert "SUM(" in total_ready.sql


def test_natural_source_record_count_can_complete_with_count_review(tmp_path):
    db = tmp_path / "count.db"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))

    first = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="how many orders",
        metric_id="metric:orders.source_record_count",
        metric_phrase="orders",
        aggregate="count",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert first.status == "clarification"
    pending = service.pending_review("review:u1")
    assert pending is not None
    assert pending.allowed_choices == ["count"]
    assert service.confirm_pending("g1", "review:u1", "count").status == "confirmed"

    ready = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="how many orders",
        metric_id="metric:orders.source_record_count",
        metric_phrase="orders",
        aggregate="count",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert ready.status == "ready"
    assert "COUNT(" in ready.sql


def test_count_existential_there_is_contextual_and_keeps_other_terms_fail_closed(
    tmp_path,
):
    from sqlalchemy import create_engine, text

    db = tmp_path / "existential-there.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text("CREATE TABLE events (primary_type VARCHAR(40), payload TEXT)")
        )
        connection.execute(
            text("INSERT INTO events VALUES ('A', NULL), ('A', NULL), ('B', NULL)")
        )
    explorer, _store, service, _summary = _onboard(str(db))
    metric_id = "metric:events.source_record_count"
    dimension = {
        "dimension_id": "dimension:events.primary_type",
        "phrase": "primary type",
    }

    intended = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="How many source event rows are there by primary type?",
        metric_id=metric_id,
        metric_phrase="source event rows",
        aggregate="count",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert intended.status == "clarification"
    assert intended.blocker == ""
    assert "표현 안에 필터·기간·조건이 섞였다면" in intended.message
    assert service.confirm_pending("g1", "review:u1", "confirm").status == "confirmed"
    ready = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="How many source event rows are there by primary type?",
        metric_id=metric_id,
        metric_phrase="source event rows",
        aggregate="count",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert ready.status == "ready"
    assert "COUNT(*)" in ready.sql

    for question in (
        "source event rows there by primary type",
        "How many source event rows are there over there by primary type?",
        "How many source event rows are there by active primary type?",
        "How many source event rows are there by primary type after 2025?",
        "How many source event rows are there by primary type before 2025?",
        "How many source event rows are there by primary type in EUR?",
        "How many source event rows are there by primary type with arrests?",
        "How many source event rows are there by primary type without arrests?",
    ):
        blocked = service.prepare_query(
            scope="g1",
            review_scope="review:u2",
            explorer=explorer,
            question=question,
            metric_id=metric_id,
            metric_phrase="source event rows",
            aggregate="count",
            dimension_bindings=[dimension],
            unresolved_obligations=[],
            limit=100,
        )
        assert blocked.status == "clarification", question
        assert blocked.sql == "", question

    explicit = service.prepare_query(
        scope="g1",
        review_scope="review:u3",
        explorer=explorer,
        question="How many source event rows are there by primary type with arrests?",
        metric_id=metric_id,
        metric_phrase="source event rows",
        aggregate="count",
        dimension_bindings=[dimension],
        unresolved_obligations=["with arrests"],
        limit=100,
    )
    assert explicit.blocker == "unsupported_obligations"
    assert explicit.sql == ""


def test_source_context_is_ignored_only_with_original_question_provenance(tmp_path):
    from sqlalchemy import create_engine, text

    from lang2sql.semantic.service import _uncovered_question_terms

    for scaffold in (
        "in source observations",
        "in the source observations",
        "in observations table",
        "in the observations dataset",
        "in source dataset",
        "in source records",
        "in the source table",
        "in source rows",
        "in the source data",
    ):
        assert (
            _uncovered_question_terms(
                f"What is average water level {scaffold}?",
                ["water level"],
                ["observations"],
            )
            == []
        )

    assert _uncovered_question_terms(
        "What is average water level in source observations above 2 meters?",
        ["water level"],
        ["observations"],
    ) == ["2", "above", "meters"]
    assert (
        _uncovered_question_terms(
            "What is total refunds in the refunds dataset?", ["refunds"], ["refunds"]
        )
        == []
    )
    assert (
        _uncovered_question_terms(
            "What is total refunds in the source refunds dataset?",
            ["refunds"],
            ["refunds"],
        )
        == []
    )

    for noun in ("region", "regions", "order", "orders", "state", "states"):
        assert noun.rstrip("s") in _uncovered_question_terms(
            f"What is total amount in {noun}?", ["amount"], [noun]
        ) or noun in _uncovered_question_terms(
            f"What is total amount in {noun}?", ["amount"], [noun]
        )

    for question in (
        "What is average water level in Boston Harbor?",
        "What is average water level in station 8518750?",
        "What is average water level in January 2025?",
        "What is average water level in feet?",
        "What is average water level in verified observations?",
        "What is average water level in observations excluding outliers?",
        "What is average water level in NOAA rather than USGS observations?",
    ):
        assert _uncovered_question_terms(
            question, ["water level"], ["observations"]
        ), question

    db = tmp_path / "source-context.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(text("CREATE TABLE observations (water_level REAL)"))
        connection.execute(text("INSERT INTO observations VALUES (1.0), (3.0)"))
    explorer, _store, service, _summary = _onboard(str(db))
    args = {
        "scope": "g1",
        "review_scope": "review:u1",
        "explorer": explorer,
        "question": "What is average water level in source observations?",
        "metric_id": "metric:observations.water_level",
        "metric_phrase": "water level",
        "aggregate": "avg",
        "dimension_bindings": [],
        "unresolved_obligations": [],
        "limit": 100,
    }
    first = service.prepare_query(**args)
    assert first.status == "clarification"
    assert first.sql == ""
    assert (
        service.confirm_pending("g1", "review:u1", "avg", reviewer_id="u1").status
        == "confirmed"
    )
    ready = service.prepare_query(**args)
    assert ready.status == "ready"
    assert "AVG(" in ready.sql

    negative = service.prepare_query(
        **{
            **args,
            "review_scope": "review:u2",
            "requester_id": "u2",
            "question": "What is average water level in Boston Harbor?",
        }
    )
    assert negative.status == "clarification"
    assert negative.sql == ""


def test_joined_dimension_table_is_allowed_only_as_explicit_source_context(tmp_path):
    db = tmp_path / "joined-source-context.db"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))
    args = {
        "scope": "g1",
        "review_scope": "review:u1",
        "explorer": explorer,
        "question": "total amount by region in the regions table",
        "metric_id": "metric:orders.amount",
        "metric_phrase": "amount",
        "aggregate": "sum",
        "dimension_bindings": [
            {
                "dimension_id": "dimension:regions.region_name",
                "phrase": "region",
            }
        ],
        "unresolved_obligations": [],
        "limit": 100,
    }
    ready, stages = _review_until_ready(service, args)
    assert stages == ["metric", "dimension"]
    assert ready.status == "ready"
    assert "JOIN regions" in ready.sql

    bare_location = service.prepare_query(
        **{
            **args,
            "review_scope": "review:u2",
            "question": "total amount by region in region",
        }
    )
    assert bare_location.status == "clarification"
    assert bare_location.blocker == "unresolved_question_terms"
    assert bare_location.sql == ""


def test_source_rows_use_count_star_without_a_primary_key_or_synthetic_id(tmp_path):
    from sqlalchemy import create_engine, text

    db = tmp_path / "source-rows.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text('CREATE TABLE "event rows" ("group label" VARCHAR(20), payload TEXT)')
        )
        connection.execute(
            text(
                'INSERT INTO "event rows" VALUES '
                "('alpha', NULL), ('alpha', NULL), ('alpha', NULL), "
                "('alpha', NULL), ('alpha', NULL), (NULL, NULL), "
                "(NULL, NULL), (NULL, NULL), (NULL, NULL), (NULL, NULL)"
            )
        )
        connection.execute(text('CREATE TABLE "empty rows" (value TEXT)'))
        connection.execute(
            text(
                "CREATE TABLE composite_rows ("
                "left_key INTEGER, right_key INTEGER, value TEXT, "
                "PRIMARY KEY (left_key, right_key))"
            )
        )
        connection.execute(
            text("INSERT INTO composite_rows VALUES (1, 1, NULL), (1, 2, NULL)")
        )

    explorer, _store, service, summary = _onboard(str(db))
    assert summary.confirmed_metric_count == 3
    assert all(
        metric.expression_kind == MetricExpressionKind.SOURCE_ROWS
        and metric.column == ""
        for metric in summary.catalog.metrics
        if metric.source_record_count
    )

    grouped_args = dict(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="event rows source record count by group label",
        metric_id="metric:event rows.source_record_count",
        metric_phrase="event rows source record count",
        aggregate="count",
        dimension_bindings=[
            {
                "dimension_id": "dimension:event rows.group label",
                "phrase": "group label",
            }
        ],
        unresolved_obligations=[],
        limit=100,
    )
    grouped, stages = _review_until_ready(service, grouped_args)
    assert stages == ["dimension"]
    assert grouped.status == "ready"
    assert "COUNT(*)" in grouped.sql
    rows = asyncio.run(explorer.execute(grouped.sql))
    rows, blocker = enforce_metric_disclosure_output(
        service.load("g1"),
        "metric:event rows.source_record_count",
        "count",
        ["dimension:event rows.group label"],
        rows,
    )
    assert blocker == ""
    rows, blocker = enforce_released_dimension_output(
        service.load("g1"),
        ["dimension:event rows.group label"],
        rows,
    )
    assert blocker == ""
    rows, blocker = decode_semantic_query_rows(
        service.load("g1"),
        ["dimension:event rows.group label"],
        rows,
    )
    assert blocker == ""
    assert {row["event rows.group label"]: row["metric_value"] for row in rows} == {
        None: 5,
        "alpha": 5,
    }

    assert (
        service.confirm_public_data_scope(
            "g1",
            StewardAssertion(
                scope="g1",
                reviewer_id="test-steward",
                authorized=True,
                public_data_confirmed=True,
            ),
        ).status
        == "confirmed"
    )
    for table, expected in (("empty rows", 0), ("composite_rows", 2)):
        outcome = service.prepare_query(
            scope="g1",
            review_scope="review:u1",
            explorer=explorer,
            question=f"{table} source record count",
            metric_id=f"metric:{table}.source_record_count",
            metric_phrase=f"{table} source record count",
            aggregate="count",
            dimension_bindings=[],
            unresolved_obligations=[],
            limit=100,
        )
        assert outcome.status == "ready"
        assert "COUNT(*)" in outcome.sql
        plain_rows, blocker = decode_semantic_query_rows(
            service.load("g1"), [], asyncio.run(explorer.execute(outcome.sql))
        )
        assert blocker == ""
        assert plain_rows == [{"metric_value": expected}]


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"column": ""}, "column metrics require a physical column"),
        (
            {"source_record_count": True},
            "column metrics cannot be source-record counts",
        ),
        (
            {
                "expression_kind": MetricExpressionKind.SOURCE_ROWS,
                "allowed_aggregates": [Aggregate.COUNT],
            },
            "source-row metrics require source_record_count",
        ),
        (
            {
                "expression_kind": MetricExpressionKind.SOURCE_ROWS,
                "source_record_count": True,
                "aggregate": Aggregate.SUM,
                "allowed_aggregates": [Aggregate.COUNT],
            },
            "source-row metrics only support COUNT",
        ),
        (
            {
                "expression_kind": MetricExpressionKind.SOURCE_ROWS,
                "source_record_count": True,
            },
            "source-row metrics must allow exactly COUNT",
        ),
    ],
)
def test_metric_expression_contract_rejects_unsafe_combinations(overrides, message):
    values = {
        "id": "metric:events.value",
        "label": "events.value",
        "table_id": "events",
        "column": "value",
    }
    values.update(overrides)
    with pytest.raises(ValueError, match=message):
        MetricSpec(**values)


def test_legacy_pk_source_count_migrates_and_keeps_reviews(tmp_path):
    db = tmp_path / "legacy-source-count.db"
    _seed_multitable(str(db))
    explorer, store, service, summary = _onboard(str(db))
    raw = json.loads(summary.catalog.to_json())
    for metric in raw["metrics"]:
        if not metric["source_record_count"]:
            continue
        metric.pop("expression_kind")
        metric["column"] = {
            "regions": "region_code",
            "customers": "customer_id",
            "orders": "order_id",
        }[metric["table_id"]]
    legacy_orders = next(
        metric
        for metric in raw["metrics"]
        if metric["id"] == "metric:orders.source_record_count"
    )
    legacy_orders["aliases"].append("legacy order rows")
    legacy_orders["reviewed_bindings"]["legacy order rows"] = ["count"]
    legacy_orders["binding_reviewers"]["legacy order rows"] = "admin"
    legacy_raw = json.dumps(raw)

    restored = SemanticCatalog.from_json(legacy_raw)
    restored_orders = restored.metric("metric:orders.source_record_count")
    assert restored_orders is not None
    assert restored_orders.expression_kind == MetricExpressionKind.SOURCE_ROWS
    assert restored_orders.column == "order_id"
    assert restored_orders.allowed_aggregates == [Aggregate.COUNT]

    store.kv_set("g1", CATALOG_KEY, legacy_raw)
    refreshed = asyncio.run(
        service.inspect("g1", explorer, carry_source_id=service.load("g1").source_id)
    ).catalog
    current_orders = refreshed.metric("metric:orders.source_record_count")
    assert current_orders is not None
    assert current_orders.expression_kind == MetricExpressionKind.SOURCE_ROWS
    assert current_orders.column == ""
    assert current_orders.reviewed_bindings["legacy order rows"] == ["count"]
    assert current_orders.binding_reviewers["legacy order rows"] == "admin"


def test_wrong_business_mapping_and_unrepresented_filter_never_become_ready(tmp_path):
    db = tmp_path / "bindings.db"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))
    dimension = {
        "dimension_id": "dimension:regions.region_name",
        "phrase": "region name",
    }

    proposed = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="profit by region name",
        metric_id="metric:orders.amount",
        metric_phrase="profit",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert proposed.status == "clarification"
    assert "확인 ID" in proposed.message
    assert "profit" not in proposed.message
    assert "orders.amount" not in proposed.message
    assert proposed.sql == ""

    service.confirm_pending("g1", "review:u1", "reject")
    rejected = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="profit by region name",
        metric_id="metric:orders.amount",
        metric_phrase="profit",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert rejected.status == "blocked"
    assert rejected.sql == ""

    filtered = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="paid amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=["paid status filter"],
        limit=100,
    )
    assert filtered.status == "clarification"
    assert filtered.blocker == "unsupported_obligations"
    assert filtered.sql == ""


def test_server_detects_aggregate_and_filter_omissions_without_model_self_report(
    tmp_path,
):
    db = tmp_path / "server-obligations.db"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))
    dimension = {
        "dimension_id": "dimension:regions.region_name",
        "phrase": "region name",
    }
    service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    service.confirm_pending("g1", "review:u1", "sum")

    wrong_aggregate = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="average amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert wrong_aggregate.status == "clarification"
    assert wrong_aggregate.blocker == "aggregate_cue_mismatch"
    assert wrong_aggregate.sql == ""

    omitted_filter = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="paid amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert omitted_filter.status == "clarification"
    assert omitted_filter.blocker == "unresolved_question_terms"
    assert "paid" in omitted_filter.message
    assert omitted_filter.sql == ""

    swallowed_filter = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="paid amount by region name",
        metric_id="metric:orders.amount",
        metric_phrase="paid amount",
        aggregate="sum",
        dimension_bindings=[dimension],
        unresolved_obligations=[],
        limit=100,
    )
    assert swallowed_filter.status == "clarification"
    assert swallowed_filter.blocker == "metric_phrase_contains_unresolved_terms"
    assert swallowed_filter.sql == ""


def test_ambiguous_physical_alias_and_shared_business_alias_cannot_false_ready(
    tmp_path,
):
    from sqlalchemy import create_engine, text

    db = tmp_path / "alias-collision.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE customers ("
                "customer_id INTEGER PRIMARY KEY, status TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "CREATE TABLE orders ("
                "order_id INTEGER PRIMARY KEY, "
                "customer_id INTEGER REFERENCES customers(customer_id), "
                "amount NUMERIC NOT NULL, weight_kg NUMERIC NOT NULL, "
                "status TEXT NOT NULL)"
            )
        )
    explorer, _store, service, summary = _onboard(str(db))
    order_status = summary.catalog.dimension("dimension:orders.status")
    customer_status = summary.catalog.dimension("dimension:customers.status")
    assert order_status is not None and "status" not in order_status.aliases
    assert customer_status is not None and "status" not in customer_status.aliases

    service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    service.confirm_pending("g1", "review:u1", "sum")

    wrong_dimension = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by order status",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[
            {"dimension_id": "dimension:customers.status", "phrase": "status"}
        ],
        unresolved_obligations=[],
        limit=100,
    )
    assert wrong_dimension.status == "clarification"
    assert wrong_dimension.sql == ""

    physical_dimension_conflict = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by order status",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[
            {
                "dimension_id": "dimension:customers.status",
                "phrase": "order status",
            }
        ],
        unresolved_obligations=[],
        limit=100,
    )
    assert physical_dimension_conflict.status == "clarification"
    assert (
        physical_dimension_conflict.blocker
        == "dimension_phrase_contains_unresolved_terms"
    )
    assert service.pending_review("review:u1") is None

    physical_metric_conflict = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount",
        metric_id="metric:orders.weight_kg",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert physical_metric_conflict.status == "clarification"
    metric_review = service.confirm_pending("g1", "review:u1", "sum")
    assert metric_review.status == "blocked"
    assert "orders.amount" in metric_review.message

    sales = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="sales",
        metric_id="metric:orders.amount",
        metric_phrase="sales",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert sales.status == "clarification"
    assert service.confirm_pending("g1", "review:u1", "sum").status == "confirmed"

    conflicting = service.prepare_query(
        scope="g1",
        review_scope="review:u2",
        explorer=explorer,
        question="sales",
        metric_id="metric:orders.weight_kg",
        metric_phrase="sales",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert conflicting.status == "clarification"
    conflict_review = service.confirm_pending("g1", "review:u2", "sum")
    assert conflict_review.status == "blocked"
    assert "orders.amount" in conflict_review.message


def test_obligation_gate_blocks_group_unit_and_fanout_false_ready(tmp_path):
    db = tmp_path / "guards.db"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))

    service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    service.confirm_pending("g1", "review:u1", "sum")
    missing_group = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount by region",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert missing_group.status == "clarification"
    assert missing_group.blocker == "grouping_dimension_missing"
    assert missing_group.sql == ""

    service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="weight",
        metric_id="metric:orders.weight_kg",
        metric_phrase="weight",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    service.confirm_pending("g1", "review:u1", "sum")
    unit = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="weight in metric tons by region name",
        metric_id="metric:orders.weight_kg",
        metric_phrase="weight",
        aggregate="sum",
        dimension_bindings=[
            {
                "dimension_id": "dimension:regions.region_name",
                "phrase": "region name",
            }
        ],
        unresolved_obligations=[],
        limit=100,
    )
    assert unit.status == "clarification"
    assert unit.blocker == "unit_conversion_not_reviewed"
    assert unit.sql == ""

    fanout = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="region records by order status",
        metric_id="metric:regions.source_record_count",
        metric_phrase="region records",
        aggregate="count",
        dimension_bindings=[
            {"dimension_id": "dimension:orders.status", "phrase": "order status"}
        ],
        unresolved_obligations=[],
        limit=100,
    )
    assert fanout.status == "blocked"
    assert fanout.blocker == "safe_join_path_missing"
    assert fanout.sql == ""


def test_governed_context_removes_raw_sql_and_sample_enrichment_tools(tmp_path):
    db = tmp_path / "tools.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))

    context = asyncio.run(concierge.build_context(identity))
    names = {item.name for item in context.tools.specs()}
    assert "semantic_query" in names
    assert "run_sql" not in names
    assert "enrich_schema" not in names
    assert "org_setup" not in names


def test_governed_prompt_distinguishes_reviewable_phrases_from_real_obligations(
    tmp_path,
):
    from lang2sql.harness.system_prompt import build_system_prompt

    db = tmp_path / "prompt-contract.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    context = asyncio.run(concierge.build_context(identity))

    prompt = asyncio.run(build_system_prompt(context))
    semantic = next(
        item for item in context.tools.specs() if item.name == "semantic_query"
    )
    obligations = semantic.parameters["properties"]["unresolved_obligations"][
        "description"
    ]

    assert "Mapping novelty alone is not an unresolved" in prompt
    assert "obligation; keep the exact phrase" in prompt
    assert "same source table or dataset" in prompt
    assert "new phrase mapped to an existing catalog ID" in semantic.description
    assert "same source table or dataset" in semantic.description
    assert "new phrase for an existing catalog ID is reviewable" in obligations
    assert "already-selected source table or dataset" in obligations


def test_corrupt_governed_catalog_does_not_fall_back_to_run_sql():
    concierge = ContextConcierge(llm=FakeLLM())
    concierge.store.kv_set("g1", CATALOG_KEY, "not-json")
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    context = asyncio.run(concierge.build_context(identity))
    names = {item.name for item in context.tools.specs()}
    assert "semantic_query" in names
    assert "run_sql" not in names


class _SemanticSlotLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, messages, tools=()):
        self.calls += 1
        if messages and messages[-1].role == Role.TOOL:
            return Completion(content="tool complete")
        semantic = next(item for item in tools if item.name == "semantic_query")
        metric_id = next(
            value
            for value in semantic.parameters["properties"]["metric_id"]["enum"]
            if value == "metric:orders.amount"
        )
        return Completion(
            tool_calls=[
                ToolCall(
                    id="semantic-1",
                    name="semantic_query",
                    arguments={
                        "metric_id": metric_id,
                        "metric_phrase": "Amount",
                        "aggregate": "sum",
                        "dimensions": [
                            {
                                "dimension_id": "dimension:regions.region_name",
                                "phrase": "region name",
                            }
                        ],
                        "unresolved_obligations": [],
                        "limit": 100,
                    },
                )
            ]
        )


class _ScalarMetricLLM:
    async def complete(self, messages, tools=()):
        if messages and messages[-1].role == Role.TOOL:
            return Completion(content="tool complete")
        return Completion(
            tool_calls=[
                ToolCall(
                    id="semantic-scalar",
                    name="semantic_query",
                    arguments={
                        "metric_id": "metric:orders.amount",
                        "metric_phrase": "amount",
                        "aggregate": "sum",
                        "dimensions": [],
                        "unresolved_obligations": [],
                        "limit": 100,
                    },
                )
            ]
        )


class _CountingSqlAlchemyExplorer(SqlAlchemyExplorer):
    def __init__(self, url: str) -> None:
        super().__init__(url)
        self.execute_calls = 0
        self.sample_calls = 0

    async def execute(self, sql, limit=1000, *, timeout_seconds=30.0):
        self.execute_calls += 1
        return await super().execute(sql, limit=limit, timeout_seconds=timeout_seconds)

    async def sample_rows(self, name: str, limit: int = 5):
        self.sample_calls += 1
        return await super().sample_rows(name, limit=limit)


def test_discord_handler_clarifies_once_then_resumes_original_question(tmp_path):
    from sqlalchemy import create_engine, text

    db = tmp_path / "discord.db"
    _seed_multitable(str(db))
    with create_engine(f"sqlite:///{db}").begin() as connection:
        connection.execute(
            text(
                "INSERT INTO orders VALUES "
                "(4, 1, 0, 0, 'paid'), (5, 1, 0, 0, 'paid'), "
                "(6, 1, 0, 0, 'paid'), (7, 3, 0, 0, 'paid'), "
                "(8, 3, 0, 0, 'paid'), (9, 3, 0, 0, 'paid'), "
                "(10, 3, 0, 0, 'paid')"
            )
        )
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    llm = _SemanticSlotLLM()
    concierge = ContextConcierge(explorer=explorer, llm=llm)
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1", is_admin=True)
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    _release_all_dimensions(concierge.semantic)
    handlers = CommandHandlers(concierge, query_channel_ids={"c1"})

    first = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert first.text.startswith("NEEDS CLARIFICATION:")
    assert "/semantic_review" in first.text
    calls_before_review = llm.calls

    # Discord channel sessions are shared. A newer message must not change the
    # immutable question/draft that this user is approving.
    shared_session = asyncio.run(concierge.store.load(identity.session_key()))
    assert shared_session is not None
    shared_session.add(Message(role=Role.USER, content="hello from another user"))
    asyncio.run(concierge.store.save(identity.session_key(), shared_session))

    metric_resumed = asyncio.run(handlers.semantic_review(identity, "sum"))
    assert "분류 표현은 별도 단계" in metric_resumed.text
    assert "NEEDS CLARIFICATION:" in metric_resumed.text
    dimension_listing = asyncio.run(
        handlers.semantic_reviews(identity, search="region name")
    )
    assert "dimension:regions.region_name" in dimension_listing.text.replace("\\", "")
    assert "region name" in dimension_listing.text
    resumed = asyncio.run(handlers.semantic_review(identity, "confirm"))
    assert "분류 연결을 저장" in resumed.text
    assert "READY:" in resumed.text
    assert "North East | 160" in resumed.text
    assert llm.calls == calls_before_review  # immutable draft, no second LLM parse
    assert "SQL:" not in resumed.text

    repeated = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert repeated.text.startswith("READY:")
    assert "NEEDS CLARIFICATION" not in repeated.text


def test_guild_review_requires_admin_and_cross_user_approval_never_executes(tmp_path):
    db = tmp_path / "guild-review.db"
    _seed_multitable(str(db))
    explorer = _CountingSqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=_ScalarMetricLLM())
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge, query_channel_ids={"c1"})
    requester = Identity(user_id="requester", guild_id="g1", channel_id="c1")
    admin = Identity(
        user_id="admin", guild_id="g1", channel_id="admin-channel", is_admin=True
    )

    first = asyncio.run(handlers.query(requester, "total amount"))
    assert first.text.startswith("NEEDS CLARIFICATION:")
    queue = concierge.semantic.pending_review_queue("g1")
    assert len(queue) == 1
    listing = asyncio.run(handlers.semantic_reviews(admin))
    rendered_id = re.search(r"ID: ([A-Za-z0-9_-]+)", listing.text)
    assert rendered_id is not None
    review_id = rendered_id.group(1)
    assert "\\" not in review_id
    assert review_id == queue[0][1].review_id
    assert review_id and explorer.execute_calls == 0

    denied = asyncio.run(
        handlers.semantic_review(requester, "sum", review_id=review_id)
    )
    assert "관리자만" in denied.text
    assert concierge.semantic.pending_review_by_id("g1", review_id) is not None

    wrong = asyncio.run(
        handlers.semantic_review(admin, "sum", review_id="not-a-real-review")
    )
    assert "찾지 못했습니다" in wrong.text.replace("\\", "")
    approved = asyncio.run(handlers.semantic_review(admin, "sum", review_id=review_id))
    assert "다른 사용자의 DB 결과" in approved.text
    assert "metric_value" not in approved.text
    assert explorer.execute_calls == 0

    assert (
        concierge.semantic.confirm_public_data_scope(
            "g1",
            StewardAssertion(
                scope="g1",
                reviewer_id="admin",
                authorized=True,
                public_data_confirmed=True,
            ),
        ).status
        == "confirmed"
    )

    retried = asyncio.run(handlers.query(requester, "total amount"))
    assert retried.text.startswith("READY:")
    assert explorer.execute_calls == 1
    events = asyncio.run(concierge.audit.query("admin"))
    review_event = next(item for item in events if item.action == "semantic_review")
    assert review_event.detail["requester_id"] == "requester"
    assert review_event.detail["review_id"] == review_id
    assert review_event.detail["review_kind"] == "metric"
    assert review_event.detail["choice"] == "sum"
    assert review_event.detail["cross_requester"] is True
    assert review_event.detail["metric_id"] == "metric:orders.amount"
    assert review_event.detail["source_id"]
    assert review_event.detail["connection_generation"] == 1
    assert review_event.detail["catalog_review_revision"] == 1

    legacy = concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:legacy-empty-requester",
        explorer=explorer,
        question="total weight",
        metric_id="metric:orders.weight_kg",
        metric_phrase="weight",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert legacy.status == "clarification"
    legacy_pending = concierge.semantic.pending_review("review:legacy-empty-requester")
    assert legacy_pending is not None and legacy_pending.requester_id == ""
    calls_before_legacy_review = explorer.execute_calls
    legacy_approved = asyncio.run(
        handlers.semantic_review(admin, "sum", review_id=legacy_pending.review_id)
    )
    assert "다른 사용자의 DB 결과" in legacy_approved.text
    assert explorer.execute_calls == calls_before_legacy_review


def test_dm_requester_can_self_review_and_resume(tmp_path):
    db = tmp_path / "dm-review.db"
    _seed_multitable(str(db))
    explorer = _CountingSqlAlchemyExplorer(f"sqlite:///{db}")
    identity = Identity(user_id="dm-user")
    concierge = ContextConcierge(explorer=explorer, llm=_ScalarMetricLLM())
    asyncio.run(concierge.semantic.onboard(identity.kv_scope, explorer))
    assert (
        concierge.semantic.confirm_public_data_scope(
            identity.kv_scope,
            StewardAssertion(
                scope=identity.kv_scope,
                reviewer_id=identity.user_id,
                authorized=True,
                public_data_confirmed=True,
            ),
        ).status
        == "confirmed"
    )
    handlers = CommandHandlers(concierge)

    first = asyncio.run(handlers.query(identity, "total amount"))
    assert first.text.startswith("NEEDS CLARIFICATION:")
    resumed = asyncio.run(handlers.semantic_review(identity, "sum"))
    assert resumed.text.startswith("✅")
    assert "READY:" in resumed.text
    assert explorer.execute_calls == 1


def test_discord_query_discards_stale_rows_before_render(tmp_path, monkeypatch):
    db = tmp_path / "query-render-race.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    asyncio.run(concierge.semantic.onboard(identity.kv_scope, explorer))
    captured: dict[str, HarnessContext] = {}

    async def fake_agent_loop(ctx: HarnessContext, user_text: str) -> str:
        captured["ctx"] = ctx
        ctx.session.add(
            Message(
                role=Role.TOOL,
                name="semantic_query",
                tool_call_id="stale-query",
                content="READY: governed result is available.",
            )
        )
        ctx.semantic_result_ready = True
        ctx.semantic_result_message = "must not render"
        ctx.semantic_result_headers = ("metric_value",)
        ctx.semantic_result_rows = [("SECRET_SENTINEL",)]
        ctx.semantic_result_stamp = ("stale", 0, "stale", 0, 0, 0)
        return "ignored"

    monkeypatch.setattr(
        "lang2sql.frontends.discord.commands.agent_loop", fake_agent_loop
    )
    output = asyncio.run(CommandHandlers(concierge).query(identity, "amount"))

    assert output.text.startswith("BLOCKED (semantic_result_stale_before_render)")
    assert "SECRET_SENTINEL" not in output.text
    stale_ctx = captured["ctx"]
    assert stale_ctx.semantic_result_ready is False
    assert stale_ctx.semantic_result_headers == ()
    assert stale_ctx.semantic_result_rows == []
    assert stale_ctx.semantic_result_stamp == ()


def test_discord_review_resume_discards_stale_rows_before_render(tmp_path, monkeypatch):
    db = tmp_path / "review-render-race.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    identity = Identity(user_id="dm-user")
    asyncio.run(concierge.semantic.onboard(identity.kv_scope, explorer))
    pending = concierge.semantic.prepare_query(
        scope=identity.kv_scope,
        review_scope=review_scope_key(identity.session_key(), identity.user_id),
        requester_id=identity.user_id,
        explorer=explorer,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert pending.status == "clarification"
    captured: dict[str, HarnessContext] = {}

    async def fake_dispatch(
        self, name: str, args: dict, ctx: HarnessContext, call_id: str
    ) -> ToolResult:
        captured["ctx"] = ctx
        ctx.semantic_result_ready = True
        ctx.semantic_result_message = "must not render"
        ctx.semantic_result_headers = ("metric_value",)
        ctx.semantic_result_rows = [("SECRET_SENTINEL",)]
        ctx.semantic_result_stamp = ("stale", 0, "stale", 0, 0, 0)
        return ToolResult(
            call_id=call_id,
            content="READY: governed result is available.",
        )

    monkeypatch.setattr(ToolRegistry, "dispatch", fake_dispatch)
    output = asyncio.run(CommandHandlers(concierge).semantic_review(identity, "sum"))

    assert "BLOCKED (semantic_result_stale_before_render)" in output.text
    assert "SECRET_SENTINEL" not in output.text
    stale_ctx = captured["ctx"]
    assert stale_ctx.semantic_result_ready is False
    assert stale_ctx.semantic_result_headers == ()
    assert stale_ctx.semantic_result_rows == []
    assert stale_ctx.semantic_result_stamp == ()


def test_metric_browse_and_map_is_metadata_only_and_keeps_aggregate_pending(tmp_path):
    from sqlalchemy import create_engine, text

    db = tmp_path / "opaque-metric.db"
    with create_engine(f"sqlite:///{db}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE observations ("
                "id INTEGER PRIMARY KEY, access2_crudeprev REAL, "
                "record_fiscal_year INTEGER)"
            )
        )
        connection.execute(text("INSERT INTO observations VALUES (1, 12.5, 2025)"))
    explorer = _CountingSqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)
    admin = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    member = Identity(user_id="member", guild_id="g1", channel_id="c1")

    shown = asyncio.run(
        handlers.semantic_metric_candidates(admin, search="access2", state="all")
    )
    assert "access2" in shown.text
    token_match = re.search(r"candidate_token: ([A-Za-z0-9_-]+)", shown.text)
    assert token_match is not None
    candidate_token = token_match.group(1)
    assert explorer.sample_calls == 0 and explorer.execute_calls == 0
    assert (
        "관리자만"
        in asyncio.run(
            handlers.semantic_metric_map(
                member,
                candidate_token,
                "uninsured prevalence",
                confirm=True,
            )
        ).text
    )

    warning = asyncio.run(
        handlers.semantic_metric_map(
            admin,
            candidate_token,
            "uninsured prevalence",
            confirm=False,
        )
    )
    assert "표현에 묶였습니다" in warning.text

    payload_swap = asyncio.run(
        handlers.semantic_metric_map(
            admin,
            candidate_token,
            "different prevalence",
            confirm=True,
        )
    )
    assert payload_swap.text.startswith("BLOCKED:")
    assert "최종 요청이 다릅니다" in payload_swap.text

    mapped = asyncio.run(
        handlers.semantic_metric_map(
            admin,
            candidate_token,
            "uninsured prevalence",
            confirm=True,
        )
    )
    assert mapped.text.startswith("✅")
    retried_map = asyncio.run(
        handlers.semantic_metric_map(
            admin,
            candidate_token,
            "uninsured prevalence",
            confirm=True,
        )
    )
    assert retried_map.text.startswith("✅")
    changed_retry = asyncio.run(
        handlers.semantic_metric_map(
            admin,
            candidate_token,
            "different prevalence",
            confirm=True,
        )
    )
    assert changed_retry.text.startswith("BLOCKED:")
    map_events = [
        item
        for item in asyncio.run(concierge.audit.query("admin"))
        if item.action == "semantic_metric_map"
    ]
    assert len(map_events) == 1
    catalog = concierge.semantic.load("g1")
    assert catalog is not None
    metric = catalog.metric("metric:observations.access2_crudeprev")
    assert metric is not None
    assert "uninsured prevalence" in metric.aliases
    assert metric.reviewed_bindings == {}
    assert explorer.sample_calls == 0 and explorer.execute_calls == 0

    outcome = concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:admin",
        requester_id="admin",
        explorer=explorer,
        question="average uninsured prevalence",
        metric_id=metric.id,
        metric_phrase="uninsured prevalence",
        aggregate="avg",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert outcome.status == "clarification"
    assert concierge.semantic.pending_review("review:admin").aggregate_pending is True
    assert (
        concierge.semantic.confirm_pending(
            "g1", "review:admin", "avg", reviewer_id="admin"
        ).status
        == "confirmed"
    )
    repeated = concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:admin",
        requester_id="admin",
        explorer=explorer,
        question="average uninsured prevalence",
        metric_id=metric.id,
        metric_phrase="uninsured prevalence",
        aggregate="avg",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert repeated.status == "ready"
    blocked_role = asyncio.run(
        handlers.semantic_metric_map(
            admin,
            "metric:observations.record_fiscal_year",
            "fiscal year amount",
            confirm=True,
        )
    )
    assert blocked_role.text.startswith("BLOCKED:")


def test_rejected_metric_phrase_cannot_be_mapped_without_explicit_reset(tmp_path):
    db = tmp_path / "rejected-map.sqlite"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))
    proposed = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        requester_id="u1",
        explorer=explorer,
        question="total revenue",
        metric_id="metric:orders.amount",
        metric_phrase="revenue",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert proposed.status == "clarification"
    assert (
        service.confirm_pending("g1", "review:u1", "reject", reviewer_id="u1").status
        == "confirmed"
    )

    mapped = service.map_metric_phrase(
        "g1",
        service.issue_metric_action_token("g1", "metric:orders.amount"),
        "revenue",
        StewardAssertion(scope="g1", reviewer_id="admin", authorized=True),
    )
    assert mapped.status == "blocked"
    catalog = service.load("g1")
    assert catalog is not None
    metric = catalog.metric("metric:orders.amount")
    assert metric is not None
    assert "revenue" in metric.rejected_aliases
    assert "revenue" not in metric.aliases


def test_catalog_rejects_overlapping_approved_and_rejected_metric_aliases() -> None:
    with pytest.raises(ValueError, match="both approved and rejected"):
        SemanticCatalog(
            fingerprint="invalid-alias-state",
            metrics=[
                MetricSpec(
                    id="metric:events.amount",
                    label="events.amount",
                    table_id="events",
                    column="amount",
                    aliases=["revenue"],
                    rejected_aliases=["revenue"],
                )
            ],
        )


def test_metric_action_token_is_atomic_under_concurrent_double_submit(tmp_path) -> None:
    db = tmp_path / "concurrent-action.sqlite"
    _seed_multitable(str(db))
    _explorer, _store, service, _summary = _onboard(str(db))
    token = service.issue_metric_action_token("g1", "metric:orders.amount")
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(
            pool.map(
                lambda _index: service.map_metric_phrase(
                    "g1", token, "net revenue", assertion
                ),
                range(2),
            )
        )

    assert [item.status for item in outcomes] == ["confirmed", "confirmed"]
    assert sum(item.mutation_applied for item in outcomes) == 1
    catalog = service.load("g1")
    assert catalog is not None
    metric = catalog.metric("metric:orders.amount")
    assert metric is not None
    assert metric.aliases.count("net revenue") == 1
    assert metric.reviewed_bindings == {}


def test_metric_page_tokens_for_distinct_targets_survive_unrelated_mapping(tmp_path):
    db = tmp_path / "metric-page-actions.sqlite"
    _seed_multitable(str(db))
    _explorer, _store, service, _summary = _onboard(str(db))
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    catalog, candidates = service.metric_candidate_snapshot("g1")
    assert catalog is not None
    target_ids = [
        metric.id
        for metric in candidates
        if metric.id in {"metric:orders.amount", "metric:orders.weight_kg"}
    ]
    assert len(target_ids) == 2
    tokens = {
        metric_id: service.issue_metric_action_token(
            "g1", metric_id, expected_catalog=catalog
        )
        for metric_id in target_ids
    }

    first = service.map_metric_phrase(
        "g1", tokens["metric:orders.amount"], "net revenue", assertion
    )
    second = service.map_metric_phrase(
        "g1", tokens["metric:orders.weight_kg"], "shipping weight", assertion
    )
    assert first.status == "confirmed" and first.mutation_applied
    assert second.status == "confirmed" and second.mutation_applied

    retry = service.map_metric_phrase(
        "g1", tokens["metric:orders.amount"], "net revenue", assertion
    )
    assert retry.status == "confirmed"
    assert retry.mutation_applied is False


def test_discord_semantic_mutation_and_audit_roll_back_together(tmp_path):
    import sqlite3

    db = tmp_path / "atomic-governance-audit.sqlite"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)
    admin = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    token = concierge.semantic.issue_metric_action_token("g1", "metric:orders.amount")
    warning = asyncio.run(
        handlers.semantic_metric_map(admin, token, "atomic revenue", confirm=False)
    )
    assert "표현에 묶였습니다" in warning.text

    concierge.store._conn.execute(
        "CREATE TRIGGER fail_semantic_audit BEFORE INSERT ON audit "
        "WHEN NEW.action = 'semantic_metric_map' "
        "BEGIN SELECT RAISE(ABORT, 'forced audit failure'); END"
    )
    concierge.store._conn.commit()
    with pytest.raises(sqlite3.DatabaseError, match="forced audit failure"):
        asyncio.run(
            handlers.semantic_metric_map(admin, token, "atomic revenue", confirm=True)
        )

    rolled_back = concierge.semantic.load("g1")
    assert rolled_back is not None
    assert "atomic revenue" not in rolled_back.metric("metric:orders.amount").aliases
    assert not [
        item
        for item in asyncio.run(concierge.audit.query("admin"))
        if item.action == "semantic_metric_map"
    ]

    concierge.store._conn.execute("DROP TRIGGER fail_semantic_audit")
    concierge.store._conn.commit()
    retried = asyncio.run(
        handlers.semantic_metric_map(admin, token, "atomic revenue", confirm=True)
    )
    assert retried.text.startswith("✅")
    events = [
        item
        for item in asyncio.run(concierge.audit.query("admin"))
        if item.action == "semantic_metric_map"
    ]
    assert len(events) == 1


def test_semantic_review_pending_catalog_receipt_and_audit_share_one_transaction(
    tmp_path,
):
    import sqlite3

    db = tmp_path / "atomic-review-audit.sqlite"
    _seed_multitable(str(db))
    explorer, store, service, _summary = _onboard(str(db))
    outcome = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        requester_id="u1",
        explorer=explorer,
        question="total amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert outcome.status == "clarification"
    pending = service.pending_review("review:u1")
    pending_raw = store.kv_get("review:u1", "semantic_pending_review:v1")
    before = service.load("g1")
    assert pending is not None and pending_raw is not None and before is not None

    store._conn.execute(
        "CREATE TRIGGER fail_review_audit BEFORE INSERT ON audit "
        "WHEN NEW.action = 'semantic_review' "
        "BEGIN SELECT RAISE(ABORT, 'forced review audit failure'); END"
    )
    store._conn.commit()
    with pytest.raises(sqlite3.DatabaseError, match="forced review audit failure"):
        service.confirm_pending_by_id(
            "g1",
            pending.review_id,
            "sum",
            reviewer_id="admin",
            authorized=True,
            audit_scope="channel:admin",
        )

    rolled_back = service.load("g1")
    assert rolled_back is not None
    assert rolled_back.review_revision == before.review_revision
    assert (
        rolled_back.metric("metric:orders.amount").aliases
        == before.metric("metric:orders.amount").aliases
    )
    assert store.kv_get("review:u1", "semantic_pending_review:v1") == pending_raw
    assert not asyncio.run(store.query("admin"))

    store._conn.execute("DROP TRIGGER fail_review_audit")
    store._conn.commit()
    applied = service.confirm_pending_by_id(
        "g1",
        pending.review_id,
        "sum",
        reviewer_id="admin",
        authorized=True,
        audit_scope="channel:admin",
    )
    assert applied.status == "confirmed" and applied.mutation_applied
    assert service.pending_review("review:u1") is None
    retry = service.confirm_pending_by_id(
        "g1",
        pending.review_id,
        "sum",
        reviewer_id="admin",
        authorized=True,
        audit_scope="channel:admin",
    )
    assert retry.status == "confirmed"
    assert retry.mutation_applied is False
    assert retry.question == "" and retry.tool_args == {}
    assert (
        service.confirm_pending_by_id(
            "g1",
            pending.review_id,
            "avg",
            reviewer_id="admin",
            authorized=True,
            audit_scope="channel:admin",
        ).status
        == "blocked"
    )
    assert (
        service.confirm_pending_by_id(
            "g1",
            pending.review_id,
            "sum",
            reviewer_id="other-admin",
            authorized=True,
            audit_scope="channel:other",
        ).status
        == "blocked"
    )
    events = [
        event
        for event in asyncio.run(store.query("admin"))
        if event.action == "semantic_review"
    ]
    assert len(events) == 1


def test_concurrent_semantic_review_has_one_mutation_and_one_audit(tmp_path):
    db = tmp_path / "concurrent-review.sqlite"
    _seed_multitable(str(db))
    explorer, store, service, _summary = _onboard(str(db))
    assert (
        service.prepare_query(
            scope="g1",
            review_scope="review:u1",
            requester_id="u1",
            explorer=explorer,
            question="total amount",
            metric_id="metric:orders.amount",
            metric_phrase="amount",
            aggregate="sum",
            dimension_bindings=[],
            unresolved_obligations=[],
            limit=100,
        ).status
        == "clarification"
    )
    pending = service.pending_review("review:u1")
    assert pending is not None

    def confirm():
        return service.confirm_pending_by_id(
            "g1",
            pending.review_id,
            "sum",
            reviewer_id="admin",
            authorized=True,
            audit_scope="channel:admin",
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(pool.map(lambda _index: confirm(), range(2)))

    assert [outcome.status for outcome in outcomes] == ["confirmed", "confirmed"]
    assert sum(outcome.mutation_applied for outcome in outcomes) == 1
    events = [
        event
        for event in asyncio.run(store.query("admin"))
        if event.action == "semantic_review"
    ]
    assert len(events) == 1


def test_metric_action_token_stales_after_same_schema_source_switch(tmp_path) -> None:
    from sqlalchemy import create_engine, text

    first = tmp_path / "token-source-a.sqlite"
    second = tmp_path / "token-source-b.sqlite"
    for path, value in ((first, 1.0), (second, 2.0)):
        with create_engine(f"sqlite:///{path}").begin() as connection:
            connection.execute(
                text("CREATE TABLE observations (id INTEGER PRIMARY KEY, opaque REAL)")
            )
            connection.execute(
                text("INSERT INTO observations VALUES (1, :value)"),
                {"value": value},
            )
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    admin = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    assert (
        "연결 완료" in asyncio.run(handlers.connect(admin, f"sqlite:///{first}")).text
    )
    stale_snapshot, stale_candidates = concierge.semantic.metric_candidate_snapshot(
        "g1"
    )
    stale_metric_id = next(
        item.id for item in stale_candidates if item.id.endswith(".opaque")
    )
    shown = asyncio.run(
        handlers.semantic_metric_candidates(admin, search="opaque", state="all")
    )
    token_match = re.search(r"candidate_token: ([A-Za-z0-9_-]+)", shown.text)
    assert token_match is not None
    warning = asyncio.run(
        handlers.semantic_metric_map(
            admin, token_match.group(1), "business value", confirm=False
        )
    )
    assert "표현에 묶였습니다" in warning.text
    assert (
        "연결 완료" in asyncio.run(handlers.connect(admin, f"sqlite:///{second}")).text
    )
    assert stale_snapshot is not None
    assert (
        concierge.semantic.issue_metric_action_token(
            "g1", stale_metric_id, expected_catalog=stale_snapshot
        )
        == ""
    )

    stale = asyncio.run(
        handlers.semantic_metric_map(
            admin, token_match.group(1), "business value", confirm=True
        )
    )
    assert stale.text.startswith("BLOCKED:")
    current = concierge.semantic.load("g1")
    assert current is not None
    metric = current.metric("metric:observations.opaque")
    assert metric is not None
    assert "business value" not in metric.aliases
    assert not [
        item
        for item in asyncio.run(concierge.audit.query("admin"))
        if item.action == "semantic_metric_map"
    ]


def test_metric_action_token_stales_after_review_revision_and_expires(tmp_path) -> None:
    db = tmp_path / "token-revision.sqlite"
    _seed_multitable(str(db))
    _explorer, store, service, _summary = _onboard(str(db))
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    stale_token = service.issue_metric_action_token("g1", "metric:orders.amount")
    assert service.reset_reviews("g1").status == "confirmed"
    stale = service.map_metric_phrase("g1", stale_token, "business value", assertion)
    assert stale.status == "blocked"

    expired_token = service.issue_metric_action_token("g1", "metric:orders.amount")
    action_key, action_raw = store.kv_list_prefix("g1", "semantic_action:v1:")[0]
    action = json.loads(action_raw)
    action["expires_at"] = 0
    store.kv_set("g1", action_key, json.dumps(action))
    expired = service.map_metric_phrase(
        "g1", expired_token, "business value", assertion
    )
    assert expired.status == "blocked"
    assert store.kv_get("g1", action_key) is None
    assert (
        service.map_metric_phrase(
            "g1", "metric:orders.amount", "business value", assertion
        ).status
        == "blocked"
    )
    bounded_token = service.issue_metric_action_token("g1", "metric:orders.amount")
    oversized = service.map_metric_phrase(
        "g1", bounded_token, "!" * 10_000 + "ok", assertion
    )
    assert oversized.status == "blocked"
    assert store.kv_list_prefix("g1", "semantic_action:v1:")


def test_status_discards_pending_invalidated_by_another_review(tmp_path) -> None:
    db = tmp_path / "stale-status.sqlite"
    _seed_multitable(str(db))
    explorer, _store, service, _summary = _onboard(str(db))
    first = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        requester_id="u1",
        explorer=explorer,
        question="total amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    second = service.prepare_query(
        scope="g1",
        review_scope="review:u2",
        requester_id="u2",
        explorer=explorer,
        question="total weight",
        metric_id="metric:orders.weight_kg",
        metric_phrase="weight",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert first.status == second.status == "clarification"
    assert (
        service.confirm_pending("g1", "review:u1", "sum", reviewer_id="u1").status
        == "confirmed"
    )
    assert service.pending_review_queue("g1") == []

    status = service.status_text("g1", "review:u2")
    assert "현재 확인 대기" not in status
    assert "폐기되었습니다" in status
    assert service.pending_review("review:u2") is None


def test_pending_compare_delete_preserves_newer_same_scope_review(
    tmp_path, monkeypatch
) -> None:
    db = tmp_path / "pending-cas-delete.sqlite"
    _seed_multitable(str(db))
    explorer, store, service, _summary = _onboard(str(db))
    common = {
        "scope": "g1",
        "review_scope": "review:u1",
        "requester_id": "u1",
        "explorer": explorer,
        "dimension_bindings": [],
        "unresolved_obligations": [],
        "limit": 100,
    }
    assert (
        service.prepare_query(
            **common,
            question="total amount",
            metric_id="metric:orders.amount",
            metric_phrase="amount",
            aggregate="sum",
        ).status
        == "clarification"
    )
    first_raw = store.kv_get("review:u1", "semantic_pending_review:v1")
    first = service.pending_review("review:u1")
    assert first_raw is not None and first is not None
    assert (
        service.prepare_query(
            **common,
            question="total weight",
            metric_id="metric:orders.weight_kg",
            metric_phrase="weight",
            aggregate="sum",
        ).status
        == "clarification"
    )
    second_raw = store.kv_get("review:u1", "semantic_pending_review:v1")
    assert second_raw is not None and second_raw != first_raw
    before = service.load("g1")
    assert before is not None
    before_revision = before.review_revision
    before_amount_aliases = list(before.metric("metric:orders.amount").aliases)

    monkeypatch.setattr(
        service,
        "_pending_review_record",
        lambda _scope: (first, first_raw),
    )
    applied = service.confirm_pending("g1", "review:u1", "sum", reviewer_id="u1")
    assert applied.status == "blocked"
    assert store.kv_get("review:u1", "semantic_pending_review:v1") == second_raw
    after = service.load("g1")
    assert after is not None
    assert after.review_revision == before_revision
    assert after.metric("metric:orders.amount").aliases == before_amount_aliases


def test_direct_connect_uses_same_encrypted_onboarding_path(tmp_path):
    db = tmp_path / "connect.db"
    _seed_multitable(str(db))
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)

    result = asyncio.run(handlers.connect(identity, f"sqlite:///{db}"))
    assert "물리 구조 검토 질문 0개" in result.text
    assert "민감·식별자·비지원 컬럼 6개" in result.text
    assert asyncio.run(concierge.secrets.get("g1", "db_dsn")) == f"sqlite:///{db}"
    assert concierge.semantic.load("g1") is not None


def test_reconnect_preserves_reviews_and_invalidates_stale_pending(tmp_path):
    db = tmp_path / "reconnect.db"
    _seed_multitable(str(db))
    dsn = f"sqlite:///{db}"
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    assert "연결 완료" in asyncio.run(handlers.connect(identity, dsn)).text
    explorer = SqlAlchemyExplorer(dsn)

    concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    concierge.semantic.confirm_pending("g1", "review:u1", "sum")
    assert "연결 완료" in asyncio.run(handlers.connect(identity, dsn)).text
    reloaded = concierge.semantic.load("g1")
    assert reloaded is not None
    assert reloaded.metric("metric:orders.amount").reviewed_bindings == {
        "amount": ["sum"]
    }

    concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:stale",
        explorer=explorer,
        question="weight",
        metric_id="metric:orders.weight_kg",
        metric_phrase="weight",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    from sqlalchemy import create_engine, text

    with create_engine(dsn).begin() as connection:
        connection.execute(text("ALTER TABLE orders ADD COLUMN discount NUMERIC"))
    asyncio.run(concierge.semantic.onboard("g1", SqlAlchemyExplorer(dsn)))
    stale = concierge.semantic.confirm_pending("g1", "review:stale", "sum")
    assert stale.status == "blocked"
    assert "이전 확인 요청" in stale.message
    assert "질문을 다시 실행" in stale.message


def test_different_connection_with_same_schema_does_not_inherit_reviews(tmp_path):
    first_db = tmp_path / "first.db"
    second_db = tmp_path / "second.db"
    _seed_multitable(str(first_db))
    _seed_multitable(str(second_db))
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    first_dsn = f"sqlite:///{first_db}"
    second_dsn = f"sqlite:///{second_db}"

    assert "연결 완료" in asyncio.run(handlers.connect(identity, first_dsn)).text
    first_explorer = SqlAlchemyExplorer(first_dsn)
    concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=first_explorer,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    concierge.semantic.confirm_pending("g1", "review:u1", "sum")
    first_catalog = concierge.semantic.load("g1")
    assert first_catalog is not None

    assert "연결 완료" in asyncio.run(handlers.connect(identity, second_dsn)).text
    reloaded = concierge.semantic.load("g1")
    assert reloaded is not None
    assert reloaded.fingerprint == first_catalog.fingerprint
    assert reloaded.metric("metric:orders.amount").reviewed_bindings == {}


def test_connection_bundle_kv_update_rolls_back_as_one_transaction():
    store = SqliteStore()
    store.kv_set("g1", "db_dsn", "old-dsn")
    store.kv_set("g1", CATALOG_KEY, "old-catalog")
    store._conn.execute(
        "CREATE TRIGGER reject_catalog_update BEFORE INSERT ON kv "
        f"WHEN NEW.key = '{CATALOG_KEY}' "
        "BEGIN SELECT RAISE(ABORT, 'catalog rejected'); END"
    )
    with pytest.raises(Exception, match="catalog rejected"):
        store.kv_apply_atomic(
            "g1",
            upserts={"db_dsn": "new-dsn", CATALOG_KEY: "new-catalog"},
        )
    assert store.kv_get("g1", "db_dsn") == "old-dsn"
    assert store.kv_get("g1", CATALOG_KEY) == "old-catalog"


def test_admin_can_reset_human_reviews_without_removing_physical_safety(tmp_path):
    db = tmp_path / "reset.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=explorer,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    concierge.semantic.confirm_pending("g1", "review:u1", "sum")
    handlers = CommandHandlers(concierge)

    non_admin = Identity(user_id="u", guild_id="g1", channel_id="c1")
    assert (
        "관리자만" in asyncio.run(handlers.semantic_reset(non_admin, confirm=True)).text
    )
    admin = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    warning = asyncio.run(handlers.semantic_reset(admin, confirm=False))
    assert "confirm:true" in warning.text
    token_match = re.search(r"action_token: ([A-Za-z0-9_-]+)", warning.text)
    assert token_match is not None
    assert (
        "초기화했습니다"
        in asyncio.run(
            handlers.semantic_reset(
                admin, confirm=True, action_token=token_match.group(1)
            )
        ).text
    )

    catalog = concierge.semantic.load("g1")
    assert catalog is not None
    assert catalog.metric("metric:orders.amount").reviewed_bindings == {}
    assert catalog.metric("metric:orders.source_record_count").reviewed_bindings
    assert "customers.email" in catalog.blocked_columns


def test_reset_invalidates_an_older_pending_review(tmp_path):
    db = tmp_path / "reset-pending.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    scope = "g-reset-pending"
    review_scope = "review:alice"
    asyncio.run(concierge.semantic.onboard(scope, explorer))

    pending = concierge.semantic.prepare_query(
        scope=scope,
        review_scope=review_scope,
        requester_id="alice",
        explorer=explorer,
        question="sum amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert pending.status == "clarification"

    concierge.semantic.reset_reviews(scope)
    stale = concierge.semantic.confirm_pending(
        scope, review_scope, "sum", reviewer_id="alice"
    )
    assert stale.status == "blocked"
    assert "이전 확인 요청" in stale.message
    assert "질문을 다시 실행" in stale.message
    assert concierge.semantic.pending_review(review_scope) is None


class _NoToolLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, messages, tools=()):
        self.calls += 1
        return Completion(content="SELECT * FROM secret_table;")


class _AskSQLLLM:
    def __init__(self) -> None:
        self.calls = 0
        self.seen_messages = []

    async def complete(self, messages, tools=()):
        self.calls += 1
        self.seen_messages.append(list(messages))
        if messages and messages[-1].role == Role.TOOL:
            return Completion(content="done")
        return Completion(
            tool_calls=[
                ToolCall(
                    id="ask-1",
                    name="ask_user",
                    arguments={"question": "SELECT * FROM secret_table;"},
                )
            ]
        )


class _FailingSqlAlchemyExplorer(SqlAlchemyExplorer):
    async def execute(
        self,
        sql: str,
        limit: int = 1000,
        *,
        timeout_seconds: float = 30.0,
    ) -> list[dict]:
        raise RuntimeError(f"driver rejected SQL: {sql}")


def test_governed_discord_blocks_model_prose_and_explicit_pii_before_llm(tmp_path):
    db = tmp_path / "governed-output.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    llm = _NoToolLLM()
    concierge = ContextConcierge(explorer=explorer, llm=llm)
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    _release_all_dimensions(concierge.semantic)
    handlers = CommandHandlers(concierge, query_channel_ids={"c1"})

    prose = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert prose.text.startswith("BLOCKED (semantic_tool_not_called)")
    assert "SELECT" not in prose.text
    calls_before_pii = llm.calls

    pii = asyncio.run(handlers.query(identity, "Amount by customer email"))
    assert pii.text.startswith("BLOCKED (policy_blocked_column)")
    assert "customers\\.email" in pii.text
    assert llm.calls == calls_before_pii


def test_governed_discord_sanitizes_ask_user_sql_and_execution_errors(tmp_path):
    db = tmp_path / "sanitized-errors.db"
    _seed_multitable(str(db))
    base_dsn = f"sqlite:///{db}"
    explorer = SqlAlchemyExplorer(base_dsn)
    concierge = ContextConcierge(explorer=explorer, llm=_AskSQLLLM())
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    _release_all_dimensions(concierge.semantic)
    handlers = CommandHandlers(concierge, query_channel_ids={"c1"})

    clarification = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert clarification.text.startswith("NEEDS CLARIFICATION:")
    assert "SELECT" not in clarification.text

    failing = _FailingSqlAlchemyExplorer(base_dsn)
    store = SqliteStore()
    service = SemanticService(store)
    asyncio.run(service.onboard("g1", failing))
    service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        explorer=failing,
        question="amount",
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    service.confirm_pending("g1", "review:u1", "sum")
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content="amount"))
    active_catalog = service.load("g1")
    tool = SemanticQuery(
        service, active_catalog, build_attention_envelope(active_catalog, "amount")
    )
    context = HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry([tool]),
        session=session,
        explorer=failing,
        safety=SafetyPipeline(),
        audit=store,
        store=store,
    )
    result = asyncio.run(
        tool.run(
            {
                "metric_id": "metric:orders.amount",
                "metric_phrase": "amount",
                "aggregate": "sum",
                "dimensions": [],
                "unresolved_obligations": [],
                "limit": 100,
            },
            context,
        )
    )
    assert result.is_error is True
    assert result.content.startswith("BLOCKED (query_execution_failed)")
    assert "SELECT" not in result.content


def test_discord_persists_only_sanitized_ask_user_for_one_real_reply(tmp_path):
    db = tmp_path / "ask-user-persistence.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    llm = _AskSQLLLM()
    concierge = ContextConcierge(explorer=explorer, llm=llm)
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    _release_all_dimensions(concierge.semantic)
    handlers = CommandHandlers(concierge, query_channel_ids={"c1"})

    first = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert first.text.startswith("NEEDS CLARIFICATION:")
    assert "SELECT" not in first.text
    persisted = asyncio.run(concierge.store.load(identity.session_key()))
    assert persisted is not None
    transient = [message for message in persisted.history() if message.transient]
    assert len(transient) == 1
    assert transient[0].content == first.text
    assert "SELECT" not in " ".join(message.content for message in persisted.history())

    second = asyncio.run(handlers.query(identity, "use amount"))
    assert second.text.startswith("NEEDS CLARIFICATION:")
    assert llm.calls == 2
    assert any(
        message.role == Role.ASSISTANT
        and message.transient
        and message.content == first.text
        for message in llm.seen_messages[1]
    )

    blocked = asyncio.run(handlers.query(identity, "customer email"))
    assert blocked.text.startswith("BLOCKED (policy_blocked_column)")
    persisted = asyncio.run(concierge.store.load(identity.session_key()))
    assert persisted is not None
    assert not any(message.transient for message in persisted.history())


def test_model_cannot_forge_the_server_owned_review_question(tmp_path):
    db = tmp_path / "forged-review-question.db"
    _seed_multitable(str(db))
    explorer, store, service, _summary = _onboard(str(db))
    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content="amount"))
    active_catalog = service.load("g1")
    tool = SemanticQuery(
        service, active_catalog, build_attention_envelope(active_catalog, "amount")
    )
    context = HarnessContext(
        identity=identity,
        llm=FakeLLM(),
        tools=ToolRegistry([tool]),
        session=session,
        explorer=explorer,
        safety=SafetyPipeline(),
        audit=store,
        store=store,
    )

    result = asyncio.run(
        tool.run(
            {
                "metric_id": "metric:orders.amount",
                "metric_phrase": "profit",
                "aggregate": "sum",
                "dimensions": [],
                "unresolved_obligations": [],
                "limit": 100,
                "_reviewed_question": "profit",
            },
            context,
        )
    )

    assert result.is_error is True
    assert result.content.startswith("BLOCKED (metric_phrase_not_grounded)")


def test_admin_can_map_opaque_dimension_phrase_without_sampling_values(tmp_path):
    from sqlalchemy import create_engine, text

    db = tmp_path / "opaque-dimension-map.db"
    with create_engine(f"sqlite:///{db}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE observations ("
                "id INTEGER PRIMARY KEY, value REAL, dmode_ttl TEXT, safe_flag BOOLEAN)"
            )
        )
        connection.execute(text("INSERT INTO observations VALUES (1, 12.5, 'rail', 1)"))
    explorer = _CountingSqlAlchemyExplorer(f"sqlite:///{db}")
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)
    admin = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    other_admin = Identity(
        user_id="other-admin", guild_id="g1", channel_id="c1", is_admin=True
    )
    member = Identity(user_id="member", guild_id="g1", channel_id="c1")

    shown = asyncio.run(
        handlers.semantic_dimension_candidates(
            admin, search="dmode_ttl", state="unmapped"
        )
    )
    token_match = re.search(r"mapping_token: ([A-Za-z0-9_-]+)", shown.text)
    assert token_match is not None
    mapping_token = token_match.group(1)
    assert explorer.sample_calls == 0 and explorer.execute_calls == 0

    denied = asyncio.run(
        handlers.semantic_dimension_map(
            member, mapping_token, "transportation mode title", confirm=True
        )
    )
    assert "관리자만" in denied.text
    warning = asyncio.run(
        handlers.semantic_dimension_map(
            admin, mapping_token, "transportation mode title", confirm=False
        )
    )
    assert "표현에 묶였습니다" in warning.text

    actor_swap = asyncio.run(
        handlers.semantic_dimension_map(
            other_admin, mapping_token, "transportation mode title", confirm=True
        )
    )
    assert actor_swap.text.startswith("BLOCKED:")
    phrase_swap = asyncio.run(
        handlers.semantic_dimension_map(
            admin, mapping_token, "different mode", confirm=True
        )
    )
    assert phrase_swap.text.startswith("BLOCKED:")

    mapped = asyncio.run(
        handlers.semantic_dimension_map(
            admin, mapping_token, "transportation mode title", confirm=True
        )
    )
    assert mapped.text.startswith("✅")
    retried = asyncio.run(
        handlers.semantic_dimension_map(
            admin, mapping_token, "transportation mode title", confirm=True
        )
    )
    assert retried.text.startswith("✅")
    catalog = concierge.semantic.load("g1")
    assert catalog is not None
    dimension = catalog.dimension("dimension:observations.dmode_ttl")
    assert dimension is not None
    assert "transportation mode title" in dimension.aliases
    assert dimension.alias_reviewers["transportation mode title"] == "admin"
    assert explorer.sample_calls == 0 and explorer.execute_calls == 0

    attention = build_attention_envelope(
        catalog,
        "What is the average value by transportation mode title?",
    )
    assert attention.state == "dimension_release_required"
    assert attention.release_required_dimension_ids == (
        "dimension:observations.dmode_ttl",
    )
    events = [
        item
        for item in asyncio.run(concierge.audit.query("admin"))
        if item.action == "semantic_dimension_map"
    ]
    assert len(events) == 1
    assert events[0].detail["dimension_id"] == "dimension:observations.dmode_ttl"

    safe = asyncio.run(
        handlers.semantic_dimension_candidates(admin, search="safe_flag", state="all")
    )
    assert "mapping_token:" in safe.text


def test_shortlist_uses_unique_phrase_ownership_for_multiple_dimensions() -> None:
    table_names = [
        "results",
        "races",
        "circuits",
        "constructors",
        "drivers",
        "teams",
        "seasons",
        "venues",
    ]
    catalog = SemanticCatalog(
        fingerprint="phrase-ownership",
        tables=[TableSpec(id=name, name=name) for name in table_names],
        metrics=[
            MetricSpec(
                id="metric:results.points",
                label="results.points",
                table_id="results",
                column="points",
                data_type="REAL",
            )
        ],
        dimensions=[
            DimensionSpec(
                id=f"dimension:{name}.name",
                label=f"{name}.name",
                table_id=name,
                column="name",
                data_type="TEXT",
            )
            for name in table_names
            if name != "results"
        ],
        joins=[
            JoinSpec(
                id="join:results.race_id->races.race_id",
                child_table_id="results",
                child_column="race_id",
                parent_table_id="races",
                parent_column="race_id",
            ),
            JoinSpec(
                id="join:races.circuit_id->circuits.circuit_id",
                child_table_id="races",
                child_column="circuit_id",
                parent_table_id="circuits",
                parent_column="circuit_id",
            ),
            JoinSpec(
                id="join:results.constructor_id->constructors.constructor_id",
                child_table_id="results",
                child_column="constructor_id",
                parent_table_id="constructors",
                parent_column="constructor_id",
            ),
        ],
    )

    specific = build_attention_envelope(
        catalog,
        "What is the total result points by race name and circuit name?",
    )

    assert specific.ready
    assert specific.metric_ids == ("metric:results.points",)
    assert specific.dimension_ids == (
        "dimension:circuits.name",
        "dimension:races.name",
    )
    assert set(specific.table_ids) == {"circuits", "races", "results"}

    ambiguous = build_attention_envelope(
        catalog,
        "What is the total result points by name?",
    )
    assert ambiguous.state == "clarify_dimension"
    assert not ambiguous.dimension_ids
