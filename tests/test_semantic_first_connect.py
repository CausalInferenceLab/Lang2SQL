"""First-connect semantic vertical slice on an unseen three-table schema."""

from __future__ import annotations

import asyncio

import pytest

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.types import Completion, Message, Role, ToolCall
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.semantic.catalog import CATALOG_KEY, Aggregate
from lang2sql.semantic.service import SemanticService
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


def _onboard(path: str):
    explorer = SqlAlchemyExplorer(f"sqlite:///{path}")
    store = SqliteStore()
    service = SemanticService(store)
    summary = asyncio.run(service.onboard("g1", explorer))
    return explorer, store, service, summary


def test_first_connect_accepts_structure_without_sampling_or_manual_cards(tmp_path):
    db = tmp_path / "unseen.db"
    _seed_multitable(str(db))
    explorer, _store, _service, summary = _onboard(str(db))

    assert summary.table_count == 3
    assert summary.declared_join_count == 2
    assert summary.blocked_column_count == 1
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
    _explorer, _store, _service, summary = _onboard(str(db))
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
    first = service.prepare_query(
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
    assert first.status == "clarification"
    assert service.confirm_pending("g1", "review:u1", "sum").status == "confirmed"

    ready = service.prepare_query(
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
    assert ready.status == "ready"
    rows = asyncio.run(explorer.execute(ready.sql))
    assert rows == [
        {dimension: "alpha", "metric_value": 40},
        {dimension: "beta", "metric_value": 20},
    ]


def test_metric_review_then_unique_two_hop_query_executes_exact_result(tmp_path):
    db = tmp_path / "join.db"
    _seed_multitable(str(db))
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

    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content="Amount by region name"))
    tool = SemanticQuery(service, service.load("g1") or summary.catalog)
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
    assert "North East | 160" in result.content
    assert "South West | 200" in result.content


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
    assert "`profit` → `orders.amount`" in proposed.message
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
    dimension_review = service.confirm_pending("g1", "review:u1", "confirm")
    assert dimension_review.status == "blocked"
    assert "orders.status" in dimension_review.message

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


def test_discord_handler_clarifies_once_then_resumes_original_question(tmp_path):
    db = tmp_path / "discord.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    llm = _SemanticSlotLLM()
    concierge = ContextConcierge(explorer=explorer, llm=llm)
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1", is_admin=True)
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)

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

    resumed = asyncio.run(handlers.semantic_review(identity, "sum"))
    assert "같은 표현은 다시 확인하지 않습니다" in resumed.text
    assert "READY:" in resumed.text
    assert "North East | 160" in resumed.text
    assert llm.calls == calls_before_review  # immutable draft, no second LLM parse
    assert "SQL:" not in resumed.text

    repeated = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert repeated.text.startswith("READY:")
    assert "NEEDS CLARIFICATION" not in repeated.text


def test_direct_connect_uses_same_encrypted_onboarding_path(tmp_path):
    db = tmp_path / "connect.db"
    _seed_multitable(str(db))
    concierge = ContextConcierge(llm=FakeLLM())
    handlers = CommandHandlers(concierge)
    identity = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)

    result = asyncio.run(handlers.connect(identity, f"sqlite:///{db}"))
    assert "물리 구조 검토 질문 0개" in result.text
    assert "개인정보 의심 컬럼 1개" in result.text
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
    assert "DB 구조가 바뀌어" in stale.message


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
    assert (
        "confirm:true"
        in asyncio.run(handlers.semantic_reset(admin, confirm=False)).text
    )
    assert (
        "초기화했습니다"
        in asyncio.run(handlers.semantic_reset(admin, confirm=True)).text
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
    assert "초기화" in stale.message
    assert concierge.semantic.pending_review(review_scope) is None


class _NoToolLLM:
    def __init__(self) -> None:
        self.calls = 0

    async def complete(self, messages, tools=()):
        self.calls += 1
        return Completion(content="SELECT * FROM secret_table;")


class _AskSQLLLM:
    async def complete(self, messages, tools=()):
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
    async def execute(self, sql: str, limit: int = 1000) -> list[dict]:
        raise RuntimeError(f"driver rejected SQL: {sql}")


def test_governed_discord_blocks_model_prose_and_explicit_pii_before_llm(tmp_path):
    db = tmp_path / "governed-output.db"
    _seed_multitable(str(db))
    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    llm = _NoToolLLM()
    concierge = ContextConcierge(explorer=explorer, llm=llm)
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)

    prose = asyncio.run(handlers.query(identity, "Amount by region name"))
    assert prose.text.startswith("BLOCKED (semantic_tool_not_called)")
    assert "SELECT" not in prose.text
    calls_before_pii = llm.calls

    pii = asyncio.run(handlers.query(identity, "Amount by customer email"))
    assert pii.text.startswith("BLOCKED (policy_blocked_column)")
    assert "customers.email" in pii.text
    assert llm.calls == calls_before_pii


def test_governed_discord_sanitizes_ask_user_sql_and_execution_errors(tmp_path):
    db = tmp_path / "sanitized-errors.db"
    _seed_multitable(str(db))
    base_dsn = f"sqlite:///{db}"
    explorer = SqlAlchemyExplorer(base_dsn)
    concierge = ContextConcierge(explorer=explorer, llm=_AskSQLLLM())
    identity = Identity(user_id="u", guild_id="g1", channel_id="c1")
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)

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
    tool = SemanticQuery(service, service.load("g1"))
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
