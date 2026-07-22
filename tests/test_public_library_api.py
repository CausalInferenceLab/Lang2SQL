from __future__ import annotations

import asyncio
from decimal import Decimal
import time

import pytest
from sqlalchemy import create_engine, text

import lang2sql.api.runtime as api_runtime
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.tenancy.concierge import ContextConcierge

from lang2sql import (
    AggregateKind,
    Blocked,
    CallContext,
    CandidateRequest,
    CandidateSet,
    Capability,
    Connected,
    ConnectRequest,
    ConnectionInput,
    DateEndpoint,
    DateWindowInput,
    ExecuteRequest,
    ExecutionReady,
    FeedbackRequest,
    FilterInput,
    FilterOperation,
    Lang2SQLRuntime,
    LiteralInput,
    PlanReady,
    PlanRequest,
    QueryDraft,
    ReviewAction,
    ReviewRequired,
    ValueKind,
)


def _seed(path: str) -> None:
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
                "amount NUMERIC NOT NULL, status TEXT NOT NULL, "
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


def _seed_wide(path: str, dimension_count: int = 25) -> None:
    dimension_names = [f"category_{index:02d}" for index in range(dimension_count)]
    dimension_columns = ", ".join(f"{name} TEXT NOT NULL" for name in dimension_names)
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE wide (record_id INTEGER PRIMARY KEY, "
                f"amount NUMERIC NOT NULL, {dimension_columns})"
            )
        )
        placeholders = ", ".join(
            [":record_id", ":amount", *(f":{name}" for name in dimension_names)]
        )
        rows: list[dict[str, object]] = []
        for record_id, amount, category_23, category_24 in (
            (1, 10, "red", "blue"),
            (2, 20, "red", "green"),
            (3, 40, "green", "blue"),
        ):
            row: dict[str, object] = {
                "record_id": record_id,
                "amount": amount,
                **{name: "other" for name in dimension_names},
            }
            row["category_23"] = category_23
            row["category_24"] = category_24
            rows.append(row)
        connection.execute(
            text(
                "INSERT INTO wide (record_id, amount, "
                f"{', '.join(dimension_names)}) VALUES ({placeholders})"
            ),
            rows,
        )


def _seed_two_predicates(path: str) -> None:
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE sales (sale_id INTEGER PRIMARY KEY, "
                "amount NUMERIC NOT NULL, status TEXT NOT NULL, "
                "region TEXT NOT NULL)"
            )
        )
        connection.execute(
            text(
                "INSERT INTO sales VALUES "
                "(1, 10, 'paid', 'north'), "
                "(2, 20, 'paid', 'south'), "
                "(3, 40, 'pending', 'north')"
            )
        )


def _context(actor: str = "owner") -> CallContext:
    return CallContext(
        scope="workspace",
        actor_id=actor,
        conversation_id="conversation",
        capabilities=frozenset(
            {Capability.CONNECT, Capability.QUERY, Capability.REVIEW_ANY}
        ),
    )


def _draft(
    source,
    candidate_token: str,
    *,
    metric_id: str = "metric:orders.amount",
    filter_id: str = "dimension:orders.status",
    time_id: str = "dimension:orders.ordered_on",
) -> QueryDraft:
    return QueryDraft(
        question=(
            "total amount where status is paid ordered on "
            "from 2025-01-01 to 2025-02-01"
        ),
        source=source,
        candidate_token=candidate_token,
        metric_id=metric_id,
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
        filters=(
            FilterInput(
                dimension_id=filter_id,
                dimension_phrase="status",
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "paid", "paid"),),
            ),
        ),
        time_window=DateWindowInput(
            dimension_id=time_id,
            dimension_phrase="ordered on",
            range_phrase="from 2025-01-01 to 2025-02-01",
            start=DateEndpoint("2025-01-01", "2025-01-01"),
            end=DateEndpoint("2025-02-01", "2025-02-01"),
        ),
    )


def test_public_runtime_connect_feedback_plan_execute_has_no_sql_surface(
    tmp_path,
) -> None:
    database = tmp_path / "orders.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local(path=str(tmp_path / "runtime.sqlite"))
    context = _context()
    connection = ConnectionInput(f"sqlite:///{database}")
    assert str(database) not in repr(connection)

    connected = asyncio.run(runtime.connect(ConnectRequest(context, connection)))
    assert isinstance(connected, Connected)
    assert connected.scan.execution_supported is True
    assert connected.scan.table_count == 1
    assert not hasattr(connected, "sql")

    public_review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )
    public_feedback = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, public_review.review_id, "confirm_public")
        )
    )
    assert public_feedback.applied is True
    for dimension_id in (
        "dimension:orders.status",
        "dimension:orders.ordered_on",
    ):
        review = next(
            item
            for item in connected.reviews
            if item.kind == "dimension_disclosure" and item.object_id == dimension_id
        )
        feedback = asyncio.run(
            runtime.feedback(
                FeedbackRequest(context, review.review_id, "public_grouped")
            )
        )
        assert feedback.applied is True

    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    candidates = asyncio.run(runtime.candidates(CandidateRequest(context, question)))
    assert isinstance(candidates, CandidateSet)
    assert "/semantic_" not in candidates.message
    metric = next(
        item for item in candidates.metrics if item.grounded_phrase == "amount"
    )
    filter_dimension = next(
        item
        for item in candidates.filter_dimensions
        if item.grounded_phrase == "status"
    )
    time_dimension = next(
        item
        for item in candidates.time_dimensions
        if item.grounded_phrase == "ordered on"
    )
    assert filter_dimension.allowed_value_kinds == (ValueKind.STRING,)
    planned = asyncio.run(
        runtime.plan(
            PlanRequest(
                context,
                _draft(
                    candidates.source,
                    candidates.candidate_token,
                    metric_id=metric.metric_id,
                    filter_id=filter_dimension.dimension_id,
                    time_id=time_dimension.dimension_id,
                ),
            )
        )
    )
    review_kinds: list[str] = []
    for _ in range(5):
        if isinstance(planned, PlanReady):
            break
        assert isinstance(planned, ReviewRequired), planned
        review_kinds.append(planned.review.kind)
        choice = "sum" if planned.review.kind == "metric" else "confirm"
        feedback = asyncio.run(
            runtime.feedback(FeedbackRequest(context, planned.review.review_id, choice))
        )
        assert feedback.next is not None
        planned = feedback.next
    assert isinstance(planned, PlanReady)
    assert review_kinds == ["metric", "dimension", "dimension"]
    assert not hasattr(planned.plan, "sql")
    assert "paid" not in repr(planned)

    intruder = _context(actor="intruder")
    denied = asyncio.run(runtime.execute(ExecuteRequest(intruder, planned.plan)))
    assert isinstance(denied, Blocked)
    assert denied.code == "plan_context_mismatch"

    executed = asyncio.run(runtime.execute(ExecuteRequest(context, planned.plan)))
    assert isinstance(executed, ExecutionReady)
    assert executed.columns == ("metric_value",)
    assert executed.rows == ((30,),)
    assert not hasattr(executed, "sql")
    assert "paid" not in repr(executed)

    replay = asyncio.run(runtime.execute(ExecuteRequest(context, planned.plan)))
    assert isinstance(replay, Blocked)
    assert replay.code == "plan_unavailable"
    runtime.close()


def test_public_runtime_requires_host_derived_capabilities(tmp_path) -> None:
    database = tmp_path / "denied.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = CallContext("workspace", "user", "conversation")

    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Blocked)
    assert connected.code == "capability_required"
    runtime.close()


def test_governance_review_survives_unauthorized_invalid_and_wrong_scope_attempts(
    tmp_path,
) -> None:
    database = tmp_path / "governance.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    owner = _context()
    connected = asyncio.run(
        runtime.connect(ConnectRequest(owner, ConnectionInput(f"sqlite:///{database}")))
    )
    assert isinstance(connected, Connected)
    review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )

    requester = CallContext(
        "workspace",
        "requester",
        "conversation",
        frozenset({Capability.QUERY}),
    )
    forbidden = asyncio.run(
        runtime.feedback(FeedbackRequest(requester, review.review_id, "confirm_public"))
    )
    assert isinstance(forbidden, Blocked)
    assert forbidden.code == "review_forbidden"

    invalid = asyncio.run(
        runtime.feedback(FeedbackRequest(owner, review.review_id, "publish_all"))
    )
    assert isinstance(invalid, Blocked)
    assert invalid.code == "review_choice_invalid"

    wrong_scope = CallContext(
        "other-workspace",
        "owner",
        "conversation",
        frozenset({Capability.QUERY, Capability.REVIEW_ANY}),
    )
    stale_attempt = asyncio.run(
        runtime.feedback(
            FeedbackRequest(wrong_scope, review.review_id, "confirm_public")
        )
    )
    assert isinstance(stale_attempt, Blocked)
    assert stale_attempt.code == "review_stale"
    assert (
        asyncio.run(runtime._concierge.audit.query(owner.actor_id)) == []
    )  # noqa: SLF001

    applied = asyncio.run(
        runtime.feedback(FeedbackRequest(owner, review.review_id, "confirm_public"))
    )
    assert applied.applied is True
    events = asyncio.run(runtime._concierge.audit.query(owner.actor_id))  # noqa: SLF001
    assert len(events) == 1
    assert events[0].action == "semantic_public_data_confirm"
    assert events[0].scope == owner.conversation_id
    runtime.close()


def test_concurrent_governance_feedback_has_one_terminal_decision(tmp_path) -> None:
    database = tmp_path / "governance-race.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )
    before = runtime._concierge.semantic.load(context.scope)  # noqa: SLF001
    assert before is not None

    async def decide_twice():
        request = FeedbackRequest(context, review.review_id, "confirm_public")
        return await asyncio.gather(
            runtime.feedback(request),
            runtime.feedback(request),
        )

    results = asyncio.run(decide_twice())
    assert sum(not isinstance(item, Blocked) for item in results) == 1
    after = runtime._concierge.semantic.load(context.scope)  # noqa: SLF001
    assert after is not None
    assert after.review_revision == before.review_revision + 1
    runtime.close()


def test_public_governance_audit_failure_rolls_back_and_keeps_review(
    tmp_path,
) -> None:
    database = tmp_path / "governance-audit.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )
    store = runtime._concierge.store  # noqa: SLF001
    store._conn.execute(  # noqa: SLF001
        "CREATE TRIGGER fail_api_governance_audit BEFORE INSERT ON audit "
        "WHEN NEW.action = 'semantic_public_data_confirm' "
        "BEGIN SELECT RAISE(ABORT, 'forced public audit failure'); END"
    )
    store._conn.commit()  # noqa: SLF001

    failed = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review_id, "confirm_public"))
    )
    assert isinstance(failed, Blocked)
    assert failed.code == "review_not_applied"
    catalog = runtime._concierge.semantic.load(context.scope)  # noqa: SLF001
    assert catalog is not None and catalog.public_data_scope is False
    assert asyncio.run(store.query(context.actor_id)) == []
    assert store.kv_list_prefix(context.scope, "semantic_action:v1:") == []
    assert store.kv_list_prefix(context.scope, "semantic_action_arm:v1:") == []
    assert store.kv_list_prefix(context.scope, "semantic_action_receipt:v1:") == []

    store._conn.execute("DROP TRIGGER fail_api_governance_audit")  # noqa: SLF001
    store._conn.commit()  # noqa: SLF001
    applied = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review_id, "confirm_public"))
    )
    assert applied.applied is True
    events = asyncio.run(store.query(context.actor_id))
    assert [event.action for event in events] == ["semantic_public_data_confirm"]
    assert store.kv_list_prefix(context.scope, "semantic_action:v1:") == []
    assert store.kv_list_prefix(context.scope, "semantic_action_arm:v1:") == []
    assert len(store.kv_list_prefix(context.scope, "semantic_action_receipt:v1:")) == 1
    runtime.close()


def test_public_feedback_blocks_non_atomic_external_audit(tmp_path) -> None:
    database = tmp_path / "external-audit.sqlite"
    _seed(str(database))
    primary = SqliteStore()
    external = SqliteStore()
    runtime = Lang2SQLRuntime(ContextConcierge(store=primary, audit=external))
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )

    blocked = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review_id, "confirm_public"))
    )
    assert isinstance(blocked, Blocked)
    assert blocked.code == "semantic_audit_not_atomic"
    catalog = runtime._concierge.semantic.load(context.scope)  # noqa: SLF001
    assert catalog is not None and catalog.public_data_scope is False
    assert asyncio.run(primary.query(context.actor_id)) == []
    assert asyncio.run(external.query(context.actor_id)) == []
    runtime.close()
    external.close()


def test_metric_review_audit_failure_is_retryable_and_keeps_pending(
    tmp_path,
) -> None:
    database = tmp_path / "metric-audit.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    draft = QueryDraft(
        question="total amount",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(review, ReviewRequired)
    store = runtime._concierge.store  # noqa: SLF001
    store._conn.execute(  # noqa: SLF001
        "CREATE TRIGGER fail_metric_review_audit BEFORE INSERT ON audit "
        "WHEN NEW.action = 'semantic_review' "
        "BEGIN SELECT RAISE(ABORT, 'forced metric audit failure'); END"
    )
    store._conn.commit()  # noqa: SLF001

    failed = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review.review_id, "sum"))
    )
    assert isinstance(failed, Blocked)
    assert failed.code == "review_not_applied"
    assert failed.retryable is True
    assert (
        runtime._concierge.semantic.pending_review_by_id(  # noqa: SLF001
            context.scope, review.review.review_id
        )
        is not None
    )
    assert asyncio.run(store.query(context.actor_id)) == []

    store._conn.execute("DROP TRIGGER fail_metric_review_audit")  # noqa: SLF001
    store._conn.commit()  # noqa: SLF001
    applied = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review.review_id, "sum"))
    )
    assert not isinstance(applied, Blocked)
    assert isinstance(applied.next, PlanReady)
    runtime.close()


def test_external_execution_audit_failure_never_publishes_rows(tmp_path) -> None:
    database = tmp_path / "execution-audit.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    public_review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )
    public_applied = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, public_review.review_id, "confirm_public")
        )
    )
    assert not isinstance(public_applied, Blocked)
    candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    draft = QueryDraft(
        question="total amount",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(review, ReviewRequired)
    applied = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review.review_id, "sum"))
    )
    assert isinstance(applied.next, PlanReady)

    class FailingAudit:
        async def record(self, _event) -> None:
            raise OSError("audit unavailable")

        async def query(self, _actor: str, limit: int = 20):
            return []

    runtime._concierge._audit = FailingAudit()  # noqa: SLF001
    blocked = asyncio.run(runtime.execute(ExecuteRequest(context, applied.next.plan)))
    assert isinstance(blocked, Blocked)
    assert blocked.code == "audit_write_failed"
    assert blocked.retryable is False
    runtime.close()


def test_hidden_filter_dimension_gets_reusable_on_demand_review(tmp_path) -> None:
    database = tmp_path / "wide.sqlite"
    _seed_wide(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    target_id = "dimension:wide.category_24"
    assert len(connected.reviews) == 20
    assert connected.remaining_review_count > 0
    assert all(item.object_id != target_id for item in connected.reviews)
    assert len(runtime._governance) == len(connected.reviews)  # noqa: SLF001

    discovered = asyncio.run(
        runtime.candidates(
            CandidateRequest(context, "total amount where category 24 is needle")
        )
    )
    assert isinstance(discovered, CandidateSet)
    review_candidate = next(
        item
        for item in discovered.review_required_dimensions
        if item.dimension_id == target_id
    )
    assert review_candidate.required_action == ReviewAction.DIMENSION_DISCLOSURE
    assert "needle" not in repr(discovered)
    assert len(runtime._governance) == len(connected.reviews)  # noqa: SLF001

    draft = QueryDraft(
        question="total amount where category 24 is needle",
        source=connected.source,
        candidate_token=discovered.candidate_token,
        metric_id="metric:wide.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
        filters=(
            FilterInput(
                dimension_id=target_id,
                dimension_phrase="category 24",
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "needle", "needle"),),
            ),
        ),
    )
    first = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(first, ReviewRequired)
    assert first.review.kind == "public_data_scope"
    assert "needle" not in repr(first)
    confirmed_public = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, first.review.review_id, "confirm_public")
        )
    )
    assert confirmed_public.applied is True

    second = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    repeated = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(second, ReviewRequired)
    assert isinstance(repeated, ReviewRequired)
    assert second.review.object_id == target_id
    assert second.review.review_id == repeated.review.review_id
    assert second.review.allowed_choices == ("public_grouped", "keep_blocked")
    assert "needle" not in repr(second)

    invalid_tier = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, second.review.review_id, "controlled_grouped")
        )
    )
    assert isinstance(invalid_tier, Blocked)
    assert invalid_tier.code == "review_choice_invalid"
    applied = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, second.review.review_id, "public_grouped")
        )
    )
    assert applied.applied is True
    runtime.close()


def test_two_hidden_filters_are_reviewed_sequentially_then_execute(tmp_path) -> None:
    database = tmp_path / "wide-two-hidden.sqlite"
    _seed_wide(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    targets = (
        "dimension:wide.category_23",
        "dimension:wide.category_24",
    )
    assert all(
        all(review.object_id != target for review in connected.reviews)
        for target in targets
    )

    question = "total amount where category 23 is red and category 24 is blue"
    discovered = asyncio.run(runtime.candidates(CandidateRequest(context, question)))
    assert isinstance(discovered, CandidateSet)
    assert {item.dimension_id for item in discovered.review_required_dimensions} >= set(
        targets
    )
    draft = QueryDraft(
        question=question,
        source=discovered.source,
        candidate_token=discovered.candidate_token,
        metric_id="metric:wide.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
        filters=(
            FilterInput(
                dimension_id=targets[0],
                dimension_phrase="category 23",
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "red", "red"),),
            ),
            FilterInput(
                dimension_id=targets[1],
                dimension_phrase="category 24",
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "blue", "blue"),),
            ),
        ),
    )

    public_review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(public_review, ReviewRequired)
    assert public_review.review.kind == "public_data_scope"
    assert asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, public_review.review.review_id, "confirm_public")
        )
    ).applied

    issued: list[str] = []
    for target in targets:
        review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
        assert isinstance(review, ReviewRequired)
        assert review.review.kind == "dimension_disclosure"
        assert review.review.object_id == target
        assert "'red'" not in repr(review.review)
        assert "'blue'" not in repr(review.review)
        issued.append(review.review.object_id)
        assert asyncio.run(
            runtime.feedback(
                FeedbackRequest(context, review.review.review_id, "public_grouped")
            )
        ).applied
    assert issued == list(targets)

    planned = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    for _ in range(6):
        if isinstance(planned, PlanReady):
            break
        assert isinstance(planned, ReviewRequired), planned
        choice = "sum" if planned.review.kind == "metric" else "confirm"
        feedback = asyncio.run(
            runtime.feedback(FeedbackRequest(context, planned.review.review_id, choice))
        )
        assert feedback.next is not None
        planned = feedback.next
    assert isinstance(planned, PlanReady)
    result = asyncio.run(runtime.execute(ExecuteRequest(context, planned.plan)))
    assert isinstance(result, ExecutionReady)
    assert result.rows == ((10,),)
    runtime.close()


def test_two_controlled_filters_upgrade_sequentially_then_execute(tmp_path) -> None:
    database = tmp_path / "two-controlled.sqlite"
    _seed_two_predicates(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    targets = (
        "dimension:sales.region",
        "dimension:sales.status",
    )
    for target in targets:
        review = next(item for item in connected.reviews if item.object_id == target)
        assert asyncio.run(
            runtime.feedback(
                FeedbackRequest(context, review.review_id, "controlled_grouped")
            )
        ).applied

    question = "total amount where status is paid and region is north"
    discovered = asyncio.run(runtime.candidates(CandidateRequest(context, question)))
    assert isinstance(discovered, CandidateSet)
    draft = QueryDraft(
        question=question,
        source=discovered.source,
        candidate_token=discovered.candidate_token,
        metric_id="metric:sales.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
        filters=(
            FilterInput(
                dimension_id="dimension:sales.status",
                dimension_phrase="status",
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "paid", "paid"),),
            ),
            FilterInput(
                dimension_id="dimension:sales.region",
                dimension_phrase="region",
                operator=FilterOperation.EQ,
                operator_phrase="is",
                values=(LiteralInput(ValueKind.STRING, "north", "north"),),
            ),
        ),
    )
    public_review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(public_review, ReviewRequired)
    assert public_review.review.kind == "public_data_scope"
    assert asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, public_review.review.review_id, "confirm_public")
        )
    ).applied

    issued: list[str] = []
    for target in targets:
        review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
        assert isinstance(review, ReviewRequired)
        assert review.review.object_id == target
        assert review.review.allowed_choices == (
            "public_grouped",
            "keep_controlled",
        )
        issued.append(review.review.object_id)
        assert asyncio.run(
            runtime.feedback(
                FeedbackRequest(context, review.review.review_id, "public_grouped")
            )
        ).applied
    assert issued == list(targets)

    planned = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    for _ in range(6):
        if isinstance(planned, PlanReady):
            break
        assert isinstance(planned, ReviewRequired), planned
        choice = "sum" if planned.review.kind == "metric" else "confirm"
        feedback = asyncio.run(
            runtime.feedback(FeedbackRequest(context, planned.review.review_id, choice))
        )
        assert feedback.next is not None
        planned = feedback.next
    assert isinstance(planned, PlanReady)
    result = asyncio.run(runtime.execute(ExecuteRequest(context, planned.plan)))
    assert isinstance(result, ExecutionReady)
    assert result.rows == ((10,),)
    runtime.close()


def test_public_connect_rejects_missing_sqlite_without_creating_it(tmp_path) -> None:
    missing = tmp_path / "missing.sqlite"
    runtime = Lang2SQLRuntime.local()
    result = asyncio.run(
        runtime.connect(
            ConnectRequest(_context(), ConnectionInput(f"sqlite:///{missing}"))
        )
    )
    assert isinstance(result, Blocked)
    assert result.code == "connection_failed"
    assert not missing.exists()
    runtime.close()


def test_controlled_filter_is_discoverable_and_upgrades_through_feedback(
    tmp_path,
) -> None:
    database = tmp_path / "controlled.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    status_review = next(
        item
        for item in connected.reviews
        if item.object_id == "dimension:orders.status"
    )
    controlled = asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, status_review.review_id, "controlled_grouped")
        )
    )
    assert controlled.applied is True

    question = "total amount where status is paid"
    first_candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, question))
    )
    assert isinstance(first_candidates, CandidateSet)
    metric = next(
        item for item in first_candidates.metrics if item.grounded_phrase == "amount"
    )
    upgrade = next(
        item
        for item in first_candidates.review_required_dimensions
        if item.grounded_phrase == "status"
    )
    assert upgrade.required_action == ReviewAction.PUBLIC_DATA_SCOPE
    assert first_candidates.filter_dimensions == ()
    assert "/semantic_" not in first_candidates.message

    draft = QueryDraft(
        question=question,
        source=first_candidates.source,
        candidate_token=first_candidates.candidate_token,
        metric_id=metric.metric_id,
        metric_phrase=metric.grounded_phrase,
        aggregate="sum",  # type: ignore[arg-type] - public coercion is intentional
        filters=(
            FilterInput(
                dimension_id=upgrade.dimension_id,
                dimension_phrase=upgrade.grounded_phrase,
                operator="eq",  # type: ignore[arg-type] - public coercion is intentional
                operator_phrase="is",
                values=(LiteralInput("string", "paid", "paid"),),  # type: ignore[arg-type]
            ),
        ),
    )
    public_required = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(public_required, ReviewRequired)
    assert public_required.review.kind == "public_data_scope"
    assert asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, public_required.review.review_id, "confirm_public")
        )
    ).applied

    second_candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, question))
    )
    assert isinstance(second_candidates, CandidateSet)
    tier_upgrade = next(
        item
        for item in second_candidates.review_required_dimensions
        if item.dimension_id == upgrade.dimension_id
    )
    assert tier_upgrade.required_action == ReviewAction.PUBLIC_GROUPED
    dimension_required = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(dimension_required, ReviewRequired)
    assert dimension_required.review.allowed_choices == (
        "public_grouped",
        "keep_controlled",
    )
    kept = asyncio.run(
        runtime.feedback(
            FeedbackRequest(
                context, dimension_required.review.review_id, "keep_controlled"
            )
        )
    )
    assert kept.applied is False
    assert "보호 그룹" in kept.message
    grouping_candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount by status"))
    )
    assert isinstance(grouping_candidates, CandidateSet)
    assert any(
        item.dimension_id == upgrade.dimension_id
        for item in grouping_candidates.grouping_dimensions
    )

    dimension_required = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(dimension_required, ReviewRequired)
    assert asyncio.run(
        runtime.feedback(
            FeedbackRequest(
                context, dimension_required.review.review_id, "public_grouped"
            )
        )
    ).applied

    final_candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, question))
    )
    assert isinstance(final_candidates, CandidateSet)
    assert next(
        item
        for item in final_candidates.filter_dimensions
        if item.dimension_id == upgrade.dimension_id
    ).allowed_value_kinds == (ValueKind.STRING,)

    planned = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    for _ in range(4):
        if isinstance(planned, PlanReady):
            break
        assert isinstance(planned, ReviewRequired)
        choice = "sum" if planned.review.kind == "metric" else "confirm"
        feedback = asyncio.run(
            runtime.feedback(FeedbackRequest(context, planned.review.review_id, choice))
        )
        assert feedback.next is not None
        planned = feedback.next
    assert isinstance(planned, PlanReady)
    result = asyncio.run(runtime.execute(ExecuteRequest(context, planned.plan)))
    assert isinstance(result, ExecutionReady)
    assert result.rows == ((70,),)
    runtime.close()


def test_candidate_source_is_rejected_after_reconnect(tmp_path) -> None:
    database = tmp_path / "reconnect.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    first = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(first, Connected)
    candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    metric = next(
        item for item in candidates.metrics if item.grounded_phrase == "amount"
    )
    second = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(second, Connected)
    assert second.source.generation > candidates.source.generation

    stale = QueryDraft(
        question="total amount",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id=metric.metric_id,
        metric_phrase=metric.grounded_phrase,
        aggregate=AggregateKind.SUM,
    )
    blocked = asyncio.run(runtime.plan(PlanRequest(context, stale)))
    assert isinstance(blocked, Blocked)
    assert blocked.code == "candidate_source_stale"
    runtime.close()


def test_candidate_token_binds_original_question_actor_and_close_erases_draft(
    tmp_path,
) -> None:
    database = tmp_path / "candidate-binding.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    assert candidates.candidate_token not in repr(candidates)

    changed_question = QueryDraft(
        question="total amount where status is paid",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    blocked = asyncio.run(runtime.plan(PlanRequest(context, changed_question)))
    assert isinstance(blocked, Blocked)
    assert blocked.code == "candidate_question_mismatch"

    other_actor = _context(actor="other")
    original = QueryDraft(
        question="total amount",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    blocked = asyncio.run(runtime.plan(PlanRequest(other_actor, original)))
    assert isinstance(blocked, Blocked)
    assert blocked.code == "candidate_question_mismatch"

    separator_context = CallContext(
        scope=context.scope,
        actor_id="alpha\x1fbeta",
        conversation_id="gamma",
        capabilities=context.capabilities,
    )
    collision_context = CallContext(
        scope=context.scope,
        actor_id="alpha",
        conversation_id="beta\x1fgamma",
        capabilities=context.capabilities,
    )
    separator_candidates = asyncio.run(
        runtime.candidates(CandidateRequest(separator_context, "total amount"))
    )
    assert isinstance(separator_candidates, CandidateSet)
    separator_draft = QueryDraft(
        question="total amount",
        source=separator_candidates.source,
        candidate_token=separator_candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    collision = asyncio.run(
        runtime.plan(PlanRequest(collision_context, separator_draft))
    )
    assert isinstance(collision, Blocked)
    assert collision.code == "candidate_question_mismatch"

    pending = asyncio.run(runtime.plan(PlanRequest(context, original)))
    assert isinstance(pending, ReviewRequired)
    semantic = runtime._concierge.semantic  # noqa: SLF001
    assert semantic._pending_drafts  # noqa: SLF001
    runtime.close()
    assert semantic._pending_drafts == {}  # noqa: SLF001
    assert semantic._pending_draft_timers == {}  # noqa: SLF001


def test_abandoned_plan_is_evicted_at_real_ttl(tmp_path, monkeypatch) -> None:
    database = tmp_path / "plan-ttl.sqlite"
    _seed(str(database))
    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"sqlite:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    candidates = asyncio.run(
        runtime.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    draft = QueryDraft(
        question="total amount",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    review = asyncio.run(runtime.plan(PlanRequest(context, draft)))
    assert isinstance(review, ReviewRequired)
    monkeypatch.setattr(api_runtime, "_PLAN_TTL_SECONDS", 0.05)
    applied = asyncio.run(
        runtime.feedback(FeedbackRequest(context, review.review.review_id, "sum"))
    )
    assert isinstance(applied.next, PlanReady)
    plan_id = applied.next.plan.plan_id
    assert plan_id in runtime._plans  # noqa: SLF001

    deadline = time.monotonic() + 1.0
    while plan_id in runtime._plans and time.monotonic() < deadline:  # noqa: SLF001
        time.sleep(0.01)
    assert plan_id not in runtime._plans  # noqa: SLF001
    assert plan_id not in runtime._plan_timers  # noqa: SLF001
    runtime.close()


def test_restart_keeps_review_but_requires_fresh_candidate_token(tmp_path) -> None:
    database = tmp_path / "restart-candidates.sqlite"
    state = tmp_path / "runtime-state.sqlite"
    _seed(str(database))
    context = _context()

    first = Lang2SQLRuntime.local(path=str(state))
    connected = asyncio.run(
        first.connect(ConnectRequest(context, ConnectionInput(f"sqlite:///{database}")))
    )
    assert isinstance(connected, Connected)
    candidates = asyncio.run(
        first.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(candidates, CandidateSet)
    old_draft = QueryDraft(
        question="total amount",
        source=candidates.source,
        candidate_token=candidates.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    review = asyncio.run(first.plan(PlanRequest(context, old_draft)))
    assert isinstance(review, ReviewRequired)
    first.close()

    restarted = Lang2SQLRuntime.local(path=str(state))
    applied = asyncio.run(
        restarted.feedback(FeedbackRequest(context, review.review.review_id, "sum"))
    )
    assert not isinstance(applied, Blocked)
    assert applied.next is None
    stale = asyncio.run(restarted.plan(PlanRequest(context, old_draft)))
    assert isinstance(stale, Blocked)
    assert stale.code == "candidate_question_mismatch"

    refreshed = asyncio.run(
        restarted.candidates(CandidateRequest(context, "total amount"))
    )
    assert isinstance(refreshed, CandidateSet)
    fresh_draft = QueryDraft(
        question="total amount",
        source=refreshed.source,
        candidate_token=refreshed.candidate_token,
        metric_id="metric:orders.amount",
        metric_phrase="amount",
        aggregate=AggregateKind.SUM,
    )
    ready = asyncio.run(restarted.plan(PlanRequest(context, fresh_draft)))
    assert isinstance(ready, PlanReady)
    restarted.close()


def test_public_runtime_executes_same_typed_plan_on_file_duckdb(tmp_path) -> None:
    duckdb = pytest.importorskip("duckdb")
    database = tmp_path / "orders.duckdb"
    connection = duckdb.connect(str(database))
    try:
        connection.execute(
            "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, "
            "amount DECIMAL(18, 2) NOT NULL, status VARCHAR NOT NULL, "
            "ordered_on DATE NOT NULL)"
        )
        connection.execute(
            "INSERT INTO orders VALUES "
            "(1, 10, 'paid', DATE '2025-01-01'), "
            "(2, 20, 'paid', DATE '2025-01-31'), "
            "(3, 40, 'paid', DATE '2025-02-01'), "
            "(4, 80, 'pending', DATE '2025-01-15')"
        )
    finally:
        connection.close()

    runtime = Lang2SQLRuntime.local()
    context = _context()
    connected = asyncio.run(
        runtime.connect(
            ConnectRequest(context, ConnectionInput(f"duckdb:///{database}"))
        )
    )
    assert isinstance(connected, Connected)
    assert connected.scan.execution_supported is True
    public_review = next(
        item for item in connected.reviews if item.kind == "public_data_scope"
    )
    assert asyncio.run(
        runtime.feedback(
            FeedbackRequest(context, public_review.review_id, "confirm_public")
        )
    ).applied
    for dimension_id in (
        "dimension:orders.status",
        "dimension:orders.ordered_on",
    ):
        review = next(
            item
            for item in connected.reviews
            if item.kind == "dimension_disclosure" and item.object_id == dimension_id
        )
        assert asyncio.run(
            runtime.feedback(
                FeedbackRequest(context, review.review_id, "public_grouped")
            )
        ).applied

    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    candidates = asyncio.run(runtime.candidates(CandidateRequest(context, question)))
    assert isinstance(candidates, CandidateSet)
    planned = asyncio.run(
        runtime.plan(
            PlanRequest(
                context,
                _draft(candidates.source, candidates.candidate_token),
            )
        )
    )
    for _ in range(5):
        if isinstance(planned, PlanReady):
            break
        assert isinstance(planned, ReviewRequired)
        choice = "sum" if planned.review.kind == "metric" else "confirm"
        feedback = asyncio.run(
            runtime.feedback(FeedbackRequest(context, planned.review.review_id, choice))
        )
        assert feedback.next is not None
        planned = feedback.next
    assert isinstance(planned, PlanReady)
    result = asyncio.run(runtime.execute(ExecuteRequest(context, planned.plan)))
    assert isinstance(result, ExecutionReady)
    assert result.rows == ((Decimal("30.00"),),)
    runtime.close()
    reopened = duckdb.connect(str(database), read_only=False)
    reopened.close()
