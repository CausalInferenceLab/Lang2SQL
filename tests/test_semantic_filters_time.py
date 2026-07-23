from __future__ import annotations

import asyncio
import json
import time

from sqlalchemy import create_engine, text

import lang2sql.semantic.service as semantic_service_module

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.types import Message, Role
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.semantic.catalog import (
    PENDING_REVIEW_KEY,
    DimensionDisclosureTier,
)
from lang2sql.semantic.service import SemanticService, StewardAssertion
from lang2sql.semantic.shortlist import build_attention_envelope
from lang2sql.tools.semantic_query import SemanticQuery


def _seed_orders(path: str) -> SqlAlchemyExplorer:
    with create_engine(f"sqlite:///{path}").begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE orders ("
                "order_id INTEGER PRIMARY KEY, amount NUMERIC NOT NULL, "
                "status TEXT NOT NULL, ordered_on DATE NOT NULL)"
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


def _public_service(path: str):
    explorer = _seed_orders(path)
    store = SqliteStore()
    service = SemanticService(store)
    asyncio.run(service.onboard("g1", explorer))
    assertion = StewardAssertion(
        scope="g1",
        reviewer_id="steward",
        authorized=True,
        public_data_confirmed=True,
    )
    assert service.confirm_public_data_scope("g1", assertion).status == "confirmed"
    for dimension_id in (
        "dimension:orders.status",
        "dimension:orders.ordered_on",
    ):
        assert (
            service.release_dimension(
                "g1",
                dimension_id,
                assertion,
                DimensionDisclosureTier.PUBLIC_GROUPED.value,
            ).status
            == "confirmed"
        )
    return explorer, store, service


def _args(question: str) -> dict[str, object]:
    return {
        "scope": "g1",
        "review_scope": "review:u1",
        "requester_id": "u1",
        "question": question,
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


def _review_until_ready(service: SemanticService, args: dict[str, object]):
    outcome = service.prepare_query(**args)
    stages: list[str] = []
    for _ in range(5):
        if outcome.status == "ready":
            return outcome, stages
        assert outcome.status == "clarification", outcome
        pending = service.pending_review(str(args["review_scope"]))
        assert pending is not None
        stages.append(pending.review_kind)
        choice = (
            pending.proposed_aggregate
            if pending.review_kind == "metric" and pending.aggregate_pending
            else "confirm"
        )
        confirmed = service.confirm_pending(
            str(args["scope"]), str(args["review_scope"]), choice, reviewer_id="u1"
        )
        assert confirmed.status == "confirmed"
        assert confirmed.tool_args["filters"][0]["values"][0]["value"] == "paid"
        assert confirmed.tool_args["time_window"]["start"]["value"] == "2025-01-01"
        assert confirmed.tool_args["time_window"]["end"]["value"] == "2025-02-01"
        outcome = service.prepare_query(**args)
    raise AssertionError("semantic review did not converge")


def test_filter_and_date_window_survive_review_bind_and_execute(tmp_path) -> None:
    explorer, store, service = _public_service(str(tmp_path / "orders.sqlite"))
    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    args = {**_args(question), "explorer": explorer}
    outcome, stages = _review_until_ready(service, args)

    assert stages == ["metric", "dimension", "dimension"]
    assert outcome.prepared is not None
    assert "paid" not in outcome.prepared.sql
    assert "2025-01-01" not in outcome.prepared.sql
    assert outcome.prepared.parameter_mapping() == {
        "p0": "paid",
        "p1": outcome.plan.time_window.start.python_value(),
        "p2": outcome.plan.time_window.end.python_value(),
    }

    catalog = service.load("g1")
    assert catalog is not None
    attention = build_attention_envelope(catalog, question)
    assert attention.dimension_ids == ()
    assert attention.filter_dimension_ids == ("dimension:orders.status",)
    assert attention.time_dimension_ids == ("dimension:orders.ordered_on",)
    tool = SemanticQuery(service, catalog, attention)
    serialized_spec = json.dumps(tool.spec.parameters, sort_keys=True)
    assert "$defs" not in serialized_spec
    assert "$ref" not in serialized_spec
    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    session = Session(identity=identity)
    session.add(Message(role=Role.USER, content=question))
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
                "metric_id": args["metric_id"],
                "metric_phrase": args["metric_phrase"],
                "aggregate": args["aggregate"],
                "dimensions": [],
                "filters": args["filter_bindings"],
                "time_window": args["time_window_binding"],
                "unresolved_obligations": [],
                "limit": 100,
            },
            context,
        )
    )
    assert result.is_error is False
    assert context.semantic_result_headers == ("metric_value",)
    assert context.semantic_result_rows == [(30,)]


def test_review_persistence_omits_question_literals_and_date_bounds(
    tmp_path,
) -> None:
    explorer, store, service = _public_service(str(tmp_path / "private-review.sqlite"))
    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    args = {**_args(question), "explorer": explorer}
    first = service.prepare_query(**args)
    assert first.status == "clarification"
    pending = service.pending_review("review:u1")
    raw = store.kv_get("review:u1", PENDING_REVIEW_KEY)
    assert pending is not None and raw is not None
    assert pending.constraint_filter_count == 1
    assert pending.constraint_has_time_window is True
    for sensitive in (question, "paid", "2025-01-01", "2025-02-01"):
        assert sensitive not in raw
        assert sensitive not in repr(pending)
        assert sensitive not in repr(first)
        assert sensitive not in repr(service._pending_drafts)  # noqa: SLF001
    assert "query_filters" not in raw
    assert "query_time_window" not in raw

    confirmed = service.confirm_pending("g1", "review:u1", "sum", reviewer_id="u1")
    assert confirmed.status == "confirmed"
    assert confirmed.question == question
    assert confirmed.tool_args["filters"][0]["values"][0]["value"] == "paid"
    assert "paid" not in repr(confirmed)
    assert question not in repr(confirmed)

    # The next one-at-a-time review is persisted without a resume payload.
    # A restarted process can apply the human decision, but the user must
    # resubmit the typed question rather than recover literals from storage.
    second = service.prepare_query(**args)
    assert second.status == "clarification"
    second_pending = service.pending_review("review:u1")
    assert second_pending is not None
    restarted = SemanticService(store)
    after_restart = restarted.confirm_pending(
        "g1", "review:u1", "confirm", reviewer_id="u1"
    )
    assert after_restart.status == "confirmed"
    assert after_restart.question == ""
    assert after_restart.tool_args == {}
    assert "다시 제출" in after_restart.message


def test_legacy_pending_review_is_scrubbed_on_first_read(tmp_path) -> None:
    explorer, store, service = _public_service(str(tmp_path / "legacy-review.sqlite"))
    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    outcome = service.prepare_query(**{**_args(question), "explorer": explorer})
    assert outcome.status == "clarification"
    current_raw = store.kv_get("review:u1", PENDING_REVIEW_KEY)
    assert current_raw is not None
    legacy = json.loads(current_raw)
    legacy.pop("constraint_filter_count", None)
    legacy.pop("constraint_has_time_window", None)
    legacy["record_version"] = 1
    legacy["question"] = question
    legacy["query_filters"] = [{"value": "paid"}]
    legacy["query_time_window"] = {
        "start": "2025-01-01",
        "end": "2025-02-01",
    }
    store.kv_set("review:u1", PENDING_REVIEW_KEY, json.dumps(legacy))

    restarted = SemanticService(store)
    pending = restarted.pending_review("review:u1")
    scrubbed = store.kv_get("review:u1", PENDING_REVIEW_KEY)
    assert pending is not None and scrubbed is not None
    assert pending.record_version == 2
    assert pending.constraint_filter_count == 1
    assert pending.constraint_has_time_window is True
    for sensitive in (question, "paid", "2025-01-01", "2025-02-01"):
        assert sensitive not in scrubbed
    assert "question" not in scrubbed
    assert "query_filters" not in scrubbed
    assert "query_time_window" not in scrubbed


def test_service_startup_scrubs_abandoned_legacy_review_without_lookup(
    tmp_path,
) -> None:
    path = tmp_path / "legacy-state.sqlite"
    store = SqliteStore(str(path))
    question = "private paid revenue from 2025-01-01 to 2025-02-01"
    legacy = {
        "metric_id": "metric:orders.amount",
        "question": question,
        "metric_phrase": "revenue",
        "dimension_bindings": [],
        "allowed_choices": ["sum"],
        "query_filters": [{"value": "paid"}],
        "query_time_window": {
            "start": "2025-01-01",
            "end": "2025-02-01",
        },
        "review_id": "legacy-review",
        "catalog_scope": "g1",
    }
    store.kv_set("abandoned-review", PENDING_REVIEW_KEY, json.dumps(legacy))
    store.close()

    reopened = SqliteStore(str(path))
    SemanticService(reopened)
    scrubbed = reopened.kv_get("abandoned-review", PENDING_REVIEW_KEY)
    assert scrubbed is not None
    for sensitive in (question, "paid", "2025-01-01", "2025-02-01"):
        assert sensitive not in scrubbed
    assert "question" not in scrubbed
    assert "query_filters" not in scrubbed
    assert "query_time_window" not in scrubbed
    reopened.close()


def test_sensitive_resume_payload_is_evicted_at_real_ttl(tmp_path, monkeypatch) -> None:
    explorer, _store, service = _public_service(str(tmp_path / "review-ttl.sqlite"))
    monkeypatch.setattr(semantic_service_module, "_PENDING_DRAFT_TTL_SECONDS", 0.05)
    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    outcome = service.prepare_query(**{**_args(question), "explorer": explorer})
    assert outcome.status == "clarification"
    assert service._pending_drafts  # noqa: SLF001

    deadline = time.monotonic() + 1.0
    while service._pending_drafts and time.monotonic() < deadline:  # noqa: SLF001
        time.sleep(0.01)
    assert service._pending_drafts == {}  # noqa: SLF001
    assert service._pending_draft_timers == {}  # noqa: SLF001


def test_expired_resume_payload_cannot_cross_delayed_review_commit(
    tmp_path, monkeypatch
) -> None:
    explorer, store, service = _public_service(str(tmp_path / "review-race.sqlite"))
    monkeypatch.setattr(semantic_service_module, "_PENDING_DRAFT_TTL_SECONDS", 0.05)
    question = "total amount where status is paid"
    outcome = service.prepare_query(
        **{**_args(question), "explorer": explorer, "time_window_binding": None}
    )
    assert outcome.status == "clarification"
    original = store.kv_mutate_scoped_snapshot

    def delayed_mutation(*, entries, mutate):
        time.sleep(0.12)
        return original(entries=entries, mutate=mutate)

    monkeypatch.setattr(store, "kv_mutate_scoped_snapshot", delayed_mutation)
    confirmed = service.confirm_pending("g1", "review:u1", "sum", reviewer_id="u1")
    assert confirmed.status == "confirmed"
    assert confirmed.question == ""
    assert confirmed.tool_args == {}
    assert question not in repr(confirmed)
    assert "paid" not in repr(confirmed)


def test_controlled_predicates_are_not_exposed_in_discord_tool_schema(
    tmp_path,
) -> None:
    explorer = _seed_orders(str(tmp_path / "controlled-tool.sqlite"))
    store = SqliteStore()
    service = SemanticService(store)
    asyncio.run(service.onboard("g1", explorer))
    assertion = StewardAssertion(scope="g1", reviewer_id="steward", authorized=True)
    for dimension_id in (
        "dimension:orders.status",
        "dimension:orders.ordered_on",
    ):
        assert (
            service.release_dimension(
                "g1",
                dimension_id,
                assertion,
                DimensionDisclosureTier.CONTROLLED_GROUPED.value,
            ).status
            == "confirmed"
        )
    catalog = service.load("g1")
    assert catalog is not None
    question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    tool = SemanticQuery(service, catalog, build_attention_envelope(catalog, question))
    properties = tool.spec.parameters["properties"]
    filter_ids = properties["filters"]["items"]["properties"]["dimension_id"]["enum"]
    time_ids = properties["time_window"]["anyOf"][1]["properties"]["dimension_id"][
        "enum"
    ]
    assert filter_ids == []
    assert time_ids == []


def test_filter_values_never_enter_sql_and_invalid_time_or_type_fails_closed(
    tmp_path,
) -> None:
    explorer, _store, service = _public_service(str(tmp_path / "safety.sqlite"))
    normal_question = (
        "total amount where status is paid ordered on " "from 2025-01-01 to 2025-02-01"
    )
    ready, _ = _review_until_ready(
        service, {**_args(normal_question), "explorer": explorer}
    )
    assert ready.status == "ready"

    attack = "paid' OR 1=1 --"
    attack_question = (
        f"total amount where status is {attack} ordered on "
        "from 2025-01-01 to 2025-02-01"
    )
    attack_args = _args(attack_question)
    attack_args["filter_bindings"][0]["values"][0] = {
        "kind": "string",
        "value": attack,
        "phrase": attack,
    }
    outcome = service.prepare_query(explorer=explorer, **attack_args)
    assert outcome.status == "ready"
    assert attack not in outcome.prepared.sql
    assert outcome.prepared.parameter_mapping()["p0"] == attack
    assert attack not in str(outcome.prepared.audit_detail())

    wrong_type = _args(normal_question)
    wrong_type["filter_bindings"][0]["values"][0] = {
        "kind": "integer",
        "value": "1",
        "phrase": "paid",
    }
    blocked = service.prepare_query(explorer=explorer, **wrong_type)
    assert blocked.status == "blocked"
    assert blocked.blocker in {
        "filter_value_not_exact",
        "filter_literal_type_mismatch",
    }

    relative = service.prepare_query(
        explorer=explorer,
        **{
            **_args("total amount last month"),
            "filter_bindings": [],
            "time_window_binding": None,
        },
    )
    assert relative.status == "clarification"
    assert relative.blocker == "time_semantics_not_reviewed"
