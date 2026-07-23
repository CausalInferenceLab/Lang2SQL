"""Adversarial coverage for metadata-only string-dimension disclosure review."""

from __future__ import annotations

import asyncio
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import json
import re

import pytest
from sqlalchemy import create_engine, text

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.adapters.llm.fake import FakeLLM
from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.types import Message, Role
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.semantic.catalog import (
    CATALOG_KEY,
    DimensionReviewPolicy,
    SemanticCatalog,
)
from lang2sql.semantic.service import (
    SemanticService,
    StewardAssertion,
    _compile_sql,
    decode_semantic_query_rows,
    enforce_released_dimension_output,
)
from lang2sql.semantic.shortlist import build_attention_envelope
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.semantic_query import SemanticQuery


def _seed_candidate_db(path: str) -> SqlAlchemyExplorer:
    engine = create_engine(f"sqlite:///{path}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE measurements ("
                "dmode_ttl TEXT NOT NULL, status TEXT NOT NULL, value REAL NOT NULL)"
            )
        )
        connection.execute(
            text("INSERT INTO measurements VALUES (:mode, 'ok', :value)"),
            [
                {"mode": mode, "value": index + 1}
                for mode in ("rail", "road")
                for index in range(5)
            ],
        )
    return SqlAlchemyExplorer(f"sqlite:///{path}")


def _onboard_candidate(path: str):
    explorer = _seed_candidate_db(path)
    store = SqliteStore()
    service = SemanticService(store)
    summary = asyncio.run(service.onboard("g1", explorer))
    return explorer, store, service, summary


def _dimension_enum(tool: SemanticQuery) -> list[str]:
    return tool.spec.parameters["properties"]["dimensions"]["items"]["properties"][
        "dimension_id"
    ]["enum"]


def _dimension_action_tokens(
    service: SemanticService, *, include_released: bool = True
) -> dict[str, str]:
    catalog, candidates = service.dimension_candidate_snapshot(
        "g1", include_released=include_released
    )
    assert catalog is not None
    return {
        candidate.id: service.issue_dimension_action_token(
            "g1",
            candidate.id,
            "dimension_set_tier",
            expected_catalog=catalog,
        )
        for candidate in candidates
    }


def test_dimension_tokens_for_different_targets_survive_sequential_mutations(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "two-targets.db")
    )
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    tokens = _dimension_action_tokens(service)
    assert len(tokens) >= 2

    outcomes = [
        service.release_dimension_with_token("g1", token, assertion)
        for token in list(tokens.values())[:2]
    ]
    assert [outcome.status for outcome in outcomes] == ["confirmed", "confirmed"]
    assert all(outcome.mutation_applied for outcome in outcomes)


def test_dimension_tokens_for_same_target_are_compare_and_swap(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "same-target-race.db")
    )
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    catalog, candidates = service.dimension_candidate_snapshot("g1")
    assert catalog is not None and candidates
    target = candidates[0]
    tokens = [
        service.issue_dimension_action_token(
            "g1",
            target.id,
            "dimension_set_tier",
            expected_catalog=catalog,
        )
        for _ in range(2)
    ]

    with ThreadPoolExecutor(max_workers=2) as pool:
        outcomes = list(
            pool.map(
                lambda token: service.release_dimension_with_token(
                    "g1", token, assertion
                ),
                tokens,
            )
        )
    assert sorted(outcome.status for outcome in outcomes) == ["blocked", "confirmed"]
    assert sum(outcome.mutation_applied for outcome in outcomes) == 1


def test_dimension_action_revision_prevents_aba_token_revival(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "dimension-aba.db")
    )
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    catalog, candidates = service.dimension_candidate_snapshot("g1")
    assert catalog is not None and candidates
    target = candidates[0]
    sibling_tokens = [
        service.issue_dimension_action_token(
            "g1",
            target.id,
            "dimension_set_tier",
            expected_catalog=catalog,
        )
        for _ in range(2)
    ]
    assert (
        service.release_dimension_with_token("g1", sibling_tokens[0], assertion).status
        == "confirmed"
    )

    released_catalog, _candidates = service.dimension_candidate_snapshot(
        "g1", include_released=True
    )
    assert released_catalog is not None
    revoke_token = service.issue_dimension_action_token(
        "g1",
        target.id,
        "dimension_revoke",
        expected_catalog=released_catalog,
    )
    assert (
        service.revoke_dimension_with_token("g1", revoke_token, assertion).status
        == "confirmed"
    )

    stale = service.release_dimension_with_token("g1", sibling_tokens[1], assertion)
    assert stale.status == "blocked"
    current = service.load("g1")
    assert current is not None
    assert current.dimension(target.id).raw_output_allowed is False
    assert current.dimension(target.id).action_revision == 2


def test_reset_invalidates_a_pending_dimension_action_without_target_change(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "reset-action-epoch.db")
    )
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    token = next(iter(_dimension_action_tokens(service).values()))
    before = service.load("g1")
    assert before is not None

    assert service.reset_reviews("g1").status == "confirmed"
    after = service.load("g1")
    assert after is not None
    assert after.dimension_action_epoch == before.dimension_action_epoch + 1
    assert (
        service.release_dimension_with_token("g1", token, assertion).status == "blocked"
    )


def test_dimension_receipt_survives_unrelated_review_but_not_target_mutation(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "dimension-receipt.db")
    )
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    target_id, token = next(iter(_dimension_action_tokens(service).items()))
    first = service.release_dimension_with_token("g1", token, assertion)
    assert first.status == "confirmed" and first.mutation_applied

    metric_token = service.issue_metric_action_token("g1", "metric:measurements.value")
    assert (
        service.map_metric_phrase(
            "g1", metric_token, "business value", assertion
        ).status
        == "confirmed"
    )
    retry = service.release_dimension_with_token("g1", token, assertion)
    assert retry.status == "confirmed"
    assert retry.mutation_applied is False

    assert service.revoke_dimension("g1", target_id, assertion).status == "confirmed"
    assert (
        service.release_dimension_with_token("g1", token, assertion).status == "blocked"
    )


def test_public_scope_epoch_invalidates_old_token_and_fresh_token_retiers(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "public-retier.db")
    )
    assertion = StewardAssertion(scope="g1", reviewer_id="admin", authorized=True)
    public_assertion = StewardAssertion(
        scope="g1",
        reviewer_id="admin",
        authorized=True,
        public_data_confirmed=True,
    )
    target_id, controlled_token = next(iter(_dimension_action_tokens(service).items()))
    assert (
        service.release_dimension_with_token("g1", controlled_token, assertion).status
        == "confirmed"
    )
    assert (
        service.confirm_public_data_scope("g1", public_assertion).status == "confirmed"
    )

    catalog, _candidates = service.dimension_candidate_snapshot(
        "g1", include_released=True
    )
    assert catalog is not None
    stale_retier_token = service.issue_dimension_action_token(
        "g1",
        target_id,
        "dimension_set_tier",
        expected_catalog=catalog,
    )
    assert service.revoke_public_data_scope("g1", assertion).status == "confirmed"
    assert (
        service.release_dimension_with_token(
            "g1",
            stale_retier_token,
            public_assertion,
            disclosure_tier="public_grouped",
        ).status
        == "blocked"
    )

    assert (
        service.confirm_public_data_scope("g1", public_assertion).status == "confirmed"
    )
    catalog, _candidates = service.dimension_candidate_snapshot(
        "g1", include_released=True
    )
    assert catalog is not None
    fresh_retier_token = service.issue_dimension_action_token(
        "g1",
        target_id,
        "dimension_set_tier",
        expected_catalog=catalog,
    )
    retiered = service.release_dimension_with_token(
        "g1",
        fresh_retier_token,
        public_assertion,
        disclosure_tier="public_grouped",
    )
    assert retiered.status == "confirmed" and retiered.mutation_applied
    assert (
        service.load("g1").dimension(target_id).disclosure_tier.value
        == "public_grouped"
    )


def test_sensitive_identifier_variants_stay_out_of_catalog_and_tool(tmp_path):
    db = tmp_path / "sensitive-identifiers.db"
    engine = create_engine(f"sqlite:///{db}")
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE users ("
                '"AdmEmail1" TEXT, "AdmFName1" TEXT, "Street" TEXT, '
                '"MailStreet" TEXT, "flavorText" TEXT, "DisplayName" TEXT, '
                '"AboutMe" TEXT, "ProfileImageUrl" TEXT, "WebsiteUrl" TEXT, '
                '"customerBirthDate" TEXT, "apiTokenV2" TEXT, '
                '"passwordHash" TEXT, "ClientSecret" TEXT, '
                '"UserDisplayName" TEXT, "UUID" TEXT, "resourceUrl" TEXT, '
                '"dmode_ttl" TEXT)'
            )
        )
        connection.execute(text('CREATE TABLE posts ("Body" TEXT, "Title" TEXT)'))

    explorer = SqlAlchemyExplorer(f"sqlite:///{db}")
    store = SqliteStore()
    service = SemanticService(store)
    summary = asyncio.run(service.onboard("g1", explorer))
    expected = {
        "users.AdmEmail1",
        "users.AdmFName1",
        "users.Street",
        "users.MailStreet",
        "users.flavorText",
        "users.DisplayName",
        "users.AboutMe",
        "users.ProfileImageUrl",
        "users.WebsiteUrl",
        "users.customerBirthDate",
        "users.apiTokenV2",
        "users.passwordHash",
        "users.ClientSecret",
        "users.UserDisplayName",
        "users.UUID",
        "users.resourceUrl",
        "posts.Body",
        "posts.Title",
    }
    assert expected.issubset(set(summary.catalog.blocked_columns))
    assert all(
        f"{dimension.table_id}.{dimension.column}" not in expected
        for dimension in summary.catalog.dimensions
    )

    tool = SemanticQuery(
        service,
        summary.catalog,
        build_attention_envelope(summary.catalog, "sum value by drive mode"),
    )
    spec_text = json.dumps(tool.spec.parameters, ensure_ascii=False)
    for reference in expected:
        assert reference not in spec_text

    candidate = summary.catalog.dimension("dimension:users.dmode_ttl")
    assert candidate is not None
    assert candidate.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
    assert candidate.raw_output_allowed is False
    assert candidate.aliases == []
    assert candidate.id not in _dimension_enum(tool)


def test_onboarding_never_samples_or_executes_raw_values(tmp_path):
    explorer = _seed_candidate_db(str(tmp_path / "metadata-only.db"))

    class SpyExplorer:
        def __init__(self, wrapped):
            self.wrapped = wrapped
            self.sample_calls = 0
            self.execute_calls = 0

        async def list_tables(self):
            return await self.wrapped.list_tables()

        async def describe_table(self, name):
            table = await self.wrapped.describe_table(name)
            # TEST_ONLY_SYNTHETIC_METADATA: exercises the real DB-comment path
            # without treating a fabricated description as product truth.
            columns = [
                (
                    replace(column, description="Gross billed amount.")
                    if column.name == "value"
                    else column
                )
                for column in table.columns
            ]
            return replace(table, columns=columns)

        async def catalog_metadata(self):
            return await self.wrapped.catalog_metadata()

        async def sample_rows(self, name, limit=5):
            self.sample_calls += 1
            raise AssertionError("onboarding must not sample rows")

        async def execute(self, sql, limit=1000):
            self.execute_calls += 1
            raise AssertionError("onboarding must not execute SQL")

        def quote_identifier(self, name):
            return self.wrapped.quote_identifier(name)

    spy = SpyExplorer(explorer)
    store = SqliteStore()
    store.kv_set(
        "g1",
        "enriched_desc:measurements:value",
        "Net sales amount.",
    )
    service = SemanticService(store)
    first = asyncio.run(service.onboard("g1", spy))
    first_metric = first.catalog.metric("metric:measurements.value")
    assert first_metric is not None
    assert "net sales amount" not in first_metric.suggested_aliases
    summary = asyncio.run(
        service.inspect(
            "g1",
            spy,
            carry_source_id=first.catalog.source_id,
        )
    )
    assert summary.table_count == 1
    assert spy.sample_calls == 0
    assert spy.execute_calls == 0
    metric = summary.catalog.metric("metric:measurements.value")
    assert metric is not None
    assert {"gross billed amount", "net sales amount"}.issubset(
        metric.suggested_aliases
    )
    assert metric.suggestion_sources == {
        "gross billed amount": "real_db_comment",
        "net sales amount": "existing_enrich_cache",
    }
    store.kv_delete("g1", "enriched_desc:measurements:value")
    refreshed = asyncio.run(
        service.inspect(
            "g1",
            spy,
            carry_source_id=first.catalog.source_id,
        )
    )
    refreshed_metric = refreshed.catalog.metric("metric:measurements.value")
    assert refreshed_metric is not None
    assert "gross billed amount" in refreshed_metric.suggested_aliases
    assert "net sales amount" not in refreshed_metric.suggested_aliases


def test_admin_release_and_requester_mapping_are_distinct_discord_gates(tmp_path):
    db = tmp_path / "release-lifecycle.db"
    explorer = _seed_candidate_db(str(db))
    concierge = ContextConcierge(explorer=explorer, llm=FakeLLM())
    asyncio.run(concierge.semantic.onboard("g1", explorer))
    handlers = CommandHandlers(concierge)
    user = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    admin = Identity(user_id="admin", guild_id="g1", channel_id="c1", is_admin=True)
    dimension_id = "dimension:measurements.dmode_ttl"

    before_context = asyncio.run(concierge.build_context(user))
    before_tool = next(
        tool for tool in before_context.tools.specs() if tool.name == "semantic_query"
    )
    before_enum = before_tool.parameters["properties"]["dimensions"]["items"][
        "properties"
    ]["dimension_id"]["enum"]
    assert dimension_id not in before_enum

    shown = asyncio.run(
        handlers.semantic_candidates(admin, search="dmode_ttl", state="pending")
    )
    token_match = re.search(r"candidate_token: ([A-Za-z0-9_-]+)", shown.text)
    assert token_match is not None
    candidate_token = token_match.group(1)

    denied = asyncio.run(handlers.semantic_release(user, candidate_token, confirm=True))
    assert "관리자만" in denied.text
    warning = asyncio.run(
        handlers.semantic_release(admin, candidate_token, confirm=False)
    )
    assert "Discord 결과에 표시" in warning.text
    assert (
        concierge.semantic.load("g1").dimension(dimension_id).raw_output_allowed
        is False
    )

    tier_swap = asyncio.run(
        handlers.semantic_release(
            admin,
            candidate_token,
            disclosure_tier="public_grouped",
            confirm=True,
        )
    )
    assert tier_swap.text.startswith("BLOCKED:")
    assert "최종 요청이 다릅니다" in tier_swap.text

    stale = concierge.semantic.prepare_query(
        scope="g1",
        review_scope="review:stale",
        requester_id="u1",
        explorer=explorer,
        question="What is total value?",
        metric_id="metric:measurements.value",
        metric_phrase="value",
        aggregate="sum",
        dimension_bindings=[],
        unresolved_obligations=[],
        limit=100,
    )
    assert stale.status == "clarification"

    released = asyncio.run(
        handlers.semantic_release(admin, candidate_token, confirm=True)
    )
    assert released.text.startswith("✅")
    assert "질문 표현 연결" in released.text
    assert (
        concierge.semantic.confirm_pending(
            "g1", "review:stale", "sum", reviewer_id="u1"
        ).status
        == "blocked"
    )

    after_context = asyncio.run(
        concierge.build_context(user, user_text="What is total value by drive mode?")
    )
    after_tool = next(
        tool for tool in after_context.tools.specs() if tool.name == "semantic_query"
    )
    after_enum = after_tool.parameters["properties"]["dimensions"]["items"][
        "properties"
    ]["dimension_id"]["enum"]
    assert dimension_id in after_enum

    args = {
        "scope": "g1",
        "review_scope": "review:u1",
        "requester_id": "u1",
        "explorer": explorer,
        "question": "What is total value by drive mode?",
        "metric_id": "metric:measurements.value",
        "metric_phrase": "value",
        "aggregate": "sum",
        "dimension_bindings": [{"dimension_id": dimension_id, "phrase": "drive mode"}],
        "unresolved_obligations": [],
        "limit": 100,
    }
    first = concierge.semantic.prepare_query(**args)
    assert first.status == "clarification"
    assert first.sql == ""
    confirmed = concierge.semantic.confirm_pending(
        "g1", "review:u1", "sum", reviewer_id="u1"
    )
    assert confirmed.status == "confirmed"
    dimension_review = concierge.semantic.prepare_query(**args)
    assert dimension_review.status == "clarification"
    assert (
        concierge.semantic.confirm_pending(
            "g1", "review:u1", "confirm", reviewer_id="u1"
        ).status
        == "confirmed"
    )
    ready = concierge.semantic.prepare_query(**args)
    assert ready.status == "ready"
    assert "__semantic_group_size" in ready.sql
    assert "__semantic_category_count" in ready.sql

    session = Session(identity=user)
    session.add(Message(role=Role.USER, content=args["question"]))
    catalog = concierge.semantic.load("g1")
    tool = SemanticQuery(
        concierge.semantic,
        catalog,
        build_attention_envelope(catalog, args["question"]),
    )
    ctx = HarnessContext(
        identity=user,
        llm=FakeLLM(),
        tools=ToolRegistry([tool]),
        session=session,
        explorer=explorer,
        safety=SafetyPipeline(),
        audit=concierge.store,
        store=concierge.store,
    )
    result = asyncio.run(
        tool.run(
            {
                "metric_id": args["metric_id"],
                "metric_phrase": args["metric_phrase"],
                "aggregate": args["aggregate"],
                "dimensions": args["dimension_bindings"],
                "unresolved_obligations": [],
                "limit": 100,
            },
            ctx,
        )
    )
    assert result.is_error is False
    assert result.content.startswith("READY:")
    assert "rail" not in result.content and "road" not in result.content
    assert ctx.semantic_result_headers == (
        "measurements.dmode_ttl",
        "metric_value",
    )
    assert ctx.semantic_result_rows == [("rail", 15.0), ("road", 15.0)]
    assert "__semantic_" not in result.content

    class ResetDuringSuccessAudit:
        async def record(self, event):
            await concierge.store.record(event)
            if event.action == "semantic_query":
                reset = concierge.semantic.reset_reviews("g1")
                assert reset.status == "confirmed"

        async def query(self, actor: str, limit: int = 20):
            return await concierge.store.query(actor, limit)

    second_session = Session(identity=user)
    second_session.add(Message(role=Role.USER, content=args["question"]))
    second_ctx = HarnessContext(
        identity=user,
        llm=FakeLLM(),
        tools=ToolRegistry([tool]),
        session=second_session,
        explorer=explorer,
        safety=SafetyPipeline(),
        audit=ResetDuringSuccessAudit(),
        store=concierge.store,
    )
    stale_result = asyncio.run(
        tool.run(
            {
                "metric_id": args["metric_id"],
                "metric_phrase": args["metric_phrase"],
                "aggregate": args["aggregate"],
                "dimensions": args["dimension_bindings"],
                "unresolved_obligations": [],
                "limit": 100,
            },
            second_ctx,
        )
    )
    assert stale_result.is_error is True
    assert "semantic_catalog_changed_before_publish" in stale_result.content
    assert second_ctx.semantic_result_ready is False
    assert second_ctx.semantic_result_rows == []


def test_unreleased_candidate_cannot_bypass_service_or_compiler(tmp_path):
    explorer, _store, service, summary = _onboard_candidate(
        str(tmp_path / "unreleased.db")
    )
    dimension_id = "dimension:measurements.dmode_ttl"
    blocked = service.prepare_query(
        scope="g1",
        review_scope="review:u1",
        requester_id="u1",
        explorer=explorer,
        question="What is total value by dmode ttl?",
        metric_id="metric:measurements.value",
        metric_phrase="value",
        aggregate="sum",
        dimension_bindings=[{"dimension_id": dimension_id, "phrase": "dmode ttl"}],
        unresolved_obligations=[],
        limit=100,
    )
    assert blocked.status == "blocked"
    assert blocked.blocker == "dimension_release_required"
    assert blocked.sql == ""

    with pytest.raises(ValueError, match="released dimensions required"):
        _compile_sql(
            catalog=summary.catalog,
            explorer=explorer,
            metric_id="metric:measurements.value",
            aggregate=summary.catalog.metric(
                "metric:measurements.value"
            ).allowed_aggregates[0],
            dimension_ids=[dimension_id],
            paths=[[]],
            limit=100,
        )


def test_released_output_guards_fail_closed_without_returning_labels(tmp_path):
    _explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "output-guards.db")
    )
    dimension_id = "dimension:measurements.dmode_ttl"
    assert (
        service.release_dimension(
            "g1",
            dimension_id,
            StewardAssertion(scope="g1", reviewer_id="admin", authorized=True),
        ).status
        == "confirmed"
    )
    catalog = service.load("g1")

    valid, blocker = enforce_released_dimension_output(
        catalog,
        [dimension_id],
        [
            {
                "__l2s_dimension_0": "rail",
                "__l2s_metric": 15,
                "__semantic_group_size": 5,
                "__semantic_category_count": 2,
            }
        ],
    )
    assert blocker == ""
    assert valid == [{"__l2s_dimension_0": "rail", "__l2s_metric": 15}]

    unsafe_rows = [
        (
            {
                "__l2s_dimension_0": "rare-label",
                "__l2s_metric": 1,
                "__semantic_group_size": 4,
                "__semantic_category_count": 2,
            },
            "released_dimension_group_too_small",
        ),
        (
            {
                "__l2s_dimension_0": "many-labels",
                "__l2s_metric": 1,
                "__semantic_group_size": 5,
                "__semantic_category_count": 51,
            },
            "released_dimension_cardinality_too_high",
        ),
        (
            {
                "__l2s_dimension_0": "x" * 129,
                "__l2s_metric": 1,
                "__semantic_group_size": 5,
                "__semantic_category_count": 2,
            },
            "released_dimension_label_too_long",
        ),
        (
            {"__l2s_dimension_0": "unguarded", "__l2s_metric": 1},
            "released_dimension_guard_missing",
        ),
    ]
    for row, expected in unsafe_rows:
        cleaned, blocker = enforce_released_dimension_output(
            catalog, [dimension_id], [row]
        )
        assert cleaned == []
        assert blocker == expected

    long_metric, blocker = enforce_released_dimension_output(
        catalog,
        [dimension_id],
        [
            {
                "__l2s_dimension_0": "safe",
                "__l2s_metric": "9" * 129,
                "__semantic_group_size": 5,
                "__semantic_category_count": 1,
            }
        ],
    )
    assert blocker == ""
    assert long_metric[0]["__l2s_metric"] == "9" * 129

    forged, blocker = enforce_released_dimension_output(
        catalog,
        [dimension_id],
        [
            {
                "__l2s_dimension_0": "safe",
                "__l2s_metric": 1,
                "forged": "ignored-by-disclosure-but-rejected-by-decoder",
                "__semantic_group_size": 5,
                "__semantic_category_count": 1,
            }
        ],
    )
    assert blocker == ""
    decoded, layout_blocker = decode_semantic_query_rows(
        catalog, [dimension_id], forged
    )
    assert decoded == []
    assert layout_blocker == "semantic_output_layout_mismatch"


def test_v1_catalog_migrates_string_dimensions_fail_closed(tmp_path):
    _explorer, store, service, summary = _onboard_candidate(
        str(tmp_path / "legacy-v1.db")
    )
    payload = json.loads(summary.catalog.to_json())
    payload["version"] = 1
    payload.pop("classification_policy_version")
    candidate = next(
        item
        for item in payload["dimensions"]
        if item["id"] == "dimension:measurements.dmode_ttl"
    )
    candidate["aliases"] = ["dmode ttl", "drive mode"]
    candidate["auto_aliases"] = ["dmode ttl"]
    candidate["alias_reviewers"] = {"drive mode": "old-user"}
    for item in payload["dimensions"]:
        for field in (
            "review_policy",
            "classification_evidence",
            "classification_policy_version",
            "raw_output_allowed",
            "release_reviewer",
            "release_catalog_fingerprint",
            "released_at",
        ):
            item.pop(field, None)

    store.kv_set("g1", CATALOG_KEY, json.dumps(payload))
    migrated = service.load("g1")
    assert migrated is not None
    assert migrated.version == 3
    legacy_dimension = migrated.dimension("dimension:measurements.dmode_ttl")
    assert legacy_dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
    assert legacy_dimension.raw_output_allowed is False
    assert legacy_dimension.aliases == []
    assert legacy_dimension.auto_aliases == []
    assert legacy_dimension.alias_reviewers == {}


def test_release_carries_only_under_same_classifier_policy_and_reset_revokes_it(
    tmp_path,
):
    explorer, _store, service, _summary = _onboard_candidate(
        str(tmp_path / "carry-release.db")
    )
    dimension_id = "dimension:measurements.dmode_ttl"
    assert (
        service.release_dimension(
            "g1",
            dimension_id,
            StewardAssertion(scope="g1", reviewer_id="admin", authorized=True),
        ).status
        == "confirmed"
    )
    catalog = service.load("g1")
    dimension = catalog.dimension(dimension_id)
    dimension.aliases = ["drive mode"]
    dimension.alias_reviewers = {"drive mode": "u1"}
    service.save("g1", catalog, expected_review_revision=catalog.review_revision)

    carried = asyncio.run(
        service.inspect("g1", explorer, carry_source_id=service.load("g1").source_id)
    ).catalog.dimension(dimension_id)
    assert carried.raw_output_allowed is True
    assert carried.release_reviewer == "admin"
    assert carried.aliases == ["drive mode"]

    catalog = service.load("g1")
    source_id = catalog.source_id
    catalog.classification_policy_version = 1
    catalog.dimension(dimension_id).classification_policy_version = 1
    service.save("g1", catalog, expected_review_revision=catalog.review_revision)
    invalidated = asyncio.run(
        service.inspect("g1", explorer, carry_source_id=source_id)
    ).catalog.dimension(dimension_id)
    assert invalidated.raw_output_allowed is False
    assert invalidated.aliases == []

    asyncio.run(service.onboard("g1", explorer))
    assert (
        service.release_dimension(
            "g1",
            dimension_id,
            StewardAssertion(scope="g1", reviewer_id="admin", authorized=True),
        ).status
        == "confirmed"
    )
    assert service.reset_reviews("g1").status == "confirmed"
    reset = service.load("g1").dimension(dimension_id)
    assert reset.raw_output_allowed is False
    assert reset.release_reviewer == ""


def test_blocked_column_dimension_overlap_is_rejected():
    from lang2sql.semantic.catalog import DimensionSpec

    with pytest.raises(
        ValueError, match="blocked columns cannot also be semantic objects"
    ):
        SemanticCatalog(
            fingerprint="f",
            dimensions=[
                DimensionSpec(
                    id="dimension:users.email",
                    label="users.email",
                    table_id="users",
                    column="email",
                    data_type="TEXT",
                )
            ],
            blocked_columns=["users.email"],
        )
