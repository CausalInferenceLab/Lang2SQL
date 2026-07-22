"""SQL-free state-machine facade over the governed semantic runtime."""

from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timedelta, timezone
import hashlib
import hmac
import json
import secrets
import threading
import time
from typing import Any, cast

from ..adapters.db.factory import build_explorer, canonicalize_connection
from ..adapters.storage.sqlite_store import SqliteStore
from ..core.identity import Identity
from ..core.ports.explorer import close_explorer
from ..semantic.catalog import (
    DimensionDisclosureTier,
    DimensionReviewPolicy,
    DimensionSpec,
    SemanticCatalog,
)
from ..semantic.execution import execute_governed_semantic
from ..semantic.policy import (
    dimension_is_released,
    predicate_dimension_is_selectable,
    public_data_scope_confirmed,
)
from ..semantic.service import (
    QueryOutcome,
    StewardAssertion,
    _normalize_phrase,
    _parse_filter_bindings,
    _parse_time_window_binding,
    _phrase_in_question,
    review_scope_key,
)
from ..semantic.shortlist import (
    SemanticAttentionEnvelope,
    build_attention_envelope,
    dimension_candidate_phrases,
    grounded_candidate_phrase,
    metric_candidate_phrases,
    question_sha256,
    safe_candidate_label,
)
from ..semantic.type_compatibility import (
    allowed_filter_literal_kinds,
    filter_compatibility_error,
    time_window_compatibility_error,
)
from ..tenancy.concierge import ContextConcierge
from ..tenancy.encrypted_secrets import EncryptedSecrets
from .models import (
    AggregateKind,
    Blocked,
    CallContext,
    CandidateRequest,
    CandidateSet,
    Capability,
    Clarification,
    ConnectRequest,
    Connected,
    DimensionCandidate,
    ExecuteRequest,
    ExecutionReady,
    FeedbackApplied,
    FeedbackRequest,
    FilterCandidate,
    MetricCandidate,
    PlanReady,
    PlanRequest,
    PlanResult,
    PreparedPlan,
    ReviewRequest,
    ReviewCandidate,
    ReviewAction,
    ReviewRequired,
    ScanSummary,
    SourceRef,
    TimeCandidate,
    ValueKind,
)

_PLAN_TTL_SECONDS = 5 * 60
_REVIEW_TTL_SECONDS = 15 * 60
_MAX_CONNECT_REVIEWS = 20
_MAX_CANDIDATE_BYTES = 12_288
_MAX_ACTIVE_PLANS = 256


@dataclass(frozen=True)
class _PlanRecord:
    scope: str
    actor_id: str
    conversation_id: str
    source: SourceRef
    outcome: QueryOutcome = field(repr=False)
    expires_monotonic: float


@dataclass(frozen=True)
class _GovernanceRecord:
    scope: str
    request: ReviewRequest
    fingerprint: str
    catalog_version: int
    classification_policy_version: int
    object_revision: int
    expires_monotonic: float


class Lang2SQLRuntime:
    """Small async API: connect, human feedback, plan, then execute.

    The facade never accepts SQL and never returns SQL. Natural-language model
    orchestration remains an adapter responsibility; callers submit a typed
    ``QueryDraft`` assembled by their model or deterministic UI.
    """

    def __init__(self, concierge: ContextConcierge) -> None:
        self._concierge = concierge
        self._plans: dict[str, _PlanRecord] = {}
        self._plan_timers: dict[str, threading.Timer] = {}
        self._governance: dict[str, _GovernanceRecord] = {}
        self._candidate_signing_key = secrets.token_bytes(32)
        self._lock = threading.RLock()
        self._closed = False

    @classmethod
    def local(
        cls,
        *,
        path: str = ":memory:",
        secret_key: bytes | None = None,
    ) -> "Lang2SQLRuntime":
        store = SqliteStore(path)
        secrets_store = EncryptedSecrets(store, key=secret_key)
        return cls(ContextConcierge(store=store, secrets=secrets_store))

    async def connect(self, request: ConnectRequest) -> Connected | Blocked:
        denied = self._require(request.context, Capability.CONNECT)
        if denied is not None:
            return denied
        explorer = None
        try:
            dsn = canonicalize_connection(request.connection.dsn)
            extras = dict(request.connection.extras)
            explorer = build_explorer(dsn, extras=extras or None)
            expected_generation = self._concierge.connection_generation(
                request.context.scope
            )
            if expected_generation < 0:
                return Blocked(
                    "connection_state_invalid",
                    "기존 연결 세대 정보가 유효하지 않습니다.",
                )
            current = self._concierge.connection_binding(request.context.scope)
            candidate_source_id = self._concierge.source_identity(
                request.context.scope, dsn, extras
            )
            carry_source_id = (
                current.source_id
                if current is not None and current.source_id == candidate_source_id
                else ""
            )
            # Metadata only: inspect never samples raw values or executes a
            # user query, and activation happens only after the scan succeeds.
            summary = await self._concierge.semantic.inspect(
                request.context.scope,
                explorer,
                carry_source_id=carry_source_id,
            )
            binding = self._concierge.activate_connection(
                scope=request.context.scope,
                dsn=dsn,
                extras=extras,
                catalog=summary.catalog,
                expected_generation=expected_generation,
            )
        except Exception:
            if explorer is not None:
                close_explorer(explorer)
            return Blocked(
                "connection_failed",
                "DB 메타데이터를 안전하게 확인하고 연결 상태를 활성화하지 못했습니다.",
                retryable=True,
            )

        source = SourceRef(binding.source_id, binding.generation)
        self._prune_governance(request.context.scope, source)
        try:
            execution_capability = getattr(
                explorer, "governed_execution_supported", None
            )
            execution_supported = bool(
                callable(execution_capability) and execution_capability()
            )
        finally:
            # Inspection uses a short-lived adapter. Query execution later
            # obtains the generation-bound cached adapter from the concierge.
            close_explorer(explorer)
        reviews, review_count = self._build_governance_reviews(
            request.context, summary.catalog, source
        )
        scan = ScanSummary(
            table_count=summary.table_count,
            declared_join_count=summary.declared_join_count,
            blocked_column_count=summary.blocked_column_count,
            pending_metric_count=summary.pending_metric_count,
            pending_disclosure_count=sum(
                item.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED
                for item in summary.catalog.dimensions
                if item.review_policy.value == "release_required"
            ),
            execution_supported=execution_supported,
        )
        return Connected(
            source=source,
            scan=scan,
            reviews=tuple(item.request for item in reviews),
            remaining_review_count=max(0, review_count - len(reviews)),
        )

    async def plan(self, request: PlanRequest) -> PlanResult:
        denied = self._require(request.context, Capability.QUERY)
        if denied is not None:
            return denied
        draft = request.draft
        binding = self._concierge.connection_binding(request.context.scope)
        if (
            binding is None
            or SourceRef(binding.source_id, binding.generation) != draft.source
        ):
            return Blocked(
                "candidate_source_stale",
                "후보를 만든 뒤 DB 연결이 바뀌었습니다. 후보를 다시 조회해 주세요.",
            )
        expected_candidate_token = self._candidate_token(
            request.context, draft.source, draft.question
        )
        if not hmac.compare_digest(expected_candidate_token, draft.candidate_token):
            return Blocked(
                "candidate_question_mismatch",
                "후보를 조회한 원 질문과 typed draft의 질문이 다릅니다. 후보를 다시 조회해 주세요.",
            )
        wire: dict[str, object] = {
            "metric_id": draft.metric_id,
            "metric_phrase": draft.metric_phrase,
            "aggregate": draft.aggregate.value,
            "dimensions": [
                {"dimension_id": item.dimension_id, "phrase": item.phrase}
                for item in draft.dimensions
            ],
            "filters": [
                {
                    "dimension_id": item.dimension_id,
                    "dimension_phrase": item.dimension_phrase,
                    "operator": item.operator.value,
                    "operator_phrase": item.operator_phrase,
                    "values": [
                        {
                            "kind": value.kind.value,
                            "value": value.value,
                            "phrase": value.phrase,
                        }
                        for value in item.values
                    ],
                }
                for item in draft.filters
            ],
            "time_window": (
                {
                    "dimension_id": draft.time_window.dimension_id,
                    "dimension_phrase": draft.time_window.dimension_phrase,
                    "range_phrase": draft.time_window.range_phrase,
                    "start": {
                        "kind": "date",
                        "value": draft.time_window.start.value,
                        "phrase": draft.time_window.start.phrase,
                    },
                    "end": {
                        "kind": "date",
                        "value": draft.time_window.end.value,
                        "phrase": draft.time_window.end.phrase,
                    },
                }
                if draft.time_window is not None
                else None
            ),
            "unresolved_obligations": list(draft.unresolved_obligations),
            "limit": draft.limit,
        }
        return await self._prepare_wire(
            request.context,
            draft.question,
            wire,
            expected_source=draft.source,
        )

    async def candidates(
        self, request: CandidateRequest
    ) -> CandidateSet | Clarification | Blocked:
        """Return bounded metadata for a host parser without touching the data DB."""

        denied = self._require(request.context, Capability.QUERY)
        if denied is not None:
            return denied
        catalog = self._concierge.semantic.load(request.context.scope)
        binding = self._concierge.connection_binding(request.context.scope)
        if (
            catalog is None
            or binding is None
            or catalog.source_id != binding.source_id
            or catalog.connection_generation != binding.generation
        ):
            return Blocked("semantic_catalog_missing", "먼저 DB를 연결해 주세요.")
        attention = build_attention_envelope(catalog, request.question)
        if not attention.ready and attention.state != "dimension_release_required":
            return Clarification(
                attention.state, self._public_attention_message(attention)
            )
        candidate_set = self._build_candidate_set(
            request.context,
            catalog,
            SourceRef(binding.source_id, binding.generation),
            request.question,
            attention,
        )
        encoded = json.dumps(
            asdict(candidate_set),
            ensure_ascii=False,
            sort_keys=True,
            default=str,
        ).encode("utf-8")
        if len(encoded) > _MAX_CANDIDATE_BYTES:
            return Blocked(
                "candidate_set_too_large",
                "질문 후보 메타데이터가 공개 API의 크기 제한을 넘었습니다.",
            )
        return candidate_set

    async def feedback(self, request: FeedbackRequest) -> FeedbackApplied | Blocked:
        denied = self._require(request.context, Capability.QUERY)
        if denied is not None:
            return denied
        # Semantic mutations and their audit event share the SqliteStore
        # transaction. An injected external audit port cannot preserve that
        # contract yet, so never silently route governance around it.
        if self._concierge.audit is not self._concierge.store:
            return Blocked(
                "semantic_audit_not_atomic",
                "의미 검토 변경에는 기본 원자적 audit 저장소가 필요합니다.",
            )

        with self._lock:
            governance = self._governance.get(request.review_id)
            if governance is not None:
                if governance.expires_monotonic < time.monotonic():
                    self._governance.pop(request.review_id, None)
                    return Blocked(
                        "review_expired",
                        "검토 요청의 유효 시간이 지나 다시 요청해야 합니다.",
                    )
                try:
                    result = self._apply_governance_feedback(request, governance)
                except Exception:
                    return Blocked(
                        "review_not_applied",
                        "검토 상태와 감사 기록을 원자적으로 저장하지 못했습니다. 다시 시도해 주세요.",
                        retryable=True,
                    )
                # Invalid, unauthorized, and stale attempts must not let a
                # caller destroy another steward's one-shot review capability.
                if isinstance(result, FeedbackApplied):
                    current = self._governance.get(request.review_id)
                    if current is governance:
                        self._governance.pop(request.review_id, None)
                return result

        located = self._concierge.semantic.pending_review_by_id(
            request.context.scope, request.review_id
        )
        if located is None:
            return Blocked(
                "review_not_found", "현재 연결에서 해당 검토 요청을 찾지 못했습니다."
            )
        review_scope, pending = located
        cross_requester = bool(
            pending.requester_id and pending.requester_id != request.context.actor_id
        )
        if (
            cross_requester
            and Capability.REVIEW_ANY not in request.context.capabilities
        ):
            return Blocked(
                "review_forbidden", "다른 사용자의 검토에는 steward 권한이 필요합니다."
            )
        try:
            if cross_requester:
                outcome = self._concierge.semantic.confirm_pending_by_id(
                    request.context.scope,
                    request.review_id,
                    request.choice,
                    reviewer_id=request.context.actor_id,
                    authorized=True,
                    audit_scope=request.context.conversation_id,
                )
            else:
                outcome = self._concierge.semantic.confirm_pending(
                    request.context.scope,
                    review_scope,
                    request.choice,
                    reviewer_id=request.context.actor_id,
                    expected_review_id=request.review_id,
                    audit_scope=request.context.conversation_id,
                )
        except Exception:
            return Blocked(
                "review_not_applied",
                "의미 검토와 감사 기록을 원자적으로 저장하지 못했습니다. 다시 시도해 주세요.",
                retryable=True,
            )
        if outcome.status != "confirmed":
            return Blocked("review_rejected", outcome.message)
        next_result: PlanResult | None = None
        if (
            not cross_requester
            and request.choice.strip().lower() != "reject"
            and outcome.question
            and outcome.tool_args
        ):
            expected_source = (
                SourceRef(outcome.source_id, outcome.connection_generation)
                if outcome.source_id and outcome.connection_generation > 0
                else None
            )
            next_result = await self._prepare_wire(
                request.context,
                outcome.question,
                outcome.tool_args,
                expected_source=expected_source,
            )
        return FeedbackApplied(outcome.mutation_applied, outcome.message, next_result)

    async def execute(self, request: ExecuteRequest) -> ExecutionReady | Blocked:
        denied = self._require(request.context, Capability.QUERY)
        if denied is not None:
            return denied
        with self._lock:
            record = self._plans.get(request.plan.plan_id)
        if record is None:
            return Blocked("plan_unavailable", "계획이 없거나 이미 사용되었습니다.")
        if record.expires_monotonic < time.monotonic():
            self._discard_plan(request.plan.plan_id, record)
            return Blocked(
                "plan_expired", "계획 유효 시간이 지나 다시 계획해야 합니다."
            )
        if (
            record.scope != request.context.scope
            or record.actor_id != request.context.actor_id
            or record.conversation_id != request.context.conversation_id
            or record.source != request.plan.source
        ):
            return Blocked(
                "plan_context_mismatch", "계획이 현재 사용자·대화·DB에 속하지 않습니다."
            )
        consumed = self._discard_plan(request.plan.plan_id, record)
        if consumed is not record:
            return Blocked("plan_unavailable", "계획이 없거나 이미 사용되었습니다.")
        identity = self._identity(request.context)
        harness = await self._concierge.build_context(identity)
        if harness.explorer is None or harness.safety is None:
            return Blocked(
                "execution_context_missing", "안전한 실행 context를 만들지 못했습니다."
            )
        execution = await execute_governed_semantic(
            service=self._concierge.semantic,
            scope=request.context.scope,
            explorer=harness.explorer,
            safety=harness.safety,
            outcome=record.outcome,
            actor=request.context.actor_id,
            audit_scope=request.context.conversation_id,
            audit=self._concierge.audit,
            row_limit=record.outcome.plan.limit if record.outcome.plan else 100,
        )
        if not execution.ready:
            return Blocked(execution.code, execution.message)
        return ExecutionReady(
            plan_id=request.plan.plan_id,
            source=record.source,
            columns=execution.headers,
            rows=execution.rows,
        )

    def close(self) -> None:
        timers: tuple[threading.Timer, ...]
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._plans.clear()
            timers = tuple(self._plan_timers.values())
            self._plan_timers.clear()
            self._governance.clear()
        for timer in timers:
            timer.cancel()
        try:
            self._concierge.close()
        finally:
            self._concierge.store.close()

    def _remember_plan(self, plan_id: str, record: _PlanRecord) -> None:
        now = time.monotonic()
        with self._lock:
            remove_ids = {
                key
                for key, item in self._plans.items()
                if item.expires_monotonic <= now
            }
            while len(self._plans) - len(remove_ids) >= _MAX_ACTIVE_PLANS:
                remove_ids.add(
                    min(
                        (
                            (key, item)
                            for key, item in self._plans.items()
                            if key not in remove_ids
                        ),
                        key=lambda pair: pair[1].expires_monotonic,
                    )[0]
                )
            for stale_id in remove_ids:
                self._plans.pop(stale_id, None)
                timer = self._plan_timers.pop(stale_id, None)
                if timer is not None:
                    timer.cancel()
            self._plans[plan_id] = record
            delay = max(0.0, record.expires_monotonic - now)
            timer = threading.Timer(
                delay,
                self._expire_plan,
                args=(plan_id, record.expires_monotonic),
            )
            timer.daemon = True
            self._plan_timers[plan_id] = timer
            timer.start()

    def _expire_plan(self, plan_id: str, expires: float) -> None:
        with self._lock:
            record = self._plans.get(plan_id)
            if (
                record is not None
                and record.expires_monotonic <= expires
                and record.expires_monotonic <= time.monotonic()
            ):
                self._plans.pop(plan_id, None)
            self._plan_timers.pop(plan_id, None)

    def _discard_plan(self, plan_id: str, expected: _PlanRecord) -> _PlanRecord | None:
        with self._lock:
            current = self._plans.get(plan_id)
            if current is not expected:
                return None
            self._plans.pop(plan_id, None)
            timer = self._plan_timers.pop(plan_id, None)
        if timer is not None:
            timer.cancel()
        return current

    async def _prepare_wire(
        self,
        context: CallContext,
        question: str,
        wire: dict[str, object],
        *,
        expected_source: SourceRef | None = None,
    ) -> PlanResult:
        identity = self._identity(context)
        harness = await self._concierge.build_context(identity, user_text=question)
        catalog = self._concierge.semantic.load(context.scope)
        if catalog is None or harness.explorer is None:
            return Blocked("semantic_catalog_missing", "먼저 DB를 연결해 주세요.")
        if expected_source is not None and (
            catalog.source_id != expected_source.source_id
            or catalog.connection_generation != expected_source.generation
        ):
            return Blocked(
                "candidate_source_stale",
                "후보를 만든 뒤 DB 연결이 바뀌었습니다. 후보를 다시 조회해 주세요.",
            )
        raw_dimensions = wire.get("dimensions", [])
        raw_filters = wire.get("filters", [])
        raw_time_window = wire.get("time_window")
        raw_obligations = wire.get("unresolved_obligations", [])
        raw_limit = wire.get("limit", 100)
        if (
            not isinstance(raw_dimensions, list)
            or not all(isinstance(item, dict) for item in raw_dimensions)
            or not isinstance(raw_filters, list)
            or not all(isinstance(item, dict) for item in raw_filters)
            or (raw_time_window is not None and not isinstance(raw_time_window, dict))
            or not isinstance(raw_obligations, list)
            or not all(isinstance(item, str) for item in raw_obligations)
            or isinstance(raw_limit, bool)
            or not isinstance(raw_limit, int)
            or not 1 <= raw_limit <= 1000
        ):
            return Blocked(
                "query_draft_invalid",
                "질의 계획 입력이 공개 typed 계약과 일치하지 않습니다.",
            )
        dimensions: list[dict[str, str]] = [
            {
                "dimension_id": str(item.get("dimension_id", "")),
                "phrase": str(item.get("phrase", "")),
            }
            for item in raw_dimensions
        ]
        filters: list[dict[str, object]] = [
            dict(cast(dict[str, object], item)) for item in raw_filters
        ]
        time_window: dict[str, object] | None = (
            dict(cast(dict[str, object], raw_time_window))
            if raw_time_window is not None
            else None
        )
        obligations = list(raw_obligations)
        attention = build_attention_envelope(catalog, question)
        if not attention.ready:
            on_demand = self._on_demand_governance_review(
                context=context,
                catalog=catalog,
                source=SourceRef(catalog.source_id, catalog.connection_generation),
                question=question,
                wire=wire,
                dimensions=dimensions,
                filters=filters,
                time_window=time_window,
                obligations=obligations,
                attention=attention,
            )
            if on_demand is not None:
                return on_demand
            return Clarification(
                attention.state, self._public_attention_message(attention)
            )
        if str(wire.get("metric_id", "")) not in attention.metric_ids:
            return Blocked(
                "candidate_not_shortlisted", "지표가 현재 질문 후보에 없습니다."
            )
        if any(
            not isinstance(item, dict)
            or str(item.get("dimension_id", "")) not in attention.dimension_ids
            for item in dimensions
        ):
            return Blocked(
                "candidate_not_shortlisted", "그룹 기준이 현재 질문 후보에 없습니다."
            )
        if any(
            not isinstance(item, dict)
            or str(item.get("dimension_id", "")) not in attention.filter_dimension_ids
            for item in filters
        ):
            return Blocked(
                "candidate_not_shortlisted", "필터 기준이 현재 질문 후보에 없습니다."
            )
        if time_window is not None and (
            not isinstance(time_window, dict)
            or str(time_window.get("dimension_id", ""))
            not in attention.time_dimension_ids
        ):
            return Blocked(
                "candidate_not_shortlisted", "기간 기준이 현재 질문 후보에 없습니다."
            )
        upgrade = self._predicate_upgrade_review(
            context=context,
            catalog=catalog,
            question=question,
            filters=filters,
            time_window=time_window,
        )
        if upgrade is not None:
            return upgrade
        outcome = self._concierge.semantic.prepare_query(
            scope=context.scope,
            review_scope=self._review_scope(context),
            requester_id=context.actor_id,
            explorer=harness.explorer,
            question=question,
            metric_id=str(wire.get("metric_id", "")),
            metric_phrase=str(wire.get("metric_phrase", "")),
            aggregate=str(wire.get("aggregate", "")),
            dimension_bindings=list(dimensions),
            filter_bindings=list(filters),
            time_window_binding=time_window,
            unresolved_obligations=obligations,
            limit=raw_limit,
            expected_source_id=(expected_source.source_id if expected_source else ""),
            expected_connection_generation=(
                expected_source.generation if expected_source else 0
            ),
        )
        return self._translate_outcome(context, outcome)

    def _build_candidate_set(
        self,
        context: CallContext,
        catalog: SemanticCatalog,
        source: SourceRef,
        question: str,
        attention: SemanticAttentionEnvelope,
    ) -> CandidateSet:
        metric_by_id = {item.id: item for item in catalog.metrics}
        dimension_by_id = {item.id: item for item in catalog.dimensions}

        metrics: list[MetricCandidate] = []
        for metric_id in attention.metric_ids:
            metric = metric_by_id.get(metric_id)
            if metric is None:
                continue
            metrics.append(
                MetricCandidate(
                    metric_id=metric.id,
                    label=safe_candidate_label(metric.label),
                    grounded_phrase=grounded_candidate_phrase(
                        question, metric_candidate_phrases(metric)
                    ),
                    allowed_aggregates=tuple(
                        AggregateKind(value.value)
                        for value in metric.allowed_aggregates
                    ),
                )
            )

        def metadata(dimension_id: str) -> tuple[str, str, str] | None:
            dimension = dimension_by_id.get(dimension_id)
            if dimension is None:
                return None
            return (
                dimension.id,
                safe_candidate_label(dimension.label),
                grounded_candidate_phrase(
                    question, dimension_candidate_phrases(dimension)
                ),
            )

        groupings: list[DimensionCandidate] = []
        for dimension_id in attention.dimension_ids:
            dimension = dimension_by_id.get(dimension_id)
            values = metadata(dimension_id)
            if (
                dimension is not None
                and values is not None
                and dimension_is_released(catalog, dimension)
            ):
                groupings.append(DimensionCandidate(*values))

        filters: list[FilterCandidate] = []
        times: list[TimeCandidate] = []
        predicate_scope_ready = public_data_scope_confirmed(catalog)
        if predicate_scope_ready:
            for dimension_id in attention.filter_dimension_ids:
                dimension = dimension_by_id.get(dimension_id)
                values = metadata(dimension_id)
                if (
                    dimension is None
                    or values is None
                    or not predicate_dimension_is_selectable(catalog, dimension)
                ):
                    continue
                literal_kinds = allowed_filter_literal_kinds(dimension)
                if literal_kinds:
                    filters.append(
                        FilterCandidate(
                            *values,
                            tuple(ValueKind(kind.value) for kind in literal_kinds),
                        )
                    )
            for dimension_id in attention.time_dimension_ids:
                dimension = dimension_by_id.get(dimension_id)
                values = metadata(dimension_id)
                if (
                    dimension is not None
                    and values is not None
                    and predicate_dimension_is_selectable(catalog, dimension)
                ):
                    times.append(TimeCandidate(*values))

        reviews: dict[str, ReviewCandidate] = {}
        for dimension_id in attention.release_required_dimension_ids:
            values = metadata(dimension_id)
            if values is not None:
                reviews[dimension_id] = ReviewCandidate(
                    *values, ReviewAction.DIMENSION_DISCLOSURE
                )
        predicate_ids = {
            *attention.filter_dimension_ids,
            *attention.time_dimension_ids,
        }
        for dimension_id in sorted(predicate_ids):
            dimension = dimension_by_id.get(dimension_id)
            values = metadata(dimension_id)
            if dimension is None or values is None:
                continue
            if (
                dimension_id in attention.filter_dimension_ids
                and not allowed_filter_literal_kinds(dimension)
                and dimension_id not in attention.time_dimension_ids
            ):
                continue
            action: ReviewAction | None = None
            if not predicate_scope_ready:
                action = ReviewAction.PUBLIC_DATA_SCOPE
            elif dimension.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED:
                action = ReviewAction.PUBLIC_GROUPED
            if action is not None:
                reviews[dimension_id] = ReviewCandidate(*values, action)

        return CandidateSet(
            source=source,
            question_sha256=question_sha256(question),
            candidate_token=self._candidate_token(context, source, question),
            metrics=tuple(metrics),
            grouping_dimensions=tuple(groupings),
            filter_dimensions=tuple(filters),
            time_dimensions=tuple(times),
            review_required_dimensions=tuple(
                reviews[dimension_id] for dimension_id in sorted(reviews)
            ),
            state=attention.state,
            message=Lang2SQLRuntime._public_attention_message(attention),
        )

    def _candidate_token(
        self, context: CallContext, source: SourceRef, question: str
    ) -> str:
        # Canonical JSON keeps field boundaries unambiguous even when a host
        # identity contains control characters or our old separator byte.
        payload = json.dumps(
            [
                context.scope,
                context.actor_id,
                context.conversation_id,
                source.source_id,
                str(source.generation),
                question_sha256(question),
            ],
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        digest = hmac.new(self._candidate_signing_key, payload, hashlib.sha256).digest()
        return base64.urlsafe_b64encode(digest).rstrip(b"=").decode("ascii")

    @staticmethod
    def _public_attention_message(attention: SemanticAttentionEnvelope) -> str:
        messages = {
            "ready": "",
            "question_required": "원 질문이 필요합니다.",
            "semantic_catalog_empty": "현재 연결에서 사용할 수 있는 의미 후보가 없습니다.",
            "clarify_table": "업무 대상 또는 테이블을 더 구체적으로 말해 주세요.",
            "clarify_metric": "계산할 지표나 물리 컬럼을 더 구체적으로 말해 주세요.",
            "clarify_dimension": "그룹·필터·기간 기준을 더 구체적으로 말해 주세요.",
            "dimension_release_required": (
                "질문과 일치하는 차원에 사람의 공개 범위 검토가 필요합니다."
            ),
        }
        return messages.get(
            attention.state,
            "현재 질문의 의미 후보를 안전하게 좁히지 못했습니다.",
        )

    def _predicate_upgrade_review(
        self,
        *,
        context: CallContext,
        catalog: SemanticCatalog,
        question: str,
        filters: list[dict[str, object]],
        time_window: dict[str, object] | None,
    ) -> ReviewRequired | None:
        parsed_filters, filter_error = _parse_filter_bindings(question, filters)
        parsed_time, time_error = _parse_time_window_binding(question, time_window)
        if filter_error is not None or time_error is not None:
            return None

        referenced = {item.dimension_id for item in parsed_filters} | (
            {parsed_time.dimension_id} if parsed_time is not None else set()
        )
        for predicate in parsed_filters:
            dimension = catalog.dimension(predicate.dimension_id)
            if dimension is None or filter_compatibility_error(dimension, predicate):
                return None
        if parsed_time is not None:
            dimension = catalog.dimension(parsed_time.dimension_id)
            if dimension is None or time_window_compatibility_error(
                dimension, parsed_time
            ):
                return None
        if not referenced:
            return None

        source = SourceRef(catalog.source_id, catalog.connection_generation)
        if not public_data_scope_confirmed(catalog):
            return ReviewRequired(
                self._public_scope_review(context.scope, catalog, source).request
            )
        controlled = [
            dimension
            for dimension_id in sorted(referenced)
            if (dimension := catalog.dimension(dimension_id)) is not None
            and dimension.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED
        ]
        if not controlled:
            return None
        # A draft may need several predicate-tier decisions.  Return only the
        # first stable item; resubmission after feedback exposes the next one
        # without storing the draft or any predicate literal in review state.
        dimension = controlled[0]
        record = self._register_governance(
            context.scope,
            ReviewRequest(
                review_id=secrets.token_urlsafe(18),
                kind="dimension_disclosure",
                object_id=dimension.id,
                phrase=dimension.label,
                allowed_choices=("public_grouped", "keep_controlled"),
                source=source,
            ),
            catalog,
            dimension.action_revision,
        )
        return ReviewRequired(record.request)

    def _translate_outcome(
        self, context: CallContext, outcome: QueryOutcome
    ) -> PlanResult:
        if outcome.status == "ready":
            source = SourceRef(outcome.source_id, outcome.connection_generation)
            plan_id = secrets.token_urlsafe(24)
            expires_at = datetime.now(timezone.utc) + timedelta(
                seconds=_PLAN_TTL_SECONDS
            )
            record = _PlanRecord(
                scope=context.scope,
                actor_id=context.actor_id,
                conversation_id=context.conversation_id,
                source=source,
                outcome=outcome,
                expires_monotonic=time.monotonic() + _PLAN_TTL_SECONDS,
            )
            self._remember_plan(plan_id, record)
            return PlanReady(PreparedPlan(plan_id, source, expires_at))
        if outcome.status == "clarification":
            pending = self._concierge.semantic.pending_review(
                self._review_scope(context)
            )
            if pending is not None and pending.review_id:
                object_id = pending.metric_id
                phrase = pending.metric_phrase
                if pending.review_kind == "dimension" and pending.dimension_bindings:
                    object_id = pending.dimension_bindings[0].get("dimension_id", "")
                    phrase = pending.dimension_bindings[0].get("phrase", "")
                review = ReviewRequest(
                    review_id=pending.review_id,
                    kind=pending.review_kind,
                    object_id=object_id,
                    phrase=phrase,
                    allowed_choices=tuple([*pending.allowed_choices, "reject"]),
                    source=SourceRef(pending.source_id, pending.connection_generation),
                )
                return ReviewRequired(review)
            return Clarification(outcome.blocker or "clarification", outcome.message)
        return Blocked(outcome.blocker or "blocked", outcome.message)

    def _on_demand_governance_review(
        self,
        *,
        context: CallContext,
        catalog: SemanticCatalog,
        source: SourceRef,
        question: str,
        wire: dict[str, object],
        dimensions: list[dict[str, str]],
        filters: list[dict[str, object]],
        time_window: dict[str, object] | None,
        obligations: list[str],
        attention: SemanticAttentionEnvelope,
    ) -> ReviewRequired | None:
        """Surface one relevant disclosure decision outside the top-20 list.

        This path never stores the query draft or its literal values. It only
        proves that releasing one exact dimension would make the server-owned
        shortlist ready, then returns the same revision-bound governance token
        that connect created (or one equivalent token if none exists).
        """

        if (
            attention.state != "dimension_release_required"
            or not attention.release_required_dimension_ids
            or obligations
        ):
            return None

        grouping_counts: dict[str, int] = {}
        for binding in dimensions:
            binding_id = str(binding.get("dimension_id", "")).strip()
            phrase = _normalize_phrase(str(binding.get("phrase", "")))
            if (
                not binding_id
                or not phrase
                or not _phrase_in_question(phrase, question)
            ):
                return None
            grouping_counts[binding_id] = grouping_counts.get(binding_id, 0) + 1
        if any(count > 1 for count in grouping_counts.values()):
            return None

        parsed_filters, filter_error = _parse_filter_bindings(question, filters)
        parsed_time, time_error = _parse_time_window_binding(question, time_window)
        if filter_error is not None or time_error is not None:
            return None
        filter_counts: dict[str, int] = {}
        for predicate in parsed_filters:
            filter_counts[predicate.dimension_id] = (
                filter_counts.get(predicate.dimension_id, 0) + 1
            )
            dimension = catalog.dimension(predicate.dimension_id)
            if dimension is None or filter_compatibility_error(dimension, predicate):
                return None
        if any(count > 1 for count in filter_counts.values()):
            return None
        if parsed_time is not None:
            time_dimension = catalog.dimension(parsed_time.dimension_id)
            if time_dimension is None or time_window_compatibility_error(
                time_dimension, parsed_time
            ):
                return None

        release_dimensions: list[tuple[DimensionSpec, bool]] = []
        for dimension_id in sorted(set(attention.release_required_dimension_ids)):
            dimension = catalog.dimension(dimension_id)
            if (
                dimension is None
                or dimension.review_policy != DimensionReviewPolicy.RELEASE_REQUIRED
                or dimension.raw_output_allowed
            ):
                return None
            matching_time = bool(
                parsed_time is not None and parsed_time.dimension_id == dimension_id
            )
            referenced = bool(
                grouping_counts.get(dimension_id)
                or filter_counts.get(dimension_id)
                or matching_time
            )
            if not referenced:
                return None
            release_dimensions.append(
                (dimension, bool(filter_counts.get(dimension_id) or matching_time))
            )

        if any(requires_public for _, requires_public in release_dimensions) and not (
            public_data_scope_confirmed(catalog)
        ):
            return ReviewRequired(
                self._public_scope_review(context.scope, catalog, source).request
            )

        # Re-run the whole typed draft against a temporary metadata-only view.
        # All exact missing dimensions are released only inside the probe so a
        # single real review can be issued safely and the rest can follow on
        # resubmission. No catalog mutation, SQL, or database read happens here.
        probe_catalog = deepcopy(catalog)
        for dimension, requires_public in release_dimensions:
            probe_dimension = probe_catalog.dimension(dimension.id)
            assert probe_dimension is not None
            probe_dimension.raw_output_allowed = True
            probe_dimension.disclosure_tier = (
                DimensionDisclosureTier.PUBLIC_GROUPED
                if requires_public
                else DimensionDisclosureTier.CONTROLLED_GROUPED
            )
            probe_dimension.release_reviewer = "shortlist-proof"
            probe_dimension.release_catalog_fingerprint = probe_catalog.fingerprint
        probe = build_attention_envelope(probe_catalog, question)
        if not probe.ready:
            return None
        if str(wire.get("metric_id", "")) not in probe.metric_ids:
            return None
        if any(
            str(item.get("dimension_id", "")) not in probe.dimension_ids
            for item in dimensions
        ):
            return None
        if any(
            str(item.get("dimension_id", "")) not in probe.filter_dimension_ids
            for item in filters
        ):
            return None
        if (
            time_window is not None
            and str(time_window.get("dimension_id", "")) not in probe.time_dimension_ids
        ):
            return None

        dimension, requires_public = release_dimensions[0]
        choices = (
            ("public_grouped", "keep_blocked")
            if requires_public
            else ("controlled_grouped", "public_grouped", "keep_blocked")
        )
        record = self._register_governance(
            context.scope,
            ReviewRequest(
                review_id=secrets.token_urlsafe(18),
                kind="dimension_disclosure",
                object_id=dimension.id,
                phrase=dimension.label,
                allowed_choices=choices,
                source=source,
            ),
            catalog,
            dimension.action_revision,
        )
        return ReviewRequired(record.request)

    def _build_governance_reviews(
        self, context: CallContext, catalog: SemanticCatalog, source: SourceRef
    ) -> tuple[list[_GovernanceRecord], int]:
        pending: list[tuple[ReviewRequest, int]] = []
        if not catalog.public_data_scope:
            pending.append(
                (
                    ReviewRequest(
                        review_id=secrets.token_urlsafe(18),
                        kind="public_data_scope",
                        object_id="dataset",
                        phrase="connected dataset",
                        allowed_choices=("confirm_public", "keep_controlled"),
                        source=source,
                    ),
                    catalog.public_scope_epoch,
                )
            )
        candidates = sorted(
            (
                item
                for item in catalog.dimensions
                if item.review_policy.value == "release_required"
                and item.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED
            ),
            key=lambda item: item.id,
        )
        for item in candidates:
            pending.append(
                (
                    ReviewRequest(
                        review_id=secrets.token_urlsafe(18),
                        kind="dimension_disclosure",
                        object_id=item.id,
                        phrase=item.label,
                        allowed_choices=(
                            ("public_grouped", "keep_controlled")
                            if item.disclosure_tier
                            == DimensionDisclosureTier.CONTROLLED_GROUPED
                            else (
                                "controlled_grouped",
                                "public_grouped",
                                "keep_blocked",
                            )
                        ),
                        source=source,
                    ),
                    item.action_revision,
                )
            )
        records = [
            self._register_governance(context.scope, request, catalog, object_revision)
            for request, object_revision in pending[:_MAX_CONNECT_REVIEWS]
        ]
        return records, len(pending)

    def _register_governance(
        self,
        scope: str,
        request: ReviewRequest,
        catalog: Any,
        object_revision: int,
    ) -> _GovernanceRecord:
        with self._lock:
            now = time.monotonic()
            self._governance = {
                review_id: item
                for review_id, item in self._governance.items()
                if item.expires_monotonic >= now
            }
            for item in self._governance.values():
                if (
                    item.scope == scope
                    and item.request.kind == request.kind
                    and item.request.object_id == request.object_id
                    and item.request.source == request.source
                    and item.fingerprint == catalog.fingerprint
                    and item.catalog_version == catalog.version
                    and item.classification_policy_version
                    == catalog.classification_policy_version
                    and item.object_revision == object_revision
                ):
                    if set(request.allowed_choices) < set(item.request.allowed_choices):
                        narrowed = replace(
                            item,
                            request=replace(
                                item.request,
                                allowed_choices=request.allowed_choices,
                            ),
                        )
                        self._governance[item.request.review_id] = narrowed
                        return narrowed
                    return item
            record = _GovernanceRecord(
                scope=scope,
                request=request,
                fingerprint=catalog.fingerprint,
                catalog_version=catalog.version,
                classification_policy_version=catalog.classification_policy_version,
                object_revision=object_revision,
                expires_monotonic=now + _REVIEW_TTL_SECONDS,
            )
            self._governance[request.review_id] = record
        return record

    def _public_scope_review(
        self, scope: str, catalog: SemanticCatalog, source: SourceRef
    ) -> _GovernanceRecord:
        return self._register_governance(
            scope,
            ReviewRequest(
                review_id=secrets.token_urlsafe(18),
                kind="public_data_scope",
                object_id="dataset",
                phrase="connected dataset",
                allowed_choices=("confirm_public", "keep_controlled"),
                source=source,
            ),
            catalog,
            catalog.public_scope_epoch,
        )

    def _prune_governance(self, scope: str, source: SourceRef) -> None:
        now = time.monotonic()
        with self._lock:
            self._governance = {
                review_id: item
                for review_id, item in self._governance.items()
                if item.expires_monotonic >= now
                and (item.scope != scope or item.request.source == source)
            }

    def _apply_governance_feedback(
        self, request: FeedbackRequest, record: _GovernanceRecord
    ) -> FeedbackApplied | Blocked:
        if Capability.REVIEW_ANY not in request.context.capabilities:
            return Blocked(
                "review_forbidden", "공개 범위 검토에는 steward 권한이 필요합니다."
            )
        binding = self._concierge.connection_binding(request.context.scope)
        catalog = self._concierge.semantic.load(request.context.scope)
        if (
            binding is None
            or catalog is None
            or SourceRef(binding.source_id, binding.generation) != record.request.source
            or catalog.fingerprint != record.fingerprint
            or catalog.version != record.catalog_version
            or catalog.classification_policy_version
            != record.classification_policy_version
        ):
            return Blocked(
                "review_stale", "DB 또는 분류 정책이 바뀌어 검토 요청이 만료되었습니다."
            )
        choice = request.choice.strip().lower()
        if choice not in record.request.allowed_choices:
            return Blocked("review_choice_invalid", "허용된 검토 선택지가 아닙니다.")
        action_token = ""
        try:
            if record.request.kind == "public_data_scope":
                if catalog.public_scope_epoch != record.object_revision:
                    return Blocked(
                        "review_stale", "공개 범위 상태가 이미 바뀌었습니다."
                    )
                if choice == "keep_controlled":
                    return FeedbackApplied(
                        False, "데이터셋을 보호 범위로 유지했습니다."
                    )
                action_token = self._concierge.semantic.issue_catalog_action_token(
                    request.context.scope, "public_data_confirm"
                )
                if not action_token:
                    return Blocked(
                        "review_stale", "공개 범위 검토 상태가 이미 바뀌었습니다."
                    )
                outcome = self._concierge.semantic.set_public_data_scope_with_token(
                    request.context.scope,
                    action_token,
                    StewardAssertion(
                        scope=request.context.scope,
                        reviewer_id=request.context.actor_id,
                        authorized=True,
                        public_data_confirmed=True,
                    ),
                    enable=True,
                    audit_scope=request.context.conversation_id,
                )
            else:
                dimension = catalog.dimension(record.request.object_id)
                if (
                    dimension is None
                    or dimension.action_revision != record.object_revision
                ):
                    return Blocked(
                        "review_stale", "차원 공개 상태가 이미 바뀌었습니다."
                    )
                if choice == "keep_blocked":
                    return FeedbackApplied(False, "차원을 비공개 상태로 유지했습니다.")
                if choice == "keep_controlled":
                    return FeedbackApplied(
                        False, "차원을 보호 그룹 상태로 유지했습니다."
                    )
                action_token = self._concierge.semantic.issue_dimension_action_token(
                    request.context.scope,
                    record.request.object_id,
                    "dimension_set_tier",
                    expected_catalog=catalog,
                )
                if not action_token:
                    return Blocked(
                        "review_stale", "차원 공개 상태가 이미 바뀌었습니다."
                    )
                outcome = self._concierge.semantic.release_dimension_with_token(
                    request.context.scope,
                    action_token,
                    StewardAssertion(
                        scope=request.context.scope,
                        reviewer_id=request.context.actor_id,
                        authorized=True,
                        public_data_confirmed=(choice == "public_grouped"),
                    ),
                    choice,
                    audit_scope=request.context.conversation_id,
                )
        except Exception:
            # Token issuance precedes the atomic catalog/audit mutation. If the
            # latter rolls back, retire the unexposed token record as well so
            # repeated storage failures cannot accumulate stale capabilities.
            if action_token:
                self._concierge.semantic.discard_action_token(
                    request.context.scope, action_token
                )
            raise
        if outcome.status != "confirmed":
            if action_token:
                self._concierge.semantic.discard_action_token(
                    request.context.scope, action_token
                )
            return Blocked("review_not_applied", outcome.message)
        return FeedbackApplied(outcome.mutation_applied, outcome.message)

    def _require(self, context: CallContext, capability: Capability) -> Blocked | None:
        if self._closed:
            return Blocked("runtime_closed", "Lang2SQL runtime이 닫혔습니다.")
        if capability not in context.capabilities:
            return Blocked(
                "capability_required", f"{capability.value} 권한이 필요합니다."
            )
        return None

    @staticmethod
    def _identity(context: CallContext) -> Identity:
        return Identity(
            user_id=context.actor_id,
            guild_id=context.scope,
            channel_id=context.conversation_id,
            is_admin=Capability.REVIEW_ANY in context.capabilities,
        )

    @staticmethod
    def _review_scope(context: CallContext) -> str:
        return review_scope_key(
            f"api:{context.scope}:{context.conversation_id}", context.actor_id
        )
