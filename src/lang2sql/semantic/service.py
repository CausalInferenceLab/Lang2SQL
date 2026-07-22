"""Concrete semantic onboarding, review, and deterministic query service.

This module is the intentionally small integration facade.  It persists one
catalog per guild scope and returns only three query states: ready,
clarification, or blocked.  There is no raw-SQL fallback.
"""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
import secrets
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Callable

from ..core.ports.audit import AuditEvent
from ..core.ports.explorer import ExplorerPort
from .catalog import (
    CATALOG_KEY,
    CONNECTION_BINDING_KEY,
    CONNECTION_GENERATION_KEY,
    PENDING_REVIEW_KEY,
    Aggregate,
    DimensionDisclosureTier,
    DimensionSpec,
    DimensionReviewPolicy,
    JoinSpec,
    MetricExpressionKind,
    MetricSpec,
    PendingReview,
    ReviewState,
    SemanticCatalog,
    ConnectionBinding,
)
from .compiler import (
    DIMENSION_OUTPUT_PREFIX as _DIMENSION_OUTPUT_PREFIX,
    METRIC_CONTRIBUTOR_COUNT_KEY as _METRIC_CONTRIBUTOR_COUNT_KEY,
    METRIC_OUTPUT_KEY as _METRIC_OUTPUT_KEY,
    RELEASE_CATEGORY_COUNT_KEY as _RELEASE_CATEGORY_COUNT_KEY,
    RELEASE_GROUP_SIZE_KEY as _RELEASE_GROUP_SIZE_KEY,
    compile_semantic_plan,
    compile_legacy_aggregate_sql,
)
from .onboarding import OnboardingSummary, build_catalog
from .plan import (
    BaseMeasure,
    DimensionSelection,
    FilterOperator,
    FilterPredicate,
    LiteralKind,
    PreparedSql,
    ScalarLiteral,
    SemanticPlan,
    SemanticStateStamp,
    TimeWindow,
)
from .policy import (
    dimension_is_released as _dimension_is_released,
    has_controlled_dimension as _has_controlled_dimension,
    public_data_scope_confirmed as _public_data_scope_confirmed,
)
from .shortlist import question_sha256
from .type_compatibility import (
    filter_compatibility_error,
    time_window_compatibility_error,
)

if TYPE_CHECKING:
    from ..adapters.storage.sqlite_store import SqliteStore


_GROUPING_CUE = re.compile(
    r"\b(by|per|each|grouped\s+by|for\s+each)\b|별|마다|각각",
    re.IGNORECASE,
)
_RELATIVE_TIME_FILTER_CUE = re.compile(
    r"\b(today|yesterday|last|previous|this|since|before|after)\b|"
    r"오늘|어제|지난|이번|이전|이후|부터|까지",
    re.IGNORECASE,
)
_TIME_UNIT_CUE = re.compile(
    r"\b(month|week|year|quarter)\b|월|주|연도|년도|분기",
    re.IGNORECASE,
)
_TIME_RANGE_CUE = re.compile(
    r"\b(from|to|between|through|until)\b|[-~–—]", re.IGNORECASE
)
_UNIT_CUES = {
    "kg": ("kg", "kilogram", "kilograms", "킬로그램"),
    "metric_ton": ("metric ton", "metric tons", "tonne", "tonnes", "톤"),
    "USD": ("usd", "dollar", "dollars", "달러"),
    "KRW": ("krw", "won", "원화"),
    "percent": ("percent", "percentage", "%", "퍼센트"),
}
_AGGREGATE_CUES = {
    Aggregate.SUM: re.compile(r"\b(sum|total)\b|합계|총합|총액", re.IGNORECASE),
    Aggregate.AVG: re.compile(r"\b(avg|average|mean)\b|평균", re.IGNORECASE),
    Aggregate.MIN: re.compile(r"\b(min|minimum|lowest)\b|최소|최솟값", re.IGNORECASE),
    Aggregate.MAX: re.compile(r"\b(max|maximum|highest)\b|최대|최댓값", re.IGNORECASE),
    Aggregate.COUNT: re.compile(
        r"\b(count|how\s+many|number\s+of)\b|개수|건수|몇\s*개",
        re.IGNORECASE,
    ),
}
_COUNT_EXISTENTIAL_SCAFFOLD = re.compile(
    r"\bhow\s+many\s+(is|are)\s+there\b", re.IGNORECASE
)
_GENERIC_SOURCE_CONTEXT_SCAFFOLD = re.compile(
    r"(?<!\S)in\s+(?:the\s+)?source\s+"
    r"(?:observations|dataset|records|table|rows|data)(?!\S)",
    re.IGNORECASE,
)
_RELEASE_MIN_GROUP_SIZE = 5
_RELEASE_MAX_CATEGORY_COUNT = 50
_RELEASE_MAX_LABEL_LENGTH = 128
_QUERY_GRAMMAR_WORDS = {
    "a",
    "all",
    "an",
    "are",
    "avg",
    "average",
    "by",
    "calculate",
    "compute",
    "count",
    "display",
    "each",
    "for",
    "get",
    "give",
    "group",
    "grouped",
    "highest",
    "how",
    "is",
    "list",
    "lowest",
    "max",
    "maximum",
    "many",
    "me",
    "mean",
    "min",
    "minimum",
    "much",
    "number",
    "of",
    "only",
    "per",
    "please",
    "report",
    "rows",
    "show",
    "sum",
    "tell",
    "the",
    "total",
    "what",
    "where",
    "whose",
    "with",
    "개수",
    "건수",
    "계산",
    "계산해줘",
    "구해줘",
    "기준",
    "마다",
    "몇",
    "보여줘",
    "별",
    "수",
    "알려줘",
    "알려",
    "전체",
    "주세요",
    "집계",
    "집계해줘",
    "최대",
    "최댓값",
    "최소",
    "최솟값",
    "평균",
    "합계",
}
_ACTION_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{22,64}$")
_ACTION_KEY_PREFIX = "semantic_action:v1:"
_ACTION_RECEIPT_KEY_PREFIX = "semantic_action_receipt:v1:"
_ACTION_ARM_KEY_PREFIX = "semantic_action_arm:v1:"
_REVIEW_RECEIPT_KEY_PREFIX = "semantic_review_receipt:v1:"
_ACTION_TTL_SECONDS = 15 * 60
_PENDING_DRAFT_TTL_SECONDS = 15 * 60
_MAX_PENDING_DRAFTS = 256


def _metric_action_digest(metric: MetricSpec) -> str:
    """Hash the typed metric metadata and human decision state for one action."""

    projection = {
        "projection_version": 1,
        "id": metric.id,
        "label": metric.label,
        "table_id": metric.table_id,
        "column": metric.column,
        "expression_kind": metric.expression_kind.value,
        "allowed_aggregates": [item.value for item in metric.allowed_aggregates],
        "data_type": metric.data_type,
        "nullable": metric.nullable,
        "classification_evidence": metric.classification_evidence,
        "source_record_count": metric.source_record_count,
        "aliases": sorted(metric.aliases),
        "auto_aliases": sorted(metric.auto_aliases),
        "rejected_aliases": sorted(metric.rejected_aliases),
        "reviewed_bindings": {
            phrase: sorted(aggregates)
            for phrase, aggregates in sorted(metric.reviewed_bindings.items())
        },
        "rejected_bindings": sorted(metric.rejected_bindings),
        "alias_reviewers": dict(sorted(metric.alias_reviewers.items())),
        "binding_reviewers": dict(sorted(metric.binding_reviewers.items())),
    }
    encoded = json.dumps(
        projection, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _dimension_action_digest(dimension: DimensionSpec) -> str:
    """Hash exactly the metadata and disclosure state shown for one action."""

    projection = {
        "projection_version": 1,
        "id": dimension.id,
        "label": dimension.label,
        "table_id": dimension.table_id,
        "column": dimension.column,
        "data_type": dimension.data_type,
        "kind": dimension.kind,
        "review_policy": dimension.review_policy.value,
        "classification_evidence": dimension.classification_evidence,
        "classification_policy_version": dimension.classification_policy_version,
        "raw_output_allowed": dimension.raw_output_allowed,
        "disclosure_tier": dimension.disclosure_tier.value,
        "release_catalog_fingerprint": dimension.release_catalog_fingerprint,
        "action_revision": dimension.action_revision,
        "aliases": sorted(dimension.aliases),
        "reserved_aliases": sorted(dimension.reserved_aliases),
        "rejected_aliases": sorted(dimension.rejected_aliases),
        "alias_reviewers": dict(sorted(dimension.alias_reviewers.items())),
    }
    encoded = json.dumps(
        projection, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(repr=False)
class QueryOutcome:
    status: str
    message: str
    sql: str = ""
    metric_id: str = ""
    aggregate: str = ""
    dimension_ids: list[str] = field(default_factory=list)
    blocker: str = ""
    catalog_fingerprint: str = ""
    catalog_review_revision: int = 0
    catalog_version: int = 0
    classification_policy_version: int = 0
    source_id: str = ""
    connection_generation: int = 0
    plan: SemanticPlan | None = field(default=None, repr=False)
    prepared: PreparedSql | None = field(default=None, repr=False)


@dataclass(frozen=True)
class StewardAssertion:
    """Authenticated frontend assertion required for disclosure state changes."""

    scope: str
    reviewer_id: str
    authorized: bool
    public_data_confirmed: bool = False


@dataclass(repr=False)
class ReviewOutcome:
    status: str
    message: str
    question: str = field(default="", repr=False)
    tool_args: dict[str, object] = field(default_factory=dict, repr=False)
    source_id: str = ""
    connection_generation: int = 0
    requester_id: str = ""
    review_id: str = ""
    mutation_applied: bool = False
    object_id: str = ""


@dataclass(frozen=True, repr=False)
class _PendingDraft:
    """Short-lived same-process resume payload, never written to storage."""

    review_scope: str
    review_id: str
    question: str = field(repr=False)
    tool_args: dict[str, object] = field(repr=False)
    expires_monotonic: float


def review_scope_key(session_key: str, user_id: str) -> str:
    """Address one user's pending review inside a shared Discord conversation."""

    return f"{session_key}:semantic-review:{user_id}"


def _apply_pending_choice(
    catalog: SemanticCatalog,
    pending: PendingReview,
    choice: str,
    reviewer_id: str,
) -> tuple[ReviewOutcome, bool]:
    """Apply one already-stamped review decision without persistence side effects."""

    metric = catalog.metric(pending.metric_id)
    if metric is None:
        return (
            ReviewOutcome("blocked", "확인 대상 지표가 더 이상 존재하지 않습니다."),
            False,
        )
    if pending.review_kind not in {"metric", "dimension"}:
        return (
            ReviewOutcome("blocked", "확인 종류가 유효하지 않아 요청을 폐기했습니다."),
            False,
        )

    normalized = choice.strip().lower()
    if normalized == "reject":
        if pending.review_kind == "metric":
            if pending.metric_alias_pending and pending.metric_phrase:
                _append_alias(metric.rejected_aliases, pending.metric_phrase)
            binding_key = _binding_key(
                pending.metric_phrase, pending.proposed_aggregate
            )
            if binding_key and binding_key not in metric.rejected_bindings:
                metric.rejected_bindings.append(binding_key)
                metric.rejected_bindings.sort()
            message = (
                "이 질문의 지표 표현·집계 연결을 사용하지 않도록 저장했습니다. "
                "분류 표현에는 영향을 주지 않았고 SQL도 실행하지 않았습니다."
            )
        else:
            for binding in pending.dimension_bindings[:1]:
                dimension = catalog.dimension(binding.get("dimension_id", ""))
                if dimension is not None:
                    _append_alias(dimension.rejected_aliases, binding.get("phrase", ""))
            message = (
                "이 질문의 분류 표현 연결만 사용하지 않도록 저장했습니다. "
                "지표 검토에는 영향을 주지 않았고 SQL도 실행하지 않았습니다."
            )
        return ReviewOutcome("confirmed", message), True

    if normalized not in pending.allowed_choices:
        allowed = ", ".join(pending.allowed_choices)
        return (
            ReviewOutcome("blocked", f"선택 가능한 값은 {allowed}, reject 입니다."),
            False,
        )
    if pending.review_kind == "metric":
        if pending.aggregate_pending:
            try:
                aggregate = Aggregate(normalized)
            except ValueError:
                return (
                    ReviewOutcome(
                        "blocked", "이 지표에는 집계 방식 선택이 필요합니다."
                    ),
                    False,
                )
            if aggregate not in metric.allowed_aggregates:
                return (
                    ReviewOutcome(
                        "blocked", "이 컬럼에는 해당 집계를 사용할 수 없습니다."
                    ),
                    False,
                )
            conflict = _metric_binding_conflict(
                catalog, metric.id, pending.metric_phrase
            )
            if conflict:
                return (
                    ReviewOutcome(
                        "blocked",
                        f"`{pending.metric_phrase}`은 이미 `{conflict}`에 연결되어 있습니다. "
                        "공유 의미를 덮어쓰지 않았습니다.",
                    ),
                    False,
                )
            metric.aggregate = aggregate
            metric.state = ReviewState.CONFIRMED
            reviewed_aggregates = metric.reviewed_bindings.setdefault(
                pending.metric_phrase, []
            )
            if aggregate.value not in reviewed_aggregates:
                reviewed_aggregates.append(aggregate.value)
                reviewed_aggregates.sort()
            metric.binding_reviewers[pending.metric_phrase] = reviewer_id or "unknown"
        elif normalized != "confirm":
            return (
                ReviewOutcome(
                    "blocked",
                    "이 단계에서는 지표 표현 연결 확인 또는 거절만 가능합니다.",
                ),
                False,
            )
        if pending.metric_alias_pending:
            _append_alias(metric.aliases, pending.metric_phrase)
            metric.alias_reviewers[_normalize_phrase(pending.metric_phrase)] = (
                reviewer_id or "unknown"
            )
        aggregate_label = (
            normalized
            if pending.aggregate_pending
            else pending.proposed_aggregate or "confirmed"
        )
        message = (
            f"`{pending.metric_phrase}` → `{metric.label}` 지표 연결을 "
            f"`{aggregate_label}` 기준으로 저장했습니다. 분류 표현은 "
            "별도 단계에서 확인합니다."
        )
        return ReviewOutcome("confirmed", message), True

    if normalized != "confirm":
        return (
            ReviewOutcome(
                "blocked", "이 단계에서는 분류 표현 연결 확인 또는 거절만 가능합니다."
            ),
            False,
        )
    if not pending.dimension_bindings:
        return (
            ReviewOutcome(
                "blocked", "확인 대상 분류 기준이 더 이상 존재하지 않습니다."
            ),
            False,
        )
    binding = pending.dimension_bindings[0]
    dimension = catalog.dimension(binding.get("dimension_id", ""))
    if dimension is None:
        return (
            ReviewOutcome(
                "blocked", "확인 대상 분류 기준이 더 이상 존재하지 않습니다."
            ),
            False,
        )
    conflict = _dimension_alias_conflict(
        catalog,
        dimension.id,
        binding.get("phrase", ""),
    )
    if conflict:
        return (
            ReviewOutcome(
                "blocked",
                f"`{binding.get('phrase', '')}`은 이미 `{conflict}`에 연결되어 있습니다. "
                "공유 의미를 덮어쓰지 않았습니다.",
            ),
            False,
        )
    _append_alias(dimension.aliases, binding.get("phrase", ""))
    dimension.alias_reviewers[_normalize_phrase(binding.get("phrase", ""))] = (
        reviewer_id or "unknown"
    )
    return (
        ReviewOutcome(
            "confirmed",
            f"`{binding.get('phrase', '')}` → `{dimension.label}` 분류 연결을 "
            "저장했습니다. 다른 미확인 연결이 있으면 다음 단계에서 이어집니다.",
        ),
        True,
    )


class SemanticService:
    def __init__(self, store: "SqliteStore") -> None:
        self._store = store
        self._unmanaged_explorer_sources: dict[int, tuple[ExplorerPort, str]] = {}
        self._pending_drafts: dict[str, _PendingDraft] = {}
        self._pending_draft_timers: dict[str, threading.Timer] = {}
        self._pending_drafts_lock = threading.RLock()
        self._scrub_legacy_pending_reviews()

    def _scrub_legacy_pending_reviews(self) -> None:
        """Remove pre-v2 question/literal payloads before serving any request."""

        # Pending reviews are disposable workflow state. Upgrade every dormant
        # record through the same value-CAS as active reads, and delete records
        # that cannot be parsed safely rather than retaining unknown secrets.
        for review_scope, raw in self._store.kv_list_key(PENDING_REVIEW_KEY):
            if self._pending_review_record(review_scope) is None:
                self._store.kv_delete_if_value(review_scope, PENDING_REVIEW_KEY, raw)

    def _remember_pending_draft(
        self,
        review_scope: str,
        review_id: str,
        question: str,
        tool_args: dict[str, object],
    ) -> None:
        """Keep one sensitive resume payload in memory for at most 15 minutes."""

        now = time.monotonic()
        expires = now + _PENDING_DRAFT_TTL_SECONDS
        with self._pending_drafts_lock:
            remove_ids = {
                key
                for key, item in self._pending_drafts.items()
                if item.expires_monotonic >= now and item.review_scope == review_scope
            }
            remove_ids.update(
                key
                for key, item in self._pending_drafts.items()
                if item.expires_monotonic < now
            )
            while len(self._pending_drafts) - len(remove_ids) >= _MAX_PENDING_DRAFTS:
                remove_ids.add(
                    min(
                        (
                            (key, item)
                            for key, item in self._pending_drafts.items()
                            if key not in remove_ids
                        ),
                        key=lambda pair: pair[1].expires_monotonic,
                    )[0]
                )
            for stale_id in remove_ids:
                self._pending_drafts.pop(stale_id, None)
                timer = self._pending_draft_timers.pop(stale_id, None)
                if timer is not None:
                    timer.cancel()
            self._pending_drafts[review_id] = _PendingDraft(
                review_scope=review_scope,
                review_id=review_id,
                question=question,
                tool_args=deepcopy(tool_args),
                expires_monotonic=expires,
            )
            timer = threading.Timer(
                _PENDING_DRAFT_TTL_SECONDS,
                self._expire_pending_draft,
                args=(review_id, expires),
            )
            timer.daemon = True
            self._pending_draft_timers[review_id] = timer
            timer.start()

    def _expire_pending_draft(self, review_id: str, expires: float) -> None:
        with self._pending_drafts_lock:
            item = self._pending_drafts.get(review_id)
            if (
                item is not None
                and item.expires_monotonic <= expires
                and item.expires_monotonic <= time.monotonic()
            ):
                self._pending_drafts.pop(review_id, None)
            self._pending_draft_timers.pop(review_id, None)

    def _pending_draft(self, review_scope: str, review_id: str) -> _PendingDraft | None:
        now = time.monotonic()
        with self._pending_drafts_lock:
            item = self._pending_drafts.get(review_id)
            if item is None:
                return None
            if item.expires_monotonic < now or item.review_scope != review_scope:
                self._forget_pending_draft(review_id)
                return None
            return item

    def _forget_pending_draft(self, review_id: str) -> None:
        with self._pending_drafts_lock:
            self._pending_drafts.pop(review_id, None)
            timer = self._pending_draft_timers.pop(review_id, None)
            if timer is not None:
                timer.cancel()

    def clear_transient_state(self) -> None:
        """Deterministically erase all question/literal resume payloads."""

        with self._pending_drafts_lock:
            timers = tuple(self._pending_draft_timers.values())
            self._pending_draft_timers.clear()
            self._pending_drafts.clear()
        for timer in timers:
            timer.cancel()

    def _source_for_unmanaged_explorer(
        self, explorer: ExplorerPort, *, create: bool = False
    ) -> str:
        entry = self._unmanaged_explorer_sources.get(id(explorer))
        if entry is not None and entry[0] is explorer:
            return entry[1]
        if not create:
            return ""
        source_id = secrets.token_hex(32)
        self._unmanaged_explorer_sources[id(explorer)] = (explorer, source_id)
        return source_id

    def unmanaged_explorer_matches(
        self, explorer: ExplorerPort, binding: ConnectionBinding
    ) -> bool:
        return (
            not binding.managed_credentials
            and self._source_for_unmanaged_explorer(explorer) == binding.source_id
        )

    def unmanaged_explorer_binding(
        self, explorer: ExplorerPort, catalog: SemanticCatalog
    ) -> ConnectionBinding | None:
        source_id = self._source_for_unmanaged_explorer(explorer)
        if not source_id or source_id != catalog.source_id:
            return None
        return ConnectionBinding(
            source_id=source_id,
            generation=catalog.connection_generation,
            managed_credentials=False,
        )

    async def inspect(
        self,
        scope: str,
        explorer: ExplorerPort,
        *,
        carry_source_id: str = "",
    ) -> OnboardingSummary:
        """Build a candidate catalog without mutating the active connection state."""

        built = await build_catalog(explorer)
        existing = self.load(scope)
        if (
            carry_source_id
            and existing is not None
            and existing.source_id == carry_source_id
            and existing.fingerprint == built.catalog.fingerprint
        ):
            _carry_forward_reviews(existing, built.catalog)
        return OnboardingSummary(
            table_count=built.table_count,
            declared_join_count=built.declared_join_count,
            blocked_column_count=built.blocked_column_count,
            confirmed_metric_count=built.catalog.confirmed_metric_count,
            pending_metric_count=built.catalog.pending_metric_count,
            catalog=built.catalog,
        )

    async def onboard(self, scope: str, explorer: ExplorerPort) -> OnboardingSummary:
        # Library callers do not provide a source identity. Carrying reviews
        # merely because a different DB has the same schema would leak aliases
        # and disclosure decisions across sources. Discord's connection path
        # calls inspect() explicitly after comparing the encrypted DSN.
        summary = await self.inspect(scope, explorer)
        raw_generation = self._store.kv_get(scope, CONNECTION_GENERATION_KEY)
        expected_generation = int(raw_generation) if raw_generation is not None else 0
        source_id = self._source_for_unmanaged_explorer(explorer, create=True)

        def build_upserts(generation: int) -> dict[str, str]:
            summary.catalog.source_id = source_id
            summary.catalog.connection_generation = generation
            binding = ConnectionBinding(
                source_id=source_id,
                generation=generation,
                managed_credentials=False,
            )
            return {
                CATALOG_KEY: summary.catalog.to_json(),
                CONNECTION_BINDING_KEY: binding.to_json(),
            }

        self._store.kv_activate_generation(
            scope,
            expected_generation=expected_generation,
            build_upserts=build_upserts,
            generation_key=CONNECTION_GENERATION_KEY,
        )
        return summary

    def load(self, scope: str) -> SemanticCatalog | None:
        snapshot = self._store.kv_get_many(
            scope,
            {CATALOG_KEY, CONNECTION_BINDING_KEY, CONNECTION_GENERATION_KEY},
        )
        raw = snapshot.get(CATALOG_KEY)
        if not raw:
            return None
        try:
            catalog = SemanticCatalog.from_json(raw)
            raw_binding = snapshot.get(CONNECTION_BINDING_KEY)
            if raw_binding is None:
                return catalog if not catalog.source_id else None
            binding = ConnectionBinding.from_json(raw_binding)
            if (
                catalog.source_id != binding.source_id
                or catalog.connection_generation != binding.generation
                or snapshot.get(CONNECTION_GENERATION_KEY) != str(binding.generation)
            ):
                return None
            return catalog
        except (KeyError, TypeError, ValueError):
            # A corrupt catalog must never silently enable the raw SQL path.
            return None

    def save(
        self,
        scope: str,
        catalog: SemanticCatalog,
        *,
        expected_review_revision: int | None = None,
    ) -> None:
        if not catalog.source_id:
            self._store.kv_set(scope, CATALOG_KEY, catalog.to_json())
            return
        if expected_review_revision is None:
            raise ValueError("bound catalog writes require an expected review revision")
        binding = ConnectionBinding(
            source_id=catalog.source_id,
            generation=catalog.connection_generation,
            managed_credentials=(self._store.kv_get(scope, "db_dsn") is not None),
        )
        self._store.kv_set_bound_catalog(
            scope,
            catalog_key=CATALOG_KEY,
            catalog_value=catalog.to_json(),
            binding_key=CONNECTION_BINDING_KEY,
            expected_binding_value=binding.to_json(),
            generation_key=CONNECTION_GENERATION_KEY,
            expected_generation=catalog.connection_generation,
            expected_review_revision=expected_review_revision,
        )

    def _commit_review_catalog(self, scope: str, catalog: SemanticCatalog) -> bool:
        """CAS one semantic decision against its exact prior review revision."""

        try:
            self.save(
                scope,
                catalog,
                expected_review_revision=catalog.review_revision - 1,
            )
        except RuntimeError:
            return False
        return True

    def blocked_column_in_question(self, scope: str, question: str) -> str:
        """Recognize explicit references to policy-blocked physical columns."""

        catalog = self.load(scope)
        if catalog is None:
            return ""
        normalized_question = _normalize_phrase(question)
        for reference in catalog.blocked_columns:
            table, _, column = reference.rpartition(".")
            column_phrase = _normalize_phrase(column)
            if column_phrase not in {"name", "address"}:
                if _phrase_in_question(column_phrase, normalized_question):
                    return reference
                continue
            table_name = table.rsplit(".", 1)[-1]
            table_forms = {table_name}
            if table_name.endswith("s") and len(table_name) > 3:
                table_forms.add(table_name[:-1])
            if any(
                _phrase_in_question(
                    _normalize_phrase(f"{table_form} {column}"),
                    normalized_question,
                )
                for table_form in table_forms
            ):
                return reference
        return ""

    def pending_review(self, review_scope: str) -> PendingReview | None:
        record = self._pending_review_record(review_scope)
        return record[0] if record is not None else None

    def _pending_review_record(
        self, review_scope: str
    ) -> tuple[PendingReview, str] | None:
        raw = self._store.kv_get(review_scope, PENDING_REVIEW_KEY)
        if not raw:
            return None
        for _attempt in range(2):
            try:
                pending = PendingReview.from_json(raw)
            except (KeyError, TypeError, ValueError):
                return None
            safe_raw = pending.to_json()
            if safe_raw == raw:
                return pending, raw

            # Older records may contain the full question, predicate literals,
            # and date bounds. Rewrite them under a value-CAS the first time
            # they are read; parsing into the v2 object alone would leave the
            # legacy secrets indefinitely present in SQLite.
            def scrub(snapshot: dict[str, str]):
                current = snapshot.get(PENDING_REVIEW_KEY, "")
                if current != raw:
                    return {}, set(), current
                return {PENDING_REVIEW_KEY: safe_raw}, set(), safe_raw

            current_raw = self._store.kv_mutate_snapshot(
                review_scope,
                keys={PENDING_REVIEW_KEY},
                mutate=scrub,
            )
            if not current_raw:
                return None
            raw = str(current_raw)
        try:
            return PendingReview.from_json(raw), raw
        except (KeyError, TypeError, ValueError):
            return None

    @staticmethod
    def _pending_stale_message(
        scope: str, catalog: SemanticCatalog, pending: PendingReview
    ) -> str:
        """Return why a server-created review no longer belongs to active state."""

        if pending.catalog_scope != scope:
            return "다른 연결 범위의 확인 요청입니다."
        if (
            pending.source_id != catalog.source_id
            or pending.connection_generation != catalog.connection_generation
        ):
            return "DB 연결이 바뀌어 이전 확인 요청을 폐기했습니다. 질문을 다시 실행해 주세요."
        if pending.catalog_fingerprint != catalog.fingerprint:
            return "DB 구조가 바뀌어 이전 확인 요청을 폐기했습니다. 질문을 다시 실행해 주세요."
        if (
            pending.catalog_version != catalog.version
            or pending.classification_policy_version
            != catalog.classification_policy_version
        ):
            return "분류 정책이 바뀌어 이전 확인 요청을 폐기했습니다. 질문을 다시 실행해 주세요."
        if pending.catalog_review_revision != catalog.review_revision:
            return "의미 검토가 바뀌어 이전 확인 요청을 폐기했습니다. 질문을 다시 실행해 주세요."
        return ""

    def pending_review_queue(self, scope: str) -> list[tuple[str, PendingReview]]:
        """Return current requester-owned reviews for one catalog scope.

        The database lookup uses an exact storage key; the catalog scope and
        source stamp inside each server-created record remain the authority.
        User-provided IDs never select an arbitrary storage scope directly.
        """

        catalog = self.load(scope)
        if catalog is None:
            return []
        pending: list[tuple[str, PendingReview]] = []
        for review_scope, _raw in self._store.kv_list_key(PENDING_REVIEW_KEY):
            record = self._pending_review_record(review_scope)
            if record is None:
                continue
            item, _safe_raw = record
            if item.review_id and not self._pending_stale_message(scope, catalog, item):
                pending.append((review_scope, item))
        return sorted(pending, key=lambda pair: pair[1].review_id)

    def pending_review_by_id(
        self, scope: str, review_id: str
    ) -> tuple[str, PendingReview] | None:
        normalized = review_id.strip()
        if not normalized:
            return None
        matches = [
            pair
            for pair in self.pending_review_queue(scope)
            if secrets.compare_digest(pair[1].review_id, normalized)
        ]
        return matches[0] if len(matches) == 1 else None

    def status_text(self, scope: str, review_scope: str = "") -> str:
        catalog = self.load(scope)
        if catalog is None:
            return "아직 의미 카탈로그가 없습니다. `/setup`으로 DB를 연결해 주세요."
        active_review_scope = review_scope or scope
        pending_record = self._pending_review_record(active_review_scope)
        pending = pending_record[0] if pending_record is not None else None
        pending_raw = pending_record[1] if pending_record is not None else ""
        stale_pending_message = ""
        if pending is not None:
            stale_pending_message = self._pending_stale_message(scope, catalog, pending)
            if stale_pending_message:
                # A requester checking status should not keep seeing an item
                # that the steward queue and confirmation path have invalidated.
                self._store.kv_delete_if_value(
                    active_review_scope, PENDING_REVIEW_KEY, pending_raw
                )
                pending = None
        release_candidates = [
            item
            for item in catalog.dimensions
            if item.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
            and not item.raw_output_allowed
        ]
        lines = [
            "**Semantic setup 상태**",
            f"- 테이블: {len(catalog.tables)}개",
            f"- 선언된 안전 조인: {len(catalog.joins)}개",
            f"- 기본 차단 컬럼: {len(catalog.blocked_columns)}개",
            f"- 확인된 지표: {catalog.confirmed_metric_count}개",
            f"- 확인된 표현·집계 연결: {sum(len(values) for metric in catalog.metrics for values in metric.reviewed_bindings.values())}개",
            f"- 관리자 값 공개 검토 대기 차원: {len(release_candidates)}개",
            f"- 연결 전체 공개 데이터 범위: {'확인됨' if catalog.public_data_scope else '아님'}",
            "- 지표 표현과 각 분류 표현은 실제 질문에서 서로 독립된 단계로 확인합니다.",
        ]
        if pending is not None:
            # Do not echo user/DB-derived phrases in this generic status text.
            # Discord's steward queue renders bounded, escaped metadata.
            lines.append(
                f"- 현재 확인 대기: review_id `{pending.review_id or 'legacy'}`"
            )
        elif stale_pending_message:
            lines.append(
                "- 이전 확인 요청은 연결 또는 의미 상태 변경으로 폐기되었습니다."
            )
        return "\n".join(lines)

    def metric_candidates(self, scope: str) -> list[MetricSpec]:
        """Return metadata-only measure candidates for steward browsing."""

        _catalog, candidates = self.metric_candidate_snapshot(scope)
        return candidates

    def metric_candidate_snapshot(
        self, scope: str
    ) -> tuple[SemanticCatalog | None, list[MetricSpec]]:
        """Read display candidates and their action-token stamp together."""

        catalog = self.load(scope)
        if catalog is None:
            return None, []
        candidates = sorted(
            (
                item
                for item in catalog.metrics
                if item.expression_kind == MetricExpressionKind.COLUMN
            ),
            key=lambda item: item.id,
        )
        return catalog, candidates

    def issue_metric_action_token(
        self,
        scope: str,
        metric_id: str,
        *,
        expected_catalog: SemanticCatalog | None = None,
    ) -> str:
        """Issue a short-lived, source-bound selector for one metric candidate."""

        catalog = expected_catalog or self.load(scope)
        if catalog is None:
            return ""
        metric = catalog.metric(metric_id)
        if metric is None or metric.expression_kind != MetricExpressionKind.COLUMN:
            return ""
        return self._issue_action_token(
            scope=scope,
            action_kind="metric_map",
            object_id=metric.id,
            catalog=catalog,
            validation_mode="metric_projection",
            object_state_digest=_metric_action_digest(metric),
        )

    def _issue_action_token(
        self,
        *,
        scope: str,
        action_kind: str,
        object_id: str,
        catalog: SemanticCatalog,
        validation_mode: str = "catalog_revision",
        object_state_digest: str = "",
    ) -> str:
        token = secrets.token_urlsafe(24)
        if not _ACTION_TOKEN_RE.fullmatch(token):
            raise RuntimeError("server action token is not copy-safe")
        now = time.time()
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        action_key = f"{_ACTION_KEY_PREFIX}{digest}"
        record = {
            "scope": scope,
            "action_kind": action_kind,
            "object_id": object_id,
            "source_id": catalog.source_id,
            "connection_generation": catalog.connection_generation,
            "catalog_fingerprint": catalog.fingerprint,
            "catalog_review_revision": catalog.review_revision,
            "catalog_version": catalog.version,
            "classification_policy_version": catalog.classification_policy_version,
            "validation_mode": validation_mode,
            "object_state_digest": object_state_digest,
            "metric_action_epoch": catalog.metric_action_epoch,
            "dimension_action_epoch": catalog.dimension_action_epoch,
            "public_scope_epoch": catalog.public_scope_epoch,
            "issued_at": now,
            "expires_at": now + _ACTION_TTL_SECONDS,
        }
        expired: set[str] = set()
        for prefix in (
            _ACTION_KEY_PREFIX,
            _ACTION_RECEIPT_KEY_PREFIX,
            _ACTION_ARM_KEY_PREFIX,
        ):
            for key, raw in self._store.kv_list_prefix(scope, prefix):
                try:
                    expires_at = float(json.loads(raw).get("expires_at", 0))
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    expires_at = 0
                if expires_at <= now:
                    expired.add(key)
        if expired:
            self._store.kv_apply_atomic(scope, upserts={}, delete_keys=expired)
        expected_catalog_raw = catalog.to_json()

        def persist_if_current(snapshot: dict[str, str]):
            if snapshot.get(CATALOG_KEY) != expected_catalog_raw:
                return {}, set(), ""
            if catalog.source_id:
                try:
                    binding = ConnectionBinding.from_json(
                        snapshot[CONNECTION_BINDING_KEY]
                    )
                    generation = int(snapshot[CONNECTION_GENERATION_KEY])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    return {}, set(), ""
                if (
                    binding.source_id != catalog.source_id
                    or binding.generation != catalog.connection_generation
                    or generation != catalog.connection_generation
                ):
                    return {}, set(), ""
            elif (
                CONNECTION_BINDING_KEY in snapshot
                or CONNECTION_GENERATION_KEY in snapshot
            ):
                return {}, set(), ""
            return {action_key: json.dumps(record, sort_keys=True)}, set(), token

        return self._store.kv_mutate_snapshot(
            scope,
            keys={
                CATALOG_KEY,
                CONNECTION_BINDING_KEY,
                CONNECTION_GENERATION_KEY,
            },
            mutate=persist_if_current,
        )

    def _arm_catalog_action_token(
        self,
        *,
        scope: str,
        action_token: str,
        action_kind: str,
        reviewer_id: str,
        payload: dict[str, str],
    ) -> ReviewOutcome:
        """Bind a Discord warning step to the exact payload later confirmed."""

        token = action_token.strip()
        if not _ACTION_TOKEN_RE.fullmatch(token):
            return ReviewOutcome(
                "blocked",
                "후보 토큰이 유효하지 않습니다. 후보 목록을 새로 열어 주세요.",
            )
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        action_key = f"{_ACTION_KEY_PREFIX}{digest}"
        arm_key = f"{_ACTION_ARM_KEY_PREFIX}{digest}"
        now = time.time()

        def mutate(snapshot: dict[str, str]):
            action_raw = snapshot.get(action_key)
            if action_raw is None:
                return (
                    {},
                    set(),
                    ReviewOutcome(
                        "blocked",
                        "후보 토큰을 찾지 못했습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            try:
                action = json.loads(action_raw)
                expires_at = float(action.get("expires_at", 0))
            except (TypeError, ValueError, json.JSONDecodeError):
                return (
                    {},
                    {action_key, arm_key},
                    ReviewOutcome(
                        "blocked",
                        "후보 상태가 유효하지 않습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            if expires_at <= now:
                return (
                    {},
                    {action_key, arm_key},
                    ReviewOutcome(
                        "blocked",
                        "후보 토큰이 만료되었습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            if action.get("scope") != scope or action.get("action_kind") != action_kind:
                return (
                    {},
                    set(),
                    ReviewOutcome(
                        "blocked", "이 후보 토큰은 현재 작업에 사용할 수 없습니다."
                    ),
                )
            arm = {
                "action_kind": action_kind,
                "payload": payload,
                "reviewer_id": reviewer_id,
                "action_record_digest": hashlib.sha256(
                    action_raw.encode("utf-8")
                ).hexdigest(),
                "expires_at": min(expires_at, now + _ACTION_TTL_SECONDS),
            }
            return (
                {arm_key: json.dumps(arm, ensure_ascii=False, sort_keys=True)},
                set(),
                ReviewOutcome(
                    "confirmed", "표시된 작업 내용에 확인 토큰을 묶었습니다."
                ),
            )

        return self._store.kv_mutate_snapshot(
            scope,
            keys={action_key, arm_key},
            mutate=mutate,
        )

    def arm_metric_mapping(
        self,
        scope: str,
        action_token: str,
        phrase: str,
        assertion: StewardAssertion,
    ) -> ReviewOutcome:
        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "관리자 또는 steward 승인 권한이 필요합니다."
            )
        normalized, error = _validate_mapping_phrase(phrase, subject="지표")
        if error:
            return ReviewOutcome("blocked", error)
        return self._arm_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="metric_map",
            reviewer_id=assertion.reviewer_id,
            payload={"normalized_phrase": normalized},
        )

    def arm_dimension_mapping(
        self,
        scope: str,
        action_token: str,
        phrase: str,
        assertion: StewardAssertion,
    ) -> ReviewOutcome:
        """Bind a dimension-map warning to one reviewer and exact phrase."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "관리자 또는 steward 승인 권한이 필요합니다."
            )
        normalized, error = _validate_mapping_phrase(phrase, subject="분류")
        if error:
            return ReviewOutcome("blocked", error)
        return self._arm_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="dimension_map",
            reviewer_id=assertion.reviewer_id,
            payload={"normalized_phrase": normalized},
        )

    def arm_dimension_release(
        self,
        scope: str,
        action_token: str,
        disclosure_tier: str,
        assertion: StewardAssertion,
    ) -> ReviewOutcome:
        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        try:
            tier = DimensionDisclosureTier(disclosure_tier)
        except ValueError:
            return ReviewOutcome("blocked", "지원하지 않는 값 공개 등급입니다.")
        if tier not in {
            DimensionDisclosureTier.CONTROLLED_GROUPED,
            DimensionDisclosureTier.PUBLIC_GROUPED,
        }:
            return ReviewOutcome("blocked", "지원하지 않는 값 공개 등급입니다.")
        return self._arm_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="dimension_set_tier",
            reviewer_id=assertion.reviewer_id,
            payload={"disclosure_tier": tier.value},
        )

    def _consume_catalog_action_token(
        self,
        *,
        scope: str,
        action_token: str,
        action_kind: str,
        reviewer_id: str,
        payload: dict[str, str],
        idempotent_message: str,
        apply: Callable[[SemanticCatalog, str], tuple[ReviewOutcome, bool]],
        audit_scope: str = "",
        audit_action: str = "",
        audit_detail: dict[str, Any] | None = None,
        audit_object_key: str = "",
        require_armed_payload: bool = False,
    ) -> ReviewOutcome:
        """Validate, apply, consume, and receipt one catalog action atomically."""

        token = action_token.strip()
        if not _ACTION_TOKEN_RE.fullmatch(token):
            return ReviewOutcome(
                "blocked",
                "후보 토큰이 유효하지 않습니다. 후보 목록을 새로 열어 주세요.",
            )
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        action_key = f"{_ACTION_KEY_PREFIX}{digest}"
        receipt_key = f"{_ACTION_RECEIPT_KEY_PREFIX}{digest}"
        arm_key = f"{_ACTION_ARM_KEY_PREFIX}{digest}"
        now = time.time()

        def current_catalog(
            snapshot: dict[str, str], record: dict[str, Any]
        ) -> SemanticCatalog | None:
            try:
                catalog = SemanticCatalog.from_json(snapshot[CATALOG_KEY])
                record_generation = int(record.get("connection_generation", -1))
                record_revision = int(record.get("catalog_review_revision", -1))
                record_version = int(record.get("catalog_version", -1))
                record_policy = int(record.get("classification_policy_version", -1))
                validation_mode = str(record.get("validation_mode", "catalog_revision"))
            except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                return None
            if catalog.source_id:
                try:
                    binding = ConnectionBinding.from_json(
                        snapshot[CONNECTION_BINDING_KEY]
                    )
                    active_generation = int(snapshot[CONNECTION_GENERATION_KEY])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError):
                    return None
                if (
                    binding.source_id != catalog.source_id
                    or binding.generation != catalog.connection_generation
                    or active_generation != catalog.connection_generation
                ):
                    return None
            elif (
                CONNECTION_BINDING_KEY in snapshot
                or CONNECTION_GENERATION_KEY in snapshot
            ):
                return None
            if (
                record.get("source_id") != catalog.source_id
                or record_generation != catalog.connection_generation
                or record.get("catalog_fingerprint") != catalog.fingerprint
                or record_version != catalog.version
                or record_policy != catalog.classification_policy_version
            ):
                return None
            if validation_mode == "catalog_revision":
                if record_revision != catalog.review_revision:
                    return None
            elif validation_mode == "metric_projection":
                try:
                    record_metric_epoch = int(record.get("metric_action_epoch", -1))
                except (TypeError, ValueError):
                    return None
                metric = catalog.metric(str(record.get("object_id", "")))
                if (
                    metric is None
                    or record_metric_epoch != catalog.metric_action_epoch
                    or record.get("object_state_digest")
                    != _metric_action_digest(metric)
                ):
                    return None
            elif validation_mode == "dimension_projection":
                try:
                    record_dimension_epoch = int(
                        record.get("dimension_action_epoch", -1)
                    )
                    record_public_epoch = int(record.get("public_scope_epoch", -1))
                except (TypeError, ValueError):
                    return None
                dimension = catalog.dimension(str(record.get("object_id", "")))
                if (
                    dimension is None
                    or record_dimension_epoch != catalog.dimension_action_epoch
                    or record_public_epoch != catalog.public_scope_epoch
                    or record.get("object_state_digest")
                    != _dimension_action_digest(dimension)
                ):
                    return None
            else:
                return None
            return catalog

        def mutate(snapshot: dict[str, str]):
            receipt_raw = snapshot.get(receipt_key)
            if receipt_raw is not None:
                try:
                    receipt = json.loads(receipt_raw)
                    receipt_expires = float(receipt.get("expires_at", 0))
                except (TypeError, ValueError, json.JSONDecodeError):
                    return (
                        {},
                        {receipt_key},
                        ReviewOutcome(
                            "blocked",
                            "후보 사용 기록이 손상되었습니다. 후보 목록을 새로 열어 주세요.",
                        ),
                    )
                if receipt_expires <= now:
                    return (
                        {},
                        {receipt_key},
                        ReviewOutcome(
                            "blocked",
                            "후보 토큰이 만료되었습니다. 후보 목록을 새로 열어 주세요.",
                        ),
                    )
                if current_catalog(snapshot, receipt) is None:
                    return (
                        {},
                        {receipt_key},
                        ReviewOutcome(
                            "blocked",
                            "연결 또는 의미 검토 상태가 바뀌었습니다. 후보 목록을 새로 열어 주세요.",
                        ),
                    )
                if (
                    receipt.get("action_kind") == action_kind
                    and receipt.get("payload") == payload
                    and receipt.get("reviewer_id") == reviewer_id
                    and receipt.get("result") == "confirmed"
                ):
                    return (
                        {},
                        set(),
                        ReviewOutcome(
                            "confirmed",
                            idempotent_message,
                            object_id=str(receipt.get("object_id", "")),
                        ),
                    )
                return (
                    {},
                    set(),
                    ReviewOutcome(
                        "blocked", "이 후보 토큰은 이미 다른 요청에 사용되었습니다."
                    ),
                )

            action_raw = snapshot.get(action_key)
            if action_raw is None:
                return (
                    {},
                    set(),
                    ReviewOutcome(
                        "blocked",
                        "후보 토큰을 찾지 못했습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            try:
                action = json.loads(action_raw)
                expires_at = float(action.get("expires_at", 0))
            except (TypeError, ValueError, json.JSONDecodeError):
                return (
                    {},
                    {action_key, arm_key},
                    ReviewOutcome(
                        "blocked",
                        "후보 상태가 유효하지 않습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            if expires_at <= now:
                return (
                    {},
                    {action_key, arm_key},
                    ReviewOutcome(
                        "blocked",
                        "후보 토큰이 만료되었습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            if action.get("scope") != scope or action.get("action_kind") != action_kind:
                return (
                    {},
                    {action_key, arm_key},
                    ReviewOutcome(
                        "blocked", "이 후보 토큰은 현재 작업에 사용할 수 없습니다."
                    ),
                )
            if require_armed_payload:
                arm_raw = snapshot.get(arm_key)
                if arm_raw is None:
                    return (
                        {},
                        set(),
                        ReviewOutcome(
                            "blocked",
                            "먼저 confirm:false로 표시된 작업 내용을 확인해 주세요.",
                        ),
                    )
                try:
                    arm = json.loads(arm_raw)
                    arm_expires_at = float(arm.get("expires_at", 0))
                except (TypeError, ValueError, json.JSONDecodeError):
                    return (
                        {},
                        {arm_key},
                        ReviewOutcome(
                            "blocked",
                            "확인 단계가 손상되었습니다. 경고를 다시 열어 주세요.",
                        ),
                    )
                if arm_expires_at <= now:
                    return (
                        {},
                        {arm_key},
                        ReviewOutcome(
                            "blocked",
                            "확인 단계가 만료되었습니다. 경고를 다시 열어 주세요.",
                        ),
                    )
                if (
                    arm.get("action_kind") != action_kind
                    or arm.get("payload") != payload
                    or arm.get("reviewer_id") != reviewer_id
                    or arm.get("action_record_digest")
                    != hashlib.sha256(action_raw.encode("utf-8")).hexdigest()
                ):
                    return (
                        {},
                        set(),
                        ReviewOutcome(
                            "blocked",
                            "경고에서 확인한 작업 내용과 최종 요청이 다릅니다. 경고를 다시 열어 주세요.",
                        ),
                    )
            catalog = current_catalog(snapshot, action)
            if catalog is None:
                return (
                    {},
                    {action_key, arm_key},
                    ReviewOutcome(
                        "blocked",
                        "연결 또는 의미 검토 상태가 바뀌었습니다. 후보 목록을 새로 열어 주세요.",
                    ),
                )
            outcome, changed = apply(catalog, str(action.get("object_id", "")))
            if outcome.status != "confirmed":
                return {}, {action_key, arm_key}, outcome
            outcome.source_id = catalog.source_id
            outcome.connection_generation = catalog.connection_generation
            outcome.mutation_applied = changed
            outcome.object_id = str(action.get("object_id", ""))
            validation_mode = str(action.get("validation_mode", "catalog_revision"))
            object_state_digest = ""
            if validation_mode == "metric_projection":
                metric = catalog.metric(outcome.object_id)
                if metric is None:
                    return (
                        {},
                        {action_key, arm_key},
                        ReviewOutcome(
                            "blocked",
                            "지표 후보 상태를 확인할 수 없습니다. 후보 목록을 새로 열어 주세요.",
                        ),
                    )
                object_state_digest = _metric_action_digest(metric)
            elif validation_mode == "dimension_projection":
                dimension = catalog.dimension(outcome.object_id)
                if dimension is None:
                    return (
                        {},
                        {action_key, arm_key},
                        ReviewOutcome(
                            "blocked",
                            "차원 후보 상태를 확인할 수 없습니다. 후보 목록을 새로 열어 주세요.",
                        ),
                    )
                object_state_digest = _dimension_action_digest(dimension)
            receipt = {
                "action_kind": action_kind,
                "object_id": str(action.get("object_id", "")),
                "payload": payload,
                "reviewer_id": reviewer_id,
                "result": "confirmed",
                "source_id": catalog.source_id,
                "connection_generation": catalog.connection_generation,
                "catalog_fingerprint": catalog.fingerprint,
                "catalog_review_revision": catalog.review_revision,
                "catalog_version": catalog.version,
                "classification_policy_version": catalog.classification_policy_version,
                "validation_mode": validation_mode,
                "object_state_digest": object_state_digest,
                "metric_action_epoch": catalog.metric_action_epoch,
                "dimension_action_epoch": catalog.dimension_action_epoch,
                "public_scope_epoch": catalog.public_scope_epoch,
                "expires_at": now + _ACTION_TTL_SECONDS,
            }
            upserts = {receipt_key: json.dumps(receipt, sort_keys=True)}
            if changed:
                upserts[CATALOG_KEY] = catalog.to_json()
            audit_event = None
            if changed and audit_scope and audit_action:
                detail = dict(audit_detail or {})
                if audit_object_key:
                    detail[audit_object_key] = outcome.object_id
                detail.update(
                    {
                        "source_id": catalog.source_id,
                        "connection_generation": catalog.connection_generation,
                        "catalog_fingerprint": catalog.fingerprint,
                        "catalog_review_revision": catalog.review_revision,
                        "catalog_version": catalog.version,
                        "classification_policy_version": (
                            catalog.classification_policy_version
                        ),
                    }
                )
                audit_event = AuditEvent(
                    actor=reviewer_id,
                    action=audit_action,
                    scope=audit_scope,
                    detail=detail,
                )
            return upserts, {action_key, arm_key}, outcome, audit_event

        return self._store.kv_mutate_snapshot(
            scope,
            keys={
                action_key,
                arm_key,
                receipt_key,
                CATALOG_KEY,
                CONNECTION_BINDING_KEY,
                CONNECTION_GENERATION_KEY,
            },
            mutate=mutate,
        )

    def map_metric_phrase(
        self,
        scope: str,
        action_token: str,
        phrase: str,
        assertion: StewardAssertion,
        *,
        audit_scope: str = "",
        require_armed_payload: bool = False,
    ) -> ReviewOutcome:
        """Consume one source-bound candidate token and bind a metric phrase."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "관리자 또는 steward 승인 권한이 필요합니다."
            )
        normalized, error = _validate_mapping_phrase(phrase, subject="지표")
        if error:
            return ReviewOutcome("blocked", error)

        def apply(catalog: SemanticCatalog, metric_id: str):
            metric = catalog.metric(metric_id)
            if metric is None or metric.expression_kind != MetricExpressionKind.COLUMN:
                return (
                    ReviewOutcome("blocked", "연결 가능한 수치 지표 후보가 아닙니다."),
                    False,
                )
            rejected_bindings = {
                item.rpartition("|")[0]
                for item in metric.rejected_bindings
                if "|" in item
            }
            if normalized in metric.rejected_aliases or normalized in rejected_bindings:
                return (
                    ReviewOutcome(
                        "blocked",
                        "이 표현은 이전 검토에서 거절되었습니다. 명시적으로 검토를 초기화한 뒤 다시 연결해 주세요.",
                    ),
                    False,
                )
            if _metric_binding_conflict(catalog, metric.id, normalized):
                return (
                    ReviewOutcome(
                        "blocked", "이 표현은 이미 다른 지표에 연결되어 있습니다."
                    ),
                    False,
                )
            changed = normalized not in metric.aliases
            if changed:
                _append_alias(metric.aliases, normalized)
                metric.alias_reviewers[normalized] = assertion.reviewer_id
                catalog.review_revision += 1
            return (
                ReviewOutcome(
                    "confirmed",
                    (
                        "지표 표현 후보를 저장했습니다. SUM/AVG 같은 집계 의미는 실제 질문에서 별도 확인합니다."
                        if changed
                        else "이 표현은 이미 해당 지표 후보에 연결되어 있습니다. 집계 방식은 질문에서 별도 확인합니다."
                    ),
                ),
                changed,
            )

        return self._consume_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="metric_map",
            reviewer_id=assertion.reviewer_id,
            payload={"normalized_phrase": normalized},
            idempotent_message=(
                "이 후보와 같은 업무 표현은 이미 저장되었습니다. 집계 방식은 질문에서 별도 확인합니다."
            ),
            apply=apply,
            audit_scope=audit_scope,
            audit_action="semantic_metric_map",
            audit_detail={"phrase_length": len(phrase)},
            audit_object_key="metric_id",
            require_armed_payload=require_armed_payload,
        )

    def map_dimension_phrase(
        self,
        scope: str,
        action_token: str,
        phrase: str,
        assertion: StewardAssertion,
        *,
        audit_scope: str = "",
        require_armed_payload: bool = False,
    ) -> ReviewOutcome:
        """Consume one source-bound token and bind an opaque dimension phrase."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "관리자 또는 steward 승인 권한이 필요합니다."
            )
        normalized, error = _validate_mapping_phrase(phrase, subject="분류")
        if error:
            return ReviewOutcome("blocked", error)

        def apply(catalog: SemanticCatalog, dimension_id: str):
            dimension = catalog.dimension(dimension_id)
            if dimension is None:
                return (
                    ReviewOutcome(
                        "blocked", "연결 가능한 비차단 분류 차원이 아닙니다."
                    ),
                    False,
                )
            if normalized in dimension.rejected_aliases:
                return (
                    ReviewOutcome(
                        "blocked",
                        "이 표현은 이전 검토에서 거절되었습니다. 명시적으로 검토를 초기화한 뒤 다시 연결해 주세요.",
                    ),
                    False,
                )
            conflict = _dimension_alias_conflict(catalog, dimension.id, normalized)
            if conflict:
                return (
                    ReviewOutcome(
                        "blocked", "이 표현은 이미 다른 분류 기준에 연결되어 있습니다."
                    ),
                    False,
                )
            changed = normalized not in dimension.aliases
            if changed:
                _append_alias(dimension.aliases, normalized)
                dimension.alias_reviewers[normalized] = assertion.reviewer_id
                catalog.review_revision += 1
            return (
                ReviewOutcome(
                    "confirmed",
                    (
                        "분류 표현 후보를 저장했습니다. 그룹 값 공개 등급은 별도로 승인해야 합니다."
                        if changed
                        else "이 표현은 이미 해당 분류 후보에 연결되어 있습니다. 값 공개 등급은 별도 정책입니다."
                    ),
                ),
                changed,
            )

        return self._consume_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="dimension_map",
            reviewer_id=assertion.reviewer_id,
            payload={"normalized_phrase": normalized},
            idempotent_message=(
                "이 후보와 같은 분류 표현은 이미 저장되었습니다. 값 공개 등급은 별도 정책입니다."
            ),
            apply=apply,
            audit_scope=audit_scope,
            audit_action="semantic_dimension_map",
            audit_detail={"phrase_length": len(phrase)},
            audit_object_key="dimension_id",
            require_armed_payload=require_armed_payload,
        )

    def release_candidates(
        self, scope: str, *, include_released: bool = False
    ) -> list[DimensionSpec]:
        """Return metadata-only candidates; callers must enforce steward access."""

        _catalog, candidates = self.dimension_candidate_snapshot(
            scope, include_released=include_released
        )
        return candidates

    def dimension_candidate_snapshot(
        self, scope: str, *, include_released: bool = False
    ) -> tuple[SemanticCatalog | None, list[DimensionSpec]]:
        """Read disclosure candidates and their action-token stamp together."""

        catalog = self.load(scope)
        if catalog is None:
            return None, []
        candidates = sorted(
            (
                item
                for item in catalog.dimensions
                if item.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
                and (include_released or not item.raw_output_allowed)
            ),
            key=lambda item: item.id,
        )
        return catalog, candidates

    def dimension_mapping_candidate_snapshot(
        self, scope: str
    ) -> tuple[SemanticCatalog | None, list[DimensionSpec]]:
        """Return every non-blocked catalog dimension for phrase stewardship."""

        catalog = self.load(scope)
        if catalog is None:
            return None, []
        # Blocked physical columns never become DimensionSpec objects during
        # onboarding, so this list cannot reintroduce PII/safety exclusions.
        return catalog, sorted(catalog.dimensions, key=lambda item: item.id)

    def issue_dimension_action_token(
        self,
        scope: str,
        dimension_id: str,
        action_kind: str,
        *,
        expected_catalog: SemanticCatalog | None = None,
    ) -> str:
        """Issue a source-bound selector for one release or revoke action."""

        if action_kind not in {
            "dimension_map",
            "dimension_set_tier",
            "dimension_revoke",
        }:
            return ""
        catalog = expected_catalog or self.load(scope)
        if catalog is None:
            return ""
        dimension = catalog.dimension(dimension_id)
        if dimension is None:
            return ""
        if (
            action_kind != "dimension_map"
            and dimension.review_policy != DimensionReviewPolicy.RELEASE_REQUIRED
        ):
            return ""
        if action_kind == "dimension_revoke" and not dimension.raw_output_allowed:
            return ""
        return self._issue_action_token(
            scope=scope,
            action_kind=action_kind,
            object_id=dimension.id,
            catalog=catalog,
            validation_mode="dimension_projection",
            object_state_digest=_dimension_action_digest(dimension),
        )

    def issue_catalog_action_token(self, scope: str, action_kind: str) -> str:
        """Issue a source-bound selector for one dataset-wide mutation."""

        if action_kind not in {
            "public_data_confirm",
            "public_data_revoke",
            "review_reset",
        }:
            return ""
        catalog = self.load(scope)
        if catalog is None:
            return ""
        return self._issue_action_token(
            scope=scope,
            action_kind=action_kind,
            object_id="catalog",
            catalog=catalog,
        )

    def discard_action_token(self, scope: str, action_token: str) -> None:
        """Retire every persisted artifact for an uncommitted internal action."""

        token = action_token.strip()
        if not _ACTION_TOKEN_RE.fullmatch(token):
            return
        digest = hashlib.sha256(token.encode("ascii")).hexdigest()
        self._store.kv_apply_atomic(
            scope,
            upserts={},
            delete_keys={
                f"{_ACTION_KEY_PREFIX}{digest}",
                f"{_ACTION_ARM_KEY_PREFIX}{digest}",
                f"{_ACTION_RECEIPT_KEY_PREFIX}{digest}",
            },
        )

    def _apply_public_data_scope(
        self,
        catalog: SemanticCatalog,
        assertion: StewardAssertion,
        *,
        enable: bool,
    ) -> tuple[ReviewOutcome, bool]:
        if (
            not assertion.authorized
            or not assertion.reviewer_id
            or (enable and not assertion.public_data_confirmed)
        ):
            return (
                ReviewOutcome(
                    "blocked",
                    (
                        "전체 데이터와 지표 값이 공개·비개인이라는 steward assertion이 필요합니다."
                        if enable
                        else "권한이 확인된 steward assertion이 필요합니다."
                    ),
                ),
                False,
            )
        if enable:
            if (
                catalog.public_data_scope
                and catalog.public_data_reviewer == assertion.reviewer_id
                and catalog.public_data_fingerprint == catalog.fingerprint
            ):
                return (
                    ReviewOutcome(
                        "confirmed",
                        "현재 연결은 이미 공개 데이터 범위로 확인되어 있습니다.",
                    ),
                    False,
                )
            catalog.public_data_scope = True
            catalog.public_data_reviewer = assertion.reviewer_id
            catalog.public_data_fingerprint = catalog.fingerprint
            catalog.public_data_confirmed_at = datetime.now(timezone.utc).isoformat()
            message = (
                "현재 연결 전체를 공개 데이터 범위로 확인했습니다. public_grouped "
                "등급을 선택한 차원에만 최소 그룹 해제가 적용됩니다."
            )
        else:
            public_dimensions = [
                dimension
                for dimension in catalog.dimensions
                if dimension.disclosure_tier == DimensionDisclosureTier.PUBLIC_GROUPED
            ]
            if not catalog.public_data_scope and not public_dimensions:
                return (
                    ReviewOutcome(
                        "confirmed", "현재 연결은 이미 공개 데이터 범위가 아닙니다."
                    ),
                    False,
                )
            catalog.public_data_scope = False
            catalog.public_data_reviewer = ""
            catalog.public_data_fingerprint = ""
            catalog.public_data_confirmed_at = ""
            for dimension in public_dimensions:
                dimension.disclosure_tier = (
                    DimensionDisclosureTier.CONTROLLED_GROUPED
                    if dimension.raw_output_allowed
                    else DimensionDisclosureTier.BLOCKED
                )
                dimension.action_revision += 1
            message = (
                "공개 데이터 범위를 철회하고 공개 차원을 보호 그룹 등급으로 낮췄습니다."
            )
        catalog.public_scope_epoch += 1
        catalog.review_revision += 1
        return ReviewOutcome("confirmed", message), True

    def _commit_catalog_outcome(
        self,
        scope: str,
        catalog: SemanticCatalog,
        outcome: ReviewOutcome,
        changed: bool,
        stale_message: str,
    ) -> ReviewOutcome:
        if outcome.status != "confirmed" or not changed:
            return outcome
        if not self._commit_review_catalog(scope, catalog):
            return ReviewOutcome("blocked", stale_message)
        outcome.mutation_applied = True
        outcome.source_id = catalog.source_id
        outcome.connection_generation = catalog.connection_generation
        return outcome

    def confirm_public_data_scope(
        self, scope: str, assertion: StewardAssertion
    ) -> ReviewOutcome:
        """Assert that the connected dataset, including metric values, is public."""

        catalog = self.load(scope)
        if catalog is None:
            return ReviewOutcome("blocked", "공개 데이터로 확인할 카탈로그가 없습니다.")
        if assertion.scope != scope:
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        outcome, changed = self._apply_public_data_scope(
            catalog, assertion, enable=True
        )
        return self._commit_catalog_outcome(
            scope,
            catalog,
            outcome,
            changed,
            "연결 또는 의미 검토 상태가 바뀌어 공개 범위를 저장하지 않았습니다.",
        )

    def revoke_public_data_scope(
        self, scope: str, assertion: StewardAssertion
    ) -> ReviewOutcome:
        catalog = self.load(scope)
        if catalog is None:
            return ReviewOutcome("blocked", "공개 범위를 철회할 카탈로그가 없습니다.")
        if assertion.scope != scope:
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        outcome, changed = self._apply_public_data_scope(
            catalog, assertion, enable=False
        )
        return self._commit_catalog_outcome(
            scope,
            catalog,
            outcome,
            changed,
            "연결 또는 의미 검토 상태가 바뀌어 공개 범위를 철회하지 않았습니다.",
        )

    def set_public_data_scope_with_token(
        self,
        scope: str,
        action_token: str,
        assertion: StewardAssertion,
        *,
        enable: bool,
        audit_scope: str = "",
    ) -> ReviewOutcome:
        """Apply a dataset-wide public assertion against the viewed snapshot."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
            or (enable and not assertion.public_data_confirmed)
        ):
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        action_kind = "public_data_confirm" if enable else "public_data_revoke"
        return self._consume_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind=action_kind,
            reviewer_id=assertion.reviewer_id,
            payload={"enable": str(enable).lower()},
            idempotent_message=("같은 공개 데이터 범위 변경은 이미 적용되었습니다."),
            apply=lambda catalog, _object_id: self._apply_public_data_scope(
                catalog, assertion, enable=enable
            ),
            audit_scope=audit_scope,
            audit_action=(
                "semantic_public_data_confirm"
                if enable
                else "semantic_public_data_revoke"
            ),
            audit_detail={"enabled": enable},
        )

    def release_dimension(
        self,
        scope: str,
        dimension_id: str,
        assertion: StewardAssertion,
        disclosure_tier: str = DimensionDisclosureTier.CONTROLLED_GROUPED.value,
    ) -> ReviewOutcome:
        """Approve grouped raw labels after an external admin/steward check."""

        catalog = self.load(scope)
        if catalog is None:
            return ReviewOutcome("blocked", "공개 검토할 의미 카탈로그가 없습니다.")
        if assertion.scope != scope:
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        outcome, changed = self._apply_dimension_release(
            catalog, dimension_id, assertion, disclosure_tier
        )
        return self._commit_catalog_outcome(
            scope,
            catalog,
            outcome,
            changed,
            "연결 또는 의미 검토 상태가 바뀌어 차원 공개를 저장하지 않았습니다.",
        )

    def _apply_dimension_release(
        self,
        catalog: SemanticCatalog,
        dimension_id: str,
        assertion: StewardAssertion,
        disclosure_tier: str,
    ) -> tuple[ReviewOutcome, bool]:
        dimension = catalog.dimension(dimension_id)
        if dimension is None:
            return (
                ReviewOutcome("blocked", "해당 차원 후보가 존재하지 않습니다."),
                False,
            )
        if dimension.review_policy != DimensionReviewPolicy.RELEASE_REQUIRED:
            return (
                ReviewOutcome(
                    "blocked", "이 차원은 별도 공개 승인이 필요하지 않습니다."
                ),
                False,
            )
        if not assertion.authorized or not assertion.reviewer_id:
            return (
                ReviewOutcome(
                    "blocked", "권한이 확인된 steward assertion이 필요합니다."
                ),
                False,
            )
        try:
            tier = DimensionDisclosureTier(disclosure_tier)
        except ValueError:
            return ReviewOutcome("blocked", "지원하지 않는 값 공개 등급입니다."), False
        if tier not in {
            DimensionDisclosureTier.CONTROLLED_GROUPED,
            DimensionDisclosureTier.PUBLIC_GROUPED,
        }:
            return (
                ReviewOutcome("blocked", "blocked 등급은 공개 승인이 아닙니다."),
                False,
            )
        if tier == DimensionDisclosureTier.PUBLIC_GROUPED and not (
            assertion.public_data_confirmed
            and catalog.public_data_scope
            and catalog.public_data_fingerprint == catalog.fingerprint
        ):
            return (
                ReviewOutcome(
                    "blocked",
                    "public_grouped는 먼저 연결 전체를 공개 데이터 범위로 확인해야 합니다.",
                ),
                False,
            )
        if dimension.raw_output_allowed and dimension.disclosure_tier == tier:
            return (
                ReviewOutcome("confirmed", "이미 같은 등급으로 공개된 차원입니다."),
                False,
            )
        dimension.raw_output_allowed = True
        dimension.disclosure_tier = tier
        dimension.release_reviewer = assertion.reviewer_id
        dimension.release_catalog_fingerprint = catalog.fingerprint
        dimension.released_at = datetime.now(timezone.utc).isoformat()
        dimension.action_revision += 1
        catalog.review_revision += 1
        return (
            ReviewOutcome(
                "confirmed",
                (
                    f"`{dimension.id}`의 그룹 값을 `{tier.value}` 등급으로 Discord "
                    "결과에 표시할 수 있도록 승인했습니다. 질문 표현 연결은 실제 "
                    "질문에서 별도로 확인합니다."
                ),
            ),
            True,
        )

    def release_dimension_with_token(
        self,
        scope: str,
        action_token: str,
        assertion: StewardAssertion,
        disclosure_tier: str = DimensionDisclosureTier.CONTROLLED_GROUPED.value,
        *,
        audit_scope: str = "",
        require_armed_payload: bool = False,
    ) -> ReviewOutcome:
        """Release only the dimension selected from the current candidate view."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        return self._consume_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="dimension_set_tier",
            reviewer_id=assertion.reviewer_id,
            payload={"disclosure_tier": disclosure_tier},
            idempotent_message="같은 차원 공개 승인은 이미 적용되었습니다.",
            apply=lambda catalog, dimension_id: self._apply_dimension_release(
                catalog, dimension_id, assertion, disclosure_tier
            ),
            audit_scope=audit_scope,
            audit_action="semantic_dimension_release",
            audit_detail={"disclosure_tier": disclosure_tier},
            audit_object_key="dimension_id",
            require_armed_payload=require_armed_payload,
        )

    def revoke_dimension(
        self, scope: str, dimension_id: str, assertion: StewardAssertion
    ) -> ReviewOutcome:
        """Revoke grouped-label disclosure without deleting physical metadata."""

        catalog = self.load(scope)
        if catalog is None:
            return ReviewOutcome("blocked", "공개 철회할 의미 카탈로그가 없습니다.")
        if assertion.scope != scope:
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        outcome, changed = self._apply_dimension_revoke(
            catalog, dimension_id, assertion
        )
        return self._commit_catalog_outcome(
            scope,
            catalog,
            outcome,
            changed,
            "연결 또는 의미 검토 상태가 바뀌어 차원 철회를 저장하지 않았습니다.",
        )

    def _apply_dimension_revoke(
        self,
        catalog: SemanticCatalog,
        dimension_id: str,
        assertion: StewardAssertion,
    ) -> tuple[ReviewOutcome, bool]:
        dimension = catalog.dimension(dimension_id)
        if (
            dimension is None
            or dimension.review_policy != DimensionReviewPolicy.RELEASE_REQUIRED
        ):
            return (
                ReviewOutcome("blocked", "해당 공개 검토 차원이 존재하지 않습니다."),
                False,
            )
        if not assertion.authorized or not assertion.reviewer_id:
            return (
                ReviewOutcome(
                    "blocked", "권한이 확인된 steward assertion이 필요합니다."
                ),
                False,
            )
        if not dimension.raw_output_allowed:
            return ReviewOutcome("confirmed", "이미 공개되지 않은 차원입니다."), False
        dimension.raw_output_allowed = False
        dimension.disclosure_tier = DimensionDisclosureTier.BLOCKED
        dimension.release_reviewer = ""
        dimension.release_catalog_fingerprint = ""
        dimension.released_at = ""
        dimension.action_revision += 1
        # Existing requester aliases remain recorded but are not selectable or
        # executable until a steward releases the dimension again.
        catalog.review_revision += 1
        return (
            ReviewOutcome(
                "confirmed",
                f"`{dimension.id}`의 그룹 값 공개를 철회했습니다.",
            ),
            True,
        )

    def revoke_dimension_with_token(
        self,
        scope: str,
        action_token: str,
        assertion: StewardAssertion,
        *,
        audit_scope: str = "",
    ) -> ReviewOutcome:
        """Revoke only the dimension selected from the current candidate view."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        return self._consume_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="dimension_revoke",
            reviewer_id=assertion.reviewer_id,
            payload={},
            idempotent_message="같은 차원 공개 철회는 이미 적용되었습니다.",
            apply=lambda catalog, dimension_id: self._apply_dimension_revoke(
                catalog, dimension_id, assertion
            ),
            audit_scope=audit_scope,
            audit_action="semantic_dimension_revoke",
            audit_object_key="dimension_id",
        )

    def reset_reviews(self, scope: str) -> ReviewOutcome:
        """Remove human semantic decisions while preserving physical catalog facts."""

        catalog = self.load(scope)
        if catalog is None:
            return ReviewOutcome("blocked", "초기화할 의미 카탈로그가 없습니다.")
        outcome, changed = self._apply_reset_reviews(catalog)
        return self._commit_catalog_outcome(
            scope,
            catalog,
            outcome,
            changed,
            "연결 또는 의미 검토 상태가 바뀌어 초기화하지 않았습니다.",
        )

    def _apply_reset_reviews(
        self, catalog: SemanticCatalog
    ) -> tuple[ReviewOutcome, bool]:
        for metric in catalog.metrics:
            metric.aliases = list(metric.auto_aliases)
            metric.rejected_aliases = []
            metric.rejected_bindings = []
            metric.binding_reviewers = {}
            metric.alias_reviewers = {}
            if metric.source_record_count:
                metric.reviewed_bindings = {
                    alias: [Aggregate.COUNT.value] for alias in metric.auto_aliases
                }
                metric.aggregate = Aggregate.COUNT
                metric.state = ReviewState.CONFIRMED
            else:
                metric.reviewed_bindings = {}
                metric.aggregate = None
                metric.state = ReviewState.PENDING
        for dimension in catalog.dimensions:
            dimension.aliases = list(dimension.auto_aliases)
            dimension.rejected_aliases = []
            dimension.alias_reviewers = {}
            if dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED:
                # Disclosure approval is a human decision too; a semantic
                # reset must not leave grouped raw labels silently enabled.
                dimension.action_revision += 1
                dimension.raw_output_allowed = False
                dimension.disclosure_tier = DimensionDisclosureTier.BLOCKED
                dimension.release_reviewer = ""
                dimension.release_catalog_fingerprint = ""
                dimension.released_at = ""
        catalog.public_data_scope = False
        catalog.public_data_reviewer = ""
        catalog.public_data_fingerprint = ""
        catalog.public_data_confirmed_at = ""
        catalog.metric_action_epoch += 1
        catalog.dimension_action_epoch += 1
        catalog.public_scope_epoch += 1
        # The physical fingerprint intentionally stays stable across a review
        # reset. A separate revision invalidates confirmations created before
        # the reset without pretending that the DB structure changed.
        catalog.review_revision += 1
        return (
            ReviewOutcome(
                "confirmed",
                "사람이 확인한 표현·집계 연결, 문자열 차원 공개 승인, 공개 데이터 범위를 "
                "초기화했습니다. 물리 catalog와 PII 차단은 유지됩니다.",
            ),
            True,
        )

    def reset_reviews_with_token(
        self,
        scope: str,
        action_token: str,
        assertion: StewardAssertion,
        *,
        audit_scope: str = "",
    ) -> ReviewOutcome:
        """Reset reviews only if the warned-about catalog is still current."""

        if (
            not assertion.authorized
            or assertion.scope != scope
            or not assertion.reviewer_id
        ):
            return ReviewOutcome(
                "blocked", "권한이 확인된 steward assertion이 필요합니다."
            )
        return self._consume_catalog_action_token(
            scope=scope,
            action_token=action_token,
            action_kind="review_reset",
            reviewer_id=assertion.reviewer_id,
            payload={},
            idempotent_message="같은 의미 검토 초기화는 이미 적용되었습니다.",
            apply=lambda catalog, _object_id: self._apply_reset_reviews(catalog),
            audit_scope=audit_scope,
            audit_action="semantic_review_reset",
        )

    @staticmethod
    def _catalog_from_review_snapshot(
        scope: str, snapshot: dict[tuple[str, str], str]
    ) -> SemanticCatalog | None:
        try:
            catalog = SemanticCatalog.from_json(snapshot[(scope, CATALOG_KEY)])
        except (KeyError, TypeError, ValueError):
            return None
        binding_raw = snapshot.get((scope, CONNECTION_BINDING_KEY))
        generation_raw = snapshot.get((scope, CONNECTION_GENERATION_KEY))
        if not catalog.source_id:
            return catalog if binding_raw is None and generation_raw is None else None
        try:
            binding = ConnectionBinding.from_json(str(binding_raw))
            generation = int(str(generation_raw))
        except (TypeError, ValueError):
            return None
        if (
            binding.source_id != catalog.source_id
            or binding.generation != catalog.connection_generation
            or generation != catalog.connection_generation
        ):
            return None
        return catalog

    @staticmethod
    def _review_receipt_result(
        catalog: SemanticCatalog,
        raw: str | None,
        *,
        review_id: str,
        choice: str,
        reviewer_id: str,
    ) -> ReviewOutcome | None:
        if raw is None:
            return None
        try:
            receipt = json.loads(raw)
            receipt_generation = int(receipt.get("connection_generation", -1))
            receipt_revision = int(receipt.get("catalog_review_revision", -1))
            receipt_version = int(receipt.get("catalog_version", -1))
            receipt_policy = int(receipt.get("classification_policy_version", -1))
        except (TypeError, ValueError, json.JSONDecodeError):
            return ReviewOutcome("blocked", "의미 검토 사용 기록이 손상되었습니다.")
        if (
            receipt.get("review_id") != review_id
            or receipt.get("source_id") != catalog.source_id
            or receipt_generation != catalog.connection_generation
            or receipt.get("catalog_fingerprint") != catalog.fingerprint
            or receipt_revision != catalog.review_revision
            or receipt_version != catalog.version
            or receipt_policy != catalog.classification_policy_version
        ):
            return ReviewOutcome(
                "blocked",
                "연결 또는 의미 검토 상태가 바뀌어 이전 결정을 재사용할 수 없습니다.",
            )
        if (
            receipt.get("reviewer_id") != (reviewer_id or "unknown")
            or receipt.get("choice") != choice
        ):
            return ReviewOutcome(
                "blocked", "이 확인 요청 ID는 이미 다른 결정에 사용되었습니다."
            )
        return ReviewOutcome(
            "confirmed",
            "같은 의미 검토 결정은 이미 저장되었습니다.",
            source_id=catalog.source_id,
            connection_generation=catalog.connection_generation,
            requester_id=str(receipt.get("requester_id", "")),
            review_id=review_id,
            mutation_applied=False,
        )

    def _replay_review_receipt(
        self,
        scope: str,
        review_id: str,
        choice: str,
        reviewer_id: str,
    ) -> ReviewOutcome | None:
        digest = hashlib.sha256(review_id.encode("utf-8")).hexdigest()
        receipt_key = f"{_REVIEW_RECEIPT_KEY_PREFIX}{digest}"
        entries = {
            (scope, CATALOG_KEY),
            (scope, CONNECTION_BINDING_KEY),
            (scope, CONNECTION_GENERATION_KEY),
            (scope, receipt_key),
        }

        def mutate(snapshot: dict[tuple[str, str], str]):
            catalog = self._catalog_from_review_snapshot(scope, snapshot)
            if catalog is None:
                return (
                    {},
                    set(),
                    ReviewOutcome("blocked", "현재 의미 카탈로그가 유효하지 않습니다."),
                )
            return (
                {},
                set(),
                self._review_receipt_result(
                    catalog,
                    snapshot.get((scope, receipt_key)),
                    review_id=review_id,
                    choice=choice,
                    reviewer_id=reviewer_id,
                ),
            )

        return self._store.kv_mutate_scoped_snapshot(entries=entries, mutate=mutate)

    def confirm_pending(
        self,
        scope: str,
        review_scope: str,
        choice: str,
        reviewer_id: str = "",
        *,
        allow_cross_requester: bool = False,
        expected_review_id: str = "",
        audit_scope: str = "",
    ) -> ReviewOutcome:
        """Commit catalog decision, pending consume, receipt, and audit atomically."""

        normalized = choice.strip().lower()
        pending_record = self._pending_review_record(review_scope)
        if pending_record is None:
            if expected_review_id:
                replay = self._replay_review_receipt(
                    scope,
                    expected_review_id.strip(),
                    normalized,
                    reviewer_id,
                )
                if replay is not None:
                    return replay
            return ReviewOutcome("blocked", "현재 확인할 항목이 없습니다.")
        pending, pending_raw = pending_record
        resume = self._pending_draft(review_scope, pending.review_id)
        if expected_review_id and (
            not pending.review_id
            or not secrets.compare_digest(pending.review_id, expected_review_id.strip())
        ):
            return ReviewOutcome("blocked", "확인 요청 ID가 일치하지 않습니다.")
        receipt_id = pending.review_id or expected_review_id.strip()
        receipt_key = (
            f"{_REVIEW_RECEIPT_KEY_PREFIX}"
            f"{hashlib.sha256(receipt_id.encode('utf-8')).hexdigest()}"
            if receipt_id
            else ""
        )
        pending_entry = (review_scope, PENDING_REVIEW_KEY)
        catalog_entry = (scope, CATALOG_KEY)
        entries = {
            catalog_entry,
            (scope, CONNECTION_BINDING_KEY),
            (scope, CONNECTION_GENERATION_KEY),
            pending_entry,
        }
        if receipt_key:
            entries.add((scope, receipt_key))

        def mutate(snapshot: dict[tuple[str, str], str]):
            catalog = self._catalog_from_review_snapshot(scope, snapshot)
            if catalog is None:
                return (
                    {},
                    set(),
                    ReviewOutcome("blocked", "현재 의미 카탈로그가 유효하지 않습니다."),
                )
            if receipt_key:
                replay = self._review_receipt_result(
                    catalog,
                    snapshot.get((scope, receipt_key)),
                    review_id=receipt_id,
                    choice=normalized,
                    reviewer_id=reviewer_id,
                )
                if replay is not None:
                    return {}, set(), replay
            if snapshot.get(pending_entry) != pending_raw:
                return (
                    {},
                    set(),
                    ReviewOutcome(
                        "blocked",
                        "확인 요청이 바뀌어 이 결정을 저장하지 않았습니다. 다시 확인해 주세요.",
                    ),
                )
            try:
                current_pending = PendingReview.from_json(pending_raw)
            except (KeyError, TypeError, ValueError):
                return (
                    {},
                    {pending_entry},
                    ReviewOutcome(
                        "blocked", "확인 요청이 손상되어 안전하게 폐기했습니다."
                    ),
                )
            if expected_review_id and (
                not current_pending.review_id
                or not secrets.compare_digest(
                    current_pending.review_id, expected_review_id.strip()
                )
            ):
                return (
                    {},
                    set(),
                    ReviewOutcome("blocked", "확인 요청 ID가 일치하지 않습니다."),
                )
            stale_message = self._pending_stale_message(scope, catalog, current_pending)
            if stale_message:
                return {}, {pending_entry}, ReviewOutcome("blocked", stale_message)
            if (
                current_pending.requester_id
                and reviewer_id != current_pending.requester_id
                and not allow_cross_requester
            ):
                return (
                    {},
                    set(),
                    ReviewOutcome(
                        "blocked", "이 확인 요청을 만든 사용자만 응답할 수 있습니다."
                    ),
                )
            outcome, changed = _apply_pending_choice(
                catalog, current_pending, normalized, reviewer_id
            )
            if outcome.status != "confirmed" or not changed:
                delete_entries = (
                    {pending_entry}
                    if current_pending.review_kind not in {"metric", "dimension"}
                    else set()
                )
                return {}, delete_entries, outcome

            catalog.review_revision += 1
            chosen_aggregate = (
                normalized
                if current_pending.aggregate_pending
                else current_pending.proposed_aggregate
            )
            if (
                normalized != "reject"
                and resume is not None
                and secrets.compare_digest(resume.review_id, current_pending.review_id)
                and resume.expires_monotonic > time.monotonic()
            ):
                outcome.tool_args = deepcopy(resume.tool_args)
                outcome.tool_args["aggregate"] = chosen_aggregate
                outcome.question = resume.question
            outcome.source_id = catalog.source_id
            outcome.connection_generation = catalog.connection_generation
            outcome.requester_id = current_pending.requester_id
            outcome.review_id = current_pending.review_id
            outcome.mutation_applied = True

            receipt = {
                "review_id": receipt_id,
                "reviewer_id": reviewer_id or "unknown",
                "requester_id": current_pending.requester_id,
                "review_kind": current_pending.review_kind,
                "choice": normalized,
                "result": "confirmed",
                "source_id": catalog.source_id,
                "connection_generation": catalog.connection_generation,
                "catalog_fingerprint": catalog.fingerprint,
                "catalog_review_revision": catalog.review_revision,
                "catalog_version": catalog.version,
                "classification_policy_version": catalog.classification_policy_version,
            }
            upserts = {catalog_entry: catalog.to_json()}
            if receipt_key:
                upserts[(scope, receipt_key)] = json.dumps(
                    receipt, ensure_ascii=False, sort_keys=True
                )
            audit_event = (
                AuditEvent(
                    actor=reviewer_id or "unknown",
                    action="semantic_review",
                    scope=audit_scope,
                    detail={
                        "requester_id": current_pending.requester_id,
                        "review_id": current_pending.review_id,
                        "review_kind": current_pending.review_kind,
                        "choice": normalized,
                        "cross_requester": bool(
                            current_pending.requester_id
                            and current_pending.requester_id != reviewer_id
                        ),
                        "metric_id": current_pending.metric_id,
                        "source_id": catalog.source_id,
                        "connection_generation": catalog.connection_generation,
                        "catalog_fingerprint": catalog.fingerprint,
                        "catalog_review_revision": catalog.review_revision,
                        "catalog_version": catalog.version,
                        "classification_policy_version": catalog.classification_policy_version,
                    },
                )
                if audit_scope
                else None
            )
            return upserts, {pending_entry}, outcome, audit_event

        result = self._store.kv_mutate_scoped_snapshot(entries=entries, mutate=mutate)
        if (
            resume is not None
            and resume.expires_monotonic <= time.monotonic()
            and (result.question or result.tool_args)
        ):
            # The transaction may have waited behind another writer after the
            # initial cache lookup. Never return a resume payload whose TTL
            # elapsed before the atomic decision finished committing.
            result.question = ""
            result.tool_args = {}
        if (
            result.status == "confirmed"
            and result.mutation_applied
            and normalized != "reject"
            and not result.question
        ):
            result.message += (
                " 원 질문과 필터 값은 저장하지 않았습니다. 같은 typed 질문을 "
                "다시 제출하면 방금 확인한 의미 연결을 사용합니다."
            )
        if (
            result.mutation_applied
            or self._store.kv_get(review_scope, PENDING_REVIEW_KEY) is None
        ):
            self._forget_pending_draft(pending.review_id)
        return result

    def confirm_pending_by_id(
        self,
        scope: str,
        review_id: str,
        choice: str,
        *,
        reviewer_id: str,
        authorized: bool,
        audit_scope: str = "",
    ) -> ReviewOutcome:
        """Steward-review one requester-owned record by its opaque ID."""

        if not authorized or not reviewer_id:
            return ReviewOutcome(
                "blocked", "관리자 또는 steward 승인 권한이 필요합니다."
            )
        replay = self._replay_review_receipt(
            scope, review_id.strip(), choice.strip().lower(), reviewer_id
        )
        if replay is not None:
            return replay
        located = self.pending_review_by_id(scope, review_id)
        if located is None:
            replay = self._replay_review_receipt(
                scope, review_id.strip(), choice.strip().lower(), reviewer_id
            )
            if replay is not None:
                return replay
            return ReviewOutcome(
                "blocked", "현재 연결에서 해당 확인 요청을 찾지 못했습니다."
            )
        review_scope, _pending = located
        return self.confirm_pending(
            scope,
            review_scope,
            choice,
            reviewer_id=reviewer_id,
            allow_cross_requester=True,
            expected_review_id=review_id.strip(),
            audit_scope=audit_scope,
        )

    def prepare_query(
        self,
        *,
        scope: str,
        review_scope: str,
        requester_id: str = "",
        explorer: ExplorerPort,
        question: str,
        metric_id: str,
        metric_phrase: str,
        aggregate: str,
        dimension_bindings: list[dict[str, str]],
        unresolved_obligations: list[str],
        limit: int,
        filter_bindings: list[dict[str, object]] | None = None,
        time_window_binding: dict[str, object] | None = None,
        expected_source_id: str = "",
        expected_connection_generation: int = 0,
    ) -> QueryOutcome:
        catalog = self.load(scope)
        if catalog is None:
            return QueryOutcome(
                "blocked",
                "의미 카탈로그가 준비되지 않았습니다. `/setup`을 먼저 실행해 주세요.",
                blocker="semantic_catalog_missing",
            )
        if expected_source_id or expected_connection_generation:
            if (
                not expected_source_id
                or expected_connection_generation <= 0
                or catalog.source_id != expected_source_id
                or catalog.connection_generation != expected_connection_generation
            ):
                return QueryOutcome(
                    "blocked",
                    "후보를 만든 뒤 DB 연결이 바뀌었습니다. 후보를 다시 조회해 주세요.",
                    blocker="candidate_source_stale",
                )
        metric = catalog.metric(metric_id)
        if metric is None:
            return QueryOutcome(
                "blocked",
                "요청한 지표는 현재 DB의 허용된 지표 목록에 없습니다.",
                blocker="unknown_metric",
            )
        if metric.state == ReviewState.REJECTED:
            return QueryOutcome(
                "blocked",
                "이 지표 후보는 이전 검토에서 사용하지 않기로 했습니다.",
                blocker="metric_rejected",
            )

        try:
            requested_aggregate = Aggregate(aggregate.strip().lower())
        except ValueError:
            return QueryOutcome(
                "blocked",
                "요청한 집계 방식은 허용된 집계 목록에 없습니다.",
                blocker="unknown_aggregate",
            )
        if requested_aggregate not in metric.allowed_aggregates:
            return QueryOutcome(
                "blocked",
                "이 지표에는 요청한 집계 방식을 사용할 수 없습니다.",
                blocker="aggregate_not_allowed",
            )
        explicit_aggregate, aggregate_error = _explicit_aggregate(question)
        if aggregate_error:
            return QueryOutcome(
                "clarification",
                aggregate_error,
                blocker="ambiguous_aggregate_cue",
            )
        if explicit_aggregate and requested_aggregate != explicit_aggregate:
            return QueryOutcome(
                "clarification",
                "질문에 명시된 집계 방식과 선택된 집계가 다릅니다. 조건을 버리지 않고 멈춥니다.",
                blocker="aggregate_cue_mismatch",
            )

        metric_phrase = _normalize_phrase(metric_phrase)
        if not metric_phrase or not _phrase_in_question(metric_phrase, question):
            return QueryOutcome(
                "blocked",
                "지표 표현은 사용자 질문에서 그대로 가져와야 합니다.",
                blocker="metric_phrase_not_grounded",
            )
        if metric_phrase in metric.rejected_aliases:
            return QueryOutcome(
                "blocked",
                "이 표현과 지표의 연결은 이전 검토에서 거절되었습니다.",
                blocker="metric_alias_rejected",
            )
        if (
            _binding_key(metric_phrase, requested_aggregate.value)
            in metric.rejected_bindings
        ):
            return QueryOutcome(
                "blocked",
                "이 표현과 집계 방식의 연결은 이전 검토에서 거절되었습니다.",
                blocker="metric_binding_rejected",
            )
        phrase_residual = _metric_phrase_residual(metric_phrase, metric.aliases)
        if phrase_residual:
            return QueryOutcome(
                "clarification",
                "지표 표현 안에 기존 지표명과 연결되지 않은 수식어가 남아 있습니다: "
                + ", ".join(phrase_residual)
                + ". 필터를 지표명으로 흡수하지 않고 멈춥니다.",
                blocker="metric_phrase_contains_unresolved_terms",
            )

        filters, filter_error = _parse_filter_bindings(question, filter_bindings or [])
        if filter_error:
            return QueryOutcome(
                "blocked",
                filter_error[1],
                blocker=filter_error[0],
            )
        time_window, time_error = _parse_time_window_binding(
            question, time_window_binding
        )
        if time_error:
            return QueryOutcome(
                "blocked",
                time_error[1],
                blocker=time_error[0],
            )

        dimensions = []
        normalized_bindings: list[dict[str, str]] = []
        unresolved_dimensions: list[dict[str, str]] = []
        seen_dimension_ids: set[str] = set()
        for binding in dimension_bindings:
            dimension_id = str(binding.get("dimension_id", ""))
            phrase = _normalize_phrase(str(binding.get("phrase", "")))
            if dimension_id in seen_dimension_ids:
                return QueryOutcome(
                    "blocked",
                    "같은 분류 기준을 두 번 선택할 수 없습니다.",
                    blocker="duplicate_dimension",
                )
            seen_dimension_ids.add(dimension_id)
            dimension = catalog.dimension(dimension_id)
            if dimension is None:
                return QueryOutcome(
                    "blocked",
                    "요청한 분류 기준은 허용된 차원 목록에 없습니다.",
                    blocker="unknown_dimension",
                )
            if not _dimension_is_released(catalog, dimension):
                return QueryOutcome(
                    "blocked",
                    "이 문자열 차원은 관리자 공개 승인이 필요합니다.",
                    blocker="dimension_release_required",
                )
            if not phrase or not _phrase_in_question(phrase, question):
                return QueryOutcome(
                    "blocked",
                    "분류 표현은 사용자 질문에서 그대로 가져와야 합니다.",
                    blocker="dimension_phrase_not_grounded",
                )
            if phrase in dimension.rejected_aliases:
                return QueryOutcome(
                    "blocked",
                    "이 표현과 분류 기준의 연결은 이전 검토에서 거절되었습니다.",
                    blocker="dimension_alias_rejected",
                )
            phrase_residual = _metric_phrase_residual(
                phrase,
                sorted(set([*dimension.aliases, *dimension.reserved_aliases])),
            )
            if phrase_residual:
                return QueryOutcome(
                    "clarification",
                    "분류 표현 안에 물리 분류명과 연결되지 않은 수식어가 "
                    "남아 있습니다: "
                    + ", ".join(phrase_residual)
                    + ". 필터를 분류명으로 흡수하지 않고 멈춥니다.",
                    blocker="dimension_phrase_contains_unresolved_terms",
                )
            normalized = {"dimension_id": dimension_id, "phrase": phrase}
            normalized_bindings.append(normalized)
            dimensions.append(dimension)
            if phrase not in dimension.aliases:
                unresolved_dimensions.append(normalized)

        extra_bindings = [
            (predicate.dimension_id, predicate.dimension_phrase, "filter")
            for predicate in filters
        ]
        if time_window is not None:
            extra_bindings.append(
                (
                    time_window.dimension_id,
                    time_window.dimension_phrase,
                    "time_window",
                )
            )
        extra_dimensions: dict[str, DimensionSpec] = {}
        for dimension_id, phrase, role in extra_bindings:
            dimension = catalog.dimension(dimension_id)
            if dimension is None:
                return QueryOutcome(
                    "blocked",
                    "필터·기간 기준은 현재 DB의 허용된 차원 목록에 있어야 합니다.",
                    blocker="unknown_predicate_dimension",
                )
            if not _dimension_is_released(catalog, dimension):
                return QueryOutcome(
                    "blocked",
                    "필터·기간 기준 차원은 관리자 공개 승인이 필요합니다.",
                    blocker="predicate_dimension_release_required",
                )
            if (
                dimension.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED
                or not _public_data_scope_confirmed(catalog)
            ):
                return QueryOutcome(
                    "blocked",
                    "보호 차원은 행을 좁히는 필터로 사용할 수 없습니다.",
                    blocker="controlled_predicate_dimension_blocked",
                )
            if not phrase or not _phrase_in_question(phrase, question):
                return QueryOutcome(
                    "blocked",
                    "필터·기간 기준 표현은 사용자 질문에서 그대로 가져와야 합니다.",
                    blocker="predicate_dimension_phrase_not_grounded",
                )
            if phrase in dimension.rejected_aliases:
                return QueryOutcome(
                    "blocked",
                    "이 표현과 필터·기간 기준의 연결은 이전 검토에서 거절되었습니다.",
                    blocker="predicate_dimension_alias_rejected",
                )
            phrase_residual = _metric_phrase_residual(
                phrase,
                sorted(set([*dimension.aliases, *dimension.reserved_aliases])),
            )
            if phrase_residual:
                return QueryOutcome(
                    "clarification",
                    "필터·기간 표현 안에 검토되지 않은 수식어가 남아 있습니다: "
                    + ", ".join(phrase_residual)
                    + ". 조건을 흡수하지 않고 멈춥니다.",
                    blocker="predicate_dimension_phrase_contains_unresolved_terms",
                )
            capability_error = (
                _validate_filter_dimension(dimension, filters)
                if role == "filter"
                else _validate_time_dimension(dimension, time_window)
            )
            if capability_error:
                return QueryOutcome(
                    "blocked",
                    capability_error[1],
                    blocker=capability_error[0],
                )
            extra_dimensions[dimension.id] = dimension
            normalized = {"dimension_id": dimension_id, "phrase": phrase}
            if (
                phrase not in dimension.aliases
                and normalized not in unresolved_dimensions
            ):
                unresolved_dimensions.append(normalized)

        controlled_dimension = _has_controlled_dimension(dimensions)
        metric_is_controlled = not _public_data_scope_confirmed(catalog)
        if requested_aggregate in {Aggregate.MIN, Aggregate.MAX} and (
            metric_is_controlled or controlled_dimension
        ):
            return QueryOutcome(
                "blocked",
                (
                    "공개·비개인 데이터 범위가 확인되지 않은 지표나 보호 그룹에서는 "
                    "단일 극값이 노출될 수 있어 MIN/MAX를 실행하지 않습니다. "
                    "공개 데이터 범위와 차원 등급을 steward가 별도로 확인해야 합니다."
                ),
                blocker=(
                    "controlled_metric_extreme_blocked"
                    if metric_is_controlled
                    else "controlled_group_extreme_metric_blocked"
                ),
            )

        remaining = [item.strip() for item in unresolved_obligations if item.strip()]
        if remaining:
            return QueryOutcome(
                "clarification",
                "현재 typed query가 표현하지 못하는 요청이 남아 있습니다: "
                + ", ".join(remaining)
                + ". 조건을 버리지 않고 멈춥니다.",
                blocker="unsupported_obligations",
            )

        obligation_error = _check_obligations(
            question,
            metric.unit,
            dimensions,
            normalized_bindings,
            filters=filters,
            time_window=time_window,
        )
        if obligation_error:
            return QueryOutcome(
                "clarification",
                obligation_error[1],
                metric_id=metric.id,
                dimension_ids=[item["dimension_id"] for item in normalized_bindings],
                blocker=obligation_error[0],
            )

        predicate_phrases = [
            phrase
            for predicate in filters
            for phrase in (
                predicate.dimension_phrase,
                predicate.operator_phrase,
                *predicate.value_phrases,
            )
        ]
        if time_window is not None:
            predicate_phrases.extend(
                [
                    time_window.dimension_phrase,
                    time_window.range_phrase,
                    time_window.start_phrase,
                    time_window.end_phrase,
                ]
            )
        if len(filters) > 1:
            # A conjunction is grammar only when the typed draft actually
            # contains multiple AND predicates. Keeping it contextual avoids
            # accepting a dangling or model-invented condition silently.
            predicate_phrases.extend(
                connector
                for connector in ("and", "그리고")
                if _phrase_in_question(connector, question)
            )
        uncovered = _uncovered_question_terms(
            question,
            [
                metric_phrase,
                *[item["phrase"] for item in normalized_bindings],
                *predicate_phrases,
            ],
            [
                metric.table_id,
                *[dimension.table_id for dimension in dimensions],
                *[dimension.table_id for dimension in extra_dimensions.values()],
            ],
        )
        if uncovered:
            return QueryOutcome(
                "clarification",
                "현재 typed query에 연결되지 않은 질문 표현이 남아 있습니다: "
                + ", ".join(uncovered)
                + ". 필터나 업무 조건일 수 있어 SQL을 만들지 않습니다.",
                blocker="unresolved_question_terms",
            )

        # Reject an impossible or fan-out join before asking the user to review
        # any business mapping that could never produce a safe query.
        dimension_ids = [item["dimension_id"] for item in normalized_bindings]
        paths: list[list[JoinSpec]] = []
        referenced_dimensions = {
            item.id: item for item in [*dimensions, *extra_dimensions.values()]
        }
        for dimension in referenced_dimensions.values():
            path, error = _unique_safe_path(
                catalog, metric.table_id, dimension.table_id
            )
            if error:
                return QueryOutcome(
                    "blocked",
                    "지표의 행 수를 늘리지 않는 유일한 조인 경로를 확인할 수 없습니다.",
                    blocker=error,
                )
            paths.append(path)

        metric_alias_pending = metric_phrase not in metric.aliases
        aggregate_pending = (
            requested_aggregate.value
            not in metric.reviewed_bindings.get(metric_phrase, [])
        )
        if metric_alias_pending or aggregate_pending or unresolved_dimensions:
            review_kind = (
                "metric" if metric_alias_pending or aggregate_pending else "dimension"
            )
            allowed_choices = (
                [item.value for item in metric.allowed_aggregates]
                if review_kind == "metric" and aggregate_pending
                else ["confirm"]
            )
            pending_dimensions = (
                [] if review_kind == "metric" else unresolved_dimensions[:1]
            )
            review_id = secrets.token_urlsafe(18)
            resume_args: dict[str, object] = {
                "metric_id": metric.id,
                "metric_phrase": metric_phrase,
                "aggregate": requested_aggregate.value,
                "dimensions": normalized_bindings,
                "filters": [_filter_to_binding(item) for item in filters],
                "time_window": (
                    _time_window_to_binding(time_window)
                    if time_window is not None
                    else None
                ),
                "unresolved_obligations": remaining,
                "limit": max(1, min(int(limit), 1000)),
            }
            pending = PendingReview(
                metric_id=metric.id,
                metric_phrase=metric_phrase,
                dimension_bindings=pending_dimensions,
                allowed_choices=allowed_choices,
                proposed_aggregate=requested_aggregate.value,
                constraint_filter_count=len(filters),
                constraint_has_time_window=time_window is not None,
                catalog_fingerprint=catalog.fingerprint,
                catalog_review_revision=catalog.review_revision,
                catalog_version=catalog.version,
                classification_policy_version=catalog.classification_policy_version,
                source_id=catalog.source_id,
                connection_generation=catalog.connection_generation,
                requester_id=requester_id,
                metric_alias_pending=(
                    metric_alias_pending if review_kind == "metric" else False
                ),
                aggregate_pending=(
                    aggregate_pending if review_kind == "metric" else False
                ),
                review_kind=review_kind,
                review_id=review_id,
                catalog_scope=scope,
            )
            self._store.kv_set(review_scope, PENDING_REVIEW_KEY, pending.to_json())
            self._remember_pending_draft(review_scope, review_id, question, resume_args)
            options = ", ".join(allowed_choices)
            return QueryOutcome(
                "clarification",
                (
                    "질문의 업무 표현과 DB 컬럼 연결을 한 단계 확인해야 합니다. "
                    f"확인 ID: `{pending.review_id}`. 선택: {options}, reject. "
                    "길드에서는 관리자가 `/semantic_review review_id:...`로 확인하고, "
                    "DM에서는 본인이 바로 확인할 수 있습니다. 본인이 확인하면 "
                    "원래 질문을 이어서 처리하지만, 길드에서 다른 관리자가 "
                    "승인한 경우 요청자가 같은 질문을 다시 보내야 합니다. "
                    "표현 안에 필터·기간·조건이 섞였다면 아래 typed 필터·기간으로 "
                    "정확히 분리됐는지 확인하고, 누락됐거나 실제 업무 의미와 다르면 "
                    "reject를 선택하세요. "
                    "필터 값과 기간 경계는 승인 후에도 질문 원문과 다시 대조됩니다."
                ),
                metric_id=metric.id,
            )

        try:
            plan = SemanticPlan(
                question_sha256=question_sha256(question),
                stamp=SemanticStateStamp(
                    source_id=catalog.source_id,
                    connection_generation=catalog.connection_generation,
                    catalog_fingerprint=catalog.fingerprint,
                    catalog_review_revision=catalog.review_revision,
                    catalog_version=catalog.version,
                    classification_policy_version=(
                        catalog.classification_policy_version
                    ),
                ),
                measure=BaseMeasure(metric.id, requested_aggregate),
                metric_phrase=metric_phrase,
                dimensions=tuple(
                    DimensionSelection(item["dimension_id"], item["phrase"])
                    for item in normalized_bindings
                ),
                filters=filters,
                time_window=time_window,
                limit=max(1, min(int(limit), 1000)),
            )
            prepared = compile_semantic_plan(
                catalog=catalog,
                explorer=explorer,
                plan=plan,
                paths=paths,
            )
        except ValueError:
            return QueryOutcome(
                "blocked",
                "검토된 의미 계획을 현재 DB의 안전한 실행 계약으로 컴파일하지 못했습니다.",
                blocker="semantic_plan_validation_failed",
            )
        return QueryOutcome(
            "ready",
            "검토된 값으로 결정론적 SQL을 준비했습니다.",
            sql=prepared.sql,
            metric_id=metric.id,
            aggregate=requested_aggregate.value,
            dimension_ids=dimension_ids,
            catalog_fingerprint=catalog.fingerprint,
            catalog_review_revision=catalog.review_revision,
            catalog_version=catalog.version,
            classification_policy_version=catalog.classification_policy_version,
            source_id=catalog.source_id,
            connection_generation=catalog.connection_generation,
            plan=plan,
            prepared=prepared,
        )


def _carry_forward_reviews(previous: SemanticCatalog, current: SemanticCatalog) -> None:
    """Reuse decisions only when the whole physical catalog fingerprint matches."""

    current.review_revision = previous.review_revision
    current.metric_action_epoch = previous.metric_action_epoch
    current.dimension_action_epoch = previous.dimension_action_epoch
    current.public_scope_epoch = previous.public_scope_epoch

    old_metrics = {item.id: item for item in previous.metrics}
    for metric in current.metrics:
        old = old_metrics.get(metric.id)
        if old is None or old.expression_kind != metric.expression_kind:
            continue
        metric.state = old.state
        metric.aggregate = old.aggregate
        metric.aliases = sorted(set([*metric.aliases, *old.aliases]))
        metric.rejected_aliases = sorted(set(old.rejected_aliases))
        metric.reviewed_bindings = {
            phrase: list(aggregates)
            for phrase, aggregates in old.reviewed_bindings.items()
        }
        metric.rejected_bindings = sorted(set(old.rejected_bindings))
        metric.binding_reviewers = dict(old.binding_reviewers)
        metric.alias_reviewers = dict(old.alias_reviewers)

    old_dimensions = {item.id: item for item in previous.dimensions}
    if (
        previous.public_data_scope
        and previous.public_data_fingerprint == current.fingerprint
    ):
        current.public_data_scope = True
        current.public_data_reviewer = previous.public_data_reviewer
        current.public_data_fingerprint = previous.public_data_fingerprint
        current.public_data_confirmed_at = previous.public_data_confirmed_at
    for dimension in current.dimensions:
        old_dimension = old_dimensions.get(dimension.id)
        if (
            old_dimension is None
            or previous.classification_policy_version
            != current.classification_policy_version
            or old_dimension.review_policy != dimension.review_policy
            or old_dimension.classification_evidence
            != dimension.classification_evidence
            or old_dimension.classification_policy_version
            != dimension.classification_policy_version
        ):
            continue
        if old_dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED:
            dimension.raw_output_allowed = old_dimension.raw_output_allowed
            dimension.disclosure_tier = old_dimension.disclosure_tier
            dimension.release_reviewer = old_dimension.release_reviewer
            dimension.release_catalog_fingerprint = (
                old_dimension.release_catalog_fingerprint
            )
            dimension.released_at = old_dimension.released_at
            dimension.action_revision = old_dimension.action_revision
        dimension.aliases = sorted(set([*dimension.aliases, *old_dimension.aliases]))
        dimension.rejected_aliases = sorted(set(old_dimension.rejected_aliases))
        dimension.alias_reviewers = dict(old_dimension.alias_reviewers)


def enforce_metric_disclosure_output(
    catalog: SemanticCatalog,
    metric_id: str,
    aggregate: str,
    dimension_ids: list[str],
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    """Enforce contributor guards before any governed aggregate is rendered."""

    metric = catalog.metric(metric_id)
    dimensions = [catalog.dimension(item) for item in dimension_ids]
    if metric is None or any(item is None for item in dimensions):
        return [], "metric_disclosure_layout_unknown"
    try:
        aggregate_value = Aggregate(aggregate)
    except ValueError:
        return [], "metric_disclosure_aggregate_unknown"
    known_dimensions = [item for item in dimensions if item is not None]
    guard_required = not _public_data_scope_confirmed(
        catalog
    ) or _has_controlled_dimension(known_dimensions)
    if aggregate_value in {Aggregate.MIN, Aggregate.MAX} and guard_required:
        return [], "controlled_metric_extreme_blocked"
    if not guard_required:
        return rows, ""
    if not rows:
        return [], "metric_contributor_count_too_small"

    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if _METRIC_CONTRIBUTOR_COUNT_KEY not in row:
            return [], "metric_contributor_guard_missing"
        try:
            contributor_count = int(row[_METRIC_CONTRIBUTOR_COUNT_KEY])
        except (TypeError, ValueError):
            return [], "metric_contributor_guard_invalid"
        if contributor_count < _RELEASE_MIN_GROUP_SIZE:
            return [], "metric_contributor_count_too_small"
        cleaned.append(
            {
                key: value
                for key, value in row.items()
                if key != _METRIC_CONTRIBUTOR_COUNT_KEY
            }
        )
    return cleaned, ""


def enforce_released_dimension_output(
    catalog: SemanticCatalog,
    dimension_ids: list[str],
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    """Strip hidden guards or fail closed before raw category labels are rendered."""

    protected = [
        dimension
        for dimension_id in dimension_ids
        if (dimension := catalog.dimension(dimension_id)) is not None
        and dimension.review_policy == DimensionReviewPolicy.RELEASE_REQUIRED
    ]
    if not protected:
        return rows, ""
    if not all(_dimension_is_released(catalog, dimension) for dimension in protected):
        return [], "dimension_release_required"
    controlled = any(
        dimension.disclosure_tier == DimensionDisclosureTier.CONTROLLED_GROUPED
        for dimension in protected
    )
    cleaned: list[dict[str, Any]] = []
    for row in rows:
        if _RELEASE_CATEGORY_COUNT_KEY not in row or (
            controlled and _RELEASE_GROUP_SIZE_KEY not in row
        ):
            return [], "released_dimension_guard_missing"
        try:
            category_count = int(row[_RELEASE_CATEGORY_COUNT_KEY])
            minimum_group_size = (
                int(row[_RELEASE_GROUP_SIZE_KEY]) if controlled else None
            )
        except (TypeError, ValueError):
            return [], "released_dimension_guard_invalid"
        if (
            minimum_group_size is not None
            and minimum_group_size < _RELEASE_MIN_GROUP_SIZE
        ):
            return [], "released_dimension_group_too_small"
        if category_count > _RELEASE_MAX_CATEGORY_COUNT:
            return [], "released_dimension_cardinality_too_high"
        visible = {
            key: value
            for key, value in row.items()
            if key not in {_RELEASE_GROUP_SIZE_KEY, _RELEASE_CATEGORY_COUNT_KEY}
        }
        dimension_slots = {
            f"{_DIMENSION_OUTPUT_PREFIX}{index}" for index in range(len(dimension_ids))
        }
        for key, value in visible.items():
            if key not in dimension_slots or value is None:
                continue
            if len(str(value)) > _RELEASE_MAX_LABEL_LENGTH:
                return [], "released_dimension_label_too_long"
        cleaned.append(visible)
    return cleaned, ""


def decode_semantic_query_rows(
    catalog: SemanticCatalog,
    dimension_ids: list[str],
    rows: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], str]:
    """Validate compiler-owned slots and map them to unique display labels.

    Physical column names are untrusted and can collide with one another or
    with metric/guard keys. SQL therefore returns only reserved positional
    aliases; this decoder is the sole place that gives those values labels.
    """

    dimensions = [catalog.dimension(item) for item in dimension_ids]
    if any(item is None for item in dimensions):
        return [], "semantic_output_layout_unknown_dimension"
    slots = [f"{_DIMENSION_OUTPUT_PREFIX}{index}" for index in range(len(dimensions))]
    expected = {*slots, _METRIC_OUTPUT_KEY}
    display_labels: list[str] = []
    for dimension in dimensions:
        assert dimension is not None
        display_labels.append(f"{dimension.table_id}.{dimension.column}")
    if len(display_labels) != len(set(display_labels)):
        return [], "semantic_output_layout_duplicate_label"
    decoded: list[dict[str, Any]] = []
    for row in rows:
        if set(row) != expected:
            return [], "semantic_output_layout_mismatch"
        visible = {
            display: row[slot]
            for display, slot in zip(display_labels, slots, strict=True)
        }
        visible["metric_value"] = row[_METRIC_OUTPUT_KEY]
        decoded.append(visible)
    return decoded, ""


def semantic_query_headers(
    catalog: SemanticCatalog, dimension_ids: list[str]
) -> tuple[str, ...]:
    """Return the compiler-owned display layout even when SQL yields zero rows."""

    headers: list[str] = []
    for dimension_id in dimension_ids:
        dimension = catalog.dimension(dimension_id)
        if dimension is None:
            raise ValueError("semantic output dimension is missing")
        headers.append(f"{dimension.table_id}.{dimension.column}")
    headers.append("metric_value")
    return tuple(headers)


def _metric_binding_conflict(
    catalog: SemanticCatalog, metric_id: str, phrase: str
) -> str:
    normalized = _normalize_phrase(phrase)
    for candidate in catalog.metrics:
        if candidate.id != metric_id and (
            normalized in candidate.aliases or normalized in candidate.reviewed_bindings
        ):
            return candidate.label
    return ""


def _dimension_alias_conflict(
    catalog: SemanticCatalog, dimension_id: str, phrase: str
) -> str:
    normalized = _normalize_phrase(phrase)
    for candidate in catalog.dimensions:
        if candidate.id != dimension_id and (
            normalized in candidate.aliases
            or normalized in candidate.reserved_aliases
            or normalized in candidate.alias_reviewers
        ):
            return candidate.label
    return ""


def _append_alias(target: list[str], value: str) -> None:
    normalized = _normalize_phrase(value)
    if normalized and normalized not in target:
        target.append(normalized)
        target.sort()


def _binding_key(phrase: str, aggregate: str) -> str:
    normalized_phrase = _normalize_phrase(phrase)
    normalized_aggregate = aggregate.strip().lower()
    if not normalized_phrase or not normalized_aggregate:
        return ""
    return f"{normalized_phrase}|{normalized_aggregate}"


def _validate_mapping_phrase(value: str, *, subject: str) -> tuple[str, str]:
    """Accept one bounded noun-like phrase, never a query or control text."""

    if not isinstance(value, str):
        return "", f"{subject} 표현은 문자열이어야 합니다."
    if len(value) > 256 or len(value.encode("utf-8")) > 1024:
        return "", f"{subject} 표현 원문은 256자와 UTF-8 1024바이트 이하여야 합니다."
    if any(character in value for character in ("\n", "\r", "\x00", ";", "?")):
        return "", f"{subject} 표현에는 줄바꿈, 질의문, 문장 구분자를 넣을 수 없습니다."
    normalized = _normalize_phrase(value)
    if len(normalized) < 2 or len(normalized) > 80:
        return "", f"{subject} 표현은 정규화 후 2~80자여야 합니다."
    if len(normalized.split()) > 8:
        return "", f"{subject} 표현은 하나의 짧은 업무 개념이어야 합니다."
    return normalized, ""


def _normalize_phrase(value: str) -> str:
    return " ".join(re.sub(r"[^0-9a-zA-Z가-힣]+", " ", value.lower()).split())


def _phrase_in_question(phrase: str, question: str) -> bool:
    normalized_question = _normalize_phrase(question)
    return f" {phrase} " in f" {normalized_question} "


def _explicit_aggregate(question: str) -> tuple[Aggregate | None, str]:
    matches = [
        aggregate
        for aggregate, pattern in _AGGREGATE_CUES.items()
        if pattern.search(question)
    ]
    if len(matches) > 1:
        labels = ", ".join(item.value for item in matches)
        return (
            None,
            f"질문에 여러 집계 표현({labels})이 함께 있어 하나로 확정할 수 없습니다.",
        )
    return (matches[0], "") if matches else (None, "")


def _uncovered_question_terms(
    question: str, selected_phrases: list[str], source_table_ids: list[str]
) -> list[str]:
    """Fail closed on domain words not represented by a typed selection.

    This is intentionally vocabulary-agnostic: query grammar is allowed, while
    unexplained domain words, values, numbers, and operators must become either
    part of a reviewed phrase or an explicit unsupported obligation.
    """

    normalized_question = _normalize_phrase(question)
    residual = f" {normalized_question} "
    # Ignore one redundant source-context phrase only when that exact phrase
    # was already contiguous in the original question. Remove it before the
    # selected semantic phrase so repeated names such as "refunds in the
    # refunds dataset" retain their source-context provenance.
    source_context_patterns = _source_context_patterns(source_table_ids)
    original_source_contexts = {
        _normalize_phrase(match.group(0))
        for pattern in source_context_patterns
        for match in pattern.finditer(normalized_question)
    }
    removed_source_context = False
    for pattern in source_context_patterns:
        for match in pattern.finditer(residual):
            if _normalize_phrase(match.group(0)) not in original_source_contexts:
                continue
            residual = residual[: match.start()] + " " + residual[match.end() :]
            removed_source_context = True
            break
        if removed_source_context:
            break
    normalized_phrases = sorted(
        {_normalize_phrase(item) for item in selected_phrases if item},
        key=len,
        reverse=True,
    )
    for phrase in normalized_phrases:
        residual = residual.replace(f" {phrase} ", " ")
    # "there" is ignorable only inside the existential COUNT scaffold after
    # selected semantic phrases have been removed. Keeping it out of the global
    # grammar set preserves deictic/location-like uses such as "over there".
    residual = _COUNT_EXISTENTIAL_SCAFFOLD.sub(r"how many \1", residual, count=1)
    tokens = residual.split()
    return sorted({token for token in tokens if token not in _QUERY_GRAMMAR_WORDS})


def _source_context_patterns(table_ids: list[str]) -> list[re.Pattern[str]]:
    """Build source-only scaffolds from the selected metric's physical table."""

    aliases: set[str] = set()
    for table_id in table_ids:
        table = _normalize_phrase(table_id.rsplit(".", 1)[-1])
        if not table:
            continue
        aliases.add(table)
        tokens = table.split()
        if tokens:
            aliases.add(tokens[-1])
        for value in list(aliases):
            if value.endswith("s") and len(value) > 3:
                aliases.add(value[:-1])
    patterns = [_GENERIC_SOURCE_CONTEXT_SCAFFOLD]
    if aliases:
        alternatives = "|".join(
            re.escape(value).replace(r"\ ", r"\s+")
            for value in sorted(aliases, key=len, reverse=True)
        )
        patterns.extend(
            [
                re.compile(
                    rf"(?<!\S)in\s+(?:the\s+)?source\s+"
                    rf"(?:{alternatives})"
                    rf"(?:\s+(?:table|dataset|records|rows|data))?(?!\S)",
                    re.IGNORECASE,
                ),
                re.compile(
                    rf"(?<!\S)in\s+(?:the\s+)?(?:{alternatives})\s+"
                    rf"(?:table|dataset|records|rows|data)(?!\S)",
                    re.IGNORECASE,
                ),
            ]
        )
    return patterns


def _metric_phrase_residual(metric_phrase: str, known_aliases: list[str]) -> list[str]:
    """Prevent a model from hiding a filter inside an already-known metric name."""

    phrase = _normalize_phrase(metric_phrase)
    contained = [alias for alias in known_aliases if f" {alias} " in f" {phrase} "]
    if not contained or phrase in contained:
        return []
    base_alias = max(contained, key=len)
    residual = f" {phrase} ".replace(f" {base_alias} ", " ")
    return sorted(
        {token for token in residual.split() if token not in _QUERY_GRAMMAR_WORDS}
    )


def _parse_filter_bindings(
    question: str, bindings: list[dict[str, object]]
) -> tuple[tuple[FilterPredicate, ...], tuple[str, str] | None]:
    """Turn model wire values into a bounded, grounded AND-only predicate set."""

    if len(bindings) > 8:
        return (), (
            "too_many_filters",
            "한 질문에는 최대 8개의 명시적 AND 필터만 사용할 수 있습니다.",
        )
    parsed: list[FilterPredicate] = []
    seen_dimensions: set[str] = set()
    for binding in bindings:
        if not isinstance(binding, dict):
            return (), ("invalid_filter", "모든 필터는 typed object여야 합니다.")
        dimension_id = str(binding.get("dimension_id", "")).strip()
        dimension_phrase = _normalize_phrase(str(binding.get("dimension_phrase", "")))
        if not dimension_id or not _phrase_in_question(dimension_phrase, question):
            return (), (
                "filter_dimension_phrase_not_grounded",
                "필터 기준 표현은 사용자 질문에 그대로 있어야 합니다.",
            )
        if dimension_id in seen_dimensions:
            return (), (
                "duplicate_filter_dimension",
                "Phase 2에서는 한 차원에 하나의 AND 필터만 사용할 수 있습니다.",
            )
        seen_dimensions.add(dimension_id)
        try:
            operator = FilterOperator(str(binding.get("operator", "")).lower())
        except ValueError:
            return (), (
                "unsupported_filter_operator",
                "지원하지 않는 필터 연산자입니다.",
            )
        if operator not in {FilterOperator.EQ, FilterOperator.IN}:
            return (), (
                "unsupported_filter_operator",
                "Phase 2 필터는 exact EQ와 bounded IN만 지원합니다.",
            )
        operator_phrase = _normalize_phrase(str(binding.get("operator_phrase", "")))
        if operator_phrase and not _phrase_in_question(operator_phrase, question):
            return (), (
                "filter_operator_phrase_not_grounded",
                "필터 연산자 표현은 사용자 질문에서 그대로 가져와야 합니다.",
            )
        if operator == FilterOperator.IN and not operator_phrase:
            return (), (
                "filter_operator_phrase_required",
                "IN 필터에는 질문에 명시된 연산자 표현이 필요합니다.",
            )
        raw_values = binding.get("values")
        if not isinstance(raw_values, list) or not raw_values:
            return (), (
                "invalid_filter_values",
                "필터에는 하나 이상의 값이 필요합니다.",
            )
        values: list[ScalarLiteral] = []
        value_phrases: list[str] = []
        for item in raw_values:
            if not isinstance(item, dict) or not isinstance(item.get("value"), str):
                return (), (
                    "invalid_filter_literal",
                    "필터 값은 종류·문자열 값·질문 원문 표현을 가져야 합니다.",
                )
            try:
                kind = LiteralKind(str(item.get("kind", "")).lower())
            except ValueError:
                return (), (
                    "invalid_filter_literal",
                    "지원하지 않는 필터 값 종류입니다.",
                )
            if kind in {LiteralKind.DATE, LiteralKind.TIMESTAMP}:
                return (), (
                    "time_value_requires_window",
                    "날짜·시각 조건은 일반 필터가 아니라 명시적 기간창으로 표현해야 합니다.",
                )
            raw_value = str(item["value"])
            value_phrase = _normalize_phrase(str(item.get("phrase", "")))
            if not value_phrase or not _phrase_in_question(value_phrase, question):
                return (), (
                    "filter_value_phrase_not_grounded",
                    "필터 값은 사용자 질문에서 그대로 가져와야 합니다.",
                )
            if _normalize_phrase(raw_value) != value_phrase:
                return (), (
                    "filter_value_not_exact",
                    "필터 실행 값과 질문 원문 값이 정확히 일치하지 않습니다.",
                )
            try:
                values.append(ScalarLiteral(kind, raw_value))
            except ValueError as exc:
                return (), ("invalid_filter_literal", str(exc))
            value_phrases.append(value_phrase)
        if len({item.kind for item in values}) != 1:
            return (), (
                "mixed_filter_literal_types",
                "하나의 IN 필터에는 같은 종류의 값만 사용할 수 있습니다.",
            )
        try:
            parsed.append(
                FilterPredicate(
                    dimension_id=dimension_id,
                    dimension_phrase=dimension_phrase,
                    operator=operator,
                    values=tuple(values),
                    value_phrases=tuple(value_phrases),
                    operator_phrase=operator_phrase,
                )
            )
        except ValueError as exc:
            return (), ("invalid_filter", str(exc))
    return tuple(parsed), None


def _parse_time_window_binding(
    question: str, binding: dict[str, object] | None
) -> tuple[TimeWindow | None, tuple[str, str] | None]:
    if binding is None:
        return None, None
    if not isinstance(binding, dict):
        return None, ("invalid_time_window", "기간창은 typed object여야 합니다.")
    dimension_id = str(binding.get("dimension_id", "")).strip()
    dimension_phrase = _normalize_phrase(str(binding.get("dimension_phrase", "")))
    if not dimension_id or not _phrase_in_question(dimension_phrase, question):
        return None, (
            "time_dimension_phrase_not_grounded",
            "기간 기준 표현은 사용자 질문에 그대로 있어야 합니다.",
        )
    range_phrase = _normalize_phrase(str(binding.get("range_phrase", "")))
    if not range_phrase or not _phrase_in_question(range_phrase, question):
        return None, (
            "time_range_phrase_not_grounded",
            "기간 관계 표현 전체를 사용자 질문에서 그대로 가져와야 합니다.",
        )

    literals: list[ScalarLiteral] = []
    phrases: list[str] = []
    for endpoint in ("start", "end"):
        raw_endpoint = binding.get(endpoint)
        if not isinstance(raw_endpoint, dict) or not isinstance(
            raw_endpoint.get("value"), str
        ):
            return None, (
                "invalid_time_endpoint",
                "기간 시작·끝은 종류·문자열 값·질문 원문 표현을 가져야 합니다.",
            )
        try:
            kind = LiteralKind(str(raw_endpoint.get("kind", "")).lower())
        except ValueError:
            return None, ("invalid_time_endpoint", "지원하지 않는 기간 값 종류입니다.")
        if kind not in {LiteralKind.DATE, LiteralKind.TIMESTAMP}:
            return None, (
                "invalid_time_endpoint",
                "기간 값은 ISO date 또는 explicit UTC timestamp여야 합니다.",
            )
        raw_value = str(raw_endpoint["value"])
        phrase = _normalize_phrase(str(raw_endpoint.get("phrase", "")))
        if not phrase or not _phrase_in_question(phrase, question):
            return None, (
                "time_endpoint_not_grounded",
                "기간 시작·끝 값은 사용자 질문에서 그대로 가져와야 합니다.",
            )
        if _normalize_phrase(raw_value) != phrase:
            return None, (
                "time_endpoint_not_exact",
                "기간 실행 값과 질문 원문 값이 정확히 일치하지 않습니다.",
            )
        try:
            literals.append(ScalarLiteral(kind, raw_value))
        except ValueError as exc:
            return None, ("invalid_time_endpoint", str(exc))
        phrases.append(phrase)
    try:
        return (
            TimeWindow(
                dimension_id=dimension_id,
                dimension_phrase=dimension_phrase,
                start=literals[0],
                end=literals[1],
                start_phrase=phrases[0],
                end_phrase=phrases[1],
                range_phrase=range_phrase,
            ),
            None,
        )
    except ValueError as exc:
        return None, ("invalid_time_window", str(exc))


def _validate_filter_dimension(
    dimension: DimensionSpec, filters: tuple[FilterPredicate, ...]
) -> tuple[str, str] | None:
    predicates = [item for item in filters if item.dimension_id == dimension.id]
    if len(predicates) != 1:
        return ("filter_dimension_mismatch", "필터 차원 계약이 일치하지 않습니다.")
    reason = filter_compatibility_error(dimension, predicates[0])
    messages = {
        "temporal_filter_requires_time_window": (
            "시간·달력 차원은 일반 필터 대신 명시적 기간창이 필요합니다."
        ),
        "unsupported_filter_operator": "Phase 2 필터는 exact EQ와 bounded IN만 지원합니다.",
        "filter_type_not_supported": (
            "이 차원의 물리 타입에는 검증된 필터 바인딩 규칙이 없습니다."
        ),
        "filter_literal_type_mismatch": (
            "필터 값 종류가 선택한 차원의 물리 타입과 일치하지 않습니다."
        ),
        "filter_dimension_mismatch": "필터 차원 계약이 일치하지 않습니다.",
    }
    if reason:
        return reason, messages.get(reason, "필터 타입 계약을 검증하지 못했습니다.")
    return None


def _validate_time_dimension(
    dimension: DimensionSpec, window: TimeWindow | None
) -> tuple[str, str] | None:
    if window is None or window.dimension_id != dimension.id:
        return ("time_dimension_mismatch", "기간 기준 차원 계약이 일치하지 않습니다.")
    reason = time_window_compatibility_error(dimension, window)
    messages = {
        "time_axis_not_reviewed": (
            "Phase 2 기간창은 native DATE 차원만 지원합니다. 문자열·달력·timestamp는 "
            "형식과 timezone 검토가 더 필요합니다."
        ),
        "time_literal_type_mismatch": (
            "native DATE 기간창에는 ISO date 값만 사용할 수 있습니다."
        ),
        "time_dimension_mismatch": "기간 기준 차원 계약이 일치하지 않습니다.",
    }
    if reason:
        return reason, messages.get(reason, "기간 타입 계약을 검증하지 못했습니다.")
    return None


def _filter_to_binding(predicate: FilterPredicate) -> dict[str, object]:
    return {
        "dimension_id": predicate.dimension_id,
        "dimension_phrase": predicate.dimension_phrase,
        "operator": predicate.operator.value,
        "operator_phrase": predicate.operator_phrase,
        "values": [
            {
                "kind": literal.kind.value,
                "value": literal.value,
                "phrase": phrase,
            }
            for literal, phrase in zip(
                predicate.values, predicate.value_phrases, strict=True
            )
        ],
    }


def _time_window_to_binding(window: TimeWindow) -> dict[str, object]:
    return {
        "dimension_id": window.dimension_id,
        "dimension_phrase": window.dimension_phrase,
        "range_phrase": window.range_phrase,
        "start": {
            "kind": window.start.kind.value,
            "value": window.start.value,
            "phrase": window.start_phrase,
        },
        "end": {
            "kind": window.end.kind.value,
            "value": window.end.value,
            "phrase": window.end_phrase,
        },
    }


def _check_obligations(
    question: str,
    metric_unit: str,
    dimensions: list[DimensionSpec],
    dimension_bindings: list[dict[str, str]],
    *,
    filters: tuple[FilterPredicate, ...] = (),
    time_window: TimeWindow | None = None,
) -> tuple[str, str] | None:
    if _GROUPING_CUE.search(question) and not dimensions:
        return (
            "grouping_dimension_missing",
            "질문에 그룹별 결과가 필요하지만 분류 기준이 선택되지 않았습니다.",
        )
    if _RELATIVE_TIME_FILTER_CUE.search(question) and time_window is None:
        return (
            "time_semantics_not_reviewed",
            "기간 질문은 아직 기준 날짜와 범위를 검토하지 않았습니다. 잘못 추측하지 않고 멈춥니다.",
        )
    temporal_phrases = [
        binding["phrase"]
        for dimension, binding in zip(dimensions, dimension_bindings)
        if dimension.kind in {"calendar", "time"}
    ]
    if temporal_phrases and not _GROUPING_CUE.search(question):
        return (
            "time_dimension_requires_grouping",
            "시간·달력 차원은 현재 그룹 기준으로만 사용할 수 있습니다.",
        )
    if any(
        re.search(r"\d", phrase) or _TIME_RANGE_CUE.search(phrase)
        for phrase in temporal_phrases
    ):
        return (
            "time_semantics_not_reviewed",
            "시간 분류 표현에 값이나 범위가 섞여 있어 검토 없이 해석하지 않습니다.",
        )
    normalized_question = _normalize_phrase(question)
    temporal_residual = f" {normalized_question} "
    for phrase in sorted(set(temporal_phrases), key=len, reverse=True):
        temporal_residual = temporal_residual.replace(f" {phrase} ", " ")
    if time_window is None and (
        _TIME_UNIT_CUE.search(temporal_residual)
        or (
            temporal_phrases
            and (
                re.search(r"\d", temporal_residual) or _TIME_RANGE_CUE.search(question)
            )
        )
    ):
        return (
            "time_semantics_not_reviewed",
            "기간 값이나 범위는 아직 검토된 시간 연산으로 표현할 수 없습니다.",
        )
    requested_unit = _requested_unit(question)
    if requested_unit and requested_unit != metric_unit:
        return (
            "unit_conversion_not_reviewed",
            "요청 단위와 지표 단위가 다르지만 검토된 변환 규칙이 없습니다.",
        )
    return None


def _requested_unit(question: str) -> str:
    lowered = question.lower()
    for canonical, phrases in _UNIT_CUES.items():
        if any(phrase.lower() in lowered for phrase in phrases):
            return canonical
    return ""


def _unique_safe_path(
    catalog: SemanticCatalog, source: str, target: str
) -> tuple[list[JoinSpec], str]:
    if source == target:
        return [], ""
    adjacency: dict[str, list[JoinSpec]] = {}
    for join in catalog.joins:
        adjacency.setdefault(join.child_table_id, []).append(join)

    frontier: list[tuple[str, list[JoinSpec]]] = [(source, [])]
    shortest: list[list[JoinSpec]] = []
    seen_depth: dict[str, int] = {source: 0}
    while frontier:
        node, path = frontier.pop(0)
        if shortest and len(path) >= len(shortest[0]):
            continue
        for edge in adjacency.get(node, []):
            next_path = [*path, edge]
            if edge.parent_table_id == target:
                shortest.append(next_path)
                continue
            depth = len(next_path)
            prior_depth = seen_depth.get(edge.parent_table_id)
            if prior_depth is not None and prior_depth < depth:
                continue
            seen_depth[edge.parent_table_id] = depth
            frontier.append((edge.parent_table_id, next_path))

    if not shortest:
        return [], "safe_join_path_missing"
    minimum = min(len(path) for path in shortest)
    candidates = [path for path in shortest if len(path) == minimum]
    if len(candidates) != 1:
        return [], "ambiguous_safe_join_path"
    return candidates[0], ""


def _compile_sql(
    *,
    catalog: SemanticCatalog,
    explorer: ExplorerPort,
    metric_id: str,
    aggregate: Aggregate,
    dimension_ids: list[str],
    paths: list[list[JoinSpec]],
    limit: int,
) -> str:
    return compile_legacy_aggregate_sql(
        catalog=catalog,
        explorer=explorer,
        metric_id=metric_id,
        aggregate=aggregate,
        dimension_ids=dimension_ids,
        paths=paths,
        limit=limit,
    )
