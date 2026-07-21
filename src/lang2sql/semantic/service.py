"""Concrete semantic onboarding, review, and deterministic query service.

This module is the intentionally small integration facade.  It persists one
catalog per guild scope and returns only three query states: ready,
clarification, or blocked.  There is no raw-SQL fallback.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..core.ports.explorer import ExplorerPort
from .catalog import (
    CATALOG_KEY,
    PENDING_REVIEW_KEY,
    Aggregate,
    JoinSpec,
    PendingReview,
    ReviewState,
    SemanticCatalog,
)
from .onboarding import OnboardingSummary, build_catalog

if TYPE_CHECKING:
    from ..adapters.storage.sqlite_store import SqliteStore


_GROUPING_CUE = re.compile(
    r"\b(by|per|each|grouped\s+by|for\s+each)\b|별|마다|각각",
    re.IGNORECASE,
)
_TIME_FILTER_CUE = re.compile(
    r"\b(today|yesterday|last|previous|this|month|week|year|quarter)\b|"
    r"오늘|어제|지난|이번|월간|주간|연간|분기",
    re.IGNORECASE,
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


@dataclass
class QueryOutcome:
    status: str
    message: str
    sql: str = ""
    metric_id: str = ""
    aggregate: str = ""
    dimension_ids: list[str] = field(default_factory=list)
    blocker: str = ""


@dataclass
class ReviewOutcome:
    status: str
    message: str
    question: str = ""
    tool_args: dict[str, object] = field(default_factory=dict)


def review_scope_key(session_key: str, user_id: str) -> str:
    """Address one user's pending review inside a shared Discord conversation."""

    return f"{session_key}:semantic-review:{user_id}"


class SemanticService:
    def __init__(self, store: "SqliteStore") -> None:
        self._store = store

    async def inspect(self, scope: str, explorer: ExplorerPort) -> OnboardingSummary:
        """Build a candidate catalog without mutating the active connection state."""

        built = await build_catalog(explorer)
        existing = self.load(scope)
        if existing is not None and existing.fingerprint == built.catalog.fingerprint:
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
        summary = await self.inspect(scope, explorer)
        self.save(scope, summary.catalog)
        return summary

    def load(self, scope: str) -> SemanticCatalog | None:
        raw = self._store.kv_get(scope, CATALOG_KEY)
        if not raw:
            return None
        try:
            return SemanticCatalog.from_json(raw)
        except (KeyError, TypeError, ValueError):
            # A corrupt catalog must never silently enable the raw SQL path.
            return None

    def save(self, scope: str, catalog: SemanticCatalog) -> None:
        self._store.kv_set(scope, CATALOG_KEY, catalog.to_json())

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
        raw = self._store.kv_get(review_scope, PENDING_REVIEW_KEY)
        if not raw:
            return None
        try:
            return PendingReview.from_json(raw)
        except (KeyError, TypeError, ValueError):
            return None

    def status_text(self, scope: str, review_scope: str = "") -> str:
        catalog = self.load(scope)
        if catalog is None:
            return "아직 의미 카탈로그가 없습니다. `/setup`으로 DB를 연결해 주세요."
        pending = self.pending_review(review_scope or scope)
        lines = [
            "**Semantic setup 상태**",
            f"- 테이블: {len(catalog.tables)}개",
            f"- 선언된 안전 조인: {len(catalog.joins)}개",
            f"- 기본 차단 컬럼: {len(catalog.blocked_columns)}개",
            f"- 확인된 지표: {catalog.confirmed_metric_count}개",
            f"- 확인된 표현·집계 연결: {sum(len(values) for metric in catalog.metrics for values in metric.reviewed_bindings.values())}개",
            "- 업무 표현은 실제 질문에 등장할 때 컬럼 연결까지 한 번 확인합니다.",
        ]
        if pending is not None:
            lines.append(f"- 현재 확인 대기: `{pending.metric_phrase}`")
        return "\n".join(lines)

    def reset_reviews(self, scope: str) -> ReviewOutcome:
        """Remove human semantic decisions while preserving physical catalog facts."""

        catalog = self.load(scope)
        if catalog is None:
            return ReviewOutcome("blocked", "초기화할 의미 카탈로그가 없습니다.")
        for metric in catalog.metrics:
            metric.aliases = list(metric.auto_aliases)
            metric.rejected_aliases = []
            metric.rejected_bindings = []
            metric.binding_reviewers = {}
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
        # The physical fingerprint intentionally stays stable across a review
        # reset. A separate revision invalidates confirmations created before
        # the reset without pretending that the DB structure changed.
        catalog.review_revision += 1
        self.save(scope, catalog)
        return ReviewOutcome(
            "confirmed",
            "사람이 확인한 표현·집계 연결을 초기화했습니다. 물리 catalog와 PII 차단은 유지됩니다.",
        )

    def confirm_pending(
        self, scope: str, review_scope: str, choice: str, reviewer_id: str = ""
    ) -> ReviewOutcome:
        catalog = self.load(scope)
        pending = self.pending_review(review_scope)
        if catalog is None or pending is None:
            return ReviewOutcome("blocked", "현재 확인할 항목이 없습니다.")
        metric = catalog.metric(pending.metric_id)
        if metric is None:
            self._store.kv_delete(review_scope, PENDING_REVIEW_KEY)
            return ReviewOutcome(
                "blocked", "확인 대상 지표가 더 이상 존재하지 않습니다."
            )
        if (
            pending.catalog_fingerprint
            and pending.catalog_fingerprint != catalog.fingerprint
        ):
            self._store.kv_delete(review_scope, PENDING_REVIEW_KEY)
            return ReviewOutcome(
                "blocked",
                "DB 구조가 바뀌어 이전 확인 요청을 폐기했습니다. 질문을 다시 실행해 주세요.",
            )
        if pending.catalog_review_revision != catalog.review_revision:
            self._store.kv_delete(review_scope, PENDING_REVIEW_KEY)
            return ReviewOutcome(
                "blocked",
                "의미 검토가 초기화되어 이전 확인 요청을 폐기했습니다. 질문을 다시 실행해 주세요.",
            )
        if pending.requester_id and reviewer_id != pending.requester_id:
            return ReviewOutcome(
                "blocked", "이 확인 요청을 만든 사용자만 응답할 수 있습니다."
            )

        normalized = choice.strip().lower()
        if normalized == "reject":
            if pending.metric_alias_pending and pending.metric_phrase:
                _append_alias(metric.rejected_aliases, pending.metric_phrase)
            for binding in pending.dimension_bindings:
                dimension = catalog.dimension(binding.get("dimension_id", ""))
                if dimension is not None:
                    _append_alias(dimension.rejected_aliases, binding.get("phrase", ""))
            binding_key = _binding_key(
                pending.metric_phrase, pending.proposed_aggregate
            )
            if binding_key and binding_key not in metric.rejected_bindings:
                metric.rejected_bindings.append(binding_key)
                metric.rejected_bindings.sort()
            message = (
                "이 질문의 표현과 선택된 컬럼 연결을 사용하지 않도록 "
                "저장했습니다. SQL은 실행하지 않았습니다."
            )
        else:
            if normalized not in pending.allowed_choices:
                allowed = ", ".join(pending.allowed_choices)
                return ReviewOutcome(
                    "blocked", f"선택 가능한 값은 {allowed}, reject 입니다."
                )
            if pending.aggregate_pending:
                try:
                    aggregate = Aggregate(normalized)
                except ValueError:
                    return ReviewOutcome(
                        "blocked", "이 지표에는 집계 방식 선택이 필요합니다."
                    )
                if aggregate not in metric.allowed_aggregates:
                    return ReviewOutcome(
                        "blocked", "이 컬럼에는 해당 집계를 사용할 수 없습니다."
                    )
                conflict = _metric_binding_conflict(
                    catalog, metric.id, pending.metric_phrase
                )
                if conflict:
                    return ReviewOutcome(
                        "blocked",
                        f"`{pending.metric_phrase}`은 이미 `{conflict}`에 연결되어 있습니다. "
                        "공유 의미를 덮어쓰지 않았습니다.",
                    )
                metric.aggregate = aggregate
                metric.state = ReviewState.CONFIRMED
                reviewed_aggregates = metric.reviewed_bindings.setdefault(
                    pending.metric_phrase, []
                )
                if aggregate.value not in reviewed_aggregates:
                    reviewed_aggregates.append(aggregate.value)
                    reviewed_aggregates.sort()
                metric.binding_reviewers[pending.metric_phrase] = (
                    reviewer_id or "unknown"
                )
            elif normalized != "confirm":
                return ReviewOutcome(
                    "blocked", "이 단계에서는 표현 연결 확인 또는 거절만 가능합니다."
                )

            if pending.metric_alias_pending:
                _append_alias(metric.aliases, pending.metric_phrase)
            for binding in pending.dimension_bindings:
                dimension = catalog.dimension(binding.get("dimension_id", ""))
                if dimension is not None:
                    conflict = _dimension_alias_conflict(
                        catalog,
                        dimension.id,
                        binding.get("phrase", ""),
                    )
                    if conflict:
                        return ReviewOutcome(
                            "blocked",
                            f"`{binding.get('phrase', '')}`은 이미 `{conflict}`에 "
                            "연결되어 있습니다. 공유 의미를 덮어쓰지 않았습니다.",
                        )
                    _append_alias(dimension.aliases, binding.get("phrase", ""))
                    dimension.alias_reviewers[
                        _normalize_phrase(binding.get("phrase", ""))
                    ] = reviewer_id or "unknown"

            aggregate_label = (
                normalized
                if pending.aggregate_pending
                else pending.proposed_aggregate or "confirmed"
            )
            message = (
                f"`{pending.metric_phrase}` → `{metric.label}` 연결을 "
                f"`{aggregate_label}` 기준으로 저장했습니다. 같은 표현은 "
                "다시 확인하지 않습니다."
            )

        self.save(scope, catalog)
        self._store.kv_delete(review_scope, PENDING_REVIEW_KEY)
        tool_args: dict[str, object] = {}
        if normalized != "reject":
            chosen_aggregate = (
                normalized if pending.aggregate_pending else pending.proposed_aggregate
            )
            tool_args = {
                "metric_id": pending.metric_id,
                "metric_phrase": pending.metric_phrase,
                "aggregate": chosen_aggregate,
                "dimensions": pending.query_dimensions,
                "unresolved_obligations": [],
                "limit": pending.query_limit,
                "_reviewed_question": pending.question,
            }
        return ReviewOutcome(
            "confirmed",
            message,
            question=pending.question,
            tool_args=tool_args,
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
    ) -> QueryOutcome:
        catalog = self.load(scope)
        if catalog is None:
            return QueryOutcome(
                "blocked",
                "의미 카탈로그가 준비되지 않았습니다. `/setup`을 먼저 실행해 주세요.",
                blocker="semantic_catalog_missing",
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
            normalized = {"dimension_id": dimension_id, "phrase": phrase}
            normalized_bindings.append(normalized)
            dimensions.append(dimension)
            if phrase not in dimension.aliases:
                unresolved_dimensions.append(normalized)

        remaining = [item.strip() for item in unresolved_obligations if item.strip()]
        if remaining:
            return QueryOutcome(
                "clarification",
                "현재 typed query가 표현하지 못하는 요청이 남아 있습니다: "
                + ", ".join(remaining)
                + ". 조건을 버리지 않고 멈춥니다.",
                blocker="unsupported_obligations",
            )

        obligation_error = _check_obligations(question, metric.unit, dimensions)
        if obligation_error:
            return QueryOutcome(
                "clarification",
                obligation_error[1],
                metric_id=metric.id,
                dimension_ids=[item["dimension_id"] for item in normalized_bindings],
                blocker=obligation_error[0],
            )

        uncovered = _uncovered_question_terms(
            question,
            [metric_phrase, *[item["phrase"] for item in normalized_bindings]],
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
        for dimension in dimensions:
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
            allowed_choices = (
                [item.value for item in metric.allowed_aggregates]
                if aggregate_pending
                else ["confirm"]
            )
            pending = PendingReview(
                metric_id=metric.id,
                question=question,
                metric_phrase=metric_phrase,
                dimension_bindings=unresolved_dimensions,
                allowed_choices=allowed_choices,
                proposed_aggregate=requested_aggregate.value,
                query_dimensions=normalized_bindings,
                query_limit=max(1, min(int(limit), 1000)),
                catalog_fingerprint=catalog.fingerprint,
                catalog_review_revision=catalog.review_revision,
                requester_id=requester_id,
                metric_alias_pending=metric_alias_pending,
                aggregate_pending=aggregate_pending,
            )
            self._store.kv_set(review_scope, PENDING_REVIEW_KEY, pending.to_json())
            options = ", ".join(allowed_choices)
            mappings = [f"`{metric_phrase}` → `{metric.label}`"]
            for item in unresolved_dimensions:
                reviewed_dimension = catalog.dimension(item["dimension_id"])
                if reviewed_dimension is not None:
                    mappings.append(
                        f"`{item['phrase']}` → `{reviewed_dimension.label}`"
                    )
            return QueryOutcome(
                "clarification",
                (
                    "다음 질문 표현과 DB 컬럼 연결을 한 번만 확인해 주세요: "
                    + ", ".join(mappings)
                    + f". 선택: {options}, reject. `/semantic_review`에서 고르면 "
                    "이 연결을 저장하고 원래 질문을 바로 다시 처리합니다."
                ),
                metric_id=metric.id,
            )

        sql = _compile_sql(
            catalog=catalog,
            explorer=explorer,
            metric_id=metric.id,
            aggregate=requested_aggregate,
            dimension_ids=dimension_ids,
            paths=paths,
            limit=max(1, min(int(limit), 1000)),
        )
        return QueryOutcome(
            "ready",
            "검토된 값으로 결정론적 SQL을 준비했습니다.",
            sql=sql,
            metric_id=metric.id,
            aggregate=requested_aggregate.value,
            dimension_ids=dimension_ids,
        )


def _carry_forward_reviews(previous: SemanticCatalog, current: SemanticCatalog) -> None:
    """Reuse decisions only when the whole physical catalog fingerprint matches."""

    current.review_revision = previous.review_revision

    old_metrics = {item.id: item for item in previous.metrics}
    for metric in current.metrics:
        old = old_metrics.get(metric.id)
        if old is None:
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

    old_dimensions = {item.id: item for item in previous.dimensions}
    for dimension in current.dimensions:
        old_dimension = old_dimensions.get(dimension.id)
        if old_dimension is None:
            continue
        dimension.aliases = sorted(set([*dimension.aliases, *old_dimension.aliases]))
        dimension.rejected_aliases = sorted(set(old_dimension.rejected_aliases))
        dimension.alias_reviewers = dict(old_dimension.alias_reviewers)


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
            normalized in candidate.aliases or normalized in candidate.alias_reviewers
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


def _uncovered_question_terms(question: str, selected_phrases: list[str]) -> list[str]:
    """Fail closed on domain words not represented by a typed selection.

    This is intentionally vocabulary-agnostic: query grammar is allowed, while
    unexplained domain words, values, numbers, and operators must become either
    part of a reviewed phrase or an explicit unsupported obligation.
    """

    residual = f" {_normalize_phrase(question)} "
    normalized_phrases = sorted(
        {_normalize_phrase(item) for item in selected_phrases if item},
        key=len,
        reverse=True,
    )
    for phrase in normalized_phrases:
        residual = residual.replace(f" {phrase} ", " ")
    tokens = residual.split()
    return sorted({token for token in tokens if token not in _QUERY_GRAMMAR_WORDS})


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


def _check_obligations(
    question: str, metric_unit: str, dimensions: list
) -> tuple[str, str] | None:
    if _GROUPING_CUE.search(question) and not dimensions:
        return (
            "grouping_dimension_missing",
            "질문에 그룹별 결과가 필요하지만 분류 기준이 선택되지 않았습니다.",
        )
    if _TIME_FILTER_CUE.search(question):
        return (
            "time_semantics_not_reviewed",
            "기간 질문은 아직 기준 날짜와 범위를 검토하지 않았습니다. 잘못 추측하지 않고 멈춥니다.",
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
    metric = catalog.metric(metric_id)
    if metric is None or aggregate not in metric.allowed_aggregates:
        raise ValueError("reviewed metric aggregate required")
    dimensions = [catalog.dimension(item) for item in dimension_ids]
    if any(item is None for item in dimensions):
        raise ValueError("known dimensions required")

    ordered_tables = [metric.table_id]
    ordered_joins: list[JoinSpec] = []
    seen_joins: set[str] = set()
    for path in paths:
        for join in path:
            if join.id not in seen_joins:
                ordered_joins.append(join)
                seen_joins.add(join.id)
            if join.parent_table_id not in ordered_tables:
                ordered_tables.append(join.parent_table_id)
    aliases = {
        table_id: f"t{index + 1}" for index, table_id in enumerate(ordered_tables)
    }

    select_parts: list[str] = []
    group_parts: list[str] = []
    for dimension in dimensions:
        assert dimension is not None
        expression = (
            f"{_quote(explorer, aliases[dimension.table_id])}."
            f"{_quote(explorer, dimension.column)}"
        )
        select_parts.append(f"{expression} AS {_quote(explorer, dimension.column)}")
        group_parts.append(expression)

    metric_expression = (
        f"{_quote(explorer, aliases[metric.table_id])}."
        f"{_quote(explorer, metric.column)}"
    )
    aggregate_sql = aggregate.value.upper()
    select_parts.append(
        f"{aggregate_sql}({metric_expression}) AS {_quote(explorer, 'metric_value')}"
    )

    source_table = catalog.table(metric.table_id)
    if source_table is None:
        raise ValueError("metric source table missing")
    lines = [
        "SELECT",
        "  " + ",\n  ".join(select_parts),
        (
            f"FROM {_qualified_table(explorer, source_table.schema, source_table.name)} "
            f"{_quote(explorer, aliases[source_table.id])}"
        ),
    ]
    for join in ordered_joins:
        parent = catalog.table(join.parent_table_id)
        if parent is None:
            raise ValueError("join parent table missing")
        child_alias = _quote(explorer, aliases[join.child_table_id])
        parent_alias = _quote(explorer, aliases[join.parent_table_id])
        lines.extend(
            [
                (
                    f"JOIN {_qualified_table(explorer, parent.schema, parent.name)} "
                    f"{parent_alias}"
                ),
                (
                    f"  ON {child_alias}.{_quote(explorer, join.child_column)} = "
                    f"{parent_alias}.{_quote(explorer, join.parent_column)}"
                ),
            ]
        )
    if group_parts:
        lines.append("GROUP BY " + ", ".join(group_parts))
    lines.append(f"LIMIT {limit}")
    return "\n".join(lines)


def _qualified_table(explorer: ExplorerPort, schema: str, table: str) -> str:
    if schema:
        return f"{_quote(explorer, schema)}.{_quote(explorer, table)}"
    return _quote(explorer, table)


def _quote(explorer: ExplorerPort, name: str) -> str:
    quote = getattr(explorer, "quote_identifier", None)
    if quote is not None:
        return str(quote(name))
    if not name.replace("_", "").isalnum():
        raise ValueError(f"unsafe identifier from catalog: {name!r}")
    return f'"{name}"'
