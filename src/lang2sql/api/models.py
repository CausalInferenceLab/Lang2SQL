"""Immutable, SQL-free public models for the Lang2SQL runtime."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from types import MappingProxyType
from typing import Mapping, TypeAlias


class Capability(str, Enum):
    QUERY = "query"
    CONNECT = "connect"
    REVIEW_ANY = "review_any"


class AggregateKind(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class FilterOperation(str, Enum):
    EQ = "eq"
    IN = "in"


class ValueKind(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"


class ReviewAction(str, Enum):
    DIMENSION_DISCLOSURE = "dimension_disclosure"
    PUBLIC_DATA_SCOPE = "public_data_scope"
    PUBLIC_GROUPED = "public_grouped"


@dataclass(frozen=True)
class CallContext:
    scope: str
    actor_id: str
    conversation_id: str
    capabilities: frozenset[Capability] = frozenset()

    def __post_init__(self) -> None:
        if not self.scope.strip() or not self.actor_id.strip():
            raise ValueError("scope and actor_id are required")
        if not self.conversation_id.strip():
            raise ValueError("conversation_id is required")
        try:
            capabilities = frozenset(Capability(item) for item in self.capabilities)
        except ValueError as exc:
            raise ValueError("unsupported call capability") from exc
        object.__setattr__(self, "capabilities", capabilities)


@dataclass(frozen=True, repr=False)
class ConnectionInput:
    dsn: str = field(repr=False)
    extras: Mapping[str, str] = field(default_factory=dict, repr=False)

    def __post_init__(self) -> None:
        if not self.dsn.strip():
            raise ValueError("connection DSN is required")
        object.__setattr__(
            self,
            "extras",
            MappingProxyType(
                {str(key): str(value) for key, value in self.extras.items()}
            ),
        )


@dataclass(frozen=True)
class SourceRef:
    source_id: str
    generation: int

    def __post_init__(self) -> None:
        if not self.source_id or self.generation <= 0:
            raise ValueError("source references require an active connection")


@dataclass(frozen=True)
class ScanSummary:
    table_count: int
    declared_join_count: int
    blocked_column_count: int
    pending_metric_count: int
    pending_disclosure_count: int
    execution_supported: bool


@dataclass(frozen=True)
class DimensionBinding:
    dimension_id: str
    phrase: str

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.phrase.strip():
            raise ValueError("dimension bindings require id and grounded phrase")


@dataclass(frozen=True, repr=False)
class LiteralInput:
    kind: ValueKind
    value: str = field(repr=False)
    phrase: str = field(repr=False)

    def __post_init__(self) -> None:
        try:
            kind = ValueKind(self.kind)
        except ValueError as exc:
            raise ValueError("unsupported literal kind") from exc
        if not isinstance(self.value, str) or not isinstance(self.phrase, str):
            raise ValueError("literal value and phrase must be strings")
        if not self.value or not self.phrase.strip():
            raise ValueError("literal value and grounded phrase are required")
        object.__setattr__(self, "kind", kind)


@dataclass(frozen=True, repr=False)
class FilterInput:
    dimension_id: str
    dimension_phrase: str
    operator: FilterOperation
    values: tuple[LiteralInput, ...] = field(repr=False)
    operator_phrase: str = ""

    def __post_init__(self) -> None:
        try:
            operator = FilterOperation(self.operator)
        except ValueError as exc:
            raise ValueError("unsupported filter operation") from exc
        values = tuple(self.values)
        if not self.dimension_id or not self.dimension_phrase.strip() or not values:
            raise ValueError("filters require a dimension and bound values")
        if operator == FilterOperation.EQ and len(values) != 1:
            raise ValueError("EQ filters require exactly one value")
        if operator == FilterOperation.IN and (
            len(values) > 20 or not self.operator_phrase.strip()
        ):
            raise ValueError(
                "IN filters require an operator phrase and at most 20 values"
            )
        object.__setattr__(self, "operator", operator)
        object.__setattr__(self, "values", values)


@dataclass(frozen=True, repr=False)
class DateEndpoint:
    value: str = field(repr=False)
    phrase: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.value or not self.phrase.strip():
            raise ValueError("date endpoints require a value and grounded phrase")
        try:
            date.fromisoformat(self.value)
        except ValueError as exc:
            raise ValueError("date endpoints require an ISO date") from exc


@dataclass(frozen=True, repr=False)
class DateWindowInput:
    dimension_id: str
    dimension_phrase: str
    range_phrase: str
    start: DateEndpoint = field(repr=False)
    end: DateEndpoint = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not self.dimension_id
            or not self.dimension_phrase.strip()
            or not self.range_phrase.strip()
        ):
            raise ValueError("date windows require a dimension and grounded range")
        if date.fromisoformat(self.start.value) >= date.fromisoformat(self.end.value):
            raise ValueError("date window start must precede its exclusive end")


@dataclass(frozen=True, repr=False)
class QueryDraft:
    question: str
    source: SourceRef
    candidate_token: str = field(repr=False)
    metric_id: str
    metric_phrase: str
    aggregate: AggregateKind
    dimensions: tuple[DimensionBinding, ...] = ()
    filters: tuple[FilterInput, ...] = field(default=(), repr=False)
    time_window: DateWindowInput | None = field(default=None, repr=False)
    unresolved_obligations: tuple[str, ...] = ()
    limit: int = 100

    def __post_init__(self) -> None:
        if not self.question.strip() or not self.metric_id or not self.metric_phrase:
            raise ValueError("question, metric_id, and metric_phrase are required")
        if not isinstance(self.source, SourceRef):
            raise ValueError("query drafts require a candidate source reference")
        if not self.candidate_token or not all(
            character.isalnum() or character in {"_", "-"}
            for character in self.candidate_token
        ):
            raise ValueError("query drafts require an opaque candidate token")
        try:
            aggregate = AggregateKind(self.aggregate)
        except ValueError as exc:
            raise ValueError("unsupported aggregate") from exc
        if (
            isinstance(self.limit, bool)
            or not isinstance(self.limit, int)
            or not 1 <= self.limit <= 1000
        ):
            raise ValueError("limit must be between 1 and 1000")
        object.__setattr__(self, "aggregate", aggregate)
        object.__setattr__(self, "dimensions", tuple(self.dimensions))
        object.__setattr__(self, "filters", tuple(self.filters))
        object.__setattr__(
            self, "unresolved_obligations", tuple(self.unresolved_obligations)
        )


@dataclass(frozen=True)
class ConnectRequest:
    context: CallContext
    connection: ConnectionInput


@dataclass(frozen=True)
class CandidateRequest:
    context: CallContext
    question: str

    def __post_init__(self) -> None:
        if not self.question.strip():
            raise ValueError("candidate discovery requires the original question")


@dataclass(frozen=True)
class MetricCandidate:
    metric_id: str
    label: str
    grounded_phrase: str
    allowed_aggregates: tuple[AggregateKind, ...]

    def __post_init__(self) -> None:
        if not self.metric_id or not self.label:
            raise ValueError("metric candidates require id and label")
        object.__setattr__(
            self,
            "allowed_aggregates",
            tuple(AggregateKind(item) for item in self.allowed_aggregates),
        )


@dataclass(frozen=True)
class DimensionCandidate:
    dimension_id: str
    label: str
    grounded_phrase: str = ""

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.label:
            raise ValueError("dimension candidates require id and label")


@dataclass(frozen=True)
class FilterCandidate:
    dimension_id: str
    label: str
    grounded_phrase: str
    allowed_value_kinds: tuple[ValueKind, ...]

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.label or not self.allowed_value_kinds:
            raise ValueError("filter candidates require id, label, and value kinds")
        object.__setattr__(
            self,
            "allowed_value_kinds",
            tuple(ValueKind(item) for item in self.allowed_value_kinds),
        )


@dataclass(frozen=True)
class TimeCandidate:
    dimension_id: str
    label: str
    grounded_phrase: str
    endpoint_kind: ValueKind = field(default=ValueKind.DATE, init=False)

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.label:
            raise ValueError("time candidates require id and label")


@dataclass(frozen=True)
class ReviewCandidate:
    dimension_id: str
    label: str
    grounded_phrase: str
    required_action: ReviewAction

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.label:
            raise ValueError("review candidates require id and label")
        object.__setattr__(self, "required_action", ReviewAction(self.required_action))


@dataclass(frozen=True)
class CandidateSet:
    source: SourceRef
    question_sha256: str
    candidate_token: str = field(repr=False)
    metrics: tuple[MetricCandidate, ...]
    grouping_dimensions: tuple[DimensionCandidate, ...] = ()
    filter_dimensions: tuple[FilterCandidate, ...] = ()
    time_dimensions: tuple[TimeCandidate, ...] = ()
    review_required_dimensions: tuple[ReviewCandidate, ...] = ()
    state: str = "ready"
    message: str = ""
    status: str = field(default="candidates", init=False)

    def __post_init__(self) -> None:
        for name in (
            "metrics",
            "grouping_dimensions",
            "filter_dimensions",
            "time_dimensions",
            "review_required_dimensions",
        ):
            object.__setattr__(self, name, tuple(getattr(self, name)))


@dataclass(frozen=True)
class PlanRequest:
    context: CallContext
    draft: QueryDraft


@dataclass(frozen=True)
class ReviewRequest:
    review_id: str
    kind: str
    object_id: str
    phrase: str
    allowed_choices: tuple[str, ...]
    source: SourceRef


@dataclass(frozen=True)
class FeedbackRequest:
    context: CallContext
    review_id: str
    choice: str


@dataclass(frozen=True)
class PreparedPlan:
    plan_id: str
    source: SourceRef
    expires_at: datetime


@dataclass(frozen=True)
class ExecuteRequest:
    context: CallContext
    plan: PreparedPlan


@dataclass(frozen=True)
class Connected:
    source: SourceRef
    scan: ScanSummary
    reviews: tuple[ReviewRequest, ...] = ()
    remaining_review_count: int = 0


@dataclass(frozen=True)
class PlanReady:
    plan: PreparedPlan
    status: str = field(default="ready", init=False)


@dataclass(frozen=True)
class ReviewRequired:
    review: ReviewRequest
    status: str = field(default="review_required", init=False)


@dataclass(frozen=True)
class Clarification:
    code: str
    message: str
    status: str = field(default="clarification", init=False)


@dataclass(frozen=True)
class Blocked:
    code: str
    message: str
    retryable: bool = False
    status: str = field(default="blocked", init=False)


PlanResult: TypeAlias = PlanReady | ReviewRequired | Clarification | Blocked


@dataclass(frozen=True)
class FeedbackApplied:
    applied: bool
    message: str
    next: PlanResult | None = None


Cell: TypeAlias = str | int | float | bool | Decimal | date | datetime | None


@dataclass(frozen=True)
class ExecutionReady:
    plan_id: str
    source: SourceRef
    columns: tuple[str, ...]
    rows: tuple[tuple[Cell, ...], ...]
    status: str = field(default="ready", init=False)
