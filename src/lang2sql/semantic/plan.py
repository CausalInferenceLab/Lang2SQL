"""Immutable semantic plan and bound-query contracts.

This module is the Phase-2 boundary between model-selected semantic values and
deterministic SQL execution.  It deliberately contains no persistence, LLM,
Discord, or database I/O.  The public API remains experimental until filter,
time, derived-metric, and dialect contracts have cross-domain evidence.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
import hashlib
import json
import re
from typing import TypeAlias

from .catalog import Aggregate

SEMANTIC_PLAN_VERSION = 1
_PARAMETER_NAME = re.compile(r"^p[0-9]+$")
_SQL_PARAMETER = re.compile(r"(?<!:):([A-Za-z_][A-Za-z0-9_]*)")


class LiteralKind(str, Enum):
    STRING = "string"
    INTEGER = "integer"
    DECIMAL = "decimal"
    BOOLEAN = "boolean"
    DATE = "date"
    TIMESTAMP = "timestamp"


class FilterOperator(str, Enum):
    EQ = "eq"
    NE = "ne"
    LT = "lt"
    LTE = "lte"
    GT = "gt"
    GTE = "gte"
    IN = "in"


class MeasureKind(str, Enum):
    BASE = "base"
    DERIVED = "derived"


class DerivedOperator(str, Enum):
    ADD = "add"
    SUBTRACT = "subtract"
    MULTIPLY = "multiply"
    DIVIDE = "divide"


@dataclass(frozen=True)
class SemanticStateStamp:
    source_id: str
    connection_generation: int
    catalog_fingerprint: str
    catalog_review_revision: int
    catalog_version: int
    classification_policy_version: int

    def __post_init__(self) -> None:
        if not self.source_id or self.connection_generation <= 0:
            raise ValueError("semantic plans require a bound execution source")
        if not self.catalog_fingerprint or self.catalog_version <= 0:
            raise ValueError("semantic plans require a versioned catalog")
        if self.catalog_review_revision < 0 or self.classification_policy_version <= 0:
            raise ValueError("invalid semantic governance revision")


@dataclass(frozen=True)
class BaseMeasure:
    metric_id: str
    aggregate: Aggregate
    kind: MeasureKind = field(default=MeasureKind.BASE, init=False)

    def __post_init__(self) -> None:
        if not self.metric_id:
            raise ValueError("base measure requires a metric id")


@dataclass(frozen=True)
class DerivedMeasure:
    """Reference to a separately reviewed derived-metric definition.

    A model never supplies an expression tree.  The catalog definition owns
    the AST, grain, unit, NULL behavior, and division policy.  Execution stays
    fail-closed until that definition has been validated by the compiler.
    """

    derived_metric_id: str
    kind: MeasureKind = field(default=MeasureKind.DERIVED, init=False)

    def __post_init__(self) -> None:
        if not self.derived_metric_id:
            raise ValueError("derived measure requires a reviewed definition id")


MeasureSelection: TypeAlias = BaseMeasure | DerivedMeasure


@dataclass(frozen=True)
class DimensionSelection:
    dimension_id: str
    phrase: str

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.phrase.strip():
            raise ValueError("dimension selection requires id and grounded phrase")


@dataclass(frozen=True)
class ScalarLiteral:
    kind: LiteralKind
    value: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.value or "\x00" in self.value:
            raise ValueError("bound literal cannot be empty or contain NUL")
        if len(self.value) > 256 or len(self.value.encode("utf-8")) > 1024:
            raise ValueError("bound literal exceeds the semantic value limit")
        # Validate at the server-owned plan boundary rather than deferring a
        # malformed model value until database execution.
        try:
            parsed = self.python_value()
        except (InvalidOperation, OverflowError, ValueError) as exc:
            raise ValueError(f"invalid {self.kind.value} literal") from exc
        if self.kind == LiteralKind.DECIMAL:
            assert isinstance(parsed, Decimal)
            if not parsed.is_finite():
                raise ValueError("decimal literal must be finite")
        if self.kind == LiteralKind.TIMESTAMP:
            assert isinstance(parsed, datetime)
            if parsed.utcoffset() != timedelta(0):
                raise ValueError("timestamp literals must be explicit UTC values")

    def python_value(self) -> object:
        """Convert only compiler-validated scalar kinds to DBAPI bind values."""

        if self.kind == LiteralKind.STRING:
            return self.value
        if self.kind == LiteralKind.INTEGER:
            return int(self.value)
        if self.kind == LiteralKind.DECIMAL:
            return Decimal(self.value)
        if self.kind == LiteralKind.BOOLEAN:
            normalized = self.value.lower()
            if normalized not in {"true", "false"}:
                raise ValueError("boolean literal must be true or false")
            return normalized == "true"
        if self.kind == LiteralKind.DATE:
            return date.fromisoformat(self.value)
        if self.kind == LiteralKind.TIMESTAMP:
            return datetime.fromisoformat(self.value.replace("Z", "+00:00"))
        raise ValueError("unsupported bound literal kind")


@dataclass(frozen=True)
class FilterPredicate:
    dimension_id: str
    dimension_phrase: str
    operator: FilterOperator
    values: tuple[ScalarLiteral, ...]
    value_phrases: tuple[str, ...]
    operator_phrase: str = ""

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.dimension_phrase.strip():
            raise ValueError("filter requires a reviewed dimension phrase")
        if not self.values or len(self.values) != len(self.value_phrases):
            raise ValueError("filter values require one grounded phrase each")
        if self.operator == FilterOperator.IN:
            if len(self.values) > 20:
                raise ValueError("IN filters support at most 20 bound values")
        elif len(self.values) != 1:
            raise ValueError("non-IN filters require exactly one value")
        if any(not item.strip() for item in self.value_phrases):
            raise ValueError("filter value phrases must be grounded")
        if self.operator == FilterOperator.IN and not self.operator_phrase.strip():
            raise ValueError("IN filters require a grounded operator phrase")


@dataclass(frozen=True)
class TimeWindow:
    """Explicit deterministic UTC interval with half-open boundaries."""

    dimension_id: str
    dimension_phrase: str
    start: ScalarLiteral
    end: ScalarLiteral
    start_phrase: str
    end_phrase: str
    range_phrase: str = ""
    timezone: str = "UTC"
    bounds: str = "[start,end)"

    def __post_init__(self) -> None:
        if not self.dimension_id or not self.dimension_phrase.strip():
            raise ValueError("time window requires a reviewed time dimension")
        if self.start.kind not in {LiteralKind.DATE, LiteralKind.TIMESTAMP}:
            raise ValueError("time window start must be a date or timestamp")
        if self.end.kind != self.start.kind:
            raise ValueError("time window endpoints must use the same literal kind")
        if not self.start_phrase.strip() or not self.end_phrase.strip():
            raise ValueError("time window endpoints must be grounded in the question")
        if self.timezone != "UTC" or self.bounds != "[start,end)":
            raise ValueError("Phase-2 time windows are explicit UTC [start,end) only")
        start_value = self.start.python_value()
        end_value = self.end.python_value()
        if self.start.kind == LiteralKind.DATE:
            assert type(start_value) is date and type(end_value) is date
        else:
            assert isinstance(start_value, datetime) and isinstance(end_value, datetime)
        if start_value >= end_value:
            raise ValueError("time window start must precede its exclusive end")


@dataclass(frozen=True)
class MetricAggregateNode:
    node_id: str
    metric_id: str
    aggregate: Aggregate


@dataclass(frozen=True)
class BinaryMetricNode:
    node_id: str
    operator: DerivedOperator
    left_node_id: str
    right_node_id: str


DerivedNode: TypeAlias = MetricAggregateNode | BinaryMetricNode


@dataclass(frozen=True)
class DerivedMetricDefinition:
    """Reviewed expression DAG contract; execution is intentionally separate."""

    id: str
    label: str
    nodes: tuple[DerivedNode, ...]
    root_node_id: str
    grain_dimension_ids: tuple[str, ...]
    unit: str
    zero_division: str = "null"
    null_policy: str = "propagate"

    def __post_init__(self) -> None:
        if not self.id or not self.label or not self.nodes or not self.root_node_id:
            raise ValueError("derived metric definition is incomplete")
        if self.zero_division != "null" or self.null_policy != "propagate":
            raise ValueError("unsupported derived metric safety policy")
        node_ids = [item.node_id for item in self.nodes]
        if len(node_ids) != len(set(node_ids)) or self.root_node_id not in node_ids:
            raise ValueError("derived metric node ids must be unique with a known root")
        known: set[str] = set()
        for node in self.nodes:
            if isinstance(node, MetricAggregateNode):
                if not node.metric_id:
                    raise ValueError("derived metric leaf requires a metric id")
            else:
                if node.left_node_id not in known or node.right_node_id not in known:
                    raise ValueError("derived metric DAG must be topologically ordered")
                if (
                    node.left_node_id == node.node_id
                    or node.right_node_id == node.node_id
                ):
                    raise ValueError("derived metric DAG cannot contain a direct cycle")
            known.add(node.node_id)


@dataclass(frozen=True)
class SemanticPlan:
    question_sha256: str
    stamp: SemanticStateStamp
    measure: MeasureSelection
    metric_phrase: str
    dimensions: tuple[DimensionSelection, ...] = ()
    filters: tuple[FilterPredicate, ...] = ()
    time_window: TimeWindow | None = None
    limit: int = 100
    version: int = SEMANTIC_PLAN_VERSION

    def __post_init__(self) -> None:
        if self.version != SEMANTIC_PLAN_VERSION:
            raise ValueError("unsupported semantic plan version")
        if not re.fullmatch(r"[0-9a-f]{64}", self.question_sha256):
            raise ValueError("semantic plan requires a SHA-256 question binding")
        if not self.metric_phrase.strip():
            raise ValueError("semantic plan requires a grounded metric phrase")
        if not 1 <= self.limit <= 1000:
            raise ValueError("semantic plan limit must be between 1 and 1000")
        dimension_ids = [item.dimension_id for item in self.dimensions]
        if len(dimension_ids) != len(set(dimension_ids)):
            raise ValueError("semantic plan cannot repeat an output dimension")
        canonical_filters = tuple(sorted(self.filters, key=_filter_sort_key))
        object.__setattr__(self, "filters", canonical_filters)
        for index, predicate in enumerate(canonical_filters):
            for other in canonical_filters[index + 1 :]:
                if predicate.dimension_id != other.dimension_id:
                    break
                if predicate != other:
                    raise ValueError(
                        "Phase-2 filters allow only one predicate per dimension"
                    )

    def canonical_dict(self) -> dict[str, object]:
        measure = asdict(self.measure)
        measure["kind"] = self.measure.kind.value
        filters = [asdict(item) for item in self.filters]
        return {
            "version": self.version,
            "question_sha256": self.question_sha256,
            "stamp": asdict(self.stamp),
            "measure": measure,
            "metric_phrase": self.metric_phrase,
            "dimensions": [asdict(item) for item in self.dimensions],
            "filters": filters,
            "time_window": asdict(self.time_window) if self.time_window else None,
            "limit": self.limit,
        }

    @property
    def plan_hash(self) -> str:
        encoded = json.dumps(
            self.canonical_dict(),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=lambda value: (
                value.value if isinstance(value, Enum) else str(value)
            ),
        ).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def _filter_sort_key(predicate: FilterPredicate) -> str:
    return json.dumps(
        asdict(predicate),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        default=lambda value: value.value if isinstance(value, Enum) else str(value),
    )


@dataclass(frozen=True)
class BoundParameter:
    name: str
    literal: ScalarLiteral = field(repr=False)

    def __post_init__(self) -> None:
        if not _PARAMETER_NAME.fullmatch(self.name):
            raise ValueError("bound parameter names must be compiler-owned pN tokens")


@dataclass(frozen=True)
class PreparedSql:
    sql: str
    parameters: tuple[BoundParameter, ...]
    plan_hash: str

    def __post_init__(self) -> None:
        if not self.sql.strip() or not re.fullmatch(r"[0-9a-f]{64}", self.plan_hash):
            raise ValueError("prepared SQL requires SQL text and a semantic plan hash")
        names = [item.name for item in self.parameters]
        if len(names) != len(set(names)):
            raise ValueError("prepared SQL parameter names must be unique")
        placeholders = set(_SQL_PARAMETER.findall(self.sql))
        if placeholders != set(names):
            raise ValueError("prepared SQL placeholders and bound parameters differ")

    def parameter_mapping(self) -> dict[str, object]:
        """Return execution values without exposing them through repr or audit."""

        return {item.name: item.literal.python_value() for item in self.parameters}

    def audit_detail(self) -> dict[str, object]:
        return {
            "sql": self.sql,
            "plan_hash": self.plan_hash,
            "parameter_kinds": {
                item.name: item.literal.kind.value for item in self.parameters
            },
        }


@dataclass(frozen=True)
class PlanReady:
    plan: SemanticPlan
    prepared: PreparedSql
    status: str = field(default="ready", init=False)

    def __post_init__(self) -> None:
        if self.prepared.plan_hash != self.plan.plan_hash:
            raise ValueError("prepared SQL must be bound to the exact semantic plan")


@dataclass(frozen=True)
class PlanNeedsReview:
    reason_code: str
    message: str
    review_id: str
    status: str = field(default="needs_review", init=False)


@dataclass(frozen=True)
class PlanClarification:
    reason_code: str
    message: str
    status: str = field(default="clarification", init=False)


@dataclass(frozen=True)
class PlanBlocked:
    reason_code: str
    message: str
    status: str = field(default="blocked", init=False)


PlanningResult: TypeAlias = (
    PlanReady | PlanNeedsReview | PlanClarification | PlanBlocked
)
