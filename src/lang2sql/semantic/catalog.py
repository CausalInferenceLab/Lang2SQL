"""Small, persisted semantic catalog used by the governed query path.

The catalog intentionally stores only facts needed to select and compile a
read-only aggregate query.  It is not a second metadata platform: physical DB
facts are captured automatically, while business choices stay pending until a
real question needs them.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from enum import Enum
from typing import Any


CATALOG_KEY = "semantic_catalog:v1"
PENDING_REVIEW_KEY = "semantic_pending_review:v1"


class Aggregate(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class ReviewState(str, Enum):
    CONFIRMED = "confirmed"
    PENDING = "pending"
    REJECTED = "rejected"


@dataclass
class TableSpec:
    id: str
    name: str
    schema: str = ""

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}" if self.schema else self.name


@dataclass
class MetricSpec:
    id: str
    label: str
    table_id: str
    column: str
    aggregate: Aggregate | None = None
    state: ReviewState = ReviewState.PENDING
    allowed_aggregates: list[Aggregate] = field(
        default_factory=lambda: [
            Aggregate.SUM,
            Aggregate.AVG,
            Aggregate.MIN,
            Aggregate.MAX,
        ]
    )
    unit: str = ""
    source_record_count: bool = False
    aliases: list[str] = field(default_factory=list)
    auto_aliases: list[str] = field(default_factory=list)
    rejected_aliases: list[str] = field(default_factory=list)
    reviewed_bindings: dict[str, list[str]] = field(default_factory=dict)
    rejected_bindings: list[str] = field(default_factory=list)
    binding_reviewers: dict[str, str] = field(default_factory=dict)


@dataclass
class DimensionSpec:
    id: str
    label: str
    table_id: str
    column: str
    data_type: str
    kind: str = "categorical"
    aliases: list[str] = field(default_factory=list)
    auto_aliases: list[str] = field(default_factory=list)
    rejected_aliases: list[str] = field(default_factory=list)
    alias_reviewers: dict[str, str] = field(default_factory=dict)


@dataclass
class JoinSpec:
    """A declared child-to-parent foreign-key edge.

    The compiler only traverses this direction.  That keeps a metric's grain
    stable and prevents an unnoticed parent-to-child fan-out.
    """

    id: str
    child_table_id: str
    child_column: str
    parent_table_id: str
    parent_column: str


@dataclass
class SemanticCatalog:
    fingerprint: str
    tables: list[TableSpec] = field(default_factory=list)
    metrics: list[MetricSpec] = field(default_factory=list)
    dimensions: list[DimensionSpec] = field(default_factory=list)
    joins: list[JoinSpec] = field(default_factory=list)
    blocked_columns: list[str] = field(default_factory=list)
    version: int = 1
    review_revision: int = 0

    def table(self, table_id: str) -> TableSpec | None:
        return next((item for item in self.tables if item.id == table_id), None)

    def metric(self, metric_id: str) -> MetricSpec | None:
        return next((item for item in self.metrics if item.id == metric_id), None)

    def dimension(self, dimension_id: str) -> DimensionSpec | None:
        return next((item for item in self.dimensions if item.id == dimension_id), None)

    @property
    def pending_metric_count(self) -> int:
        return sum(not item.reviewed_bindings for item in self.metrics)

    @property
    def confirmed_metric_count(self) -> int:
        return sum(bool(item.reviewed_bindings) for item in self.metrics)

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False, sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> "SemanticCatalog":
        data: dict[str, Any] = json.loads(raw)
        return cls(
            version=int(data.get("version", 1)),
            review_revision=int(data.get("review_revision", 0)),
            fingerprint=str(data["fingerprint"]),
            tables=[TableSpec(**item) for item in data.get("tables", [])],
            metrics=[
                MetricSpec(
                    id=item["id"],
                    label=item["label"],
                    table_id=item["table_id"],
                    column=item["column"],
                    aggregate=(
                        Aggregate(item["aggregate"]) if item.get("aggregate") else None
                    ),
                    state=ReviewState(item.get("state", ReviewState.PENDING.value)),
                    allowed_aggregates=[
                        Aggregate(value)
                        for value in item.get(
                            "allowed_aggregates",
                            [
                                Aggregate.SUM.value,
                                Aggregate.AVG.value,
                                Aggregate.MIN.value,
                                Aggregate.MAX.value,
                            ],
                        )
                    ],
                    unit=item.get("unit", ""),
                    source_record_count=bool(item.get("source_record_count", False)),
                    aliases=list(item.get("aliases", [])),
                    auto_aliases=list(
                        item.get("auto_aliases", item.get("aliases", []))
                    ),
                    rejected_aliases=list(item.get("rejected_aliases", [])),
                    reviewed_bindings={
                        phrase: (
                            list(values) if isinstance(values, list) else [str(values)]
                        )
                        for phrase, values in item.get("reviewed_bindings", {}).items()
                    },
                    rejected_bindings=list(item.get("rejected_bindings", [])),
                    binding_reviewers=dict(item.get("binding_reviewers", {})),
                )
                for item in data.get("metrics", [])
            ],
            dimensions=[
                DimensionSpec(
                    **{
                        **item,
                        "auto_aliases": item.get(
                            "auto_aliases", item.get("aliases", [])
                        ),
                    }
                )
                for item in data.get("dimensions", [])
            ],
            joins=[JoinSpec(**item) for item in data.get("joins", [])],
            blocked_columns=list(data.get("blocked_columns", [])),
        )


@dataclass
class PendingReview:
    metric_id: str
    question: str
    metric_phrase: str
    dimension_bindings: list[dict[str, str]]
    allowed_choices: list[str]
    proposed_aggregate: str = ""
    query_dimensions: list[dict[str, str]] = field(default_factory=list)
    query_limit: int = 100
    catalog_fingerprint: str = ""
    catalog_review_revision: int = 0
    requester_id: str = ""
    metric_alias_pending: bool = False
    aggregate_pending: bool = False

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> "PendingReview":
        data = json.loads(raw)
        return cls(
            metric_id=data["metric_id"],
            question=data.get("question", ""),
            metric_phrase=data.get("metric_phrase", ""),
            dimension_bindings=list(data.get("dimension_bindings", [])),
            allowed_choices=list(
                data.get(
                    "allowed_choices",
                    data.get("allowed_aggregates", []),
                )
            ),
            proposed_aggregate=data.get("proposed_aggregate", ""),
            query_dimensions=list(data.get("query_dimensions", [])),
            query_limit=int(data.get("query_limit", 100)),
            catalog_fingerprint=data.get("catalog_fingerprint", ""),
            catalog_review_revision=int(data.get("catalog_review_revision", 0)),
            requester_id=data.get("requester_id", ""),
            metric_alias_pending=bool(data.get("metric_alias_pending", False)),
            aggregate_pending=bool(data.get("aggregate_pending", True)),
        )
