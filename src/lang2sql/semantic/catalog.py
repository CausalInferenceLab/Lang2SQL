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
CONNECTION_BINDING_KEY = "semantic_connection_binding:v1"
CONNECTION_GENERATION_KEY = "semantic_connection_generation:v1"
CLASSIFICATION_POLICY_VERSION = 4


@dataclass(frozen=True)
class ConnectionBinding:
    """Server-owned identity for one activated execution source."""

    source_id: str
    generation: int
    managed_credentials: bool = True

    def to_json(self) -> str:
        return json.dumps(asdict(self), sort_keys=True)

    @classmethod
    def from_json(cls, raw: str) -> "ConnectionBinding":
        data = json.loads(raw)
        binding = cls(
            source_id=str(data["source_id"]),
            generation=int(data["generation"]),
            managed_credentials=bool(data.get("managed_credentials", True)),
        )
        if not binding.source_id or binding.generation <= 0:
            raise ValueError("invalid connection binding")
        return binding


class Aggregate(str, Enum):
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"


class MetricExpressionKind(str, Enum):
    """Typed physical expression used by the deterministic compiler."""

    COLUMN = "column"
    SOURCE_ROWS = "source_rows"


class ReviewState(str, Enum):
    CONFIRMED = "confirmed"
    PENDING = "pending"
    REJECTED = "rejected"


class DimensionReviewPolicy(str, Enum):
    """Whether raw grouped labels are metadata-safe or steward-released."""

    AUTO_SAFE = "auto_safe"
    RELEASE_REQUIRED = "release_required"


class DimensionDisclosureTier(str, Enum):
    """Steward assertion governing grouped value disclosure."""

    BLOCKED = "blocked"
    CONTROLLED_GROUPED = "controlled_grouped"
    PUBLIC_GROUPED = "public_grouped"


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
    expression_kind: MetricExpressionKind = MetricExpressionKind.COLUMN
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
    data_type: str = ""
    nullable: bool = True
    classification_evidence: str = "numeric_measure_metadata_only"
    source_record_count: bool = False
    aliases: list[str] = field(default_factory=list)
    auto_aliases: list[str] = field(default_factory=list)
    # Enrich suggestions improve candidate discovery only. They never count as
    # reviewed business meaning or an approved aggregate binding.
    suggested_aliases: list[str] = field(default_factory=list)
    suggestion_sources: dict[str, str] = field(default_factory=dict)
    rejected_aliases: list[str] = field(default_factory=list)
    reviewed_bindings: dict[str, list[str]] = field(default_factory=dict)
    rejected_bindings: list[str] = field(default_factory=list)
    binding_reviewers: dict[str, str] = field(default_factory=dict)
    alias_reviewers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Reject expression combinations the compiler cannot interpret safely."""

        if self.expression_kind == MetricExpressionKind.SOURCE_ROWS:
            if not self.source_record_count:
                raise ValueError("source-row metrics require source_record_count")
            if self.aggregate not in {None, Aggregate.COUNT}:
                raise ValueError("source-row metrics only support COUNT")
            if self.allowed_aggregates != [Aggregate.COUNT]:
                raise ValueError("source-row metrics must allow exactly COUNT")
            # A non-empty column remains accepted only so catalogs written by
            # the older PK-based representation can migrate without losing
            # reviewed aliases. The compiler keys exclusively on expression_kind.
            return
        if self.expression_kind != MetricExpressionKind.COLUMN:
            raise ValueError("unsupported metric expression kind")
        if self.source_record_count:
            raise ValueError("column metrics cannot be source-record counts")
        if not self.column:
            raise ValueError("column metrics require a physical column")


@dataclass
class DimensionSpec:
    id: str
    label: str
    table_id: str
    column: str
    data_type: str
    kind: str = "categorical"
    review_policy: DimensionReviewPolicy = DimensionReviewPolicy.AUTO_SAFE
    classification_evidence: str = "metadata_safe"
    classification_policy_version: int = CLASSIFICATION_POLICY_VERSION
    raw_output_allowed: bool = True
    disclosure_tier: DimensionDisclosureTier = DimensionDisclosureTier.BLOCKED
    release_reviewer: str = ""
    release_catalog_fingerprint: str = ""
    released_at: str = ""
    action_revision: int = 0
    aliases: list[str] = field(default_factory=list)
    auto_aliases: list[str] = field(default_factory=list)
    # Candidate-only evidence from DB comments or metadata-only enrichment.
    # A human review is still required before a new phrase becomes an alias.
    suggested_aliases: list[str] = field(default_factory=list)
    suggestion_sources: dict[str, str] = field(default_factory=dict)
    # Physical-name aliases reserve ownership for conflict checks but never
    # make a release-required dimension selectable or review-complete.
    reserved_aliases: list[str] = field(default_factory=list)
    rejected_aliases: list[str] = field(default_factory=list)
    alias_reviewers: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.action_revision < 0:
            raise ValueError("dimension action revision cannot be negative")
        if self.review_policy == DimensionReviewPolicy.AUTO_SAFE:
            if not self.raw_output_allowed:
                raise ValueError("auto-safe dimensions must allow grouped output")
            if self.disclosure_tier == DimensionDisclosureTier.BLOCKED:
                self.disclosure_tier = DimensionDisclosureTier.PUBLIC_GROUPED
            if self.disclosure_tier != DimensionDisclosureTier.PUBLIC_GROUPED:
                raise ValueError("auto-safe dimensions require the public grouped tier")
            return
        if self.review_policy != DimensionReviewPolicy.RELEASE_REQUIRED:
            raise ValueError("unsupported dimension review policy")
        if self.auto_aliases:
            raise ValueError("release-required dimensions cannot have auto aliases")
        if not self.raw_output_allowed:
            if self.disclosure_tier != DimensionDisclosureTier.BLOCKED:
                raise ValueError("unreleased dimensions must use the blocked tier")
            return
        if self.disclosure_tier not in {
            DimensionDisclosureTier.CONTROLLED_GROUPED,
            DimensionDisclosureTier.PUBLIC_GROUPED,
        }:
            raise ValueError("released dimensions require a grouped disclosure tier")
        if self.raw_output_allowed and not (
            self.release_reviewer and self.release_catalog_fingerprint
        ):
            raise ValueError("released dimensions require reviewer and fingerprint")


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
    version: int = 3
    review_revision: int = 0
    classification_policy_version: int = CLASSIFICATION_POLICY_VERSION
    source_id: str = ""
    connection_generation: int = 0
    public_data_scope: bool = False
    public_data_reviewer: str = ""
    public_data_fingerprint: str = ""
    public_data_confirmed_at: str = ""
    # These epochs invalidate action tokens for global governance changes
    # without invalidating unrelated per-dimension actions on every review.
    metric_action_epoch: int = 0
    dimension_action_epoch: int = 0
    public_scope_epoch: int = 0
    # Connect-time Enrich diagnostics are persisted so every frontend can
    # explain whether the optional metadata-only model pass succeeded.
    enrichment_status: str = "metadata_ready"
    enrichment_reason: str = ""

    def __post_init__(self) -> None:
        if (
            self.metric_action_epoch < 0
            or self.dimension_action_epoch < 0
            or self.public_scope_epoch < 0
        ):
            raise ValueError("semantic governance epochs cannot be negative")
        if self.enrichment_status not in {
            "metadata_ready",
            "llm_ready",
            "llm_degraded",
        }:
            raise ValueError("unsupported semantic enrichment status")
        if self.enrichment_status != "llm_degraded" and self.enrichment_reason:
            raise ValueError("only degraded enrichment may retain a reason")
        if bool(self.source_id) != (self.connection_generation > 0):
            raise ValueError("source identity and connection generation must pair")
        if self.public_data_scope:
            if not (
                self.public_data_reviewer
                and self.public_data_fingerprint == self.fingerprint
                and self.public_data_confirmed_at
            ):
                raise ValueError(
                    "public data scope requires reviewer, fingerprint, and timestamp"
                )
        elif any(
            (
                self.public_data_reviewer,
                self.public_data_fingerprint,
                self.public_data_confirmed_at,
            )
        ):
            raise ValueError("inactive public data scope cannot retain provenance")
        stale_dimensions = [
            item.id
            for item in self.dimensions
            if item.classification_policy_version != self.classification_policy_version
        ]
        if stale_dimensions:
            raise ValueError(
                "dimension classification policy mismatch: "
                + ", ".join(sorted(stale_dimensions))
            )
        dimension_refs = {f"{item.table_id}.{item.column}" for item in self.dimensions}
        metric_refs = {
            f"{item.table_id}.{item.column}"
            for item in self.metrics
            if item.expression_kind == MetricExpressionKind.COLUMN
        }
        overlap = (dimension_refs | metric_refs).intersection(self.blocked_columns)
        if overlap:
            raise ValueError(
                "blocked columns cannot also be semantic objects: "
                + ", ".join(sorted(overlap))
            )
        for metric in self.metrics:
            alias_overlap = set(metric.aliases).intersection(metric.rejected_aliases)
            if alias_overlap:
                raise ValueError(
                    f"metric aliases cannot be both approved and rejected: {metric.id}"
                )
            suggestion_overlap = set(metric.aliases).intersection(
                metric.suggested_aliases
            )
            if suggestion_overlap:
                raise ValueError(
                    f"metric aliases cannot be both approved and suggested: {metric.id}"
                )
            unknown_sources = set(metric.suggestion_sources).difference(
                metric.suggested_aliases
            )
            if unknown_sources:
                raise ValueError(
                    "metric suggestion provenance requires a matching alias: "
                    + metric.id
                )
            reviewed_bindings = {
                f"{phrase}|{aggregate}"
                for phrase, aggregates in metric.reviewed_bindings.items()
                for aggregate in aggregates
            }
            if reviewed_bindings.intersection(metric.rejected_bindings):
                raise ValueError(
                    "metric bindings cannot be both approved and rejected: " + metric.id
                )
        for dimension in self.dimensions:
            if set(dimension.aliases).intersection(dimension.rejected_aliases):
                raise ValueError(
                    "dimension aliases cannot be both approved and rejected: "
                    + dimension.id
                )
            if set(dimension.aliases).intersection(dimension.suggested_aliases):
                raise ValueError(
                    "dimension aliases cannot be both approved and suggested: "
                    + dimension.id
                )
            unknown_sources = set(dimension.suggestion_sources).difference(
                dimension.suggested_aliases
            )
            if unknown_sources:
                raise ValueError(
                    "dimension suggestion provenance requires a matching alias: "
                    + dimension.id
                )

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
        version = int(data.get("version", 1))
        if version not in {1, 2, 3}:
            raise ValueError(f"unsupported semantic catalog version: {version}")

        def metric_from_mapping(item: dict[str, Any]) -> MetricSpec:
            source_record_count = bool(item.get("source_record_count", False))
            expression_kind = MetricExpressionKind(
                item.get(
                    "expression_kind",
                    (
                        MetricExpressionKind.SOURCE_ROWS.value
                        if source_record_count
                        else MetricExpressionKind.COLUMN.value
                    ),
                )
            )
            default_aggregates = (
                [Aggregate.COUNT.value]
                if expression_kind == MetricExpressionKind.SOURCE_ROWS
                else [
                    Aggregate.SUM.value,
                    Aggregate.AVG.value,
                    Aggregate.MIN.value,
                    Aggregate.MAX.value,
                ]
            )
            return MetricSpec(
                id=item["id"],
                label=item["label"],
                table_id=item["table_id"],
                column=item["column"],
                expression_kind=expression_kind,
                aggregate=(
                    Aggregate(item["aggregate"]) if item.get("aggregate") else None
                ),
                state=ReviewState(item.get("state", ReviewState.PENDING.value)),
                allowed_aggregates=[
                    Aggregate(value)
                    for value in item.get("allowed_aggregates", default_aggregates)
                ],
                unit=item.get("unit", ""),
                data_type=str(item.get("data_type", "")),
                nullable=bool(item.get("nullable", True)),
                classification_evidence=str(
                    item.get("classification_evidence", "legacy_numeric_measure")
                ),
                source_record_count=source_record_count,
                aliases=list(item.get("aliases", [])),
                auto_aliases=list(item.get("auto_aliases", item.get("aliases", []))),
                suggested_aliases=list(item.get("suggested_aliases", [])),
                suggestion_sources=dict(item.get("suggestion_sources", {})),
                rejected_aliases=list(item.get("rejected_aliases", [])),
                reviewed_bindings={
                    phrase: (
                        list(values) if isinstance(values, list) else [str(values)]
                    )
                    for phrase, values in item.get("reviewed_bindings", {}).items()
                },
                rejected_bindings=list(item.get("rejected_bindings", [])),
                binding_reviewers=dict(item.get("binding_reviewers", {})),
                alias_reviewers=dict(item.get("alias_reviewers", {})),
            )

        def dimension_from_mapping(item: dict[str, Any]) -> DimensionSpec:
            data_type = str(item["data_type"])
            lowered_type = data_type.lower()
            legacy_string = version == 1 and not any(
                marker in lowered_type
                for marker in (
                    "date",
                    "time",
                    "timestamp",
                    "datetime",
                    "bool",
                )
            )
            policy = (
                DimensionReviewPolicy.RELEASE_REQUIRED
                if legacy_string
                else DimensionReviewPolicy(
                    item.get("review_policy", DimensionReviewPolicy.AUTO_SAFE.value)
                )
            )
            raw_output_allowed = bool(
                item.get(
                    "raw_output_allowed",
                    policy == DimensionReviewPolicy.AUTO_SAFE,
                )
            )
            if legacy_string:
                # V1 had no distinct disclosure review. Migrating its string
                # aliases as selectable would silently reinterpret an old
                # phrase mapping as permission to reveal grouped raw values.
                raw_output_allowed = False
            default_tier = (
                DimensionDisclosureTier.PUBLIC_GROUPED
                if policy == DimensionReviewPolicy.AUTO_SAFE
                else (
                    DimensionDisclosureTier.CONTROLLED_GROUPED
                    if raw_output_allowed
                    else DimensionDisclosureTier.BLOCKED
                )
            )
            return DimensionSpec(
                id=item["id"],
                label=item["label"],
                table_id=item["table_id"],
                column=item["column"],
                data_type=data_type,
                kind=item.get("kind", "categorical"),
                review_policy=policy,
                classification_evidence=(
                    "legacy_catalog_requires_release"
                    if legacy_string
                    else item.get("classification_evidence", "legacy_catalog")
                ),
                classification_policy_version=(
                    CLASSIFICATION_POLICY_VERSION
                    if version == 1
                    else int(item.get("classification_policy_version", 1))
                ),
                raw_output_allowed=raw_output_allowed,
                disclosure_tier=DimensionDisclosureTier(
                    item.get("disclosure_tier", default_tier.value)
                ),
                release_reviewer=item.get("release_reviewer", ""),
                release_catalog_fingerprint=item.get("release_catalog_fingerprint", ""),
                released_at=item.get("released_at", ""),
                action_revision=int(item.get("action_revision", 0)),
                aliases=[] if legacy_string else list(item.get("aliases", [])),
                auto_aliases=(
                    []
                    if legacy_string
                    else list(item.get("auto_aliases", item.get("aliases", [])))
                ),
                suggested_aliases=list(item.get("suggested_aliases", [])),
                suggestion_sources=dict(item.get("suggestion_sources", {})),
                reserved_aliases=list(
                    item.get(
                        "reserved_aliases",
                        [] if version == 1 else item.get("auto_aliases", []),
                    )
                ),
                rejected_aliases=list(item.get("rejected_aliases", [])),
                alias_reviewers=(
                    {} if legacy_string else dict(item.get("alias_reviewers", {}))
                ),
            )

        persisted_policy_version = int(data.get("classification_policy_version", 1))
        if (
            version in {2, 3}
            and persisted_policy_version != CLASSIFICATION_POLICY_VERSION
        ):
            raise ValueError(
                "semantic catalog classification policy requires re-onboarding"
            )

        return cls(
            # Loading V1 performs a fail-closed in-memory migration. Keeping
            # the existing storage key prevents a missing-catalog state from
            # re-enabling the legacy raw-SQL tool surface.
            version=3,
            review_revision=int(data.get("review_revision", 0)),
            classification_policy_version=(
                CLASSIFICATION_POLICY_VERSION
                if version == 1
                else persisted_policy_version
            ),
            public_data_scope=bool(data.get("public_data_scope", False)),
            public_data_reviewer=str(data.get("public_data_reviewer", "")),
            public_data_fingerprint=str(data.get("public_data_fingerprint", "")),
            public_data_confirmed_at=str(data.get("public_data_confirmed_at", "")),
            metric_action_epoch=int(data.get("metric_action_epoch", 0)),
            dimension_action_epoch=int(data.get("dimension_action_epoch", 0)),
            public_scope_epoch=int(data.get("public_scope_epoch", 0)),
            enrichment_status=str(data.get("enrichment_status", "metadata_ready")),
            enrichment_reason=str(data.get("enrichment_reason", "")),
            source_id=str(data.get("source_id", "")),
            connection_generation=int(data.get("connection_generation", 0)),
            fingerprint=str(data["fingerprint"]),
            tables=[TableSpec(**item) for item in data.get("tables", [])],
            metrics=[metric_from_mapping(item) for item in data.get("metrics", [])],
            dimensions=[
                dimension_from_mapping(item) for item in data.get("dimensions", [])
            ],
            joins=[JoinSpec(**item) for item in data.get("joins", [])],
            blocked_columns=list(data.get("blocked_columns", [])),
        )


@dataclass(repr=False)
class PendingReview:
    metric_id: str
    metric_phrase: str
    dimension_bindings: list[dict[str, str]]
    allowed_choices: list[str]
    proposed_aggregate: str = ""
    # Persist safe shape metadata only. Original questions, predicate literals,
    # date bounds, and complete typed drafts never enter the review KV record.
    constraint_filter_count: int = 0
    constraint_has_time_window: bool = False
    catalog_fingerprint: str = ""
    catalog_review_revision: int = 0
    catalog_version: int = 1
    classification_policy_version: int = 1
    source_id: str = ""
    connection_generation: int = 0
    requester_id: str = ""
    metric_alias_pending: bool = False
    aggregate_pending: bool = False
    review_kind: str = "metric"
    review_id: str = ""
    catalog_scope: str = ""
    record_version: int = 2

    def to_json(self) -> str:
        return json.dumps(asdict(self), ensure_ascii=False)

    @classmethod
    def from_json(cls, raw: str) -> "PendingReview":
        data = json.loads(raw)
        legacy_filters = data.get("query_filters", [])
        return cls(
            metric_id=data["metric_id"],
            metric_phrase=data.get("metric_phrase", ""),
            dimension_bindings=list(data.get("dimension_bindings", [])),
            allowed_choices=list(
                data.get(
                    "allowed_choices",
                    data.get("allowed_aggregates", []),
                )
            ),
            proposed_aggregate=data.get("proposed_aggregate", ""),
            constraint_filter_count=int(
                data.get(
                    "constraint_filter_count",
                    len(legacy_filters) if isinstance(legacy_filters, list) else 0,
                )
            ),
            constraint_has_time_window=bool(
                data.get(
                    "constraint_has_time_window",
                    isinstance(data.get("query_time_window"), dict),
                )
            ),
            catalog_fingerprint=data.get("catalog_fingerprint", ""),
            catalog_review_revision=int(data.get("catalog_review_revision", 0)),
            catalog_version=int(data.get("catalog_version", 1)),
            classification_policy_version=int(
                data.get("classification_policy_version", 1)
            ),
            source_id=str(data.get("source_id", "")),
            connection_generation=int(data.get("connection_generation", 0)),
            requester_id=data.get("requester_id", ""),
            metric_alias_pending=bool(data.get("metric_alias_pending", False)),
            aggregate_pending=bool(data.get("aggregate_pending", True)),
            review_kind=str(
                data.get(
                    "review_kind",
                    (
                        "metric"
                        if data.get("metric_alias_pending")
                        or data.get("aggregate_pending", True)
                        else "dimension"
                    ),
                )
            ),
            review_id=str(data.get("review_id", "")),
            catalog_scope=str(data.get("catalog_scope", "")),
            record_version=2,
        )
