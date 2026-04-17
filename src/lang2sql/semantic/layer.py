"""SemanticLayer — business meaning layer on top of physical DB."""

from __future__ import annotations

from dataclasses import replace
from itertools import combinations
from typing import Any

from .types import BusinessRule, Dimension, Metric, Relationship


class SemanticLayer:
    """Business meaning layer on top of physical DB.

    Holds metrics, dimensions, relationships, and business rules.
    Built interactively through conversation with the user.
    """

    def __init__(self) -> None:
        self.metrics: dict[str, Metric] = {}
        self.dimensions: dict[str, Dimension] = {}
        self.relationships: list[Relationship] = []
        self.business_rules: list[BusinessRule] = []

    # ------------------------------------------------------------------ #
    #  Metrics CRUD                                                       #
    # ------------------------------------------------------------------ #

    def add_metric(self, metric: Metric) -> None:
        self.metrics[metric.name] = metric

    def update_metric(self, name: str, **kwargs: Any) -> Metric:
        metric = self.metrics[name]
        self.metrics[name] = replace(metric, **kwargs)
        return self.metrics[name]

    def remove_metric(self, name: str) -> None:
        del self.metrics[name]

    def get_metric(self, name: str) -> Metric | None:
        return self.metrics.get(name)

    # ------------------------------------------------------------------ #
    #  Dimensions CRUD                                                    #
    # ------------------------------------------------------------------ #

    def add_dimension(self, dimension: Dimension) -> None:
        self.dimensions[dimension.name] = dimension

    def update_dimension(self, name: str, **kwargs: Any) -> Dimension:
        dim = self.dimensions[name]
        self.dimensions[name] = replace(dim, **kwargs)
        return self.dimensions[name]

    def remove_dimension(self, name: str) -> None:
        del self.dimensions[name]

    def get_dimension(self, name: str) -> Dimension | None:
        return self.dimensions.get(name)

    # ------------------------------------------------------------------ #
    #  Relationships                                                      #
    # ------------------------------------------------------------------ #

    def add_relationship(self, rel: Relationship) -> None:
        self.relationships.append(rel)

    def get_relationships_for(self, table: str) -> list[Relationship]:
        return [
            r
            for r in self.relationships
            if r.from_table == table or r.to_table == table
        ]

    # ------------------------------------------------------------------ #
    #  Business Rules                                                     #
    # ------------------------------------------------------------------ #

    def add_rule(self, rule: BusinessRule) -> None:
        self.business_rules.append(rule)

    def get_rules_for_metric(self, metric_name: str) -> list[BusinessRule]:
        return [
            r for r in self.business_rules if metric_name in r.applies_to
        ]

    # ------------------------------------------------------------------ #
    #  Search                                                             #
    # ------------------------------------------------------------------ #

    def search(self, query: str) -> dict[str, list]:
        """Search metrics, dimensions, and rules by keyword.

        Case-insensitive substring match across name, display_name,
        description, and rule text.
        """
        q = query.lower()

        def _match_metric(m: Metric) -> bool:
            return q in " ".join(
                [m.name, m.display_name, m.description, m.expression]
            ).lower()

        def _match_dimension(d: Dimension) -> bool:
            return q in " ".join(
                [d.name, d.display_name, d.expression]
            ).lower()

        def _match_rule(r: BusinessRule) -> bool:
            return q in " ".join([r.name, r.rule]).lower()

        return {
            "metrics": [m for m in self.metrics.values() if _match_metric(m)],
            "dimensions": [
                d for d in self.dimensions.values() if _match_dimension(d)
            ],
            "rules": [r for r in self.business_rules if _match_rule(r)],
        }

    # ------------------------------------------------------------------ #
    #  Tables                                                             #
    # ------------------------------------------------------------------ #

    def get_required_tables(
        self,
        metric_name: str,
        dimension_names: list[str] | None = None,
    ) -> set[str]:
        """Get all tables needed for a metric and optional dimensions."""
        tables: set[str] = set()
        metric = self.metrics.get(metric_name)
        if metric:
            tables.add(metric.table)
        for dim_name in dimension_names or []:
            dim = self.dimensions.get(dim_name)
            if dim:
                tables.add(dim.table)
        return tables

    def find_join_path(self, tables: set[str]) -> list[Relationship]:
        """Find relationships needed to JOIN the given set of tables.

        For each pair of tables, returns matching relationships.
        """
        needed: list[Relationship] = []
        for a, b in combinations(tables, 2):
            for rel in self.relationships:
                pair = {rel.from_table, rel.to_table}
                if pair == {a, b} and rel not in needed:
                    needed.append(rel)
        return needed

    # ------------------------------------------------------------------ #
    #  Serialization                                                      #
    # ------------------------------------------------------------------ #

    def to_dict(self) -> dict[str, Any]:
        """Convert entire layer to a JSON-serializable dict."""
        return {
            "metrics": [m.to_dict() for m in self.metrics.values()],
            "dimensions": [d.to_dict() for d in self.dimensions.values()],
            "relationships": [r.to_dict() for r in self.relationships],
            "business_rules": [r.to_dict() for r in self.business_rules],
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SemanticLayer:
        """Reconstruct from dict."""
        layer = cls()
        for m in data.get("metrics", []):
            layer.add_metric(Metric.from_dict(m))
        for d in data.get("dimensions", []):
            layer.add_dimension(Dimension.from_dict(d))
        for r in data.get("relationships", []):
            layer.add_relationship(Relationship.from_dict(r))
        for r in data.get("business_rules", []):
            layer.add_rule(BusinessRule.from_dict(r))
        return layer

    def to_context_string(self) -> str:
        """Human-readable summary for system prompt injection."""
        parts: list[str] = []

        if self.metrics:
            parts.append("## Metrics")
            for m in self.metrics.values():
                line = f"- **{m.display_name}** (`{m.name}`): `{m.expression}`"
                if m.description:
                    line += f" — {m.description}"
                if m.filters:
                    line += f"  Filters: {', '.join(m.filters)}"
                parts.append(line)

        if self.dimensions:
            parts.append("\n## Dimensions")
            for d in self.dimensions.values():
                parts.append(
                    f"- **{d.display_name}** (`{d.name}`, {d.type}): "
                    f"`{d.expression}`"
                )

        if self.business_rules:
            parts.append("\n## Business Rules")
            for r in self.business_rules:
                line = f"- **{r.name}**: {r.rule}"
                if r.sql_condition:
                    line += f" → `{r.sql_condition}`"
                parts.append(line)

        if self.relationships:
            parts.append("\n## Relationships")
            for r in self.relationships:
                parts.append(
                    f"- {r.from_table} {r.join_type} {r.to_table} "
                    f"ON {r.on_clause}"
                )

        return "\n".join(parts)

    def is_empty(self) -> bool:
        return (
            not self.metrics
            and not self.dimensions
            and not self.relationships
            and not self.business_rules
        )
