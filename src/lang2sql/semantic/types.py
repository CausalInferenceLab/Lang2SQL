"""Core types for the semantic layer sitting on top of physical DB."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


@dataclass
class Metric:
    """A measurable business value (e.g., revenue, order count).

    ``expression`` is a SQL aggregate fragment like ``SUM(orders.amount)``.
    ``filters`` are SQL WHERE conditions that are always applied (e.g.,
    ``["status = 'completed'"]``).
    """

    name: str
    display_name: str
    expression: str
    table: str
    description: str = ""
    filters: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "expression": self.expression,
            "table": self.table,
            "description": self.description,
            "filters": list(self.filters),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Metric:
        return cls(
            name=d["name"],
            display_name=d.get("display_name", d["name"]),
            expression=d["expression"],
            table=d["table"],
            description=d.get("description", ""),
            filters=d.get("filters", []),
        )


@dataclass
class Dimension:
    """An axis for grouping or filtering data (e.g., time, region).

    ``expression`` is a SQL expression like
    ``DATE_TRUNC('month', orders.order_date)`` or ``customers.city``.
    """

    name: str
    display_name: str
    expression: str
    table: str
    type: Literal["time", "categorical", "geographic"] = "categorical"

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "display_name": self.display_name,
            "expression": self.expression,
            "table": self.table,
            "type": self.type,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Dimension:
        return cls(
            name=d["name"],
            display_name=d.get("display_name", d["name"]),
            expression=d["expression"],
            table=d["table"],
            type=d.get("type", "categorical"),
        )


@dataclass
class Relationship:
    """A JOIN relationship between two physical tables."""

    from_table: str
    to_table: str
    on_clause: str
    join_type: str = "LEFT JOIN"

    def to_dict(self) -> dict[str, Any]:
        return {
            "from_table": self.from_table,
            "to_table": self.to_table,
            "on_clause": self.on_clause,
            "join_type": self.join_type,
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> Relationship:
        return cls(
            from_table=d["from_table"],
            to_table=d["to_table"],
            on_clause=d["on_clause"],
            join_type=d.get("join_type", "LEFT JOIN"),
        )


@dataclass
class BusinessRule:
    """A business rule that constrains how data should be interpreted.

    ``sql_condition`` is a WHERE fragment like ``status != 'cancelled'``.
    ``applies_to`` lists metric names this rule affects.
    """

    name: str
    rule: str
    sql_condition: str = ""
    applies_to: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "rule": self.rule,
            "sql_condition": self.sql_condition,
            "applies_to": list(self.applies_to),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> BusinessRule:
        return cls(
            name=d["name"],
            rule=d["rule"],
            sql_condition=d.get("sql_condition", ""),
            applies_to=d.get("applies_to", []),
        )
