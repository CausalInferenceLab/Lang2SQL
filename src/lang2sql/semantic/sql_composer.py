"""SQL composer — builds queries from semantic layer definitions."""

from __future__ import annotations

from typing import Any


class SQLComposer:
    """Composes SQL queries from semantic layer definitions.

    Given a metric + optional dimensions + optional filters,
    produces a correct SQL query using the semantic layer's
    definitions for expressions, joins, and business rules.
    """

    def __init__(self, layer: Any, dialect: str = "sqlite") -> None:
        self._layer = layer
        self._dialect = dialect

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def compose(
        self,
        metric_name: str,
        dimension_names: list[str] | None = None,
        filters: list[str] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Compose a SQL query from semantic definitions.

        Steps:
            1. Look up metric → expression, table, built-in filters
            2. Look up dimensions → expressions, tables
            3. Collect business rules that apply to this metric
            4. Determine all required tables
            5. Resolve JOIN path between tables
            6. Assemble SELECT / FROM / JOIN / WHERE / GROUP BY / ORDER BY / LIMIT
        """
        metric = self._find_metric(metric_name)

        dimensions = [
            self._find_dimension(d) for d in (dimension_names or [])
        ]

        # --- SELECT ---------------------------------------------------
        select_exprs: list[str] = []
        for dim in dimensions:
            select_exprs.append(f"{dim.expression} AS {dim.name}")
        select_exprs.append(f"{metric.expression} AS {metric.name}")

        # --- FROM (anchor = metric table) -----------------------------
        from_table: str = metric.table

        # --- Required tables & JOINs ---------------------------------
        required_tables: set[str] = {metric.table}
        for dim in dimensions:
            required_tables.add(dim.table)

        join_clauses = self._resolve_joins(from_table, required_tables)

        # --- WHERE ----------------------------------------------------
        where_parts: list[str] = []
        where_parts.extend(metric.filters)
        where_parts.extend(self._applicable_rules(metric_name))
        where_parts.extend(filters or [])
        where_parts = _deduplicate(where_parts)

        # --- GROUP BY (positional, one per dimension) -----------------
        group_by: list[str] | None = None
        if dimensions:
            group_by = [str(i + 1) for i in range(len(dimensions))]

        return self.compose_raw(
            select_exprs=select_exprs,
            from_table=from_table,
            joins=join_clauses or None,
            where=where_parts or None,
            group_by=group_by,
            order_by=order_by,
            limit=limit,
        )

    def compose_raw(
        self,
        select_exprs: list[str],
        from_table: str,
        joins: list[str] | None = None,
        where: list[str] | None = None,
        group_by: list[str] | None = None,
        order_by: str | None = None,
        limit: int | None = None,
    ) -> str:
        """Low-level SQL composition from raw parts."""
        parts: list[str] = []

        # SELECT
        parts.append("SELECT " + ",\n       ".join(select_exprs))

        # FROM
        parts.append(f"FROM {from_table}")

        # JOINs
        if joins:
            parts.extend(joins)

        # WHERE
        if where:
            clauses = "\n  AND ".join(where)
            parts.append(f"WHERE {clauses}")

        # GROUP BY
        if group_by:
            parts.append("GROUP BY " + ", ".join(group_by))

        # ORDER BY
        if order_by is not None:
            parts.append(f"ORDER BY {order_by}")

        # LIMIT
        if limit is not None:
            parts.append(f"LIMIT {limit}")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_metric(self, name: str) -> Any:
        """Look up a metric by name from the semantic layer."""
        metrics = self._layer.metrics
        if isinstance(metrics, dict):
            if name in metrics:
                return metrics[name]
        else:
            for m in metrics:
                if m.name == name:
                    return m
        raise KeyError(f"Metric '{name}' not found in semantic layer")

    def _find_dimension(self, name: str) -> Any:
        """Look up a dimension by name from the semantic layer."""
        dimensions = self._layer.dimensions
        if isinstance(dimensions, dict):
            if name in dimensions:
                return dimensions[name]
        else:
            for d in dimensions:
                if d.name == name:
                    return d
        raise KeyError(f"Dimension '{name}' not found in semantic layer")

    def _applicable_rules(self, metric_name: str) -> list[str]:
        """Return SQL conditions from business rules that apply to this metric."""
        rules: list[str] = []
        for br in getattr(self._layer, "business_rules", []):
            if not br.sql_condition:
                continue
            # Rule applies if applies_to is empty (global) or contains this metric.
            if not br.applies_to or metric_name in br.applies_to:
                rules.append(br.sql_condition)
        return rules

    def _resolve_joins(
        self, anchor: str, required: set[str]
    ) -> list[str]:
        """Build JOIN clauses for every table beyond the anchor."""
        remaining = required - {anchor}
        if not remaining:
            return []

        relationships: list[Any] = getattr(
            self._layer, "relationships", []
        )

        # Index relationships bidirectionally for BFS.
        adj: dict[str, list[Any]] = {}
        for rel in relationships:
            adj.setdefault(rel.from_table, []).append(rel)
            adj.setdefault(rel.to_table, []).append(rel)

        clauses: list[str] = []
        joined: set[str] = {anchor}

        for target in sorted(remaining):
            path = self._bfs_path(anchor, target, adj, joined)
            if path is None:
                raise KeyError(
                    f"No relationship path from '{anchor}' to '{target}'"
                )
            for rel, next_table in path:
                if next_table in joined:
                    continue
                on = rel.on_clause
                clauses.append(f"{rel.join_type} {next_table} ON {on}")
                joined.add(next_table)

        return clauses

    @staticmethod
    def _bfs_path(
        start: str,
        end: str,
        adj: dict[str, list[Any]],
        already_joined: set[str],
    ) -> list[tuple[Any, str]] | None:
        """BFS over relationships to find a join path from *start* to *end*.

        Returns a list of ``(Relationship, next_table)`` pairs, or ``None``
        if no path exists.  Prefers going through tables that are already
        in the joined set so we don't introduce unnecessary intermediates.
        """
        from collections import deque

        # Each queue entry: (current_table, path_so_far)
        queue: deque[tuple[str, list[tuple[Any, str]]]] = deque()
        queue.append((start, []))
        visited: set[str] = {start}

        while queue:
            current, path = queue.popleft()
            for rel in adj.get(current, []):
                # Determine which side is "next".
                if rel.from_table == current:
                    next_table = rel.to_table
                else:
                    next_table = rel.from_table

                if next_table in visited:
                    continue
                visited.add(next_table)

                new_path = path + [(rel, next_table)]
                if next_table == end:
                    return new_path
                queue.append((next_table, new_path))

        return None


def _deduplicate(items: list[str]) -> list[str]:
    """Deduplicate while preserving order."""
    seen: set[str] = set()
    result: list[str] = []
    for item in items:
        normalised = item.strip()
        if normalised not in seen:
            seen.add(normalised)
            result.append(normalised)
    return result
