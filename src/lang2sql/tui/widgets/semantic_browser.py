"""Tree widget for semantic layer + physical schema browsing."""

from __future__ import annotations

from typing import Any

try:
    from textual.widgets import Tree

    _HAS_TEXTUAL = True
except ImportError:
    _HAS_TEXTUAL = False

if _HAS_TEXTUAL:

    class SemanticBrowser(Tree):
        """Tree widget showing semantic layer metrics/dimensions + physical tables."""

        def populate(
            self,
            semantic_layer: dict[str, Any],
            schema_cache: dict[str, str],
        ) -> None:
            """Rebuild the tree from semantic layer + schema data."""
            self.clear()
            root = self.root

            # Semantic section
            if semantic_layer:
                metrics = semantic_layer.get("metrics", {})
                if metrics:
                    metrics_node = root.add("Metrics")
                    for name, m in metrics.items():
                        display = (
                            m.get("display_name", name)
                            if isinstance(m, dict)
                            else name
                        )
                        metrics_node.add_leaf(f"{display} ({name})")

                dims = semantic_layer.get("dimensions", {})
                if dims:
                    dims_node = root.add("Dimensions")
                    for name, d in dims.items():
                        display = (
                            d.get("display_name", name)
                            if isinstance(d, dict)
                            else name
                        )
                        dims_node.add_leaf(f"{display} ({name})")

                rules = semantic_layer.get("business_rules", [])
                if rules:
                    rules_node = root.add("Rules")
                    for r in rules:
                        rule_name = (
                            r.get("name", "?") if isinstance(r, dict) else str(r)
                        )
                        rules_node.add_leaf(rule_name)

            # Physical schema section
            if schema_cache:
                tables_node = root.add("Tables")
                for table_name in sorted(schema_cache):
                    tables_node.add_leaf(table_name)

            root.expand_all()
