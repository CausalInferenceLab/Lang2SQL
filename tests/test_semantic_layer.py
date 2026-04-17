"""Tests for Phase 2: semantic layer types, CRUD, SQL composer, persistence."""

from __future__ import annotations

import json
import tempfile

import pytest

from lang2sql.semantic.types import (
    BusinessRule,
    Dimension,
    Metric,
    Relationship,
)
from lang2sql.semantic.layer import SemanticLayer
from lang2sql.semantic.sql_composer import SQLComposer
from lang2sql.semantic.store import load_layer, save_layer


# =====================================================================
# Fixtures
# =====================================================================


def _sample_layer() -> SemanticLayer:
    sl = SemanticLayer()
    sl.add_metric(Metric(
        name="revenue",
        display_name="매출",
        expression="SUM(orders.amount)",
        table="orders",
        filters=["status = 'completed'"],
        description="완료 주문 합계",
    ))
    sl.add_metric(Metric(
        name="order_count",
        display_name="주문수",
        expression="COUNT(orders.id)",
        table="orders",
        filters=["status = 'completed'"],
    ))
    sl.add_dimension(Dimension(
        name="order_month",
        display_name="주문월",
        expression="DATE_TRUNC('month', orders.order_date)",
        table="orders",
        type="time",
    ))
    sl.add_dimension(Dimension(
        name="region",
        display_name="지역",
        expression="customers.city",
        table="customers",
        type="geographic",
    ))
    sl.add_dimension(Dimension(
        name="category",
        display_name="카테고리",
        expression="products.category",
        table="products",
        type="categorical",
    ))
    sl.add_relationship(Relationship(
        from_table="orders",
        to_table="customers",
        on_clause="orders.customer_id = customers.id",
    ))
    sl.add_relationship(Relationship(
        from_table="orders",
        to_table="products",
        on_clause="orders.product_id = products.id",
    ))
    sl.add_rule(BusinessRule(
        name="no_cancel",
        rule="매출 계산 시 취소 제외",
        sql_condition="status != 'cancelled'",
        applies_to=["revenue", "order_count"],
    ))
    sl.add_rule(BusinessRule(
        name="no_refund",
        rule="매출 계산 시 환불 제외",
        sql_condition="status != 'refunded'",
        applies_to=["revenue"],
    ))
    return sl


# =====================================================================
# Types serialization
# =====================================================================


class TestTypes:
    def test_metric_round_trip(self):
        m = Metric(name="revenue", display_name="매출", expression="SUM(amount)", table="orders", filters=["x=1"])
        m2 = Metric.from_dict(m.to_dict())
        assert m.name == m2.name
        assert m.filters == m2.filters
        assert m.display_name == m2.display_name

    def test_dimension_round_trip(self):
        d = Dimension(name="month", display_name="월", expression="DATE_TRUNC('month', dt)", table="t", type="time")
        d2 = Dimension.from_dict(d.to_dict())
        assert d.name == d2.name
        assert d.type == d2.type

    def test_relationship_round_trip(self):
        r = Relationship(from_table="a", to_table="b", on_clause="a.id = b.a_id", join_type="INNER JOIN")
        r2 = Relationship.from_dict(r.to_dict())
        assert r.from_table == r2.from_table
        assert r.join_type == r2.join_type

    def test_business_rule_round_trip(self):
        br = BusinessRule(name="rule1", rule="desc", sql_condition="x=1", applies_to=["m1"])
        br2 = BusinessRule.from_dict(br.to_dict())
        assert br.name == br2.name
        assert br.applies_to == br2.applies_to


# =====================================================================
# SemanticLayer CRUD
# =====================================================================


class TestSemanticLayerCRUD:
    def test_add_and_get_metric(self):
        sl = SemanticLayer()
        m = Metric(name="rev", display_name="Revenue", expression="SUM(x)", table="t")
        sl.add_metric(m)
        assert sl.get_metric("rev") is m
        assert sl.get_metric("nonexistent") is None

    def test_remove_metric(self):
        sl = _sample_layer()
        sl.remove_metric("revenue")
        assert sl.get_metric("revenue") is None

    def test_update_metric(self):
        sl = _sample_layer()
        updated = sl.update_metric("revenue", display_name="총매출")
        assert updated.display_name == "총매출"
        assert sl.get_metric("revenue").display_name == "총매출"

    def test_add_and_get_dimension(self):
        sl = SemanticLayer()
        d = Dimension(name="month", display_name="월", expression="x", table="t")
        sl.add_dimension(d)
        assert sl.get_dimension("month") is d

    def test_add_relationship(self):
        sl = _sample_layer()
        rels = sl.get_relationships_for("orders")
        assert len(rels) >= 1

    def test_add_rule_and_get_for_metric(self):
        sl = _sample_layer()
        rules = sl.get_rules_for_metric("revenue")
        assert len(rules) == 2  # no_cancel + no_refund
        rules_order = sl.get_rules_for_metric("order_count")
        assert len(rules_order) == 1  # only no_cancel

    def test_is_empty(self):
        assert SemanticLayer().is_empty()
        assert not _sample_layer().is_empty()


# =====================================================================
# Search
# =====================================================================


class TestSearch:
    def test_search_by_name(self):
        sl = _sample_layer()
        results = sl.search("revenue")
        assert len(results["metrics"]) == 1
        assert results["metrics"][0].name == "revenue"

    def test_search_by_display_name(self):
        sl = _sample_layer()
        results = sl.search("매출")
        assert len(results["metrics"]) >= 1

    def test_search_dimensions(self):
        sl = _sample_layer()
        results = sl.search("지역")
        assert len(results["dimensions"]) == 1

    def test_search_rules(self):
        sl = _sample_layer()
        results = sl.search("환불")
        assert len(results["rules"]) == 1

    def test_search_case_insensitive(self):
        sl = _sample_layer()
        results = sl.search("REVENUE")
        assert len(results["metrics"]) >= 1

    def test_search_no_results(self):
        sl = _sample_layer()
        results = sl.search("nonexistent_xyz")
        assert all(len(v) == 0 for v in results.values())


# =====================================================================
# Required tables + join path
# =====================================================================


class TestTableResolution:
    def test_single_table(self):
        sl = _sample_layer()
        tables = sl.get_required_tables("revenue", ["order_month"])
        assert tables == {"orders"}

    def test_multi_table(self):
        sl = _sample_layer()
        tables = sl.get_required_tables("revenue", ["region"])
        assert "orders" in tables
        assert "customers" in tables

    def test_three_tables(self):
        sl = _sample_layer()
        tables = sl.get_required_tables("revenue", ["region", "category"])
        assert tables == {"orders", "customers", "products"}

    def test_join_path(self):
        sl = _sample_layer()
        joins = sl.find_join_path({"orders", "customers"})
        assert len(joins) >= 1
        join_tables = set()
        for j in joins:
            join_tables.add(j.from_table)
            join_tables.add(j.to_table)
        assert "orders" in join_tables
        assert "customers" in join_tables


# =====================================================================
# SQL Composer
# =====================================================================


class TestSQLComposer:
    def test_single_metric(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("revenue")
        assert "SUM(orders.amount)" in sql
        assert "status = 'completed'" in sql
        assert "GROUP BY" not in sql  # no dimensions

    def test_metric_with_dimension(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("revenue", dimension_names=["order_month"])
        assert "SUM(orders.amount)" in sql
        assert "GROUP BY" in sql
        assert "DATE_TRUNC" in sql

    def test_multi_table_join(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("revenue", dimension_names=["region"])
        sql_upper = sql.upper()
        assert "JOIN" in sql_upper
        assert "customers" in sql.lower()

    def test_business_rules_applied(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("revenue")
        assert "cancelled" in sql
        assert "refunded" in sql

    def test_extra_filters(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("revenue", filters=["order_date >= '2026-01-01'"])
        assert "2026-01-01" in sql

    def test_limit(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("revenue", dimension_names=["order_month"], limit=10)
        assert "LIMIT 10" in sql

    def test_order_by(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose(
            "revenue",
            dimension_names=["order_month"],
            order_by="revenue DESC",
        )
        assert "ORDER BY" in sql

    def test_compose_raw(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose_raw(
            select_exprs=["COUNT(*) AS total"],
            from_table="orders",
            where=["status = 'completed'"],
            limit=5,
        )
        assert "COUNT(*)" in sql
        assert "LIMIT 5" in sql

    def test_order_count_metric(self):
        sl = _sample_layer()
        sql = SQLComposer(sl).compose("order_count")
        assert "COUNT(orders.id)" in sql
        assert "cancelled" in sql  # no_cancel rule applies
        assert "refunded" not in sql  # no_refund does NOT apply to order_count


# =====================================================================
# Serialization + to_context_string
# =====================================================================


class TestSerialization:
    def test_to_dict_from_dict(self):
        sl = _sample_layer()
        d = sl.to_dict()
        sl2 = SemanticLayer.from_dict(d)
        assert len(sl2.metrics) == len(sl.metrics)
        assert len(sl2.dimensions) == len(sl.dimensions)
        assert len(sl2.relationships) == len(sl.relationships)
        assert len(sl2.business_rules) == len(sl.business_rules)

    def test_to_context_string(self):
        sl = _sample_layer()
        ctx = sl.to_context_string()
        assert "매출" in ctx
        assert "주문월" in ctx
        assert "취소" in ctx

    def test_empty_context_string(self):
        sl = SemanticLayer()
        ctx = sl.to_context_string()
        assert isinstance(ctx, str)


# =====================================================================
# Persistence (file I/O)
# =====================================================================


class TestPersistence:
    def test_save_and_load(self):
        sl = _sample_layer()
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        save_layer(sl, path)
        sl2 = load_layer(path)

        assert len(sl2.metrics) == 2
        assert sl2.get_metric("revenue").display_name == "매출"
        assert len(sl2.dimensions) == 3
        assert len(sl2.relationships) == 2
        assert len(sl2.business_rules) == 2

    def test_json_is_valid(self):
        sl = _sample_layer()
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            path = f.name
        save_layer(sl, path)
        with open(path) as f:
            data = json.load(f)
        assert "metrics" in data
        assert "dimensions" in data
