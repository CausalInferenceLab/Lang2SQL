"""Tests for visualization chart type selection."""

from lang2sql.viz.selector import select_chart_type


class TestSelectChartType:
    def test_explicit_hint(self):
        assert select_chart_type([], hint="bar") == "bar"

    def test_empty_data(self):
        assert select_chart_type([]) == "table"

    def test_single_value_stat_card(self):
        rows = [{"total": 42}]
        assert select_chart_type(rows) == "stat_card"

    def test_time_numeric_line(self):
        rows = [
            {"month": "2026-01", "revenue": 100},
            {"month": "2026-02", "revenue": 200},
            {"month": "2026-03", "revenue": 300},
        ]
        assert select_chart_type(rows, x_column="month", y_column="revenue") == "line"

    def test_categorical_numeric_bar(self):
        rows = [
            {"city": "Seoul", "count": 50},
            {"city": "Busan", "count": 30},
            {"city": "Daegu", "count": 20},
        ]
        assert select_chart_type(rows, x_column="city", y_column="count") == "bar"

    def test_many_categories_horizontal_bar(self):
        rows = [{"cat": f"item_{i}", "val": i} for i in range(15)]
        assert select_chart_type(rows, x_column="cat", y_column="val") == "horizontal_bar"

    def test_two_numeric_scatter(self):
        rows = [{"x": 1.0, "y": 2.0}, {"x": 3.0, "y": 4.0}]
        assert select_chart_type(rows, x_column="x", y_column="y") == "scatter"

    def test_default_table(self):
        rows = [{"name": "Alice", "role": "admin"}, {"name": "Bob", "role": "user"}]
        assert select_chart_type(rows) == "table"
