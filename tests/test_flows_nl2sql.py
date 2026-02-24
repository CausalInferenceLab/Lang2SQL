"""End-to-end tests for BaselineNL2SQL."""

from __future__ import annotations

import pytest

from lang2sql.core.exceptions import ComponentError
from lang2sql.core.hooks import MemoryHook
from lang2sql.flows.nl2sql import BaselineNL2SQL

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeLLM:
    def __init__(self, response: str = "```sql\nSELECT COUNT(*) FROM orders\n```"):
        self._response = response

    def invoke(self, messages: list[dict]) -> str:
        return self._response


class FakeDB:
    def __init__(self, rows: list[dict] | None = None):
        self._rows = rows if rows is not None else [{"count": 42}]

    def execute(self, sql: str) -> list[dict]:
        return self._rows


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

CATALOG = [
    {
        "name": "orders",
        "description": "Monthly order records",
        "columns": {"order_id": "primary key", "amount": "order amount"},
    },
    {
        "name": "customers",
        "description": "Customer master data",
        "columns": {"customer_id": "primary key", "name": "customer name"},
    },
]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_pipeline_e2e_returns_rows():
    pipeline = BaselineNL2SQL(
        catalog=CATALOG,
        llm=FakeLLM(),
        db=FakeDB([{"count": 42}]),
    )
    result = pipeline.run("지난달 주문 건수")
    assert result == [{"count": 42}]


def test_pipeline_emits_3_component_start_events():
    hook = MemoryHook()
    pipeline = BaselineNL2SQL(
        catalog=CATALOG,
        llm=FakeLLM(),
        db=FakeDB(),
        hook=hook,
    )
    pipeline.run("주문 건수")

    component_starts = [
        e for e in hook.snapshot() if e.name == "component.run" and e.phase == "start"
    ]
    assert len(component_starts) == 3


def test_pipeline_propagates_component_error_on_bad_llm_response():
    pipeline = BaselineNL2SQL(
        catalog=CATALOG,
        llm=FakeLLM("No SQL here, just text."),
        db=FakeDB(),
    )
    with pytest.raises(ComponentError):
        pipeline.run("주문 건수")


def test_pipeline_hook_records_all_phases():
    hook = MemoryHook()
    pipeline = BaselineNL2SQL(
        catalog=CATALOG,
        llm=FakeLLM(),
        db=FakeDB(),
        hook=hook,
    )
    pipeline.run("test")

    events = hook.snapshot()
    component_events = [e for e in events if e.name == "component.run"]
    flow_events = [e for e in events if e.name == "flow.run"]

    assert len(component_events) == 6  # 3 components × (start + end)
    assert len(flow_events) == 2  # flow start + flow end


def test_pipeline_advanced_usage_manual_composition():
    """고급 사용자 — 직접 컴포넌트 조합."""
    from lang2sql.components.execution.sql_executor import SQLExecutor
    from lang2sql.components.generation.sql_generator import SQLGenerator
    from lang2sql.components.retrieval.keyword import KeywordRetriever

    retriever = KeywordRetriever(catalog=CATALOG)
    generator = SQLGenerator(llm=FakeLLM())
    executor = SQLExecutor(db=FakeDB([{"total": 99}]))

    query = "주문 건수"
    schemas = retriever(query)
    sql = generator(query, schemas)
    result = executor(sql)

    assert result == [{"total": 99}]
