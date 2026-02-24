"""Tests for SQLGenerator."""
from __future__ import annotations

import pytest

from lang2sql.components.generation.sql_generator import SQLGenerator
from lang2sql.core.catalog import CatalogEntry
from lang2sql.core.exceptions import ComponentError
from lang2sql.core.hooks import MemoryHook


# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------

class FakeLLM:
    def __init__(self, response: str = "```sql\nSELECT 1\n```"):
        self._response = response

    def invoke(self, messages: list[dict]) -> str:
        return self._response


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _catalog() -> list[CatalogEntry]:
    return [
        {
            "name": "orders",
            "description": "Monthly order records",
            "columns": {"order_id": "primary key", "amount": "order amount"},
        }
    ]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_sql_generator_returns_sql_string():
    gen = SQLGenerator(llm=FakeLLM("```sql\nSELECT COUNT(*) FROM orders\n```"))
    result = gen.run("주문 건수", _catalog())
    assert result == "SELECT COUNT(*) FROM orders"


def test_sql_generator_strips_trailing_semicolon():
    gen = SQLGenerator(llm=FakeLLM("```sql\nSELECT 1;\n```"))
    result = gen.run("test", _catalog())
    assert not result.endswith(";")


def test_sql_generator_raises_on_no_code_block():
    gen = SQLGenerator(llm=FakeLLM("Here is your answer: SELECT 1"))
    with pytest.raises(ComponentError):
        gen.run("test", _catalog())


def test_sql_generator_passes_query_and_schema_to_llm():
    received: list[list[dict]] = []

    class CaptureLLM:
        def invoke(self, messages):
            received.append(messages)
            return "```sql\nSELECT 1\n```"

    gen = SQLGenerator(llm=CaptureLLM())
    gen.run("주문 건수", _catalog())

    assert received, "invoke was not called"
    msgs = received[0]
    user_content = next(m["content"] for m in msgs if m["role"] == "user")
    assert "orders" in user_content
    assert "주문 건수" in user_content


def test_sql_generator_custom_system_prompt():
    received: list[list[dict]] = []

    class CaptureLLM:
        def invoke(self, messages):
            received.append(messages)
            return "```sql\nSELECT 1\n```"

    custom_prompt = "You are a DBA."
    gen = SQLGenerator(llm=CaptureLLM(), system_prompt=custom_prompt)
    gen.run("test", _catalog())

    system_content = next(m["content"] for m in received[0] if m["role"] == "system")
    assert system_content == custom_prompt


def test_sql_generator_emits_hook_events():
    hook = MemoryHook()
    gen = SQLGenerator(llm=FakeLLM(), hook=hook)
    gen.run("test", _catalog())

    events = hook.snapshot()
    phases = [e.phase for e in events]
    assert "start" in phases
    assert "end" in phases


def test_sql_generator_empty_schemas():
    gen = SQLGenerator(llm=FakeLLM("```sql\nSELECT 1\n```"))
    result = gen.run("test", [])
    assert result == "SELECT 1"
