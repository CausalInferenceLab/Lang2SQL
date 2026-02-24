"""Tests for SQLExecutor."""

from __future__ import annotations

import pytest

from lang2sql.components.execution.sql_executor import SQLExecutor
from lang2sql.core.exceptions import ComponentError
from lang2sql.core.hooks import MemoryHook

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class FakeDB:
    def __init__(self, rows: list[dict] | None = None):
        self._rows = rows if rows is not None else [{"count": 1}]

    def execute(self, sql: str) -> list[dict]:
        return self._rows


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_sql_executor_returns_rows():
    rows = [{"order_id": 1, "amount": 100}]
    executor = SQLExecutor(db=FakeDB(rows))
    result = executor.run("SELECT * FROM orders")
    assert result == rows


def test_sql_executor_raises_on_empty_sql():
    executor = SQLExecutor(db=FakeDB())
    with pytest.raises(ComponentError):
        executor.run("")


def test_sql_executor_raises_on_whitespace_sql():
    executor = SQLExecutor(db=FakeDB())
    with pytest.raises(ComponentError):
        executor.run("   ")


def test_sql_executor_returns_empty_list():
    executor = SQLExecutor(db=FakeDB([]))
    result = executor.run("SELECT * FROM empty_table")
    assert result == []


def test_sql_executor_emits_hook_events():
    hook = MemoryHook()
    executor = SQLExecutor(db=FakeDB(), hook=hook)
    executor.run("SELECT 1")

    events = hook.snapshot()
    phases = [e.phase for e in events]
    assert "start" in phases
    assert "end" in phases


def test_sql_executor_error_event_on_db_failure():
    class BrokenDB:
        def execute(self, sql: str):
            raise RuntimeError("connection refused")

    hook = MemoryHook()
    executor = SQLExecutor(db=BrokenDB(), hook=hook)

    with pytest.raises(ComponentError):
        executor.run("SELECT 1")

    error_events = [e for e in hook.snapshot() if e.phase == "error"]
    assert error_events, "expected an error event"
