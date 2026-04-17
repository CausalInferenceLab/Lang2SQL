"""Tests for Phase 1 harness core: types, tool registry, session, loop."""

from __future__ import annotations

import asyncio
import json
import tempfile
from dataclasses import dataclass
from typing import Any

import pytest

from lang2sql.harness.types import (
    AssistantEvent,
    DataEvent,
    ErrorEvent,
    Message,
    PlanApprovalEvent,
    ToolCall,
    ToolCallEvent,
    ToolResult,
    ToolResultEvent,
    UserPromptEvent,
)
from lang2sql.harness.tool import Tool, ToolRegistry, ToolSpec
from lang2sql.harness.session import Session
from lang2sql.harness.system_prompt import build_system_prompt
from lang2sql.harness.loop import agent_loop
from lang2sql.core.hooks import MemoryHook


# =====================================================================
# Helpers / Fakes
# =====================================================================


class FakeToolCallLLM:
    """Fake LLM that returns pre-scripted responses in order."""

    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = list(responses)
        self._call_count = 0

    def invoke_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> dict[str, Any]:
        if self._call_count >= len(self._responses):
            return {"content": "(no more scripted responses)", "tool_calls": None, "stop_reason": "end_turn"}
        resp = self._responses[self._call_count]
        self._call_count += 1
        return resp


class EchoTool:
    """Simple tool that echoes input."""

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="echo",
            description="Echoes the input text back.",
            input_schema={
                "type": "object",
                "properties": {"text": {"type": "string"}},
                "required": ["text"],
            },
        )

    def execute(self, text: str = "") -> ToolResult:
        return ToolResult(tool_call_id="", content=f"Echo: {text}")


class FailTool:
    """Tool that always raises an exception."""

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="fail_tool",
            description="Always fails.",
            input_schema={"type": "object", "properties": {}},
        )

    def execute(self) -> ToolResult:
        raise RuntimeError("Intentional failure")


class FakeRunSQLTool:
    """Fake run_sql tool that returns data."""

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="run_sql",
            description="Execute SQL.",
            input_schema={
                "type": "object",
                "properties": {"sql": {"type": "string"}},
                "required": ["sql"],
            },
        )

    def execute(self, sql: str = "") -> ToolResult:
        rows = [{"id": 1, "amount": 100}, {"id": 2, "amount": 200}]
        return ToolResult(
            tool_call_id="",
            content=json.dumps(rows),
            data=rows,
        )


class FakeAskUserTool:
    """Placeholder — ask_user is handled by the loop, not executed."""

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="ask_user",
            description="Ask the user a question.",
            input_schema={
                "type": "object",
                "properties": {
                    "question": {"type": "string"},
                    "options": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["question"],
            },
        )

    def execute(self, **kwargs: Any) -> ToolResult:
        return ToolResult(tool_call_id="", content="should not be called")


class FakeShowPlanTool:
    """Placeholder — show_plan is handled by the loop, not executed."""

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="show_plan",
            description="Show analysis plan.",
            input_schema={
                "type": "object",
                "properties": {
                    "plan": {"type": "string"},
                    "steps": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["plan"],
            },
        )

    def execute(self, **kwargs: Any) -> ToolResult:
        return ToolResult(tool_call_id="", content="should not be called")


def _make_registry(*tools_list) -> ToolRegistry:
    reg = ToolRegistry()
    for t in tools_list:
        reg.register(t)
    return reg


async def _collect_events(gen) -> list:
    """Collect all events from an async generator (no send)."""
    events = []
    try:
        event = await gen.__anext__()
        while True:
            events.append(event)
            event = await gen.asend(None)
    except StopAsyncIteration:
        pass
    return events


async def _collect_events_with_answers(gen, answers: dict[type, Any]) -> list:
    """Collect events, auto-responding to interactive events."""
    events = []
    try:
        event = await gen.__anext__()
        while True:
            events.append(event)
            reply = None
            for evt_type, answer in answers.items():
                if isinstance(event, evt_type):
                    reply = answer
                    break
            event = await gen.asend(reply)
    except StopAsyncIteration:
        pass
    return events


# =====================================================================
# ToolResult tests
# =====================================================================


class TestToolResult:
    def test_to_llm_text_short(self):
        r = ToolResult(tool_call_id="1", content="hello")
        assert r.to_llm_text() == "hello"

    def test_to_llm_text_truncated(self):
        r = ToolResult(tool_call_id="1", content="x" * 5000)
        text = r.to_llm_text(max_chars=100)
        assert len(text) <= 100
        assert "truncated" in text


# =====================================================================
# ToolRegistry tests
# =====================================================================


class TestToolRegistry:
    def test_register_and_get(self):
        reg = _make_registry(EchoTool())
        assert len(reg) == 1
        assert "echo" in reg
        tool = reg.get("echo")
        assert tool.spec.name == "echo"

    def test_duplicate_registration_raises(self):
        reg = _make_registry(EchoTool())
        with pytest.raises(ValueError, match="already registered"):
            reg.register(EchoTool())

    def test_get_unknown_raises(self):
        reg = ToolRegistry()
        with pytest.raises(KeyError, match="Unknown tool"):
            reg.get("nonexistent")

    def test_tool_specs_for_llm(self):
        reg = _make_registry(EchoTool(), FailTool())
        specs = reg.tool_specs_for_llm()
        assert len(specs) == 2
        names = {s["name"] for s in specs}
        assert names == {"echo", "fail_tool"}
        # Each spec should have the required keys
        for s in specs:
            assert "name" in s
            assert "description" in s
            assert "input_schema" in s

    @pytest.mark.asyncio
    async def test_execute_success(self):
        reg = _make_registry(EchoTool())
        result = await reg.execute("echo", {"text": "hi"}, tool_call_id="tc1")
        assert not result.is_error
        assert "Echo: hi" in result.content
        assert result.tool_call_id == "tc1"

    @pytest.mark.asyncio
    async def test_execute_error_returns_tool_result(self):
        reg = _make_registry(FailTool())
        result = await reg.execute("fail_tool", {}, tool_call_id="tc2")
        assert result.is_error
        assert "Intentional failure" in result.content


# =====================================================================
# Session tests
# =====================================================================


class TestSession:
    def test_push_messages(self):
        s = Session()
        s.push_user("hello")
        s.push_assistant("world")
        assert len(s.conversation) == 2
        assert s.conversation[0].role == "user"
        assert s.conversation[1].role == "assistant"

    def test_push_assistant_with_tool_calls(self):
        s = Session()
        tc = [ToolCall(id="tc1", name="echo", arguments={"text": "hi"})]
        s.push_assistant(None, tool_calls=tc)
        assert s.conversation[0].tool_calls is not None
        assert s.conversation[0].tool_calls[0].name == "echo"

    def test_push_tool_result_from_string(self):
        s = Session()
        s.push_tool_result("tc1", "result text")
        msg = s.conversation[0]
        assert msg.role == "tool_result"
        assert msg.content == "result text"
        assert msg.tool_call_id == "tc1"

    def test_push_tool_result_from_object(self):
        s = Session()
        s.push_tool_result("tc1", ToolResult(tool_call_id="tc1", content="data"))
        msg = s.conversation[0]
        assert msg.role == "tool_result"
        assert msg.tool_call_id == "tc1"

    def test_build_messages(self):
        s = Session()
        s.push_user("query")
        s.push_assistant("answer")
        msgs = s.build_messages("system prompt")
        assert msgs[0] == {"role": "system", "content": "system prompt"}
        assert msgs[1] == {"role": "user", "content": "query"}
        assert msgs[2] == {"role": "assistant", "content": "answer"}

    def test_build_messages_with_tool_calls(self):
        s = Session()
        tc = [ToolCall(id="tc1", name="echo", arguments={"text": "hi"})]
        s.push_assistant(None, tool_calls=tc)
        s.push_tool_result("tc1", "echo result")
        msgs = s.build_messages("sys")
        assert "tool_calls" in msgs[1]
        assert msgs[2]["tool_call_id"] == "tc1"

    def test_has_new_data(self):
        s = Session()
        assert not s.has_new_data()
        s.set_last_result("SELECT 1", [{"x": 1}])
        assert s.has_new_data()
        assert not s.has_new_data()  # consumed

    def test_save_and_load(self):
        s = Session(db_url="sqlite:///test.db", db_dialect="sqlite", mode="setup")
        s.push_user("hello")
        tc = [ToolCall(id="tc1", name="echo", arguments={"text": "hi"})]
        s.push_assistant("calling tool", tool_calls=tc)
        s.push_tool_result("tc1", "echo result")
        s.set_last_result("SELECT 1", [{"x": 1}])

        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as f:
            s.save(f.name)
            loaded = Session.load(f.name)

        assert loaded.db_url == "sqlite:///test.db"
        assert loaded.mode == "setup"
        assert len(loaded.conversation) == 3
        assert loaded.conversation[1].tool_calls[0].name == "echo"
        assert loaded.last_sql == "SELECT 1"


# =====================================================================
# System prompt tests
# =====================================================================


class TestSystemPrompt:
    def test_basic_build(self):
        prompt = build_system_prompt(mode="query", dialect="postgresql")
        assert "query" in prompt.lower()
        assert "postgresql" in prompt

    def test_semantic_layer_context(self):
        sem = {
            "metrics": {
                "revenue": {
                    "display_name": "매출",
                    "expression": "SUM(amount)",
                    "filters": ["status = 'completed'"],
                    "description": "완료 주문 합계",
                }
            },
            "dimensions": {
                "order_month": {
                    "display_name": "주문월",
                    "expression": "DATE_TRUNC('month', order_date)",
                    "type": "time",
                }
            },
        }
        prompt = build_system_prompt(semantic_layer=sem)
        assert "매출" in prompt
        assert "SUM(amount)" in prompt
        assert "주문월" in prompt

    def test_schema_context(self):
        cache = {"orders": "CREATE TABLE orders (id INT, amount DECIMAL)"}
        prompt = build_system_prompt(schema_cache=cache)
        assert "CREATE TABLE orders" in prompt

    def test_tool_descriptions(self):
        prompt = build_system_prompt(tool_descriptions="- **echo**: Echo input")
        assert "echo" in prompt


# =====================================================================
# Agent loop tests
# =====================================================================


class TestAgentLoop:
    @pytest.mark.asyncio
    async def test_simple_text_response(self):
        """LLM returns text only — loop yields AssistantEvent and stops."""
        llm = FakeToolCallLLM([
            {"content": "The answer is 42.", "tool_calls": None, "stop_reason": "end_turn"},
        ])
        session = Session()
        reg = _make_registry(EchoTool())

        events = await _collect_events(
            agent_loop("What is the answer?", llm=llm, tools=reg, session=session)
        )

        assert len(events) == 1
        assert isinstance(events[0], AssistantEvent)
        assert "42" in events[0].content

    @pytest.mark.asyncio
    async def test_tool_call_then_text(self):
        """LLM calls a tool, sees result, then responds with text."""
        llm = FakeToolCallLLM([
            # Turn 1: call echo tool
            {
                "content": None,
                "tool_calls": [{"id": "tc1", "name": "echo", "arguments": {"text": "hello"}}],
                "stop_reason": "tool_use",
            },
            # Turn 2: respond with text
            {
                "content": "The echo said: hello",
                "tool_calls": None,
                "stop_reason": "end_turn",
            },
        ])
        session = Session()
        reg = _make_registry(EchoTool())

        events = await _collect_events(
            agent_loop("Echo hello", llm=llm, tools=reg, session=session)
        )

        # Should have: ToolCallEvent, ToolResultEvent, AssistantEvent
        types = [type(e).__name__ for e in events]
        assert "ToolCallEvent" in types
        assert "ToolResultEvent" in types
        assert "AssistantEvent" in types

    @pytest.mark.asyncio
    async def test_run_sql_emits_data_event(self):
        """run_sql tool results trigger a DataEvent."""
        llm = FakeToolCallLLM([
            {
                "content": None,
                "tool_calls": [{"id": "tc1", "name": "run_sql", "arguments": {"sql": "SELECT 1"}}],
                "stop_reason": "tool_use",
            },
            {
                "content": "Here are your results.",
                "tool_calls": None,
                "stop_reason": "end_turn",
            },
        ])
        session = Session()
        reg = _make_registry(FakeRunSQLTool())

        events = await _collect_events(
            agent_loop("Run a query", llm=llm, tools=reg, session=session)
        )

        data_events = [e for e in events if isinstance(e, DataEvent)]
        assert len(data_events) == 1
        assert data_events[0].row_count == 2
        assert data_events[0].sql == "SELECT 1"

    @pytest.mark.asyncio
    async def test_ask_user_pauses_and_resumes(self):
        """ask_user tool yields UserPromptEvent; send() resumes the loop."""
        llm = FakeToolCallLLM([
            {
                "content": None,
                "tool_calls": [{"id": "tc1", "name": "ask_user", "arguments": {"question": "Which table?"}}],
                "stop_reason": "tool_use",
            },
            {
                "content": "Got it, using the orders table.",
                "tool_calls": None,
                "stop_reason": "end_turn",
            },
        ])
        session = Session()
        reg = _make_registry(FakeAskUserTool())

        events = await _collect_events_with_answers(
            agent_loop("Help me", llm=llm, tools=reg, session=session),
            answers={UserPromptEvent: "orders"},
        )

        prompt_events = [e for e in events if isinstance(e, UserPromptEvent)]
        assert len(prompt_events) == 1
        assert prompt_events[0].question == "Which table?"

        # The user answer should be in the conversation
        tool_results = [m for m in session.conversation if m.role == "tool_result"]
        assert any("orders" in (m.content or "") for m in tool_results)

    @pytest.mark.asyncio
    async def test_show_plan_approval(self):
        """show_plan tool yields PlanApprovalEvent; approved continues."""
        llm = FakeToolCallLLM([
            {
                "content": None,
                "tool_calls": [{"id": "tc1", "name": "show_plan", "arguments": {"plan": "Step 1: Query data"}}],
                "stop_reason": "tool_use",
            },
            {
                "content": "Plan approved, executing...",
                "tool_calls": None,
                "stop_reason": "end_turn",
            },
        ])
        session = Session()
        reg = _make_registry(FakeShowPlanTool())

        events = await _collect_events_with_answers(
            agent_loop("Analyze churn", llm=llm, tools=reg, session=session),
            answers={PlanApprovalEvent: True},
        )

        plan_events = [e for e in events if isinstance(e, PlanApprovalEvent)]
        assert len(plan_events) == 1

        # Approval result should be in conversation
        tool_results = [m for m in session.conversation if m.role == "tool_result"]
        assert any("approved" in (m.content or "") for m in tool_results)

    @pytest.mark.asyncio
    async def test_tool_error_is_recoverable(self):
        """Tool error returns ToolResult with is_error=True; loop continues."""
        llm = FakeToolCallLLM([
            {
                "content": None,
                "tool_calls": [{"id": "tc1", "name": "fail_tool", "arguments": {}}],
                "stop_reason": "tool_use",
            },
            {
                "content": "The tool failed, let me try another approach.",
                "tool_calls": None,
                "stop_reason": "end_turn",
            },
        ])
        session = Session()
        reg = _make_registry(FailTool())

        events = await _collect_events(
            agent_loop("Do something", llm=llm, tools=reg, session=session)
        )

        result_events = [e for e in events if isinstance(e, ToolResultEvent)]
        assert len(result_events) == 1
        assert result_events[0].result.is_error

        # Loop should still produce a final assistant message
        assistant_events = [e for e in events if isinstance(e, AssistantEvent)]
        assert len(assistant_events) == 1

    @pytest.mark.asyncio
    async def test_max_turns_guard(self):
        """Loop stops after max_turns and yields ErrorEvent."""
        # LLM always calls a tool → infinite loop
        responses = [
            {
                "content": None,
                "tool_calls": [{"id": f"tc{i}", "name": "echo", "arguments": {"text": "loop"}}],
                "stop_reason": "tool_use",
            }
            for i in range(10)
        ]
        llm = FakeToolCallLLM(responses)
        session = Session()
        reg = _make_registry(EchoTool())

        events = await _collect_events(
            agent_loop("Loop forever", llm=llm, tools=reg, session=session, max_turns=3)
        )

        error_events = [e for e in events if isinstance(e, ErrorEvent)]
        assert len(error_events) == 1
        assert "exceeded" in error_events[0].error.lower()

    @pytest.mark.asyncio
    async def test_hook_events_emitted(self):
        """Hook receives events during the loop."""
        llm = FakeToolCallLLM([
            {
                "content": None,
                "tool_calls": [{"id": "tc1", "name": "echo", "arguments": {"text": "hi"}}],
                "stop_reason": "tool_use",
            },
            {
                "content": "Done.",
                "tool_calls": None,
                "stop_reason": "end_turn",
            },
        ])
        hook = MemoryHook()
        session = Session()
        reg = _make_registry(EchoTool())

        await _collect_events(
            agent_loop("Hi", llm=llm, tools=reg, session=session, hook=hook)
        )

        events = hook.snapshot()
        assert len(events) > 0
        names = {e.name for e in events}
        assert "harness.turn" in names
        assert "harness.tool" in names

    @pytest.mark.asyncio
    async def test_conversation_state_persisted(self):
        """Session conversation is updated after the loop."""
        llm = FakeToolCallLLM([
            {"content": "Hello!", "tool_calls": None, "stop_reason": "end_turn"},
        ])
        session = Session()
        reg = _make_registry(EchoTool())

        await _collect_events(
            agent_loop("Hi", llm=llm, tools=reg, session=session)
        )

        # Should have: user message + assistant message
        assert len(session.conversation) == 2
        assert session.conversation[0].role == "user"
        assert session.conversation[0].content == "Hi"
        assert session.conversation[1].role == "assistant"
        assert session.conversation[1].content == "Hello!"
