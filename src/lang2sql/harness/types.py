"""Core data types for the harness agent loop."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


# ---------------------------------------------------------------------------
# Messages (conversation history)
# ---------------------------------------------------------------------------

@dataclass
class ToolCall:
    """A single tool invocation requested by the LLM."""

    id: str
    name: str
    arguments: dict[str, Any]


@dataclass
class Message:
    """One message in the conversation history.

    Roles:
        system       – system prompt
        user         – human input
        assistant    – LLM reply (may include tool_calls)
        tool_result  – result of a tool execution
    """

    role: Literal["system", "user", "assistant", "tool_result"]
    content: str | None = None
    tool_calls: list[ToolCall] | None = None
    tool_call_id: str | None = None
    name: str | None = None


# ---------------------------------------------------------------------------
# Tool results
# ---------------------------------------------------------------------------

@dataclass
class ToolResult:
    """Result returned by a tool execution."""

    tool_call_id: str
    content: str
    data: Any = None
    is_error: bool = False

    def to_llm_text(self, max_chars: int = 4000) -> str:
        """Truncated text representation for the LLM context."""
        text = self.content
        if len(text) > max_chars:
            text = text[: max_chars - 20] + "\n... (truncated)"
        return text


# ---------------------------------------------------------------------------
# Agent events (yielded by the loop, consumed by TUI / CLI)
# ---------------------------------------------------------------------------

@dataclass
class AgentEvent:
    """Base class for all events emitted by the agent loop."""

    pass


@dataclass
class ToolCallEvent(AgentEvent):
    """The agent is invoking a tool."""

    tool_call: ToolCall


@dataclass
class ToolResultEvent(AgentEvent):
    """A tool has returned a result."""

    result: ToolResult


@dataclass
class AssistantEvent(AgentEvent):
    """The agent produced a text response."""

    content: str


@dataclass
class UserPromptEvent(AgentEvent):
    """The agent needs input from the user (via ask_user tool).

    The loop yields this event and expects the caller to send() back
    the user's answer as a string.
    """

    question: str
    options: list[str] | None = None


@dataclass
class PlanApprovalEvent(AgentEvent):
    """The agent presents an analysis plan for user approval.

    The loop yields this event and expects the caller to send() back
    a boolean (True = approved, False = rejected/modified).
    """

    plan: str
    steps: list[str] = field(default_factory=list)


@dataclass
class DataEvent(AgentEvent):
    """SQL execution produced tabular data."""

    rows: list[dict[str, Any]]
    sql: str
    row_count: int = 0
    truncated: bool = False


@dataclass
class VizEvent(AgentEvent):
    """A visualization spec to be rendered by the TUI."""

    chart_type: str
    data: list[dict[str, Any]]
    title: str = ""
    columns: dict[str, str] = field(default_factory=dict)


@dataclass
class ErrorEvent(AgentEvent):
    """An error occurred during agent execution."""

    error: str
    recoverable: bool = True
