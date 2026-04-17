"""lang2sql agentic harness — interactive data agent loop."""

from .types import (
    AgentEvent,
    AssistantEvent,
    DataEvent,
    Message,
    PlanApprovalEvent,
    ToolCall,
    ToolCallEvent,
    ToolResult,
    ToolResultEvent,
    UserPromptEvent,
    VizEvent,
)
from .tool import Tool, ToolRegistry, ToolSpec
from .session import Session
from .loop import agent_loop


def build_harness(**kwargs):  # type: ignore[no-untyped-def]
    """Lazy import to avoid circular dependency with tools package."""
    from .builder import build_harness as _build

    return _build(**kwargs)


__all__ = [
    "AgentEvent",
    "AssistantEvent",
    "DataEvent",
    "Message",
    "PlanApprovalEvent",
    "Session",
    "Tool",
    "ToolCall",
    "ToolCallEvent",
    "ToolRegistry",
    "ToolResult",
    "ToolResultEvent",
    "ToolSpec",
    "UserPromptEvent",
    "VizEvent",
    "agent_loop",
    "build_harness",
]
