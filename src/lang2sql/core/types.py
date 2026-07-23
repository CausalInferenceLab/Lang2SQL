"""Pure domain types shared across the whole system.

These carry no behaviour and no I/O — they are the vocabulary every layer
(harness, tools, frontends, adapters) speaks. Keeping them dependency-free is
what lets the ``core`` package sit at the bottom of the import graph.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Role(str, Enum):
    """Author of a conversation message."""

    SYSTEM = "system"
    USER = "user"
    ASSISTANT = "assistant"
    TOOL = "tool"


@dataclass
class ToolCall:
    """A model's request to invoke one tool.

    ``arguments`` is the already-parsed JSON object the model emitted; adapters
    are responsible for turning provider-specific payloads into this shape.
    """

    id: str
    name: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass
class ToolResult:
    """Outcome of running a :class:`ToolCall`, fed back to the model."""

    call_id: str
    content: str
    is_error: bool = False


@dataclass
class Message:
    """One entry in a conversation transcript.

    A single assistant turn may both say something (``content``) and request
    tools (``tool_calls``). Tool-role messages carry ``tool_call_id`` to bind
    their result back to the originating call.
    """

    role: Role
    content: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    tool_call_id: str | None = None
    name: str | None = None
    # Frontends may persist one sanitized clarification for exactly the next
    # user turn.  It is provider-visible but removed by the next compression.
    transient: bool = False


@dataclass
class ToolSpec:
    """Catalog entry the harness advertises to the LLM.

    ``parameters`` is a JSON-Schema object describing the tool's arguments,
    matching what tool-calling LLM APIs expect.
    """

    name: str
    description: str
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass
class Completion:
    """What an :class:`~lang2sql.core.ports.llm.LLMPort` returns for one call.

    Either ``content`` (final answer) or ``tool_calls`` (the model wants to act)
    or both. The agent loop branches on whether ``tool_calls`` is non-empty.
    """

    content: str = ""
    tool_calls: list[ToolCall] = field(default_factory=list)
    finish_reason: str | None = None
