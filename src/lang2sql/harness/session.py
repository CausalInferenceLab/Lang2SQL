"""Session — holds all state across agent conversation turns."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal

from .types import Message, ToolCall, ToolResult


@dataclass
class Session:
    """Holds all state across agent conversation turns."""

    # Conversation
    conversation: list[Message] = field(default_factory=list)

    # Database
    db_url: str | None = None
    db_dialect: str | None = None

    # Schema cache (table_name -> DDL string)
    schema_cache: dict[str, str] = field(default_factory=dict)

    # Semantic layer (will hold Metric/Dimension/etc. — placeholder)
    semantic_layer: dict[str, Any] = field(default_factory=dict)

    # Last execution state
    last_sql: str | None = None
    last_result: list[dict[str, Any]] | None = None
    last_intent: str | None = None

    # Mode
    mode: Literal["setup", "query"] = "query"

    # Internal flag for has_new_data tracking
    _data_version: int = field(default=0, repr=False)
    _data_seen: int = field(default=0, repr=False)

    # ------------------------------------------------------------------
    # Conversation helpers
    # ------------------------------------------------------------------

    def push_user(self, content: str) -> None:
        """Add a user message to conversation."""
        self.conversation.append(Message(role="user", content=content))

    def push_assistant(
        self,
        content: str | None,
        tool_calls: list[ToolCall] | None = None,
    ) -> None:
        """Add an assistant message to conversation."""
        self.conversation.append(
            Message(role="assistant", content=content, tool_calls=tool_calls),
        )

    def push_tool_result(
        self,
        tool_call_id: str,
        result: ToolResult | str,
    ) -> None:
        """Add a tool result message to conversation.

        If *result* is a plain string, wrap it in a ``ToolResult``.
        """
        if isinstance(result, str):
            result = ToolResult(tool_call_id=tool_call_id, content=result)
        self.conversation.append(
            Message(
                role="tool_result",
                content=result.to_llm_text(),
                tool_call_id=result.tool_call_id,
            ),
        )

    # ------------------------------------------------------------------
    # LLM message formatting
    # ------------------------------------------------------------------

    def build_messages(self, system_prompt: str) -> list[dict[str, Any]]:
        """Convert conversation to the dict format expected by LLM APIs.

        Returns ``[{"role": "system", "content": ...}, ...]``.
        """
        msgs: list[dict[str, Any]] = [
            {"role": "system", "content": system_prompt},
        ]
        for msg in self.conversation:
            entry: dict[str, Any] = {"role": msg.role, "content": msg.content}

            if msg.role == "assistant" and msg.tool_calls:
                entry["tool_calls"] = [
                    {
                        "id": tc.id,
                        "name": tc.name,
                        "arguments": tc.arguments,
                    }
                    for tc in msg.tool_calls
                ]

            if msg.role == "tool_result":
                entry["tool_call_id"] = msg.tool_call_id

            msgs.append(entry)
        return msgs

    # ------------------------------------------------------------------
    # Data tracking
    # ------------------------------------------------------------------

    def set_last_result(
        self,
        sql: str,
        result: list[dict[str, Any]],
    ) -> None:
        """Store the latest SQL execution result and bump the data version."""
        self.last_sql = sql
        self.last_result = result
        self._data_version += 1

    def has_new_data(self) -> bool:
        """Check if there's new SQL execution data since last check.

        Returns ``True`` once per new result, then resets until the next
        ``set_last_result`` call.
        """
        if self._data_version > self._data_seen:
            self._data_seen = self._data_version
            return True
        return False

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save(self, path: str) -> None:
        """Serialize session to a JSON file."""
        data = _session_to_dict(self)
        Path(path).write_text(json.dumps(data, ensure_ascii=False, indent=2))

    @classmethod
    def load(cls, path: str) -> Session:
        """Deserialize session from a JSON file."""
        data = json.loads(Path(path).read_text())
        return _session_from_dict(data)


# ======================================================================
# Private serialisation helpers
# ======================================================================

def _session_to_dict(session: Session) -> dict[str, Any]:
    """Convert a Session to a JSON-friendly dict."""
    return {
        "conversation": [_message_to_dict(m) for m in session.conversation],
        "db_url": session.db_url,
        "db_dialect": session.db_dialect,
        "schema_cache": session.schema_cache,
        "semantic_layer": session.semantic_layer,
        "last_sql": session.last_sql,
        "last_result": session.last_result,
        "last_intent": session.last_intent,
        "mode": session.mode,
        "_data_version": session._data_version,
        "_data_seen": session._data_seen,
    }


def _message_to_dict(msg: Message) -> dict[str, Any]:
    d: dict[str, Any] = {"role": msg.role, "content": msg.content}
    if msg.tool_calls is not None:
        d["tool_calls"] = [asdict(tc) for tc in msg.tool_calls]
    if msg.tool_call_id is not None:
        d["tool_call_id"] = msg.tool_call_id
    if msg.name is not None:
        d["name"] = msg.name
    return d


def _session_from_dict(data: dict[str, Any]) -> Session:
    """Reconstruct a Session from a dict produced by ``_session_to_dict``."""
    conversation: list[Message] = []
    for m in data.get("conversation", []):
        tool_calls = None
        if "tool_calls" in m:
            tool_calls = [
                ToolCall(id=tc["id"], name=tc["name"], arguments=tc["arguments"])
                for tc in m["tool_calls"]
            ]
        conversation.append(
            Message(
                role=m["role"],
                content=m.get("content"),
                tool_calls=tool_calls,
                tool_call_id=m.get("tool_call_id"),
                name=m.get("name"),
            ),
        )

    return Session(
        conversation=conversation,
        db_url=data.get("db_url"),
        db_dialect=data.get("db_dialect"),
        schema_cache=data.get("schema_cache", {}),
        semantic_layer=data.get("semantic_layer", {}),
        last_sql=data.get("last_sql"),
        last_result=data.get("last_result"),
        last_intent=data.get("last_intent"),
        mode=data.get("mode", "query"),
        _data_version=data.get("_data_version", 0),
        _data_seen=data.get("_data_seen", 0),
    )
