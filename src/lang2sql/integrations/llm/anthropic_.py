from __future__ import annotations

from typing import Any

from ...core.exceptions import IntegrationMissingError
from ...core.ports import LLMPort

try:
    import anthropic as _anthropic
except ImportError:
    _anthropic = None  # type: ignore[assignment]


class AnthropicLLM(LLMPort):
    """LLMPort implementation backed by the Anthropic Messages API."""

    def __init__(
        self, *, model: str, api_key: str | None = None, max_tokens: int = 4096
    ) -> None:
        if _anthropic is None:
            raise IntegrationMissingError(
                "anthropic", hint="pip install anthropic  # or: uv sync"
            )
        self._client = _anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._max_tokens = max_tokens

    def invoke(self, messages: list[dict[str, str]]) -> str:
        system = next((m["content"] for m in messages if m["role"] == "system"), None)
        user_msgs = [m for m in messages if m["role"] != "system"]
        resp = self._client.messages.create(
            model=self._model,
            max_tokens=self._max_tokens,
            system=system or "",
            messages=user_msgs,
        )
        return resp.content[0].text


class AnthropicToolCallLLM:
    """ToolCallLLMPort backed by the Anthropic Messages API with tool_use."""

    def __init__(
        self, *, model: str, api_key: str | None = None, max_tokens: int = 4096
    ) -> None:
        if _anthropic is None:
            raise IntegrationMissingError(
                "anthropic", hint="pip install anthropic  # or: uv sync"
            )
        self._client = _anthropic.Anthropic(api_key=api_key)
        self._model = model
        self._max_tokens = max_tokens

    def invoke_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> dict[str, Any]:
        system = next(
            (m["content"] for m in messages if m["role"] == "system"), None
        )
        api_messages = self._convert_messages(
            [m for m in messages if m["role"] != "system"]
        )
        api_tools = [
            {
                "name": t["name"],
                "description": t.get("description", ""),
                "input_schema": t.get("input_schema", {}),
            }
            for t in tools
        ]

        kwargs: dict[str, Any] = {
            "model": self._model,
            "max_tokens": self._max_tokens,
            "messages": api_messages,
            "tools": api_tools,
        }
        if system:
            kwargs["system"] = system

        resp = self._client.messages.create(**kwargs)

        text_parts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        for block in resp.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(
                    {"id": block.id, "name": block.name, "arguments": block.input}
                )

        return {
            "content": "\n".join(text_parts) if text_parts else None,
            "tool_calls": tool_calls or None,
            "stop_reason": resp.stop_reason,
        }

    @staticmethod
    def _convert_messages(
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        converted: list[dict[str, Any]] = []
        for m in messages:
            role = m["role"]

            if role == "assistant" and m.get("tool_calls"):
                content: list[dict[str, Any]] = []
                if m.get("content"):
                    content.append({"type": "text", "text": m["content"]})
                for tc in m["tool_calls"]:
                    content.append(
                        {
                            "type": "tool_use",
                            "id": tc["id"],
                            "name": tc["name"],
                            "input": tc["arguments"],
                        }
                    )
                converted.append({"role": "assistant", "content": content})

            elif role == "tool":
                converted.append(
                    {
                        "role": "user",
                        "content": [
                            {
                                "type": "tool_result",
                                "tool_use_id": m["tool_call_id"],
                                "content": m.get("content", ""),
                            }
                        ],
                    }
                )

            else:
                converted.append({"role": role, "content": m["content"]})

        return converted
