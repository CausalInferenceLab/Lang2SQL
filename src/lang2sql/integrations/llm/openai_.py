from __future__ import annotations

import json
from typing import Any

from ...core.exceptions import IntegrationMissingError
from ...core.ports import LLMPort

try:
    import openai as _openai
except ImportError:
    _openai = None  # type: ignore[assignment]


class OpenAILLM(LLMPort):
    """LLMPort implementation backed by the OpenAI Chat Completions API."""

    def __init__(self, *, model: str, api_key: str | None = None) -> None:
        if _openai is None:
            raise IntegrationMissingError(
                "openai", hint="pip install openai  # or: uv sync"
            )
        self._client = _openai.OpenAI(api_key=api_key)
        self._model = model

    def invoke(self, messages: list[dict[str, str]]) -> str:
        resp = self._client.chat.completions.create(
            model=self._model,
            messages=messages,  # type: ignore[arg-type]
        )
        return resp.choices[0].message.content or ""


class OpenAIToolCallLLM:
    """ToolCallLLMPort backed by OpenAI Chat Completions with tools."""

    def __init__(
        self, *, model: str, api_key: str | None = None, max_tokens: int = 4096
    ) -> None:
        if _openai is None:
            raise IntegrationMissingError(
                "openai", hint="pip install openai  # or: uv sync"
            )
        self._client = _openai.OpenAI(api_key=api_key)
        self._model = model
        self._max_tokens = max_tokens

    def invoke_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> dict[str, Any]:
        api_messages = self._convert_messages(messages)
        api_tools = [
            {
                "type": "function",
                "function": {
                    "name": t["name"],
                    "description": t.get("description", ""),
                    "parameters": t.get("input_schema", {}),
                },
            }
            for t in tools
        ]

        resp = self._client.chat.completions.create(
            model=self._model,
            max_tokens=self._max_tokens,
            messages=api_messages,  # type: ignore[arg-type]
            tools=api_tools,  # type: ignore[arg-type]
        )

        msg = resp.choices[0].message
        finish_reason = resp.choices[0].finish_reason or "stop"

        tool_calls: list[dict[str, Any]] | None = None
        if msg.tool_calls:
            tool_calls = [
                {
                    "id": tc.id,
                    "name": tc.function.name,
                    "arguments": json.loads(tc.function.arguments),
                }
                for tc in msg.tool_calls
            ]

        return {
            "content": msg.content,
            "tool_calls": tool_calls,
            "stop_reason": finish_reason,
        }

    @staticmethod
    def _convert_messages(
        messages: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        converted: list[dict[str, Any]] = []
        for m in messages:
            role = m["role"]

            if role == "assistant" and m.get("tool_calls"):
                msg: dict[str, Any] = {
                    "role": "assistant",
                    "content": m.get("content") or "",
                    "tool_calls": [
                        {
                            "id": tc["id"],
                            "type": "function",
                            "function": {
                                "name": tc["name"],
                                "arguments": json.dumps(tc["arguments"]),
                            },
                        }
                        for tc in m["tool_calls"]
                    ],
                }
                converted.append(msg)

            elif role == "tool":
                converted.append(
                    {
                        "role": "tool",
                        "tool_call_id": m["tool_call_id"],
                        "content": m.get("content", ""),
                    }
                )

            else:
                converted.append({"role": role, "content": m["content"]})

        return converted
