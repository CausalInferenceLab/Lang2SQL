"""Benchmark Responses codec: preserve opaque reasoning within exact history.

The existing core message/tool codec remains the domain translation boundary.
This cache is local to one measured case, never shared across user sessions.
"""

from __future__ import annotations

import hashlib
import json

from lang2sql.adapters.llm.openai_ import (
    _decode_completion,
    _encode_message,
    _encode_tool,
)
from lang2sql.core.types import Message, Role


class ResponsesCodec:
    def __init__(self, max_entries):
        self.outputs = {}
        self.max_entries = max_entries

    @staticmethod
    def _key(messages, tools):
        payload = {
            "messages": [_encode_message(m) for m in messages],
            "tools": [_encode_tool(t) for t in tools],
        }
        return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

    def encode(self, messages, tools):
        items = []
        for index, message in enumerate(messages):
            cached = self.outputs.get(self._key(messages[: index + 1], tools))
            if message.role == Role.ASSISTANT and cached is not None:
                items.extend(cached)
            elif message.role == Role.TOOL:
                items.append(
                    {
                        "type": "function_call_output",
                        "call_id": message.tool_call_id,
                        "output": message.content,
                    }
                )
            else:
                if message.content:
                    items.append(
                        {"role": message.role.value, "content": message.content}
                    )
                for call in message.tool_calls:
                    items.append(
                        {
                            "type": "function_call",
                            "call_id": call.id,
                            "name": call.name,
                            "arguments": json.dumps(call.arguments),
                        }
                    )
        encoded_tools = [
            {"type": "function", **_encode_tool(tool)["function"], "strict": False}
            for tool in tools
        ]
        return items, encoded_tools

    def decode(self, raw, messages, tools):
        text, calls = [], []
        for item in raw.get("output", []):
            if item["type"] == "message":
                for part in item.get("content", []):
                    if part["type"] == "output_text":
                        text.append(part["text"])
                    elif part["type"] == "refusal":
                        text.append(part["refusal"])
            elif item["type"] == "function_call":
                calls.append(
                    {
                        "id": item["call_id"],
                        "type": "function",
                        "function": {
                            "name": item["name"],
                            "arguments": item["arguments"],
                        },
                    }
                )
        completion = _decode_completion(
            {
                "choices": [
                    {
                        "message": {"content": "\n".join(text), "tool_calls": calls},
                        "finish_reason": raw.get("status"),
                    }
                ]
            }
        )
        assistant = Message(
            role=Role.ASSISTANT,
            content=completion.content,
            tool_calls=completion.tool_calls,
        )
        self.outputs[self._key([*messages, assistant], tools)] = raw.get("output", [])
        while len(self.outputs) > self.max_entries:
            del self.outputs[next(iter(self.outputs))]
        return completion
