"""Benchmark-only multi-provider port; reuse Lang2SQL's message/response mapping."""

from __future__ import annotations
import asyncio
import logging
import time
from typing import Sequence
from lang2sql.adapters.llm.openai_ import (
    _encode_message,
    _encode_tool,
    _decode_completion,
)
from lang2sql.core.types import Message, ToolSpec

from .recovery import response_fields, tool_choice

logger = logging.getLogger(__name__)


class MeasuredLLM:
    def __init__(self, profile: dict, key: str, timeout: float, max_calls: int, *, save_attempt=None):
        self.profile, self.key = profile, key
        self.timeout, self.max_calls = timeout, max_calls
        self.calls: list[dict] = []
        self.save_attempt = save_attempt
        self.provider_messages: dict[str, dict] = {}

    async def complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()
    ):
        return await self._complete(messages, tools)

    async def complete_with_required_tool(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec], tool_name: str
    ):
        return await self._complete(messages, tools, required_tool=tool_name)

    async def _complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec], *, required_tool=None
    ):
        from litellm import acompletion

        if len(self.calls) >= self.max_calls:
            raise RuntimeError(
                "Total model-call budget exhausted (including auxiliary tools)"
            )
        wire = []
        for m in messages:
            # Preserve provider-specific tool-call metadata (e.g. Gemini thought signatures)
            # without changing core domain types or exposing it to other components.
            saved = (
                self.provider_messages.get(m.tool_calls[0].id) if m.tool_calls else None
            )
            encoded = _encode_message(m)
            wire.append({**saved, "content": encoded["content"]} if saved else encoded)
        kwargs = dict(
            model=self.profile["model"],
            api_key=self.key,
            messages=wire,
            timeout=self.timeout,
            num_retries=0,
            max_tokens=4096,
        )
        if self.profile.get("api_base"):
            kwargs["api_base"] = self.profile["api_base"]
        if tools:
            kwargs["tools"] = [_encode_tool(t) for t in tools]
            kwargs["tool_choice"] = tool_choice(required_tool)
        entry = {
            "seconds": 0.0, "usage": None, "status": "started",
            "tool_choice": kwargs.get("tool_choice"),
            "harness": {"explore_schema", "run_sql"} <= {tool.name for tool in tools},
        }
        self.calls.append(entry)
        if self.save_attempt:
            self.save_attempt(self.calls)
        start = time.monotonic()
        try:
            response = await asyncio.wait_for(acompletion(**kwargs), timeout=self.timeout)
            raw = response.model_dump()
            entry.update(
                usage=raw.get("usage"), response_model=raw.get("model"), request_id=raw.get("id")
            )
            msg = raw["choices"][0]["message"]
            for call in msg.get("tool_calls") or []:
                self.provider_messages[call["id"]] = {
                    k: v for k, v in msg.items() if v is not None
                }
            completion = _decode_completion(raw)
            entry.update(status="ok", **response_fields(completion, tools))
            return completion
        except asyncio.CancelledError:
            entry["status"] = "cancelled"
            raise
        except Exception as exc:
            status = getattr(exc, "status_code", None)
            entry.update(status="error", error_type=type(exc).__name__, status_code=status if isinstance(status, int) else None)
            logger.error(
                "API failed profile=%s error=%s",
                self.profile["name"],
                type(exc).__name__,
            )
            raise
        finally:
            entry["seconds"] = time.monotonic() - start
            if self.save_attempt:
                self.save_attempt(self.calls)
