"""Benchmark-only multi-provider port; reuse Lang2SQL's message/response mapping."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence

from lang2sql.adapters.llm.openai_ import (
    _decode_completion,
    _encode_message,
    _encode_tool,
)
from lang2sql.core.types import Message, ToolSpec

from .responses import ResponsesCodec

logger = logging.getLogger(__name__)


class MeasuredLLM:
    def __init__(self, profile: dict, key: str, timeout: float, max_calls: int):
        self.profile, self.key = profile, key
        self.timeout, self.max_calls = timeout, max_calls
        self.calls: list[dict] = []
        self.provider_messages: dict[str, dict] = {}
        self.responses = ResponsesCodec(max_calls)

    async def complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()
    ):
        from litellm import APIConnectionError, RateLimitError, Timeout, acompletion

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
            wire.append(saved if saved is not None else _encode_message(m))
        kwargs = {
            "model": self.profile["model"],
            "api_key": self.key,
            "messages": wire,
            "timeout": self.timeout,
            "num_retries": 0,
            "max_completion_tokens": self.profile.get("max_output_tokens", 8192),
        }
        if self.profile.get("reasoning_effort"):
            kwargs["reasoning_effort"] = self.profile["reasoning_effort"]
        if self.profile.get("api_base"):
            kwargs["api_base"] = self.profile["api_base"]
        if tools:
            kwargs["tools"] = [_encode_tool(t) for t in tools]
        api = acompletion
        if self.profile.get("api") == "responses":
            from litellm import aresponses

            items, response_tools = self.responses.encode(messages, tools)
            kwargs.pop("messages")
            kwargs.pop("reasoning_effort", None)
            kwargs.pop("tools", None)
            kwargs["input"] = items
            kwargs["max_output_tokens"] = kwargs.pop("max_completion_tokens")
            kwargs["store"] = False
            kwargs["include"] = ["reasoning.encrypted_content"]
            kwargs["reasoning"] = {
                "effort": self.profile.get("reasoning_effort", "high")
            }
            if response_tools:
                kwargs["tools"] = response_tools
            api = aresponses
        retries = self.profile.get("transport_retries", 0)
        for attempt in range(retries + 1):
            if len(self.calls) >= self.max_calls:
                raise RuntimeError(
                    "Total model-call budget exhausted, including transport retries"
                )
            entry = {
                "seconds": 0.0,
                "usage": None,
                "status": "started",
                "attempt": attempt + 1,
            }
            self.calls.append(entry)
            start = time.monotonic()
            try:
                response = await api(**kwargs)
                raw = (
                    response.model_dump(by_alias=True, exclude_none=True)
                    if self.profile.get("api") == "responses"
                    else response.model_dump()
                )
                entry.update(
                    status="ok", usage=raw.get("usage"), response_model=raw.get("model")
                )
                if self.profile.get("api") == "responses":
                    entry["response_status"] = raw.get("status")
                    entry["reasoning_items_returned"] = sum(
                        item.get("type") == "reasoning"
                        for item in raw.get("output", [])
                    )
                    entry["reasoning_items_replayed"] = sum(
                        item.get("type") == "reasoning" for item in kwargs["input"]
                    )
                    entry["incomplete_details"] = raw.get("incomplete_details")
                    return self.responses.decode(raw, messages, tools)
                entry["finish_reason"] = raw["choices"][0].get("finish_reason")
                msg = raw["choices"][0]["message"]
                for call in msg.get("tool_calls") or []:
                    self.provider_messages[call["id"]] = {
                        k: v for k, v in msg.items() if v is not None
                    }
                return _decode_completion(raw)
            except Exception as exc:
                entry.update(status="error", error_type=type(exc).__name__)
                retry = (
                    isinstance(exc, (Timeout, APIConnectionError, RateLimitError))
                    and attempt < retries
                )
                # Provider exception text may contain credentials; record type, never raw text.
                if retry:
                    logger.warning(
                        "API transport retry profile=%s attempt=%d error=%s",
                        self.profile["name"],
                        attempt + 1,
                        type(exc).__name__,
                    )
                    entry["retry_scheduled"] = True
                    await asyncio.sleep(self.profile.get("retry_delay_seconds", 1.0))
                else:
                    logger.error(
                        "API failed profile=%s error=%s",
                        self.profile["name"],
                        type(exc).__name__,
                    )
                    raise
            finally:
                entry["seconds"] = time.monotonic() - start
        raise AssertionError("Unreachable retry loop")
