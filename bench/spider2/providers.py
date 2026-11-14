"""Benchmark-only multi-provider port; reuse Lang2SQL's message/response mapping."""

from __future__ import annotations
import logging
import time
from typing import Sequence
from lang2sql.adapters.llm.openai_ import (
    _encode_message,
    _encode_tool,
    _decode_completion,
)
from lang2sql.core.types import Message, ToolSpec

logger = logging.getLogger(__name__)


class MeasuredLLM:
    def __init__(self, profile: dict, key: str, timeout: float, max_calls: int):
        self.profile, self.key = profile, key
        self.timeout, self.max_calls = timeout, max_calls
        self.calls: list[dict] = []
        self.provider_messages: dict[str, dict] = {}

    async def complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()
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
            wire.append(saved if saved is not None else _encode_message(m))
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
        entry = {"seconds": 0.0, "usage": None, "status": "started"}
        self.calls.append(entry)
        start = time.monotonic()
        try:
            response = await acompletion(**kwargs)
            raw = response.model_dump()
            entry.update(
                status="ok", usage=raw.get("usage"), response_model=raw.get("model")
            )
            msg = raw["choices"][0]["message"]
            for call in msg.get("tool_calls") or []:
                self.provider_messages[call["id"]] = {
                    k: v for k, v in msg.items() if v is not None
                }
            return _decode_completion(raw)
        except Exception as exc:
            entry.update(status="error", error_type=type(exc).__name__)
            logger.error(
                "API failed profile=%s error=%s",
                self.profile["name"],
                type(exc).__name__,
            )
            raise
        finally:
            entry["seconds"] = time.monotonic() - start
