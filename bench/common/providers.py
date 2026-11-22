"""Measured LLMPort following the provider adapter from the Spider2 benchmark."""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable, Sequence
from typing import Any, Protocol

from lang2sql.adapters.llm.openai_ import (
    _decode_completion,
    _encode_message,
    _encode_tool,
)
from lang2sql.core.types import Completion, Message, ToolSpec


class ProviderResponse(Protocol):
    def model_dump(self) -> dict[str, Any]: ...


Transport = Callable[..., Awaitable[ProviderResponse]]


class CallBudgetExhausted(RuntimeError):
    pass


class ModelCallError(RuntimeError):
    def __init__(self, reason: str, error_type: str, status_code: int | None = None):
        self.reason = reason
        self.error_type = error_type
        self.status_code = status_code
        super().__init__(reason)


class MeasuredLLM:
    def __init__(
        self,
        *,
        model: str,
        api_base: str | None,
        api_key: str,
        timeout: float,
        max_calls: int,
        max_output_tokens: int,
        transport: Transport,
        observe_first_request: Callable[[list[dict], list[dict]], None],
        save_attempt: Callable[[list[dict]], None],
    ):
        self.model = model
        self.api_base = api_base
        self.api_key = api_key
        self.timeout = timeout
        self.max_calls = max_calls
        self.max_output_tokens = max_output_tokens
        self.transport = transport
        self.observe_first_request = observe_first_request
        self.save_attempt = save_attempt
        self.calls: list[dict] = []
        self._provider_messages: dict[str, dict] = {}

    async def complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()
    ) -> Completion:
        if len(self.calls) >= self.max_calls:
            raise CallBudgetExhausted
        wire = []
        for message in messages:
            saved = (
                self._provider_messages.get(message.tool_calls[0].id)
                if message.tool_calls
                else None
            )
            wire.append(saved if saved is not None else _encode_message(message))
        specs = [_encode_tool(tool) for tool in tools]
        if not self.calls:
            self.observe_first_request(wire, specs)
        kwargs = {
            "model": self.model,
            "api_key": self.api_key,
            "messages": wire,
            "timeout": self.timeout,
            "num_retries": 0,
            "max_tokens": self.max_output_tokens,
        }
        if self.api_base:
            kwargs["api_base"] = self.api_base
        if specs:
            kwargs["tools"] = specs
        entry = {"status": "started", "usage": None}
        self.calls.append(entry)
        self.save_attempt(self.calls)
        started = time.monotonic()
        try:
            response = await asyncio.wait_for(
                self.transport(**kwargs), timeout=self.timeout
            )
            raw = response.model_dump()
            entry.update(
                request_id=raw.get("id"),
                response_model=raw.get("model"),
                usage=raw.get("usage"),
            )
            completion = _decode_completion(raw)
            entry.update(status="ok", finish_reason=completion.finish_reason)
            provider_message = raw["choices"][0]["message"]
            for call in completion.tool_calls:
                self._provider_messages[call.id] = {
                    key: value
                    for key, value in provider_message.items()
                    if value is not None
                }
            return completion
        except asyncio.CancelledError:
            entry["status"] = "cancelled"
            raise
        except Exception as error:  # noqa: BLE001 - do not persist raw provider errors
            status_code = getattr(error, "status_code", None)
            if not isinstance(status_code, int):
                status_code = None
            is_timeout = isinstance(error, (asyncio.TimeoutError, TimeoutError))
            reason = "provider_timeout" if is_timeout else "provider_error"
            failure = ModelCallError(reason, type(error).__name__, status_code)
            entry.update(
                status="error", error_type=failure.error_type, status_code=status_code
            )
            raise failure from None
        finally:
            entry["seconds"] = time.monotonic() - started
            self.save_attempt(self.calls)
