"""OpenAILLM — the V1 :class:`LLMPort`, talking OpenAI chat/completions.

stdlib only: HTTP via :mod:`urllib.request`, JSON via :mod:`json`. No openai
SDK. The adapter's whole job is translation — core :class:`Message`/:class:`ToolSpec`
in, OpenAI wire dict out, OpenAI response back into a core :class:`Completion`.
The loop never sees an OpenAI shape.

Construction is offline-safe: a missing key only bites when :meth:`complete` is
actually called, so importing/wiring this in a no-key environment is fine.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import urllib.error
import urllib.request
from typing import Any, Sequence

from ...core.types import Completion, Message, Role, ToolCall, ToolSpec

_DEFAULT_URL = "https://api.openai.com/v1/chat/completions"


class OpenAILLM:
    """Tool-calling chat completion backed by OpenAI's REST API."""

    def __init__(
        self,
        model: str = "gpt-4o-mini",
        api_key: str | None = None,
        *,
        base_url: str = _DEFAULT_URL,
        timeout: float = 60.0,
    ) -> None:
        self.model = model
        # Resolve lazily-ish: read env now, but tolerate absence until complete().
        raw_key = api_key if api_key is not None else os.environ.get("OPENAI_API_KEY")
        self._api_key = raw_key.strip() if raw_key else raw_key
        self._base_url = base_url
        self._timeout = timeout

    async def complete(
        self,
        messages: Sequence[Message],
        tools: Sequence[ToolSpec] = (),
    ) -> Completion:
        if not self._api_key:
            raise RuntimeError("OPENAI_API_KEY not set")

        payload: dict[str, Any] = {
            "model": self.model,
            "messages": [_encode_message(m) for m in messages],
        }
        if tools:
            payload["tools"] = [_encode_tool(t) for t in tools]

        raw = await asyncio.to_thread(self._post, payload)
        return _decode_completion(raw)

    def _post(self, payload: dict[str, Any]) -> dict[str, Any]:
        body = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(
            self._base_url,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=self._timeout) as resp:
                text = resp.read().decode("utf-8")
        except urllib.error.HTTPError as exc:
            detail = exc.read().decode("utf-8", "replace") if exc.fp else ""
            raise RuntimeError(f"OpenAI HTTP {exc.code}: {detail}") from exc
        except urllib.error.URLError as exc:
            raise RuntimeError(f"OpenAI request failed: {exc.reason}") from exc

        try:
            return json.loads(text)
        except (ValueError, TypeError) as exc:
            raise RuntimeError(
                f"OpenAI returned non-JSON response: {text[:200]!r}"
            ) from exc


def _strip_thinking(text: str) -> str:
    return re.sub(r"<think>.*?</think>", "", text, flags=re.DOTALL).strip()


def _encode_message(m: Message) -> dict[str, Any]:
    """Core :class:`Message` → an OpenAI chat message dict."""
    out: dict[str, Any] = {"role": m.role.value}
    # OpenAI allows null content only when tool_calls are present.
    # For plain assistant messages (after session compress), force empty string.
    if m.role == Role.ASSISTANT and not m.tool_calls:
        out["content"] = m.content or ""
    else:
        out["content"] = m.content or None
    if m.role == Role.TOOL:
        out["tool_call_id"] = m.tool_call_id
        if m.name:
            out["name"] = m.name
    if m.tool_calls:
        out["tool_calls"] = [
            {
                "id": tc.id,
                "type": "function",
                "function": {
                    "name": tc.name,
                    "arguments": json.dumps(tc.arguments),
                },
            }
            for tc in m.tool_calls
        ]
    return out


def _encode_tool(t: ToolSpec) -> dict[str, Any]:
    """Core :class:`ToolSpec` → an OpenAI tool dict."""
    return {
        "type": "function",
        "function": {
            "name": t.name,
            "description": t.description,
            "parameters": t.parameters or {"type": "object", "properties": {}},
        },
    }


def _decode_completion(raw: dict[str, Any]) -> Completion:
    """OpenAI response JSON → core :class:`Completion`."""
    try:
        choice = raw["choices"][0]
        msg = choice["message"]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError(f"unexpected OpenAI response shape: {raw!r}") from exc

    tool_calls: list[ToolCall] = []
    for tc in msg.get("tool_calls") or []:
        fn = tc.get("function", {})
        raw_args = fn.get("arguments", "") or "{}"
        try:
            decoded_args = json.loads(raw_args)
        except (ValueError, TypeError):
            # Provider/model output is untrusted.  Keep ToolCall.arguments an
            # object so malformed local-model output cannot crash dispatch.
            args = {"__invalid_argument_shape__": "malformed_json"}
        else:
            args = (
                decoded_args
                if isinstance(decoded_args, dict)
                else {
                    "__invalid_argument_shape__": type(decoded_args).__name__
                }
            )
        tool_calls.append(
            ToolCall(id=tc.get("id", ""), name=fn.get("name", ""), arguments=args)
        )

    return Completion(
        content=_strip_thinking(msg.get("content") or ""),
        tool_calls=tool_calls,
        finish_reason=choice.get("finish_reason"),
    )
