from __future__ import annotations

from ...core.exceptions import IntegrationMissingError

try:
    import anthropic as _anthropic
except ImportError:
    _anthropic = None  # type: ignore[assignment]


class AnthropicLLM:
    """LLMPort implementation backed by the Anthropic Messages API."""

    def __init__(self, *, model: str, api_key: str | None = None) -> None:
        if _anthropic is None:
            raise IntegrationMissingError(
                "anthropic", hint="pip install anthropic  # or: uv sync"
            )
        self._client = _anthropic.Anthropic(api_key=api_key)
        self._model = model

    def invoke(self, messages: list[dict[str, str]]) -> str:
        system = next(
            (m["content"] for m in messages if m["role"] == "system"), None
        )
        user_msgs = [m for m in messages if m["role"] != "system"]
        resp = self._client.messages.create(
            model=self._model,
            max_tokens=1024,
            system=system or "",
            messages=user_msgs,
        )
        return resp.content[0].text
