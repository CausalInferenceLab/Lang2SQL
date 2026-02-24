from __future__ import annotations

from ...core.exceptions import IntegrationMissingError

try:
    import openai as _openai
except ImportError:
    _openai = None  # type: ignore[assignment]


class OpenAILLM:
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
