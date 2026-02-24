from __future__ import annotations

from typing import Any, Protocol


class LLMPort(Protocol):
    """Abstracts LLM backends (Anthropic, OpenAI, etc.)."""

    def invoke(self, messages: list[dict[str, str]]) -> str: ...


class DBPort(Protocol):
    """Abstracts database backends (SQLAlchemy, etc.)."""

    def execute(self, sql: str) -> list[dict[str, Any]]: ...


class EmbeddingPort(Protocol):
    """
    Placeholder — will be implemented in OQ-2 (VectorRetriever).

    Abstracts embedding backends (OpenAI, Azure, Bedrock, etc.)
    so VectorRetriever is not coupled to any specific provider.
    """

    def embed_query(self, text: str) -> list[float]: ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]: ...
