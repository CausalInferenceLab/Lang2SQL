from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from .catalog import CatalogEntry, TextDocument


class LLMPort(Protocol):
    """Abstracts LLM backends (Anthropic, OpenAI, etc.)."""

    def invoke(self, messages: list[dict[str, str]]) -> str: ...


class DBPort(Protocol):
    """Abstracts database backends (SQLAlchemy, etc.)."""

    def execute(self, sql: str) -> list[dict[str, Any]]: ...


class EmbeddingPort(Protocol):
    """Abstracts embedding backends (OpenAI, Azure, Bedrock, etc.)."""

    def embed_query(self, text: str) -> list[float]: ...

    def embed_texts(self, texts: list[str]) -> list[list[float]]: ...


class VectorStorePort(Protocol):
    """Abstracts vector store backends (InMemory, FAISS, pgvector, etc.)."""

    def search(self, vector: list[float], k: int) -> list[tuple[str, float]]:
        """
        Return the k nearest vectors.

        Returns:
            List of (chunk_id, score) sorted by score descending.
            Score range: [-1, 1] (cosine similarity).
        """
        ...

    def upsert(self, ids: list[str], vectors: list[list[float]]) -> None:
        """
        Store or update vectors by chunk_id.

        Implementations must merge incoming entries into existing ones —
        calling upsert twice must not lose entries from the first call.

        Args:
            ids:     List of chunk_ids.
            vectors: Corresponding embedding vectors (len(ids) == len(vectors)).
        """
        ...


@runtime_checkable
class DocumentLoaderPort(Protocol):
    """Converts a file path or directory to list[TextDocument]."""

    def load(self, path: str) -> list[TextDocument]: ...


class CatalogLoaderPort(Protocol):
    """Abstracts catalog loading from external sources (DataHub, file, database, etc.)."""

    def load(self) -> list[CatalogEntry]: ...


class ToolCallLLMPort(Protocol):
    """LLM that supports native function/tool calling.

    Returns a dict with:
        content   – text reply (may be None when the model only emits tool calls)
        tool_calls – list of ``{"id": str, "name": str, "arguments": dict}``
        stop_reason – provider stop reason string
    """

    def invoke_with_tools(
        self,
        messages: list[dict[str, Any]],
        tools: list[dict[str, Any]],
    ) -> dict[str, Any]: ...


class DBExplorerPort(Protocol):
    """DB 에이전틱 탐색 인터페이스. Agent가 DB를 직접 탐색할 때 사용.

    메서드 선정 원칙:
    - DDL에 이미 있는 정보(컬럼 목록, FK, PK)는 별도 메서드 없음
    - 통계/집계는 execute_read_only()로 직접 질의
    - 관계 추론은 LLM에 위임 (휴리스틱 제거)
    """

    def list_tables(self, schema: str | None = None) -> list[str]: ...

    def get_ddl(self, table: str, *, schema: str | None = None) -> str: ...

    def sample_data(
        self, table: str, *, limit: int = 5, schema: str | None = None
    ) -> list[dict]: ...

    def execute_read_only(self, sql: str) -> list[dict]: ...
