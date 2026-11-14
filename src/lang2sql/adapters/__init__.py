"""Outbound adapters — concrete impls of the ``core.ports`` Protocols (v4.1 §2.1).

stdlib only in V1: OpenAI via ``urllib``, storage via ``sqlite3``, the Postgres
explorer a canned stub until psycopg lands in v1.5.
"""

from __future__ import annotations

from .db.postgres_explorer import PostgresExplorer
from .llm.codex import CodexLLM
from .llm.fake import FakeLLM
from .llm.openai_ import OpenAILLM
from .storage.sqlite_store import SqliteStore

__all__ = ["CodexLLM", "FakeLLM", "OpenAILLM", "PostgresExplorer", "SqliteStore"]
