"""Compose a tool-only runtime; never instantiate a provider or run agent_loop."""

from __future__ import annotations

from ..adapters.db.factory import explorer_from_env
from ..adapters.storage.sqlite_store import SqliteStore
from ..core.identity import Identity
from ..core.ports.explorer import ExplorerPort
from ..harness.context import HarnessContext
from ..harness.session import Session
from ..harness.tool_registry import ToolRegistry
from ..memory import InjectAllRecall, InMemoryStore, ManualExtractor, MemoryService
from ..safety.pipeline import SafetyPipeline
from ..tools.explore_schema import ExploreSchema
from ..tools.recall_memory import RecallMemory
from ..tools.remember import Remember
from ..tools.run_sql import RunSQL
from ..tools.semantic_federation import SemanticFederationTool


class ExternalAgentLLM:
    async def complete(self, messages, tools=()):
        raise RuntimeError("External-agent tools must not invoke an internal LLM")


def build_tool_context(
    identity: Identity,
    *,
    store_path: str = ":memory:",
    explorer: ExplorerPort | None = None,
) -> HarnessContext:
    """Bind one connection and identity for the lifetime of a local tool process."""
    database = explorer if explorer is not None else explorer_from_env()
    if database is None:
        raise ValueError(
            "Configure LANG2SQL_DB_URL or the existing D1 environment settings"
        )
    store = SqliteStore(store_path)
    memory = MemoryService(InMemoryStore(), InjectAllRecall(), ManualExtractor())
    registry = ToolRegistry(
        [
            RunSQL(),
            ExploreSchema(),
            SemanticFederationTool(),
            Remember(memory),
            RecallMemory(memory),
        ]
    )
    return HarnessContext(
        identity=identity,
        llm=ExternalAgentLLM(),
        tools=registry,
        session=Session(identity=identity),
        explorer=database,
        safety=SafetyPipeline(),
        audit=store,
        store=store,
    )
