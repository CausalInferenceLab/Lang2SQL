"""ContextConcierge — the assembly point that builds one HarnessContext.

This is the only module allowed to import the concrete semantic/safety/adapter
classes; everywhere else depends on the ``core.ports`` Protocols. Per request it
picks an LLM (OpenAI when keyed, else the FakeLLM), restores or starts a
:class:`Session`, and wires the explorer, safety pipeline, scope resolver, and
audit store into a :class:`HarnessContext` for the loop.

Dependency-injection friendly: every collaborator can be overridden in the
ctor so tests (and v1.5 swaps) need no network and no globals.
"""

from __future__ import annotations

import os

from ..adapters.db.factory import build_explorer, explorer_from_env
from ..adapters.db.postgres_explorer import PostgresExplorer
from ..adapters.llm.fake import FakeLLM
from ..adapters.llm.openai_ import OpenAILLM
from ..adapters.storage.sqlite_semantic import SqliteSemanticStore
from ..adapters.storage.sqlite_store import SqliteStore
from ..core.identity import Identity, Scope, ScopeLevel
from ..core.ports.audit import AuditPort
from ..core.ports.explorer import ExplorerPort
from ..core.ports.llm import LLMPort
from ..core.ports.safety import SafetyPipelinePort
from ..core.ports.secrets import SecretsPort
from ..core.ports.semantic_scope import ScopeResolverPort
from ..core.types import Message, Role
from ..harness.context import HarnessContext
from ..harness.session import Session
from ..harness.tool_registry import ToolRegistry
from ..ingestion import FileSource, IngestionPipeline, LLMExtractor
from ..memory import InjectAllRecall, InMemoryStore, ManualExtractor, MemoryService
from ..safety.pipeline import SafetyPipeline
from ..semantic.types import SemanticEntry, SemanticKind
from ..tools import build_default_tools
from .encrypted_secrets import EncryptedSecrets
from .scope_resolver import ScopeResolver

# DSN used for the V1 explorer stub when a scope has registered none yet.
_DEFAULT_DSN = "postgresql://stub/v1"


class ContextConcierge:
    """Assembles per-request :class:`HarnessContext` from injected ports."""

    def __init__(
        self,
        *,
        path: str = ":memory:",
        store: SqliteStore | None = None,
        llm: LLMPort | None = None,
        explorer: ExplorerPort | None = None,
        safety: SafetyPipelinePort | None = None,
        scope_resolver: ScopeResolverPort | None = None,
        secrets: SecretsPort | None = None,
        audit: AuditPort | None = None,
        max_turns: int = 8,
    ) -> None:
        # ``path`` drives the default persistence backends; ``:memory:`` keeps
        # tests isolated, a file path makes sessions/definitions/secrets durable.
        self._store = store if store is not None else SqliteStore(path)
        # Audit + session persistence both ride the one sqlite store by default.
        self._llm = llm if llm is not None else _default_llm()
        # Explorer precedence: explicit injection → env-configured real DB
        # (LANG2SQL_DB_URL / Cloudflare D1) → the canned stub for offline dev.
        self._explorer = explorer or explorer_from_env() or PostgresExplorer(_DEFAULT_DSN)
        self._safety = safety if safety is not None else SafetyPipeline()
        # Persistent semantic store by default so definitions survive restart.
        self._scope_resolver = (
            scope_resolver
            if scope_resolver is not None
            else ScopeResolver(SqliteSemanticStore(path))
        )
        # Secrets share the session/audit store's kv table (and sqlite file).
        self._secrets = (
            secrets if secrets is not None else EncryptedSecrets(self._store)
        )
        self._audit = audit if audit is not None else self._store
        self._max_turns = max_turns

        # V1 memory (in-memory + inject-all + manual) and ingestion (file × LLM).
        self._memory = MemoryService(InMemoryStore(), InjectAllRecall(), ManualExtractor())
        self._ingestion = IngestionPipeline()
        self._source = FileSource()
        self._extractor = LLMExtractor(self._llm)

        # Per-scope explorer cache. /setup stores a DSN under the guild scope;
        # the next build_context for that scope materialises an explorer from
        # it on demand and reuses it across turns (lazy + cached).
        self._scope_explorers: dict[str, ExplorerPort] = {}

        # Scopes that have already been pre-warmed against this concierge
        # instance — avoids re-running the (LLM-paid) schema scan every turn.
        self._prewarmed_scopes: set[str] = set()
        self.prewarm_enabled: bool = True
        self.prewarm_table_limit: int = 8

    @property
    def store(self) -> SqliteStore:
        return self._store

    @property
    def secrets(self) -> SecretsPort:
        """Per-scope encrypted credential store (DSNs/API keys via ``/connect``)."""
        return self._secrets

    @property
    def scope_resolver(self) -> ScopeResolverPort:
        """Federation resolver over the (by default persistent) semantic store."""
        return self._scope_resolver

    def forget_explorer(self, scope: str) -> None:
        """Bust the cached explorer for ``scope`` (call after /setup updates a DSN)."""
        self._scope_explorers.pop(scope, None)

    async def _explorer_for(self, identity: Identity) -> ExplorerPort:
        """Pick the right explorer for this identity's guild scope.

        If the wizard has stored a DSN for the guild (under ``db_dsn`` in
        secrets), build an explorer from it (cached). Otherwise fall back to
        the concierge's default explorer (env-configured or stub).
        """
        scope = identity.guild_id or f"dm:{identity.user_id}"
        cached = self._scope_explorers.get(scope)
        if cached is not None:
            return cached
        dsn = await self._secrets.get(scope, "db_dsn")
        if not dsn:
            return self._explorer
        extras: dict[str, str] = {}
        d1_token = await self._secrets.get(scope, "db_extras.d1_token")
        if d1_token:
            extras["d1_token"] = d1_token
        explorer = build_explorer(dsn, extras=extras or None)
        self._scope_explorers[scope] = explorer
        return explorer

    async def build_context(
        self, identity: Identity, user_text: str | None = None
    ) -> HarnessContext:
        session = await self._store.load(identity.session_key())
        if session is None:
            session = Session(identity=identity)

        explorer = await self._explorer_for(identity)

        # ★ β — first-time, scope-level pre-warm. Walk the schema once and
        # write SemanticEntry rows into the scope_resolver via its existing
        # define(). The system_prompt path then naturally surfaces these as
        # "Semantic layer" to every future turn — no new wiring, just data.
        await self._prewarm_semantic_layer(identity, explorer)

        tools = ToolRegistry(
            build_default_tools(
                memory=self._memory,
                ingestion=self._ingestion,
                source=self._source,
                extractor=self._extractor,
            )
        )

        return HarnessContext(
            identity=identity,
            llm=self._llm,
            tools=tools,
            session=session,
            explorer=explorer,
            safety=self._safety,
            audit=self._audit,
            scope_resolver=self._scope_resolver,
            max_turns=self._max_turns,
        )

    async def _prewarm_semantic_layer(
        self, identity: Identity, explorer: ExplorerPort
    ) -> None:
        """One-shot LLM-driven schema → SemanticEntry pre-fill at guild scope.

        Stays inside the V1 harness: it only writes through the existing
        ``ScopeResolverPort.define``. The system prompt's "Semantic layer"
        section then surfaces these entries on every subsequent turn, exactly
        as if a human had typed them via ``/define_metric``. Skipped when:

        - prewarm is disabled
        - the guild scope already has any SemanticEntry (don't overwrite humans)
        - this scope was already pre-warmed in this process
        - explorer has no tables to describe
        """
        if not self.prewarm_enabled:
            return
        scope = _guild_scope(identity)
        if scope.key in self._prewarmed_scopes:
            return
        existing = await self._scope_resolver.entries_at(scope)
        if existing:
            self._prewarmed_scopes.add(scope.key)
            return
        try:
            tables = await explorer.list_tables()
        except Exception:
            return
        tables = tables[: self.prewarm_table_limit]
        if not tables:
            return

        # Describe each table once and ask the LLM for a short DIMENSION
        # definition for every column. One LLM call total (cheap on context).
        try:
            described = []
            for t in tables:
                described.append(await explorer.describe_table(t.name))
            schema_dump = "\n".join(
                f"{t.qualified or t.name}: "
                + ", ".join(f"{c.name} ({c.type})" for c in t.columns)
                for t in described
            )
            prompt = (
                "For the database schema below, write a one-sentence description "
                "(≤120 chars) for EACH column, explaining what it likely means. "
                "Return STRICT JSON: an object mapping `\"<table>.<column>\"` to a "
                "description string. No markdown, no commentary.\n\n"
                f"{schema_dump}"
            )
            comp = await self._llm.complete(
                [Message(role=Role.USER, content=prompt)], tools=()
            )
        except Exception:
            self._prewarmed_scopes.add(scope.key)
            return

        text = (comp.content or "").strip()
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        import json as _json
        try:
            mapping = _json.loads(text)
            if not isinstance(mapping, dict):
                mapping = {}
        except _json.JSONDecodeError:
            mapping = {}

        actor = f"prewarm:{identity.user_id}"
        for key, desc in mapping.items():
            if not isinstance(key, str) or not isinstance(desc, str):
                continue
            if "." in key:
                table_name, col = key.split(".", 1)
            else:
                table_name, col = "", key
            await self._scope_resolver.define(
                scope,
                SemanticEntry(
                    kind=SemanticKind.DIMENSION,
                    name=col,
                    definition=desc[:200],
                    applies_to=table_name,
                    source_id="prewarm",
                    created_by=actor,
                ),
            )
        self._prewarmed_scopes.add(scope.key)


def _guild_scope(identity: Identity) -> Scope:
    """Pre-warm targets the guild (so all channels in the guild share). DMs use
    a per-user pseudo-guild so personal connections don't leak."""
    if identity.guild_id:
        return Scope(ScopeLevel.GUILD, identity.guild_id)
    return Scope(ScopeLevel.GUILD, f"dm:{identity.user_id}")


def _default_llm() -> LLMPort:
    """OpenAI when a key is present, otherwise the offline FakeLLM."""
    if os.environ.get("OPENAI_API_KEY"):
        return OpenAILLM()
    return FakeLLM()
