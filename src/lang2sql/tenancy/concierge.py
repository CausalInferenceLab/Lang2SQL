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
from ..adapters.storage.sqlite_store import SqliteStore
from ..core.identity import Identity
from ..core.ports.audit import AuditPort
from ..core.ports.explorer import ExplorerPort
from ..core.ports.llm import LLMPort
from ..core.ports.safety import SafetyPipelinePort
from ..core.ports.secrets import SecretsPort
from ..harness.context import HarnessContext
from ..harness.session import Session
from ..harness.tool_registry import ToolRegistry
from ..ingestion import FileSource, IngestionPipeline, LLMExtractor
from ..memory import InjectAllRecall, InMemoryStore, ManualExtractor, MemoryService
from ..safety.pipeline import SafetyPipeline
from ..semantic.catalog import CATALOG_KEY, SemanticCatalog
from ..semantic.service import SemanticService
from ..tools import build_default_tools
from ..tools.semantic_query import SemanticQuery
from .encrypted_secrets import EncryptedSecrets

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
        secrets: SecretsPort | None = None,
        audit: AuditPort | None = None,
        max_turns: int = 8,
    ) -> None:
        self._store = store if store is not None else SqliteStore(path)
        self._llm = llm if llm is not None else _default_llm()
        self._explorer = (
            explorer or explorer_from_env() or PostgresExplorer(_DEFAULT_DSN)
        )
        self._safety = safety if safety is not None else SafetyPipeline()
        self._secrets = (
            secrets if secrets is not None else EncryptedSecrets(self._store)
        )
        self._audit = audit if audit is not None else self._store
        self._max_turns = max_turns
        self._semantic = SemanticService(self._store)

        # V1 memory (in-memory + inject-all + manual) and ingestion (file × LLM).
        self._memory = MemoryService(
            InMemoryStore(), InjectAllRecall(), ManualExtractor()
        )
        self._ingestion = IngestionPipeline()
        self._source = FileSource()
        self._extractor = LLMExtractor(self._llm)

        # Per-scope explorer cache. /setup stores a DSN under the guild scope;
        # the next build_context for that scope materialises an explorer from
        # it on demand and reuses it across turns (lazy + cached).
        self._scope_explorers: dict[str, ExplorerPort] = {}

    @property
    def store(self) -> SqliteStore:
        return self._store

    @property
    def secrets(self) -> SecretsPort:
        """Per-scope encrypted credential store used by the setup workflow."""
        return self._secrets

    @property
    def semantic(self) -> SemanticService:
        """Concrete first-connect semantic service shared by all frontends."""

        return self._semantic

    def forget_explorer(self, scope: str) -> None:
        """Bust the cached explorer for ``scope`` (call after /setup updates a DSN)."""
        self._scope_explorers.pop(scope, None)

    def activate_connection(
        self,
        *,
        scope: str,
        dsn: str,
        extras: dict[str, str],
        catalog: SemanticCatalog,
    ) -> None:
        """Atomically activate credentials and their matching semantic catalog.

        A custom SecretsPort cannot guarantee the same SQLite transaction, so
        first-connect fails explicitly instead of silently using compensating
        writes that can split the DSN/catalog pair on process death.
        """

        if not isinstance(self._secrets, EncryptedSecrets):
            raise RuntimeError("atomic connection activation requires EncryptedSecrets")
        managed_extra_keys = {"d1_token", *extras.keys()}
        upserts = {
            "db_dsn": self._secrets.encode_for_storage(dsn),
            CATALOG_KEY: catalog.to_json(),
            **{
                f"db_extras.{key}": self._secrets.encode_for_storage(value)
                for key, value in extras.items()
            },
        }
        self._store.kv_apply_atomic(
            scope,
            upserts=upserts,
            delete_keys={
                f"db_extras.{key}" for key in managed_extra_keys if key not in extras
            },
        )
        self.forget_explorer(scope)

    async def _explorer_for(self, identity: Identity) -> ExplorerPort:
        """Pick the right explorer for this identity's guild scope.

        If the wizard has stored a DSN for the guild (under ``db_dsn`` in
        secrets), build an explorer from it (cached). Otherwise fall back to
        the concierge's default explorer (env-configured or stub).
        """
        scope = identity.kv_scope
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
        catalog = self._semantic.load(identity.kv_scope)
        raw_catalog_exists = (
            self._store.kv_get(identity.kv_scope, CATALOG_KEY) is not None
        )
        if raw_catalog_exists and catalog is None:
            # A corrupt governed catalog must fail closed.  An empty semantic
            # tool advertises no selectable IDs and blocks at service lookup;
            # it never falls back to the legacy raw-SQL tool.
            catalog = SemanticCatalog(fingerprint="invalid")
        semantic_query = (
            SemanticQuery(self._semantic, catalog) if catalog is not None else None
        )
        tools = ToolRegistry(
            build_default_tools(
                memory=self._memory,
                ingestion=self._ingestion,
                source=self._source,
                extractor=self._extractor,
                semantic_query=semantic_query,
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
            store=self._store,
            okf_bundle_dir=os.getenv("OKF_BUNDLE_DIR"),
            max_turns=self._max_turns,
        )


def _default_llm() -> LLMPort:
    """Local vLLM/Ollama when LANG2SQL_LLM_BASE_URL is set, OpenAI when keyed, else FakeLLM."""
    base_url = os.environ.get("LANG2SQL_LLM_BASE_URL")
    if base_url:
        model = os.environ.get("LANG2SQL_LLM_MODEL", "default")
        # Local servers (vLLM, Ollama) speak OpenAI-compatible API; dummy key satisfies the header.
        api_key = os.environ.get("OPENAI_API_KEY") or "local"
        url = base_url.rstrip("/")
        if not url.endswith("/chat/completions"):
            if not url.endswith("/v1"):
                url = url + "/v1"
            url = url + "/chat/completions"
        return OpenAILLM(model=model, api_key=api_key, base_url=url)
    if os.environ.get("OPENAI_API_KEY"):
        return OpenAILLM()
    return FakeLLM()
