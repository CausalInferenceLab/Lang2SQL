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
from ..core.ports.explorer import ExplorerPort, close_explorer
from ..core.ports.llm import LLMPort
from ..core.ports.safety import SafetyPipelinePort
from ..core.ports.secrets import SecretsPort
from ..harness.context import HarnessContext
from ..harness.session import Session
from ..harness.tool_registry import ToolRegistry
from ..ingestion import FileSource, IngestionPipeline, LLMExtractor
from ..memory import InjectAllRecall, InMemoryStore, ManualExtractor, MemoryService
from ..safety.pipeline import SafetyPipeline
from ..semantic.catalog import (
    CATALOG_KEY,
    CONNECTION_BINDING_KEY,
    CONNECTION_GENERATION_KEY,
    ConnectionBinding,
    SemanticCatalog,
)
from ..semantic.service import SemanticService
from ..semantic.shortlist import build_attention_envelope
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
        self._scope_explorers: dict[tuple[str, str, int], ExplorerPort] = {}

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

    @property
    def audit(self) -> AuditPort:
        """Audit sink shared by semantic governance commands and query tools."""

        return self._audit

    def forget_explorer(self, scope: str) -> None:
        """Bust the cached explorer for ``scope`` (call after /setup updates a DSN)."""
        removed = [
            value for key, value in self._scope_explorers.items() if key[0] == scope
        ]
        self._scope_explorers = {
            key: value
            for key, value in self._scope_explorers.items()
            if key[0] != scope
        }
        for explorer in {id(item): item for item in removed}.values():
            close_explorer(explorer)

    def close(self) -> None:
        """Release cached database handles and all transient sensitive state."""

        explorers = {
            id(item): item for item in [self._explorer, *self._scope_explorers.values()]
        }
        self._scope_explorers.clear()
        for explorer in explorers.values():
            close_explorer(explorer)
        self._semantic.clear_transient_state()

    def connection_binding(self, scope: str) -> ConnectionBinding | None:
        raw = self._store.kv_get(scope, CONNECTION_BINDING_KEY)
        if raw is None:
            return None
        try:
            return ConnectionBinding.from_json(raw)
        except (KeyError, TypeError, ValueError):
            return None

    def connection_generation(self, scope: str) -> int:
        raw = self._store.kv_get(scope, CONNECTION_GENERATION_KEY)
        try:
            return int(raw) if raw is not None else 0
        except ValueError:
            return -1

    def source_identity(self, scope: str, dsn: str, extras: dict[str, str]) -> str:
        if not isinstance(self._secrets, EncryptedSecrets):
            raise RuntimeError("source identity requires EncryptedSecrets")
        return self._secrets.source_identity(scope, dsn, extras)

    def activate_connection(
        self,
        *,
        scope: str,
        dsn: str,
        extras: dict[str, str],
        catalog: SemanticCatalog,
        expected_generation: int,
    ) -> ConnectionBinding:
        """Atomically activate credentials and their matching semantic catalog.

        A custom SecretsPort cannot guarantee the same SQLite transaction, so
        first-connect fails explicitly instead of silently using compensating
        writes that can split the DSN/catalog pair on process death.
        """

        if not isinstance(self._secrets, EncryptedSecrets):
            raise RuntimeError("atomic connection activation requires EncryptedSecrets")
        encrypted_secrets = self._secrets
        managed_extra_keys = {"d1_token", *extras.keys()}
        source_id = encrypted_secrets.source_identity(scope, dsn, extras)

        def build_upserts(generation: int) -> dict[str, str]:
            catalog.source_id = source_id
            catalog.connection_generation = generation
            binding = ConnectionBinding(
                source_id=source_id,
                generation=generation,
                managed_credentials=True,
            )
            return {
                "db_dsn": encrypted_secrets.encode_for_storage(dsn),
                CATALOG_KEY: catalog.to_json(),
                CONNECTION_BINDING_KEY: binding.to_json(),
                **{
                    f"db_extras.{key}": encrypted_secrets.encode_for_storage(value)
                    for key, value in extras.items()
                },
            }

        generation = self._store.kv_activate_generation(
            scope,
            expected_generation=expected_generation,
            build_upserts=build_upserts,
            generation_key=CONNECTION_GENERATION_KEY,
            delete_keys={
                f"db_extras.{key}" for key in managed_extra_keys if key not in extras
            },
        )
        self.forget_explorer(scope)
        return ConnectionBinding(
            source_id=source_id,
            generation=generation,
            managed_credentials=True,
        )

    async def _active_state(
        self, identity: Identity
    ) -> tuple[ExplorerPort, SemanticCatalog | None, bool, ConnectionBinding | None]:
        """Read credentials, binding, and catalog as one generation snapshot."""

        scope = identity.kv_scope
        keys = {
            "db_dsn",
            "db_extras.d1_token",
            CATALOG_KEY,
            CONNECTION_BINDING_KEY,
            CONNECTION_GENERATION_KEY,
        }
        snapshot = self._store.kv_get_many(scope, keys)
        raw_catalog = snapshot.get(CATALOG_KEY)
        raw_catalog_exists = raw_catalog is not None
        try:
            catalog = SemanticCatalog.from_json(raw_catalog) if raw_catalog else None
        except (KeyError, TypeError, ValueError):
            catalog = None
        try:
            binding = (
                ConnectionBinding.from_json(snapshot[CONNECTION_BINDING_KEY])
                if CONNECTION_BINDING_KEY in snapshot
                else None
            )
        except (KeyError, TypeError, ValueError):
            binding = None

        encrypted_dsn = snapshot.get("db_dsn")
        has_bound_material = encrypted_dsn is not None or binding is not None
        binding_matches = bool(
            binding
            and catalog
            and catalog.source_id == binding.source_id
            and catalog.connection_generation == binding.generation
            and snapshot.get(CONNECTION_GENERATION_KEY) == str(binding.generation)
        )
        if has_bound_material and not binding_matches:
            # Legacy or torn credential/catalog bundles fail closed. Keeping a
            # semantic catalog object prevents raw-SQL fallback in callers.
            return self._explorer, SemanticCatalog(fingerprint="invalid"), True, None
        if binding is None:
            return self._explorer, catalog, raw_catalog_exists, None
        if not binding.managed_credentials:
            if self._semantic.unmanaged_explorer_matches(self._explorer, binding):
                return self._explorer, catalog, raw_catalog_exists, binding
            return self._explorer, SemanticCatalog(fingerprint="invalid"), True, None
        assert encrypted_dsn is not None
        if not isinstance(self._secrets, EncryptedSecrets):
            return self._explorer, SemanticCatalog(fingerprint="invalid"), True, None
        try:
            dsn = self._secrets.decode_from_storage(encrypted_dsn)
        except Exception:
            return self._explorer, SemanticCatalog(fingerprint="invalid"), True, None
        extras: dict[str, str] = {}
        encrypted_token = snapshot.get("db_extras.d1_token")
        if encrypted_token:
            try:
                extras["d1_token"] = self._secrets.decode_from_storage(encrypted_token)
            except Exception:
                return (
                    self._explorer,
                    SemanticCatalog(fingerprint="invalid"),
                    True,
                    None,
                )
        cache_key = (scope, binding.source_id, binding.generation)
        cached = self._scope_explorers.get(cache_key)
        if cached is not None:
            return cached, catalog, raw_catalog_exists, binding
        explorer = build_explorer(dsn, extras=extras or None)
        self._scope_explorers[cache_key] = explorer
        return explorer, catalog, raw_catalog_exists, binding

    async def build_context(
        self, identity: Identity, user_text: str | None = None
    ) -> HarnessContext:
        session = await self._store.load(identity.session_key())
        if session is None:
            session = Session(identity=identity)

        explorer, catalog, raw_catalog_exists, binding = await self._active_state(
            identity
        )
        if binding is not None and (
            session.source_id != binding.source_id
            or session.connection_generation != binding.generation
        ):
            # DB-derived conversation context belongs to one connection only.
            session.reset()
            session.source_id = binding.source_id
            session.connection_generation = binding.generation
        if raw_catalog_exists and catalog is None:
            # A corrupt governed catalog must fail closed.  An empty semantic
            # tool advertises no selectable IDs and blocks at service lookup;
            # it never falls back to the legacy raw-SQL tool.
            catalog = SemanticCatalog(fingerprint="invalid")
        attention = (
            build_attention_envelope(catalog, user_text or "")
            if catalog is not None
            else None
        )
        semantic_query = (
            SemanticQuery(self._semantic, catalog, attention)
            if catalog is not None and attention is not None
            else None
        )
        if semantic_query is not None:
            # Materialize once so the cap is measured against the exact schema
            # sent to the model, not an approximate candidate projection.
            _ = semantic_query.spec
        tool_list = build_default_tools(
            memory=self._memory,
            ingestion=self._ingestion,
            source=self._source,
            extractor=self._extractor,
            semantic_query=semantic_query,
        )
        tools = ToolRegistry(
            tool_list,
            # Governed natural-language turns expose only the typed query and
            # clarification surfaces. Direct slash commands may still dispatch
            # registered memory/ingestion tools without letting DB metadata
            # prompt the model into side effects.
            advertised_names=(
                {"semantic_query", "ask_user"} if semantic_query is not None else None
            ),
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
            semantic_attention_state=(
                "candidate_schema_too_large"
                if semantic_query is not None and semantic_query.schema_blocker
                else attention.state if attention else ""
            ),
            semantic_attention_message=(
                semantic_query.schema_blocker
                if semantic_query is not None and semantic_query.schema_blocker
                else attention.message if attention else ""
            ),
            semantic_table_ids=attention.table_ids if attention else (),
            semantic_query=semantic_query,
            source_id=binding.source_id if binding else "",
            connection_generation=binding.generation if binding else 0,
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
