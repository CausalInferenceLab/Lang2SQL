"""HarnessContext — the assembled unit handed to every turn and every tool.

v4.1 §2.1 describes the harness as "one bundled thing": LLM + tools + scope-
aware semantic + session + safety + explorer + audit. The ContextConcierge
(tenancy) builds one of these per request; the agent loop and ctx-aware tools
read from it. Optional fields are the pieces a bare CLI smoke-test can omit.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from ..core.identity import Identity

if TYPE_CHECKING:
    from ..adapters.storage.sqlite_store import SqliteStore
    from ..tools.semantic_query import SemanticQuery
from ..core.ports.audit import AuditPort
from ..core.ports.explorer import ExplorerPort
from ..core.ports.llm import LLMPort
from ..core.ports.safety import SafetyPipelinePort
from .session import Session
from .tool_registry import ToolRegistry


@dataclass
class HarnessContext:
    identity: Identity
    llm: LLMPort
    tools: ToolRegistry
    session: Session
    explorer: ExplorerPort | None = None
    safety: SafetyPipelinePort | None = None
    audit: AuditPort | None = None
    store: SqliteStore | None = None
    okf_bundle_dir: str | None = None
    max_turns: int = 8
    # Server-side capability used only by the Discord review replay. Keeping
    # it outside tool arguments prevents a model from forging question context.
    trusted_reviewed_question: str | None = None
    semantic_attention_state: str = ""
    semantic_attention_message: str = ""
    semantic_table_ids: tuple[str, ...] = ()
    semantic_query: SemanticQuery | None = None
    source_id: str = ""
    connection_generation: int = 0
    # Ephemeral, server-owned result transport. Values never enter ToolResult,
    # the transcript, or a later model call; a frontend consumes them directly.
    semantic_result_ready: bool = False
    semantic_result_message: str = ""
    semantic_result_headers: tuple[str, ...] = ()
    semantic_result_rows: list[tuple[object, ...]] = field(default_factory=list)
    # Exact catalog stamp rechecked by the frontend immediately before render.
    semantic_result_stamp: tuple[str, int, str, int, int, int] | tuple[()] = ()
