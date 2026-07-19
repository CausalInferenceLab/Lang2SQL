"""HarnessContext — the assembled unit handed to every turn and every tool.

v4.1 §2.1 describes the harness as "one bundled thing": LLM + tools + scope-
aware semantic + session + safety + explorer + audit. The ContextConcierge
(tenancy) builds one of these per request; the agent loop and ctx-aware tools
read from it. Optional fields are the pieces a bare CLI smoke-test can omit.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from ..core.identity import Identity

if TYPE_CHECKING:
    from ..adapters.storage.sqlite_store import SqliteStore
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
