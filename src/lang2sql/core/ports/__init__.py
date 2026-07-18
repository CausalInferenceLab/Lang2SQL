"""Abstract ports — the standardised "wall sockets" of the system.

Every outbound dependency and every pluggable strategy is one Protocol here.
V1 ships the simplest implementation of each; later versions add adapters
without the harness or tools changing.
"""

from .audit import AuditEvent, AuditPort
from .explorer import Column, ExplorerPort, Table
from .frontend import FrontendPort, InboundMessage, OutboundMessage
from .ingestion import (
    CandidateKind,
    DocExtractorPort,
    Document,
    SemanticCandidate,
    SourcePort,
)
from .llm import LLMPort
from .memory import ExtractorPort, Fact, RecallPort, StorePort
from .safety import (
    SafetyContext,
    SafetyDecision,
    SafetyLayerPort,
    SafetyPipelinePort,
    Verdict,
)
from .secrets import SecretsPort
from .semantic_scope import ScopeResolverPort
from .session_store import SessionStorePort
from .tool import ToolPort

__all__ = [
    "AuditEvent",
    "AuditPort",
    "Column",
    "ExplorerPort",
    "Table",
    "FrontendPort",
    "InboundMessage",
    "OutboundMessage",
    "CandidateKind",
    "DocExtractorPort",
    "Document",
    "SemanticCandidate",
    "SourcePort",
    "LLMPort",
    "ExtractorPort",
    "Fact",
    "RecallPort",
    "StorePort",
    "SafetyContext",
    "SafetyDecision",
    "SafetyLayerPort",
    "SafetyPipelinePort",
    "Verdict",
    "SecretsPort",
    "ScopeResolverPort",
    "SessionStorePort",
    "ToolPort",
]
