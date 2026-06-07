"""Tools — the ctx-aware capabilities the agent can invoke.

``build_default_tools`` assembles the V1 tool set. Tools that need only ports
read them from the live :class:`HarnessContext`; tools backed by a service
(remember, ingest_doc) take it by injection here so the wiring stays in one
place (the ContextConcierge calls this).
"""

from __future__ import annotations

from ..core.ports.ingestion import DocExtractorPort, SourcePort
from ..core.ports.tool import ToolPort
from ..ingestion.pipeline import IngestionPipeline
from ..memory.service import MemoryService
from .ask_user import AskUser
from .define_metric import DefineMetric
from .enrich_schema import EnrichSchema
from .explore_schema import ExploreSchema
from .ingest_doc import IngestDoc
from .org_setup import OrgSetupTool
from .remember import Remember
from .run_sql import RunSQL
from .semantic_federation import SemanticFederationTool

__all__ = [
    "build_default_tools",
    "RunSQL", "ExploreSchema", "EnrichSchema", "DefineMetric", "SemanticFederationTool",
    "OrgSetupTool", "Remember", "AskUser", "IngestDoc",
]


def build_default_tools(
    *,
    memory: MemoryService,
    ingestion: IngestionPipeline,
    source: SourcePort,
    extractor: DocExtractorPort,
) -> list[ToolPort]:
    """The V1 tools."""
    return [
        RunSQL(),
        ExploreSchema(),
        EnrichSchema(),
        DefineMetric(),
        SemanticFederationTool(),
        OrgSetupTool(),
        AskUser(),
        Remember(memory),
        IngestDoc(ingestion, source, extractor),
    ]
