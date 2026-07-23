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
from .confirm_ingest import ConfirmIngest
from .enrich_schema import EnrichSchema
from .explore_schema import ExploreSchema
from .ingest_doc import IngestDoc
from .org_setup import OrgSetupTool
from .remember import Remember
from .run_sql import RunSQL
from .semantic_query import SemanticQuery
from .semantic_federation import SemanticFederationTool

__all__ = [
    "build_default_tools",
    "RunSQL",
    "SemanticQuery",
    "ExploreSchema",
    "EnrichSchema",
    "SemanticFederationTool",
    "OrgSetupTool",
    "Remember",
    "AskUser",
    "IngestDoc",
    "ConfirmIngest",
]


def build_default_tools(
    *,
    memory: MemoryService,
    ingestion: IngestionPipeline,
    source: SourcePort,
    extractor: DocExtractorPort,
    semantic_query: SemanticQuery | None = None,
) -> list[ToolPort]:
    """Build tools for either legacy or governed query mode.

    Once a first-connect catalog exists, ``semantic_query`` replaces
    ``run_sql`` rather than sitting beside it.  Keeping both would let a model
    bypass the reviewed value path with raw SQL.
    """

    query_tool: ToolPort = semantic_query if semantic_query is not None else RunSQL()
    tools: list[ToolPort] = [query_tool]
    if semantic_query is None:
        # Legacy enrichment sends samples to an LLM.  It is intentionally not
        # advertised after PII-safe first-connect onboarding is active.
        tools.extend([ExploreSchema(), EnrichSchema(), OrgSetupTool()])
    tools.extend(
        [
            SemanticFederationTool(),
            AskUser(),
            Remember(memory),
            IngestDoc(ingestion, source, extractor),
            ConfirmIngest(),
        ]
    )
    return tools
