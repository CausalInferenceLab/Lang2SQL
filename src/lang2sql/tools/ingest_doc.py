"""ingest_doc — turn an uploaded document into semantic candidates (★③).

Runs the Source × Extractor pipeline and returns the proposed metric/rule
definitions for the user to confirm. V1 does NOT auto-register — confirmation
is a frontend step (Discord buttons in Week 4); this tool surfaces the
candidates so the human stays in the loop (documents are the source of truth).
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from ..core.ports.ingestion import DocExtractorPort, SourcePort
from ..core.types import ToolResult, ToolSpec
from ..ingestion.pipeline import IngestionPipeline

if TYPE_CHECKING:
    from ..harness.context import HarnessContext


class IngestDoc:
    def __init__(
        self,
        pipeline: IngestionPipeline,
        source: SourcePort,
        extractor: DocExtractorPort,
    ) -> None:
        self._pipeline = pipeline
        self._source = source
        self._extractor = extractor

    @property
    def spec(self) -> ToolSpec:
        return ToolSpec(
            name="ingest_doc",
            description=(
                "Read a document and propose metric/dimension/rule definitions "
                "for the user to confirm before they enter the semantic layer."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "ref": {
                        "type": "string",
                        "description": "document path or identifier",
                    },
                    "content": {
                        "type": "string",
                        "description": "inline document text (alternative to ref)",
                    },
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx: "HarnessContext") -> ToolResult:
        ref = (args.get("ref") or "inline").strip()
        content = args.get("content")
        blob = content.encode("utf-8") if isinstance(content, str) else None
        if not content and ref == "inline":
            return ToolResult(
                call_id="",
                content="provide a document 'ref' or inline 'content'",
                is_error=True,
            )

        candidates = await self._pipeline.ingest(
            self._source, self._extractor, ref, blob
        )
        if not candidates:
            return ToolResult(
                call_id="", content="No definitions found in the document."
            )

        lines = ["Proposed definitions (confirm to register):"]
        for c in candidates:
            applies = f" [{c.applies_to}]" if c.applies_to else ""
            lines.append(f"- {c.kind.value.upper()} {c.name}{applies} → {c.definition}")
        return ToolResult(call_id="", content="\n".join(lines))
