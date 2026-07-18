"""ingest_doc — turn an uploaded document into semantic candidates (★③).

Runs the Source × Extractor pipeline and returns the proposed metric/rule
definitions for the user to confirm. Candidates are stored in KV under a
``pending_ingest:{ref}`` key so ``confirm_ingest`` can retrieve and register
them once the user approves.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..core.ports.ingestion import DocExtractorPort, SemanticCandidate, SourcePort
from ..core.types import ToolResult, ToolSpec
from ..ingestion.pipeline import IngestionPipeline

if TYPE_CHECKING:
    from ..harness.context import HarnessContext

PENDING_PREFIX = "pending_ingest"


def _candidate_to_dict(c: SemanticCandidate) -> dict:
    return {
        "kind": c.kind.value,
        "name": c.name,
        "definition": c.definition,
        "applies_to": c.applies_to,
        "source_id": c.source_id,
    }


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
                "for the user to confirm before they enter the semantic layer. "
                "Use confirm_ingest to register the approved candidates."
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
        ref = (args.get("ref") or "").strip()
        content = args.get("content")
        blob = content.encode("utf-8") if isinstance(content, str) else None
        if not content and not ref:
            return ToolResult(
                call_id="",
                content="provide a document 'ref' or inline 'content'",
                is_error=True,
            )
        if not ref:
            import hashlib

            ref = "inline:" + hashlib.md5(blob or b"").hexdigest()[:8]

        candidates = await self._pipeline.ingest(
            self._source, self._extractor, ref, blob
        )
        if not candidates:
            return ToolResult(
                call_id="", content="No definitions found in the document."
            )

        if ctx.store is not None:
            pending_key = f"{PENDING_PREFIX}:{ref}"
            ctx.store.kv_set(
                ctx.identity.kv_scope,
                pending_key,
                json.dumps([_candidate_to_dict(c) for c in candidates]),
            )

        lines = [f"Proposed definitions from '{ref}' (use confirm_ingest to register):"]
        for i, c in enumerate(candidates, 1):
            applies = f" [{c.applies_to}]" if c.applies_to else ""
            lines.append(
                f"  {i}. [{c.kind.value.upper()}] {c.name}{applies} — {c.definition}"
            )
        lines.append(
            f"\nRun confirm_ingest(ref='{ref}', accept='all') to register all, "
            "or specify indices like accept='1,3'."
        )
        return ToolResult(call_id="", content="\n".join(lines))
