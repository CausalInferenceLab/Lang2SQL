"""Find candidate tables for a question, then inspect them with explore_schema."""

from ..adapters.storage.qdrant_schema import QdrantSchema
from ..core.types import ToolResult, ToolSpec


class SearchSchema:
    spec = ToolSpec(
        name="search_schema",
        description="When the relevant tables are unknown, find candidate tables by question. Verify columns with explore_schema before writing SQL.",
        parameters={
            "type": "object",
            "properties": {"query": {"type": "string", "minLength": 1}},
            "required": ["query"],
        },
    )

    def __init__(self, index: QdrantSchema):
        self.index = index

    async def run(self, args, ctx):
        query = args["query"].strip()
        if not query:
            raise ValueError("query must not be blank")
        hits = await self.index.search(ctx.identity.kv_scope, query)
        return ToolResult(
            call_id="",
            content="\n\n".join(p["description"] for p in hits) or "(no tables)",
        )
