"""Stage externally extracted definitions with evidence; operator approval promotes them."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any

from ..core.ports.audit import AuditEvent
from ..core.types import ToolResult, ToolSpec
from .semantic_federation import SemanticFederationTool

logger = logging.getLogger(__name__)


def candidate_scope(identity):
    return "candidates:" + json.dumps(
        [identity.guild_id, identity.channel_id, identity.user_id]
    )


class ContextCandidates:
    @property
    def spec(self):
        return ToolSpec(
            name="context_candidates",
            description="Stage a definition extracted from the user's conversation, or list pending candidates. Include the exact user quote as evidence. Candidates are NOT approved definitions; ask the user to have an operator approve the candidate ID before using it.",
            parameters={
                "type": "object",
                "properties": {
                    "term": {"type": "string"},
                    "definition": {"type": "string"},
                    "evidence": {"type": "string"},
                    "layer": {"type": "string", "enum": ["team", "user", "org"]},
                },
            },
        )

    async def run(self, args: dict[str, Any], ctx):
        if ctx.store is None:
            return ToolResult(call_id="", content="No metadata store", is_error=True)
        scope = candidate_scope(ctx.identity)
        if not args:
            values = [
                json.loads(raw)
                for _, raw in ctx.store.kv_list_prefix(scope, "candidate:")
            ]
            return ToolResult(
                call_id="", content=json.dumps(values, ensure_ascii=False)
            )
        if any(
            not isinstance(args.get(k), str) or not args[k].strip()
            for k in ("term", "definition", "evidence")
        ):
            return ToolResult(
                call_id="",
                content="term, definition and exact user evidence are required",
                is_error=True,
            )
        candidate = {k: args[k].strip() for k in ("term", "definition", "evidence")}
        candidate.update(
            id=uuid.uuid4().hex, layer=args.get("layer", "team"), status="pending"
        )
        ctx.store.kv_set(
            scope,
            "candidate:" + candidate["id"],
            json.dumps(candidate, ensure_ascii=False),
        )
        if ctx.audit:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="context_candidate_proposed",
                    scope=scope,
                    detail={"candidate_id": candidate["id"]},
                )
            )
        logger.info(
            "context_candidate_proposed candidate_id=%s actor=%s",
            candidate["id"],
            ctx.identity.user_id,
        )
        return ToolResult(call_id="", content=json.dumps(candidate, ensure_ascii=False))


async def decide_candidate(ctx, candidate_id, *, approve):
    """Operator CLI boundary: the external model cannot invoke this action via MCP."""
    scope = candidate_scope(ctx.identity)
    key = "candidate:" + candidate_id
    raw = ctx.store.kv_get(scope, key)
    if raw is None:
        raise ValueError("Candidate not found in this organization/team/user scope")
    candidate = json.loads(raw)
    if candidate["status"] != "pending":
        raise ValueError("Candidate already decided")
    candidate["status"] = "processing" if approve else "rejected"
    claimed = json.dumps(candidate, ensure_ascii=False)
    if not ctx.store.kv_compare_replace(scope, key, raw, claimed):
        raise RuntimeError("Candidate changed concurrently; inspect before retrying")
    if approve:
        result = await SemanticFederationTool().run(
            {k: candidate[k] for k in ("term", "definition", "layer")}, ctx
        )
        if result.is_error:
            candidate["status"] = "failed"
            ctx.store.kv_compare_replace(
                scope, key, claimed, json.dumps(candidate, ensure_ascii=False)
            )
            logger.warning(
                "context_candidate_failed candidate_id=%s actor=%s",
                candidate_id,
                ctx.identity.user_id,
            )
            raise ValueError(result.content)
    candidate["status"] = "approved" if approve else "rejected"
    if not ctx.store.kv_compare_replace(
        scope, key, claimed, json.dumps(candidate, ensure_ascii=False)
    ):
        raise RuntimeError("Candidate changed concurrently; inspect before retrying")
    if ctx.audit:
        await ctx.audit.record(
            AuditEvent(
                actor=ctx.identity.user_id,
                action="context_candidate_" + candidate["status"],
                scope=scope,
                detail={"candidate_id": candidate_id},
            )
        )
    logger.info(
        "context_candidate_decided candidate_id=%s status=%s actor=%s",
        candidate_id,
        candidate["status"],
        ctx.identity.user_id,
    )
    return candidate
