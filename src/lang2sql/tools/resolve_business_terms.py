"""Require explicit, scoped definitions before the external agent can execute SQL."""

from __future__ import annotations

import json
import logging
import secrets
from dataclasses import asdict

from ..core.ports.audit import AuditEvent
from ..core.types import ToolResult, ToolSpec
from .bound_metric import query_metric
from .run_sql import RunSQL
from .semantic_federation import FedEntry, _load_all, _resolve_entry

logger = logging.getLogger(__name__)


class ResolveBusinessTerms:
    """One process-local, single-use receipt; failed checks invalidate prior receipts."""

    def __init__(self):
        self.receipt = None

    @property
    def spec(self):
        return ToolSpec(
            name="resolve_business_terms",
            description=(
                "Before every business SQL query, submit ALL business terms in the user's "
                "question verbatim. Business terms are domain metrics/concepts, not literal dates, "
                "numbers, SQL identifiers or ordinary filters; for example a calendar month is "
                "a filter, but fiscal-period rules require a business definition. "
                "Returns exact current-scope definitions and a single-use "
                "definition_check_id. If missing or unconfirmed, ask the user for the exact "
                "definition and scope; NEVER invent/register a definition just to unblock SQL. "
                "Do not replace unknown terms with familiar synonyms. Schema discovery uses explore_schema."
            ),
            parameters={
                "type": "object",
                "properties": {
                    "terms": {
                        "type": "array",
                        "items": {"type": "string"},
                        "minItems": 1,
                    }
                },
                "required": ["terms"],
            },
        )

    def definitions(self, terms, ctx):
        if ctx.store is None:
            return None
        entries = _load_all(ctx.store, ctx.identity.kv_scope)
        resolved = []
        for term in terms:
            # Exact canonical names only: fuzzy/alias guessing defeats clarification.
            entry = _resolve_entry(
                entries.get(term.strip().lower(), []),
                ctx.identity.effective_channel_id,
                ctx.identity.user_id,
            )
            if entry is None or entry.inferred or not entry.definition.strip():
                return None
            resolved.append(asdict(entry))
        return resolved

    async def reject(self, ctx, reason):
        logger.warning(
            "definition_check_blocked scope=%s reason=%s",
            ctx.identity.session_key(),
            reason,
        )
        if ctx.audit is not None:
            await ctx.audit.record(
                AuditEvent(
                    actor=ctx.identity.user_id,
                    action="definition_check_blocked",
                    scope=ctx.identity.session_key(),
                    detail={"reason": reason},
                )
            )
        return ToolResult(
            call_id="",
            is_error=True,
            content=(
                "DEFINITION_REQUIRED: " + reason + ". SQL was NOT executed. "
                "Ask the user for the exact business definition and intended team/org/user scope. "
                "Do not infer a definition or register a guessed definition. "
                "After explicit user clarification, stage the definition with context_candidates. An operator must approve it before you resolve_business_terms again."
            ),
        )

    async def run(self, args, ctx):
        self.receipt = None
        terms = args.get("terms")
        if (
            not isinstance(terms, list)
            or not terms
            or any(not isinstance(t, str) or not t.strip() for t in terms)
        ):
            return await self.reject(ctx, "Supply all business terms as nonempty names")
        definitions = self.definitions(terms, ctx)
        if definitions is None:
            return await self.reject(
                ctx,
                "At least one requested term has no confirmed definition in this scope",
            )
        token = secrets.token_urlsafe()
        self.receipt = (token, tuple(terms), definitions, ctx.identity.session_key())
        logger.info(
            "definition_check_ready scope=%s terms=%d",
            ctx.identity.session_key(),
            len(terms),
        )
        return ToolResult(
            call_id="",
            content=json.dumps(
                {
                    "definition_check_id": token,
                    "definitions": definitions,
                    "instruction": "For oracle_view the operator has confirmed the contract: call run_sql with this definition_check_id, period_start and exclusive period_end, without sql. An explicit calendar year/month is sufficient to derive these dates. Ask only if the period or business meaning is actually unclear. For text definitions use SQL. Discover schema and use qualified table names before run_sql. A receipt is not proof of SQL semantic correctness.",
                },
                ensure_ascii=False,
            ),
        )


class DefinitionCheckedSQL:
    """Guard only the external MCP path; delegate SQL validation/execution unchanged."""

    def __init__(self, resolver):
        self.resolver = resolver
        self.sql = RunSQL()

    @property
    def spec(self):
        original = self.sql.spec
        params = original.parameters
        return ToolSpec(
            name=original.name,
            description=(
                "Execute SQL only after resolve_business_terms returned exact confirmed definitions. "
                "For an oracle_view definition omit sql and provide period_start and exclusive period_end (YYYY-MM-DD). "
                "Use the returned definition_check_id; never guess a missing definition. "
                + original.description
            ),
            parameters={
                **params,
                "properties": {
                    **{
                        k: v
                        for k, v in params["properties"].items()
                        if k != "metric_term"
                    },
                    "definition_check_id": {"type": "string"},
                },
                "required": ["definition_check_id"],
            },
        )

    async def run(self, args, ctx):
        receipt = self.resolver.receipt
        self.resolver.receipt = None
        if receipt is None or args.get("definition_check_id") != receipt[0]:
            return await self.resolver.reject(
                ctx,
                "Resolve definitions before every SQL execution; missing, invalid or already-used check",
            )
        _, terms, definitions, identity = receipt
        if (
            identity != ctx.identity.session_key()
            or self.resolver.definitions(terms, ctx) != definitions
        ):
            return await self.resolver.reject(
                ctx, "Definitions or identity changed; resolve again"
            )
        if any(d.get("oracle_view") for d in definitions):
            if len(definitions) != 1:
                return await self.resolver.reject(
                    ctx, "Query one bound metric at a time"
                )
            return await query_metric(ctx, FedEntry(**definitions[0]), args)
        if not args.get("sql") or "period_start" in args or "period_end" in args:
            return await self.resolver.reject(ctx, "Text definitions require SQL")
        return await self.sql.run(
            {k: v for k, v in args.items() if k != "definition_check_id"}, ctx
        )
