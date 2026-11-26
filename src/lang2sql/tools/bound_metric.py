"""Confirm and execute a scoped business metric backed by one Oracle view."""

import json
import logging
from datetime import datetime, timezone

from sqlalchemy.exc import SQLAlchemyError

from ..adapters.db.oracle_view import metric_query, view_contract
from ..core.ports.audit import AuditEvent
from ..core.ports.safety import SafetyContext, Verdict
from ..core.types import ToolResult
from .semantic_federation import FedEntry, _kv_key, _validate_layer

logger = logging.getLogger(__name__)


async def audit(ctx, action, detail):
    if ctx.audit:
        await ctx.audit.record(
            AuditEvent(
                actor=ctx.identity.user_id,
                action=action,
                scope=ctx.identity.kv_scope,
                detail=detail,
            )
        )


async def bind_view(ctx, term, layer, definition, raw):
    """Operator-only CLI action; the same querying DB account must verify the view."""
    try:
        if ctx.store is None or not hasattr(ctx.explorer, "oracle_metric"):
            raise ValueError("Configure the metadata store and Oracle connection")
        channel = ctx.identity.effective_channel_id
        layer, error = _validate_layer(layer, channel, ctx.identity.is_admin)
        if error:
            raise ValueError(error)
        entity = {"user": ctx.identity.user_id, "team": channel, "org": ""}[layer]
        key = _kv_key(term, layer, entity)
        existing = ctx.store.kv_get(ctx.identity.kv_scope, key)
        if not existing:
            raise ValueError(
                "Confirm and register the text definition in this exact layer first"
            )
        entry = FedEntry.from_json(existing)
        if entry.inferred or entry.definition != definition or not definition.strip():
            raise ValueError(
                "The supplied definition must match the confirmed definition exactly"
            )
        contract = view_contract(raw)
        stamp = await ctx.explorer.oracle_metric(contract)
        entry.oracle_view = {
            "contract": contract,
            "stamp": stamp,
            "confirmed_by": ctx.identity.user_id,
            "confirmed_at": datetime.now(timezone.utc).isoformat(),
        }
        if not ctx.store.kv_compare_replace(
            ctx.identity.kv_scope, key, existing, entry.to_json()
        ):
            raise ValueError("Definition changed during validation; confirm again")
    except (ValueError, SQLAlchemyError) as exc:
        await audit(
            ctx,
            "oracle_binding_rejected",
            {"term": term, "layer": layer, "error": type(exc).__name__},
        )
        raise
    await audit(
        ctx, "oracle_binding_saved", {"term": term, "layer": layer, **entry.oracle_view}
    )
    logger.info(
        "oracle_binding_saved term=%s layer=%s actor=%s",
        term,
        layer,
        ctx.identity.user_id,
    )
    return entry.oracle_view


async def query_metric(ctx, entry, args):
    binding = entry.oracle_view
    detail = {
        "term": entry.term,
        "layer": entry.layer,
        "entity": entry.entity,
        "definition": entry.definition,
    }
    try:
        detail["object"] = (
            binding["contract"]["owner"] + "." + binding["contract"]["view"]
        )
        if not isinstance(binding.get("stamp"), str) or not binding["stamp"]:
            raise ValueError("Missing confirmed view fingerprint")
        if (
            args.get("sql")
            or not args.get("period_start")
            or not args.get("period_end")
        ):
            raise ValueError(
                "Supply period_start and exclusive period_end only; free-form SQL is disabled for this metric"
            )
        if "expected_columns" in args or "expected_rows" in args:
            raise ValueError("Output contracts are supported only for SQL mode, not bound metrics")
        sql, params = metric_query(
            binding["contract"], args["period_start"], args["period_end"]
        )
        if not ctx.safety or not hasattr(ctx.explorer, "oracle_metric"):
            raise ValueError("Oracle connection and SQL safety pipeline required")
        decision = ctx.safety.evaluate(
            sql, SafetyContext(row_limit=1, extras={"dialect": "oracle"})
        )
        if decision.verdict not in (Verdict.PASS, Verdict.REWRITE):
            raise ValueError("SQL safety pipeline rejected the metric")
        detail.update(sql=decision.sql, params={k: str(v) for k, v in params.items()})
        rows = await ctx.explorer.oracle_metric(
            binding["contract"], stamp=binding["stamp"], sql=decision.sql, params=params
        )
    except (KeyError, TypeError, ValueError, SQLAlchemyError) as exc:
        reason = str(exc) if isinstance(exc, ValueError) else type(exc).__name__
        logger.warning("metric_blocked term=%s reason=%s", entry.term, reason)
        await audit(ctx, "oracle_metric_blocked", {**detail, "reason": reason})
        return ToolResult(
            call_id="",
            is_error=True,
            content=f"Oracle metric unavailable: {reason}. Reconfirm the view and period; do not substitute raw-table SQL.",
        )
    await audit(ctx, "oracle_metric_query", detail)
    logger.info(
        "oracle_metric_query term=%s scope=%s", entry.term, ctx.identity.session_key()
    )
    return ToolResult(
        call_id="",
        content=json.dumps({**detail, "rows": rows}, ensure_ascii=False, default=str),
    )
