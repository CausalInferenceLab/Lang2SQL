"""Week-2 integration: the ContextConcierge wires a working agent.

Exercises the assembled stack (tools + safety + scope resolver + explorer stub
+ sqlite) without network, using the offline FakeLLM. Locks in that the six V1
tools are registered and that run_sql/define_metric behave through the live
HarnessContext.
"""

from __future__ import annotations

import asyncio

from lang2sql.core.identity import Identity
from lang2sql.core.ports.safety import SafetyContext, Verdict
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.run_sql import RunSQL
from lang2sql.tools.semantic_federation import SemanticFederationTool


def _ctx():
    concierge = ContextConcierge()
    ident = Identity(user_id="u1", guild_id="g1", channel_id="c-mkt")
    return ident, asyncio.run(concierge.build_context(ident))


def test_v1_tools_registered():
    _, ctx = _ctx()
    names = {s.name for s in ctx.tools.specs()}
    assert names == {
        "run_sql",
        "explore_schema",
        "enrich_schema",
        "term_custom",
        "org_setup",
        "ask_user",
        "remember",
        "ingest_doc",
        "confirm_ingest",
    }


def test_run_sql_passes_gate_and_returns_rows():
    _, ctx = _ctx()
    res = asyncio.run(RunSQL().run({"sql": "SELECT * FROM orders", "limit": 10}, ctx))
    assert not res.is_error
    assert "row" in res.content.lower()


def test_run_sql_blocks_ddl():
    _, ctx = _ctx()
    res = asyncio.run(RunSQL().run({"sql": "DROP TABLE users"}, ctx))
    assert res.is_error and "BLOCKED" in res.content


def test_run_sql_tolerates_bad_limit():
    _, ctx = _ctx()
    res = asyncio.run(RunSQL().run({"sql": "SELECT 1", "limit": "demo"}, ctx))
    assert not res.is_error  # malformed limit must not crash the tool


def test_term_custom_is_scope_local():
    from lang2sql.tools.semantic_federation import _render_effective

    ident, ctx = _ctx()
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "active_user", "definition": "30d login", "layer": "team"}, ctx
        )
    )
    rendered = _render_effective(
        ctx.store, ident.kv_scope, ident.effective_channel_id, ident.user_id
    )
    assert "active_user" in rendered
    # a different channel does not see this channel-level definition
    other_rendered = _render_effective(ctx.store, ident.kv_scope, "c-fin", "u1")
    assert "active_user" not in other_rendered


def test_term_custom_emits_audit_event():
    concierge = ContextConcierge()
    ident = Identity(user_id="u1", guild_id="g1", channel_id="c-mkt", is_admin=True)
    ctx = asyncio.run(concierge.build_context(ident))
    asyncio.run(
        SemanticFederationTool().run(
            {"term": "revenue", "definition": "gross revenue", "layer": "org"}, ctx
        )
    )
    events = asyncio.run(ctx.audit.query(ident.user_id))
    assert any(
        e.action == "term_custom" and e.detail.get("term") == "revenue" for e in events
    )


def test_safety_pipeline_on_context():
    _, ctx = _ctx()
    assert ctx.safety.evaluate("SELECT 1", SafetyContext()).verdict == Verdict.PASS
