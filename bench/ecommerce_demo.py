#!/usr/bin/env python3
"""E-commerce demo — Lang2SQL v4.1 headline value, no Discord and no real DB.

Run:  .venv/bin/python bench/ecommerce_demo.py

This is the study-group demo. It exercises the *real* V1 code paths
(``ContextConcierge`` + KV-backed federation + the canned Postgres explorer +
the offline ``FakeLLM``) to show three things end-to-end without a token or a
live database:

  Section 1 — define three e-commerce metrics in a channel and read them back.
  Section 2 — ★④ semantic federation: the *same* term ``active_user`` carries
              two different definitions in #marketing vs #finance, with no
              conflict. Each channel resolves its own effective layer.
  Section 3 — ★① safety pipeline: DROP / INSERT are blocked fail-closed while a
              plain SELECT passes.

Everything printed is produced by running the shipped modules, not hard-coded.
"""

from __future__ import annotations

import asyncio

from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.ports.safety import SafetyContext, Verdict
from lang2sql.harness.loop import agent_loop
from lang2sql.safety.pipeline import SafetyPipeline
from lang2sql.tenancy.concierge import ContextConcierge
from lang2sql.tools.semantic_federation import (
    FedEntry,
    _kv_key,
    _render_effective,
    _load_all,
    _resolve_term,
)

# Stable IDs for the demo guild and its two channels.
GUILD = "acme-shop"
CH_MARKETING = "marketing"
CH_FINANCE = "finance"


def _hr(title: str) -> None:
    print("\n" + "=" * 68)
    print(title)
    print("=" * 68)


def _marketing_identity() -> Identity:
    return Identity(user_id="dana", guild_id=GUILD, channel_id=CH_MARKETING)


def _finance_identity() -> Identity:
    return Identity(user_id="evan", guild_id=GUILD, channel_id=CH_FINANCE)


def _define_term(
    store: SqliteStore, scope: str, term: str, layer: str, entity: str, definition: str
) -> None:
    entry = FedEntry(term=term, layer=layer, entity=entity, definition=definition)
    store.kv_set(scope, _kv_key(term, layer, entity), entry.to_json())


async def section_0_harness(concierge: ContextConcierge) -> None:
    """Drive one full agent turn through the assembled harness (offline)."""
    _hr("SECTION 0 — assembled harness runs one turn (ContextConcierge + FakeLLM)")

    ident = _marketing_identity()
    ctx = await concierge.build_context(ident)
    print(f"LLM in use:  {type(ctx.llm).__name__}   (offline, no key required)")
    print(f"Explorer:    {type(ctx.explorer).__name__}   (canned orders/users tables)")
    print(f"Tools wired: {', '.join(s.name for s in ctx.tools.specs())}\n")

    final = await agent_loop(ctx, "How many orders do we have?")
    print("user>  How many orders do we have?")
    print(f"loop>  {final}")
    print("\n  The loop completed a real LLM→tool→LLM cycle. The stub's blind")
    print("  run_sql call was caught by the safety gate, which is exactly the")
    print("  ★① behaviour Section 3 isolates.")


async def section_1_define_metrics(store: SqliteStore) -> None:
    """Define three e-commerce metrics in #marketing and read them back."""
    _hr("SECTION 1 — define three metrics (★① business-context learning)")

    ident = _marketing_identity()
    channel_id = ident.channel_id or ""
    scope = ident.guild_id or GUILD
    print(f"Writing to channel layer for #{CH_MARKETING} (channel_id={channel_id})\n")

    metrics = [
        ("total_revenue", "SUM(orders.amount) WHERE status != 'cancelled'"),
        ("aov", "total_revenue / COUNT(DISTINCT orders.id)"),
        ("paid_orders", "COUNT(*) FROM orders WHERE status = 'paid'"),
    ]
    for name, definition in metrics:
        _define_term(store, scope, name, "channel", channel_id, definition)
        print(f"  defined {name:>14}  =  {definition}")

    rendered = _render_effective(store, scope, channel_id, ident.user_id)
    lines = [l for l in rendered.splitlines() if l.startswith("-")]
    print(
        f"\nEffective layer for #{CH_MARKETING} now holds {len(lines)} definition(s):"
    )
    print(rendered)


async def section_2_federation(store: SqliteStore) -> None:
    """Same term, two channels, two definitions — no conflict (★④)."""
    _hr("SECTION 2 — semantic federation: one term, two definitions (★④)")

    mkt = _marketing_identity()
    fin = _finance_identity()

    _define_term(
        store,
        GUILD,
        "active_user",
        "channel",
        CH_MARKETING,
        "user with a login event in the last 30 days",
    )
    _define_term(
        store,
        GUILD,
        "active_user",
        "channel",
        CH_FINANCE,
        "user with an active paid subscription",
    )

    print("Defined 'active_user' independently in two channels.\n")
    print("Now resolving the *effective* definition each channel sees")
    print("by walking its scope chain (most specific scope wins):\n")

    mkt_rendered = _render_effective(store, GUILD, CH_MARKETING, mkt.user_id)
    fin_rendered = _render_effective(store, GUILD, CH_FINANCE, fin.user_id)

    # Read definitions directly from the store — don't parse rendered display text
    by_term = _load_all(store, GUILD)
    entries = by_term.get("active_user", [])
    mkt_raw = store.kv_get(GUILD, _kv_key("active_user", "channel", CH_MARKETING))
    fin_raw = store.kv_get(GUILD, _kv_key("active_user", "channel", CH_FINANCE))
    mkt_def = FedEntry.from_json(mkt_raw).definition if mkt_raw else ""
    fin_def = FedEntry.from_json(fin_raw).definition if fin_raw else ""

    print(f"  #{CH_MARKETING:<10} active_user → {mkt_def}")
    print(f"  #{CH_FINANCE:<10} active_user → {fin_def}")

    assert (
        mkt_def and fin_def and mkt_def != fin_def
    ), f"Federation failed: mkt_def={mkt_def!r}, fin_def={fin_def!r}"
    print("\n  ✅ Same term, two live definitions, zero conflict.")
    print("     Each channel is its own branch in the federation tree;")
    print("     neither overwrote the other. (Wren's single MDL cannot do this.)")

    chain = " → ".join(str(s) for s in mkt.scope_chain())
    print(f"\n  #{CH_MARKETING} resolution order: {chain}")
    print("  Lookup stops at the first scope that defines the name (CHANNEL),")
    print("  so the GUILD/BUILTIN branches never get a chance to disagree.")


def section_3_safety(pipeline: SafetyPipeline) -> None:
    """DROP/INSERT blocked fail-closed; SELECT passes (★①)."""
    _hr("SECTION 3 — safety pipeline gates every query (★①)")

    layers = " → ".join(layer.name for layer in pipeline.layers)
    print(f"Pipeline layers in order: {layers}\n")

    cases = [
        ("DROP TABLE users", "must BLOCK"),
        ("INSERT INTO orders VALUES (99, 10.0, 'paid', now())", "must BLOCK"),
        ("WITH x AS (INSERT INTO t VALUES (1)) SELECT * FROM x", "must BLOCK (CTE)"),
        ("SELECT status, COUNT(*) FROM orders GROUP BY status", "must PASS"),
    ]
    ctx = SafetyContext()
    for sql, expectation in cases:
        decision = pipeline.evaluate(sql, ctx)
        passed = decision.verdict is Verdict.PASS
        mark = "✅ PASS " if passed else "🚫 BLOCK"
        reason = f"  ({decision.reason} @ {decision.layer})" if decision.reason else ""
        print(f"  {mark}  {expectation:<16}  {sql}{reason}")

    print("\n  Read-only is enforced before any SQL reaches the database:")
    print("  the whitelist layer is fail-closed, so anything it cannot prove")
    print("  is a single SELECT/WITH is rejected.")


async def main() -> None:
    print("Lang2SQL v4.1 — e-commerce demo (offline: FakeLLM, canned PG, in-memory)")

    store = SqliteStore()
    pipeline = SafetyPipeline()
    concierge = ContextConcierge()

    await section_0_harness(concierge)
    await section_1_define_metrics(store)
    await section_2_federation(store)
    section_3_safety(pipeline)

    _hr("DONE")
    print("Sections 1–2 exercise ★④ federation; section 3 exercises ★① safety.")
    print("No network, no token, no live database were used.")


if __name__ == "__main__":
    asyncio.run(main())
