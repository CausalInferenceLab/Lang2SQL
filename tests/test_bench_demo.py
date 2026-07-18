"""Smoke test: the bench demo runs clean and shows its headline behaviours.

Guards the study-group demo against drift in the modules it exercises
(ContextConcierge, KV-backed federation, SafetyPipeline). Runs the demo's
``main()`` in-process and asserts the federation + safety claims it prints
are real.
"""

from __future__ import annotations

import asyncio
import importlib.util
from pathlib import Path

_DEMO = Path(__file__).resolve().parent.parent / "bench" / "ecommerce_demo.py"


def _load_demo():
    spec = importlib.util.spec_from_file_location("ecommerce_demo", _DEMO)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_demo_runs_clean(capsys):
    demo = _load_demo()
    asyncio.run(demo.main())
    out = capsys.readouterr().out

    # ★④ federation: same term, two definitions, no conflict.
    assert "user with a login event in the last 30 days" in out
    assert "user with an active paid subscription" in out
    assert "zero conflict" in out

    # ★① safety: a mutating statement is blocked, a SELECT passes.
    assert "BLOCK" in out
    assert "PASS" in out


def test_demo_federation_resolves_distinct_definitions():
    """Reach into the demo's KV building blocks directly (no printing)."""
    demo = _load_demo()
    from lang2sql.adapters.storage.sqlite_store import SqliteStore
    from lang2sql.tools.semantic_federation import _render_effective

    store = SqliteStore()
    mkt = demo._marketing_identity()
    fin = demo._finance_identity()

    demo._define_term(
        store, demo.GUILD, "active_user", "channel", demo.CH_MARKETING, "30d login"
    )
    demo._define_term(
        store, demo.GUILD, "active_user", "channel", demo.CH_FINANCE, "paid sub"
    )

    mkt_rendered = _render_effective(store, demo.GUILD, demo.CH_MARKETING, mkt.user_id)
    fin_rendered = _render_effective(store, demo.GUILD, demo.CH_FINANCE, fin.user_id)

    assert "30d login" in mkt_rendered
    assert "paid sub" not in mkt_rendered
    assert "paid sub" in fin_rendered
    assert "30d login" not in fin_rendered
