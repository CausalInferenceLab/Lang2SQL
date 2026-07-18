"""confirm_ingest — pending 후보 등록 및 OkfBundle 연동 테스트."""

from __future__ import annotations

import asyncio
import json
import tempfile
from pathlib import Path
from typing import Sequence

from lang2sql.adapters.storage.sqlite_store import SqliteStore
from lang2sql.core.identity import Identity
from lang2sql.core.ports.ingestion import CandidateKind, SemanticCandidate
from lang2sql.core.types import Completion, Message, ToolSpec
from lang2sql.harness.context import HarnessContext
from lang2sql.harness.session import Session
from lang2sql.harness.tool_registry import ToolRegistry
from lang2sql.tools import build_default_tools
from lang2sql.tools.confirm_ingest import ConfirmIngest, _dict_to_candidate, _select
from lang2sql.tools.ingest_doc import PENDING_PREFIX, IngestDoc, _candidate_to_dict
from lang2sql.tools.semantic_federation import FedEntry, _kv_key

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _FakeLLM:
    def __init__(self, content: str = "[]") -> None:
        self._content = content

    async def complete(
        self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()
    ) -> Completion:
        return Completion(content=self._content, finish_reason="stop")


def _make_ctx(store: SqliteStore, okf_bundle_dir: str | None = None) -> HarnessContext:
    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    from lang2sql.ingestion import FileSource, IngestionPipeline, LLMExtractor
    from lang2sql.memory import (
        InjectAllRecall,
        InMemoryStore,
        ManualExtractor,
        MemoryService,
    )

    memory = MemoryService(InMemoryStore(), InjectAllRecall(), ManualExtractor())
    ingestion = IngestionPipeline()
    source = FileSource()
    extractor = LLMExtractor(_FakeLLM())
    tools = ToolRegistry(
        build_default_tools(
            memory=memory, ingestion=ingestion, source=source, extractor=extractor
        )
    )
    return HarnessContext(
        identity=identity,
        llm=_FakeLLM(),
        tools=tools,
        session=Session(identity=identity),
        store=store,
        okf_bundle_dir=okf_bundle_dir,
    )


def _seed_pending(
    store: SqliteStore, scope: str, ref: str, candidates: list[SemanticCandidate]
) -> None:
    key = f"{PENDING_PREFIX}:{ref}"
    store.kv_set(scope, key, json.dumps([_candidate_to_dict(c) for c in candidates]))


_SAMPLE = [
    SemanticCandidate(
        CandidateKind.METRIC,
        "monthly_revenue",
        "SUM(orders.amount)",
        applies_to="orders",
    ),
    SemanticCandidate(CandidateKind.RULE, "exclude_cancelled", "status != 'cancelled'"),
    SemanticCandidate(CandidateKind.DIMENSION, "customer_tier", "users.tier"),
]


# ---------------------------------------------------------------------------
# 직렬화 단위 테스트
# ---------------------------------------------------------------------------


def test_candidate_roundtrip() -> None:
    for c in _SAMPLE:
        d = _candidate_to_dict(c)
        restored = _dict_to_candidate(d)
        assert restored.kind == c.kind
        assert restored.name == c.name
        assert restored.definition == c.definition


def test_select_all() -> None:
    assert _select(_SAMPLE, "all") == _SAMPLE


def test_select_indices() -> None:
    result = _select(_SAMPLE, "1,3")
    assert result is not None
    assert [c.name for c in result] == ["monthly_revenue", "customer_tier"]


def test_select_out_of_range_returns_none() -> None:
    assert _select(_SAMPLE, "9") is None


def test_select_invalid_string_returns_none() -> None:
    assert _select(_SAMPLE, "foo") is None


# ---------------------------------------------------------------------------
# confirm_ingest 동작 테스트
# ---------------------------------------------------------------------------


def test_confirm_all_saves_fed_entries() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    scope = ctx.identity.kv_scope
    _seed_pending(store, scope, "defs.md", _SAMPLE)

    tool = ConfirmIngest()
    result = asyncio.run(
        tool.run({"ref": "defs.md", "accept": "all", "layer": "guild"}, ctx)
    )

    assert not result.is_error
    assert "3 term(s)" in result.content

    for cand in _SAMPLE:
        raw = store.kv_get(scope, _kv_key(cand.name, "guild", ""))
        assert raw is not None
        entry = FedEntry.from_json(raw)
        assert entry.term == cand.name
        assert entry.kind == cand.kind.value
        assert entry.layer == "guild"


def test_confirm_by_index_saves_selected_only() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    scope = ctx.identity.kv_scope
    _seed_pending(store, scope, "defs.md", _SAMPLE)

    result = asyncio.run(
        ConfirmIngest().run({"ref": "defs.md", "accept": "2", "layer": "guild"}, ctx)
    )

    assert not result.is_error
    assert "1 term(s)" in result.content

    assert store.kv_get(scope, _kv_key("exclude_cancelled", "guild", "")) is not None
    assert store.kv_get(scope, _kv_key("monthly_revenue", "guild", "")) is None


def test_confirm_channel_layer_uses_channel_entity() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    scope = ctx.identity.kv_scope
    _seed_pending(store, scope, "defs.md", [_SAMPLE[0]])

    asyncio.run(
        ConfirmIngest().run(
            {"ref": "defs.md", "accept": "all", "layer": "channel"}, ctx
        )
    )

    ch_id = ctx.identity.effective_channel_id
    raw = store.kv_get(scope, _kv_key("monthly_revenue", "channel", ch_id))
    assert raw is not None
    entry = FedEntry.from_json(raw)
    assert entry.entity == ch_id


def test_confirm_member_layer_uses_user_id() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    scope = ctx.identity.kv_scope
    _seed_pending(store, scope, "defs.md", [_SAMPLE[0]])

    asyncio.run(
        ConfirmIngest().run({"ref": "defs.md", "accept": "all", "layer": "member"}, ctx)
    )

    raw = store.kv_get(
        scope, _kv_key("monthly_revenue", "member", ctx.identity.user_id)
    )
    assert raw is not None


def test_confirm_missing_ref_returns_error() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    result = asyncio.run(ConfirmIngest().run({"ref": "nonexistent.md"}, ctx))
    assert result.is_error
    assert "ingest_doc" in result.content


def test_confirm_no_ref_arg_returns_error() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    result = asyncio.run(ConfirmIngest().run({}, ctx))
    assert result.is_error


# ---------------------------------------------------------------------------
# ingest_doc → confirm_ingest 전체 연동 테스트
# ---------------------------------------------------------------------------


def test_ingest_doc_saves_pending_key() -> None:
    store = SqliteStore()
    ctx = _make_ctx(store)
    scope = ctx.identity.kv_scope

    candidates = [SemanticCandidate(CandidateKind.METRIC, "active_user", "30d login")]
    _seed_pending(store, scope, "test.md", candidates)

    raw = store.kv_get(scope, f"{PENDING_PREFIX}:test.md")
    assert raw is not None
    loaded = json.loads(raw)
    assert loaded[0]["name"] == "active_user"


def test_confirm_with_okf_bundle_exports_files() -> None:
    store = SqliteStore()
    with tempfile.TemporaryDirectory() as bundle_dir:
        ctx = _make_ctx(store, okf_bundle_dir=bundle_dir)
        scope = ctx.identity.kv_scope
        _seed_pending(store, scope, "defs.md", [_SAMPLE[0]])

        result = asyncio.run(
            ConfirmIngest().run(
                {"ref": "defs.md", "accept": "all", "layer": "guild"}, ctx
            )
        )
        assert not result.is_error

        md_files = list(Path(bundle_dir).rglob("*.md"))
        assert len(md_files) >= 1
        assert any("monthly_revenue" in f.name for f in md_files)


def test_registered_tool_name_in_registry() -> None:
    from lang2sql.tenancy.concierge import ContextConcierge

    concierge = ContextConcierge()
    identity = Identity(user_id="u1", guild_id="g1", channel_id="c1")
    ctx = asyncio.run(concierge.build_context(identity))
    names = {s.name for s in ctx.tools.specs()}
    assert "confirm_ingest" in names
    assert "ingest_doc" in names
