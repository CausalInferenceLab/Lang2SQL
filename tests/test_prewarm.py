"""β — ContextConcierge 선제 시멘틱 보강 (build_context 첫 호출 시 1회).

새 포트/모듈 0. 기존 ScopeResolverPort.define()을 통해서만 시멘틱 레이어를
채우므로 system_prompt 경로가 그대로 사용. 이 테스트는 그 흐름이 정확히
일어나는지 확인.
"""

from __future__ import annotations

import asyncio
import json
from typing import Sequence

from lang2sql.core.identity import Identity, Scope, ScopeLevel
from lang2sql.core.ports.explorer import Column, Table
from lang2sql.core.types import Completion, Message, ToolSpec
from lang2sql.tenancy.concierge import ContextConcierge


class _StubExplorer:
    """3 테이블을 가진 stub explorer (선제 보강 입력용)."""

    def __init__(self) -> None:
        self.tables = [
            Table(name="ord_tx", schema="main", columns=[
                Column("tx_id", "INTEGER"), Column("amt", "DECIMAL"), Column("st", "VARCHAR"),
            ]),
            Table(name="usr", schema="main", columns=[
                Column("u_id", "INTEGER"), Column("e_addr", "VARCHAR"),
            ]),
        ]

    async def list_tables(self) -> list[Table]:
        return self.tables

    async def describe_table(self, name: str) -> Table:
        return next(t for t in self.tables if t.name == name)

    async def sample_rows(self, name, limit=5): return []
    async def execute(self, sql, limit=1000): return []


class _ScriptedLLM:
    def __init__(self, payload: str) -> None:
        self.payload, self.calls = payload, 0
    async def complete(self, messages: Sequence[Message], tools: Sequence[ToolSpec] = ()) -> Completion:
        self.calls += 1
        return Completion(content=self.payload)


def _prewarm_response() -> str:
    return json.dumps({
        "ord_tx.tx_id": "Order transaction id (primary key).",
        "ord_tx.amt": "Order total in the store's base currency.",
        "ord_tx.st": "Order status code (paid/cancelled and variants).",
        "usr.u_id": "User id (primary key).",
        "usr.e_addr": "User email address.",
    })


def test_prewarm_writes_semantic_entries_into_guild_scope():
    llm = _ScriptedLLM(_prewarm_response())
    concierge = ContextConcierge(llm=llm, explorer=_StubExplorer())
    ident = Identity(user_id="alice", guild_id="g1", channel_id="c-mkt")

    asyncio.run(concierge.build_context(ident))

    guild_scope = Scope(ScopeLevel.GUILD, "g1")
    entries = asyncio.run(concierge.scope_resolver.entries_at(guild_scope))
    names = {e.name for e in entries}
    assert {"tx_id", "amt", "st", "u_id", "e_addr"}.issubset(names)
    assert llm.calls == 1


def test_prewarm_skips_when_existing_entries():
    """사람이 이미 박은 정의가 있으면 선제 보강이 덮어쓰지 않음."""
    llm = _ScriptedLLM(_prewarm_response())
    concierge = ContextConcierge(llm=llm, explorer=_StubExplorer())
    ident = Identity(user_id="alice", guild_id="g2", channel_id="c")
    from lang2sql.semantic.types import SemanticEntry, SemanticKind
    asyncio.run(concierge.scope_resolver.define(
        Scope(ScopeLevel.GUILD, "g2"),
        SemanticEntry(SemanticKind.METRIC, "revenue", "SUM(amt) of paid orders")
    ))

    asyncio.run(concierge.build_context(ident))
    assert llm.calls == 0  # 사람 정의가 있으면 LLM 호출 0


def test_prewarm_runs_only_once_per_scope():
    llm = _ScriptedLLM(_prewarm_response())
    concierge = ContextConcierge(llm=llm, explorer=_StubExplorer())
    ident = Identity(user_id="alice", guild_id="g3", channel_id="c")

    asyncio.run(concierge.build_context(ident))
    asyncio.run(concierge.build_context(ident))
    asyncio.run(concierge.build_context(ident))
    assert llm.calls == 1


def test_prewarm_failsoft_on_bad_json():
    llm = _ScriptedLLM("this is not json")
    concierge = ContextConcierge(llm=llm, explorer=_StubExplorer())
    ident = Identity(user_id="alice", guild_id="g4", channel_id="c")
    # 크래시 없이 통과해야 함
    asyncio.run(concierge.build_context(ident))
    entries = asyncio.run(concierge.scope_resolver.entries_at(Scope(ScopeLevel.GUILD, "g4")))
    assert entries == []


def test_prewarm_can_be_disabled():
    llm = _ScriptedLLM(_prewarm_response())
    concierge = ContextConcierge(llm=llm, explorer=_StubExplorer())
    concierge.prewarm_enabled = False
    ident = Identity(user_id="alice", guild_id="g5", channel_id="c")
    asyncio.run(concierge.build_context(ident))
    assert llm.calls == 0


def test_prewarm_entries_surface_in_system_prompt():
    """end-to-end: 선제 보강 → build_system_prompt 가 그 정의를 출력해야 함."""
    from lang2sql.harness.system_prompt import build_system_prompt

    llm = _ScriptedLLM(_prewarm_response())
    concierge = ContextConcierge(llm=llm, explorer=_StubExplorer())
    ident = Identity(user_id="alice", guild_id="g6", channel_id="c")
    ctx = asyncio.run(concierge.build_context(ident))

    prompt = asyncio.run(build_system_prompt(ctx))
    # 선제 보강된 컬럼 설명이 시스템 프롬프트에 포함되어야 함
    assert "Semantic layer" in prompt
    assert "amt" in prompt  # 보강된 dimension 이름
    assert "currency" in prompt  # 설명 본문 일부
