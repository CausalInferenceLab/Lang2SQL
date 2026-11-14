"""Default frontend and auxiliary inference migration, with the actual adapter seam."""

import asyncio
import json

import pytest

from lang2sql.adapters.llm.codex import CodexLLM
from lang2sql.adapters.llm import codex
from lang2sql.core.identity import Identity
from lang2sql.core.ports.ingestion import Document
from lang2sql.core.types import Message, Role, ToolSpec
from lang2sql.frontends.cli.app import _run
from lang2sql.frontends.discord.commands import CommandHandlers
from lang2sql.tenancy.concierge import ContextConcierge


def test_default_is_codex_even_with_legacy_provider_env(monkeypatch):
    monkeypatch.setenv("OPENAI_API_KEY", "unused")
    monkeypatch.setenv("LANG2SQL_LLM_BASE_URL", "http://unused")
    assert isinstance(ContextConcierge()._llm, CodexLLM)


@pytest.mark.parametrize("frontend", ["cli", "discord"])
def test_existing_frontends_reach_codex_and_dispatch_real_tools(monkeypatch, frontend):
    calls = []

    async def execute(prompt, **kwargs):
        calls.append(prompt)
        response = (
            {
                "content": "",
                "tool_calls": [
                    {
                        "name": "run_sql",
                        "arguments_json": '{"sql":"SELECT * FROM orders"}',
                    }
                ],
            }
            if len(calls) == 1
            else {"content": "Codex 결과", "tool_calls": []}
        )
        return {"answer": json.dumps(response)}

    monkeypatch.setattr(codex, "execute", execute)

    async def check():
        if frontend == "cli":
            return await _run("주문 조회")
        return (
            await CommandHandlers(ContextConcierge()).query(
                Identity(user_id="u", guild_id="g", channel_id="c"), "주문 조회"
            )
        ).text

    assert "Codex 결과" in asyncio.run(check())
    assert len(calls) == 2 and '"role": "tool"' in calls[1]
    assert "row(s)" in calls[1]


def test_document_extraction_uses_codex_content_json(monkeypatch):
    async def execute(prompt, **kwargs):
        assert "매출은 주문 총액" in prompt
        return {
            "answer": json.dumps(
                {
                    "content": json.dumps(
                        [
                            {
                                "kind": "metric",
                                "name": "매출",
                                "definition": "주문 총액",
                                "applies_to": "orders",
                            }
                        ]
                    ),
                    "tool_calls": [],
                }
            )
        }

    monkeypatch.setattr(codex, "execute", execute)
    concierge = ContextConcierge()
    rows = asyncio.run(
        concierge._extractor.extract(Document("metrics", "매출은 주문 총액"))
    )
    assert rows[0].name == "매출"


@pytest.mark.parametrize(
    "tool,args,content",
    [
        (
            "enrich_schema",
            {},
            {"columns": {"orders.id": "주문 식별자"}, "relationships": []},
        ),
        (
            "org_setup",
            {"team": "영업"},
            {"domain": "sales", "terms": [{"term": "매출", "definition": "주문 총액"}]},
        ),
    ],
)
def test_auxiliary_tools_share_codex(monkeypatch, tool, args, content):
    prompts = []

    async def execute(prompt, **kwargs):
        prompts.append(prompt)
        return {
            "answer": json.dumps({"content": json.dumps(content), "tool_calls": []})
        }

    monkeypatch.setattr(codex, "execute", execute)

    async def check():
        ctx = await ContextConcierge().build_context(
            Identity(user_id="u", guild_id="g", channel_id="c", is_admin=True)
        )
        result = await ctx.tools.dispatch(tool, args, ctx, "test")
        assert prompts, result.content

    asyncio.run(check())


@pytest.mark.parametrize(
    "raw",
    [
        "not json",
        {"content": "", "tool_calls": []},
        {"content": "", "tool_calls": [{"name": "unknown", "arguments_json": "{}"}]},
        {"content": "", "tool_calls": [{"name": "run_sql", "arguments_json": "[]"}]},
    ],
)
def test_invalid_completion_never_dispatches(monkeypatch, raw):
    async def execute(*args, **kwargs):
        return {"answer": raw if isinstance(raw, str) else json.dumps(raw)}

    monkeypatch.setattr(codex, "execute", execute)
    with pytest.raises(RuntimeError, match="invalid Lang2SQL completion"):
        asyncio.run(
            CodexLLM(env={}).complete(
                [Message(Role.USER, "q")], [ToolSpec("run_sql", "query")]
            )
        )


def test_codex_failure_propagates_without_provider_fallback(monkeypatch):
    async def execute(*args, **kwargs):
        raise RuntimeError("Codex authentication failed")

    monkeypatch.setattr(codex, "execute", execute)
    with pytest.raises(RuntimeError, match="authentication failed"):
        asyncio.run(_run("orders"))
