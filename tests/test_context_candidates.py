import json

import pytest

from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context
from lang2sql.tools.context_candidates import decide_candidate
from lang2sql.tools.resolve_business_terms import ResolveBusinessTerms


@pytest.mark.asyncio
async def test_candidate_requires_operator_and_preserves_evidence(tmp_path):
    ctx = build_tool_context(
        Identity("u", "o", "t"), store_path=str(tmp_path / "m.db"), explorer=object()
    )
    result = await ctx.tools.dispatch(
        "context_candidates",
        {
            "term": "순매출",
            "definition": "반품 제외",
            "evidence": "우리 팀 순매출은 반품 제외야",
        },
        ctx,
        "a",
    )
    candidate = json.loads(result.content)
    resolver = ResolveBusinessTerms()
    assert resolver.definitions(["순매출"], ctx) is None
    decided = await decide_candidate(ctx, candidate["id"], approve=True)
    assert decided["evidence"] == "우리 팀 순매출은 반품 제외야"
    assert resolver.definitions(["순매출"], ctx)
    with pytest.raises(ValueError, match="already decided"):
        await decide_candidate(ctx, candidate["id"], approve=True)
    ctx.store.close()


@pytest.mark.asyncio
async def test_reject_and_scope_isolation(tmp_path):
    path = str(tmp_path / "m.db")
    ctx = build_tool_context(
        Identity("u", "o", "t"), store_path=path, explorer=object()
    )
    result = await ctx.tools.dispatch(
        "context_candidates",
        {"term": "x", "definition": "y", "evidence": "z"},
        ctx,
        "a",
    )
    candidate = json.loads(result.content)
    other = build_tool_context(
        Identity("u", "other", "t"), store_path=path, explorer=object()
    )
    with pytest.raises(ValueError, match="not found"):
        await decide_candidate(other, candidate["id"], approve=True)
    await decide_candidate(ctx, candidate["id"], approve=False)
    assert ResolveBusinessTerms().definitions(["x"], ctx) is None
    ctx.store.close()
    other.store.close()


@pytest.mark.asyncio
async def test_invalid_candidate_and_failed_org_approval(tmp_path):
    ctx = build_tool_context(
        Identity("u", "o", "t"), store_path=str(tmp_path / "m.db"), explorer=object()
    )
    invalid = await ctx.tools.dispatch(
        "context_candidates", {"term": "x"}, ctx, "invalid"
    )
    assert invalid.is_error
    proposed = await ctx.tools.dispatch(
        "context_candidates",
        {"term": "x", "definition": "y", "evidence": "z", "layer": "org"},
        ctx,
        "propose",
    )
    with pytest.raises(ValueError):
        await decide_candidate(ctx, json.loads(proposed.content)["id"], approve=True)
    listed = await ctx.tools.dispatch("context_candidates", {}, ctx, "list")
    assert json.loads(listed.content)[0]["status"] == "failed"
    assert ResolveBusinessTerms().definitions(["x"], ctx) is None
    ctx.store.close()
