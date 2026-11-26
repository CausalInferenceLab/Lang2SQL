import json
from pathlib import Path

import pytest

from lang2sql.adapters.db.oracle_view import metric_query, view_contract
from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context
from lang2sql.tools.bound_metric import bind_view
from lang2sql.tools.resolve_business_terms import (
    DefinitionCheckedSQL,
    ResolveBusinessTerms,
)

CONTRACT = json.loads(
    (Path(__file__).parents[1] / "bench/oracle_view/contract.json").read_text()
)


class MetricExplorer:
    def __init__(self):
        self.stamp = "original"
        self.reads = 0

    async def oracle_metric(self, contract, *, stamp=None, sql=None, params=None):
        if stamp is None:
            return self.stamp
        if stamp != self.stamp:
            raise ValueError("Oracle view changed")
        self.reads += 1
        return [{"metric_value": 118}]

    async def execute(self, *args):
        pytest.fail("Raw SQL called for a bound metric")


@pytest.mark.asyncio
async def test_scoped_view_lifecycle(tmp_path):
    db = MetricExplorer()
    path = str(tmp_path / "terms.sqlite")
    creator = build_tool_context(
        Identity("alice", "org", "sales"), explorer=db, store_path=path
    )
    await creator.tools.dispatch(
        "term_custom", {"term": "net", "definition": "posted orders"}, creator, "1"
    )
    await bind_view(creator, "net", "team", "posted orders", CONTRACT)
    edit = await creator.tools.dispatch(
        "term_custom", {"term": "net", "definition": "something else"}, creator, "edit"
    )
    assert edit.is_error
    creator.store.close()

    resolver = ResolveBusinessTerms()
    checked = DefinitionCheckedSQL(resolver)
    ctx = build_tool_context(
        Identity("bob", "org", "sales"), explorer=db, store_path=path
    )

    async def query(**kwargs):
        resolved = await resolver.run({"terms": ["net"]}, ctx)
        token = json.loads(resolved.content)["definition_check_id"]
        return await checked.run({"definition_check_id": token, **kwargs}, ctx)

    period = {"period_start": "2025-10-01", "period_end": "2025-11-01"}
    try:
        result = await query(**period)
        evidence = json.loads(result.content)
        assert evidence["rows"] == [{"metric_value": 118}]
        assert evidence["layer"] == "team" and evidence["entity"] == "sales"
        assert "L2S_VIEW_OWNER.V_NET_SALES" in evidence["sql"]
        assert evidence["params"]["filter_0"] == "POSTED"
        assert (await query(sql="SELECT 0 FROM dual")).is_error
        assert (
            await query(period_start="2025-11-01", period_end="2025-10-01")
        ).is_error
        unsupported = await query(**period, expected_columns=["metric_value"])
        assert unsupported.is_error and "SQL mode" in unsupported.content
        db.stamp = "replaced"
        assert (await query(**period)).is_error
        assert db.reads == 1
        assert any(
            e.action == "oracle_metric_blocked" for e in await ctx.store.query("bob")
        )
        ctx.identity = Identity("bob", "org", "finance")
        assert (await resolver.run({"terms": ["net"]}, ctx)).is_error
    finally:
        ctx.store.close()


@pytest.mark.parametrize(
    "change",
    [
        {"amount": "broken.column"},
        {"aggregation": "AVG"},
        {"period": "2025/10"},
        {"grain": ""},
        {"filters": {"STATUS": None}},
        {"function": "DAILY_METRIC"},
    ],
)
def test_contract_needs_explicit_supported_fields(change):
    with pytest.raises(ValueError):
        view_contract({**CONTRACT, **change})


def test_filter_values_are_bound_parameters():
    sql, params = metric_query(
        {**CONTRACT, "filters": {"status": "customer's order"}},
        "2025-10-01",
        "2025-11-01",
    )
    assert "customer's order" not in sql and params["filter_0"] == "customer's order"
