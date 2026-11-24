"""Replay the Oracle view demo through real MCP processes; no LLM or DDL required."""

import argparse
import asyncio
import json
import os
import sqlite3
import sys
from contextlib import asynccontextmanager
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context
from lang2sql.tools.bound_metric import bind_view

TERM = "순매출"
DEFINITION = (
    "확정(POSTED) 주문의 할인·반품 차감액 합계. NULL 할인·반품은 0, 주문일 기준 원화."
)


@asynccontextmanager
async def connected(store, user="colleague", team="sales"):
    params = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "lang2sql.frontends.mcp.server",
            "--store",
            str(store),
            "--org-id",
            "demo",
            "--user-id",
            user,
            "--team-id",
            team,
        ],
        env={
            k: os.environ[k]
            for k in ("LANG2SQL_DB_URL", "LANG2SQL_DB_SCHEMA")
            if k in os.environ
        },
    )
    async with stdio_client(params) as streams, ClientSession(*streams) as client:
        await client.initialize()
        yield client


async def replay(store):
    records = []

    async def call(client, name, args, *, error=False):
        result = await client.call_tool(name, args)
        content = "\n".join(c.text for c in result.content if hasattr(c, "text"))
        records.append(
            {
                "tool": name,
                "arguments": args,
                "is_error": result.isError,
                "content": content,
            }
        )
        if bool(result.isError) != error:
            raise AssertionError(f"Unexpected status for {name}: {content}")
        return content

    async with connected(store) as client:
        objects = await call(client, "explore_schema", {})
        assert "v_net_sales" in objects.lower() and "[view]" in objects
        columns = await call(
            client, "explore_schema", {"table": "L2S_VIEW_OWNER.V_NET_SALES"}
        )
        assert "net_amount" in columns.lower()
        await call(client, "resolve_business_terms", {"terms": [TERM]}, error=True)
        await call(
            client,
            "run_sql",
            {"sql": "SELECT 1 FROM dual", "definition_check_id": "invented"},
            error=True,
        )
    with sqlite3.connect(store) as conn:
        assert (
            conn.execute(
                "SELECT count(*) FROM audit WHERE action IN ('run_sql','oracle_metric_query')"
            ).fetchone()[0]
            == 0
        )

    # Operator confirms the binding out of band; the agent cannot approve a view.
    ctx = build_tool_context(
        Identity("registrar", "demo", "sales"), store_path=str(store)
    )
    try:
        result = await ctx.tools.dispatch(
            "term_custom",
            {"term": TERM, "definition": DEFINITION, "kind": "metric", "layer": "team"},
            ctx,
            "register",
        )
        assert not result.is_error
        contract = json.loads(Path(__file__).with_name("contract.json").read_text())
        await bind_view(ctx, TERM, "team", DEFINITION, contract)
    finally:
        ctx.store.close()

    for month, end, expected in [
        (9, 10, 25),
        (10, 11, 118),
        (11, 12, 10),
        (12, 1, None),
    ]:
        async with connected(store) as client:
            receipt = json.loads(
                await call(client, "resolve_business_terms", {"terms": [TERM]})
            )
            args = {
                "definition_check_id": receipt["definition_check_id"],
                "period_start": f"2025-{month:02d}-01",
                "period_end": f"{2026 if end == 1 else 2025}-{end:02d}-01",
            }
            evidence = json.loads(await call(client, "run_sql", args))
            assert list(evidence["rows"][0].values()) == [expected]
            assert evidence["object"] == "L2S_VIEW_OWNER.V_NET_SALES"
            assert (
                evidence["entity"] == "sales" and evidence["definition"] == DEFINITION
            )
            await call(client, "run_sql", args, error=True)
    async with connected(store) as client:
        receipt = json.loads(
            await call(client, "resolve_business_terms", {"terms": [TERM]})
        )
        await call(
            client,
            "run_sql",
            {
                "definition_check_id": receipt["definition_check_id"],
                "sql": "SELECT SUM(gross) FROM L2S_VIEW_OWNER.ORDERS",
            },
            error=True,
        )
    async with connected(store, user="finance-colleague", team="finance") as client:
        await call(client, "resolve_business_terms", {"terms": [TERM]}, error=True)
    with sqlite3.connect(store) as conn:
        counts = dict(
            conn.execute("SELECT action,count(*) FROM audit GROUP BY action").fetchall()
        )
        assert counts.get("oracle_metric_query") == 4 and counts.get("run_sql", 0) == 0
    return {
        "passed": True,
        "expected_monthly_values": [25, 118, 10, None],
        "audit_counts": counts,
        "records": records,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--store",
        type=Path,
        required=True,
        help="New metadata DB; existing files are refused",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.store.exists() or args.output.exists():
        parser.error(
            "Choose new store/output paths; existing evidence is never overwritten"
        )
    if not os.environ.get("LANG2SQL_DB_URL"):
        parser.error("Set LANG2SQL_DB_URL to the disposable Oracle fixture reader")
    report = asyncio.run(replay(args.store))
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(
        f"PASS: four monthly results, view discovery, team scope, receipt and raw-SQL guards. Evidence: {args.output}"
    )


if __name__ == "__main__":
    main()
