"""Operator preparation/inspection for the official CO sample; no hidden approval."""

import argparse
import asyncio
import json
import os
import uuid
from pathlib import Path

from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context
from lang2sql.tools.bound_metric import bind_view
from lang2sql.tools.context_candidates import candidate_scope, decide_candidate
from lang2sql.tools.semantic_federation import SemanticFederationTool

SALES_DEFINITION = "주문일 기준으로 COMPLETE 주문의 주문 항목 UNIT_PRICE × QUANTITY 합계. 취소와 환불 주문은 제외하며 샘플에는 통화 정보가 없어 통화 단위는 붙이지 않는다."
FINANCE_DEFINITION = "주문일 기준으로 COMPLETE와 REFUNDED 주문의 주문 항목 UNIT_PRICE × QUANTITY 합계. 환불 전 주문액을 보고 취소 주문만 제외하며 통화 단위는 붙이지 않는다."
COUNT_DEFINITION = (
    "CO.ORDERS의 주문일 기준 행 개수. 상태와 관계없이 주문 한 행을 한 건으로 센다."
)


def contract(term, team):
    return {
        "owner": "CO",
        "view": (
            "L2S_ORDER_COUNT"
            if term == "주문 건수"
            else ("L2S_SALES" if team == "sales" else "L2S_FINANCE_SALES")
        ),
        "amount": "ORDER_COUNT" if term == "주문 건수" else "AMOUNT",
        "period": "ORDER_DAY",
        "aggregation": "SUM",
        "grain": "one row per order",
        "filters": (
            {"ORDER_STATUS": "COMPLETE"} if term == "매출" and team == "sales" else {}
        ),
    }


async def main(args):
    if args.command == "new":
        state = {"org": "co-demo-" + uuid.uuid4().hex, "user": "presenter"}
        with args.state.open("x") as file:
            json.dump(state, file, indent=2)
        print(json.dumps(state))
        return
    state = json.loads(args.state.read_text())
    ctx = build_tool_context(
        Identity(state["user"], state["org"], args.team),
        metadata_url=os.environ["LANG2SQL_METADATA_URL"],
    )
    try:
        if args.command == "prepare":
            # Basic literal count is preconfigured, never claim all knowledge is empty.
            entries = [("주문 건수", COUNT_DEFINITION)]
            if args.team == "finance":
                entries.append(("매출", FINANCE_DEFINITION))
            for term, definition in entries:
                if ctx.store.kv_list_prefix(
                    ctx.identity.kv_scope, f"cterm:{term}:team:{args.team}"
                ):
                    raise ValueError("Use a new scope for preparation")
                result = await SemanticFederationTool().run(
                    {"term": term, "definition": definition, "layer": "team"}, ctx
                )
                if result.is_error:
                    raise ValueError(result.content)
                await bind_view(
                    ctx, term, "team", definition, contract(term, args.team)
                )
            print(
                "Prepared explicit basic count"
                + (
                    " and finance definition"
                    if args.team == "finance"
                    else "; sales 매출 remains undefined"
                )
            )
        elif args.command == "inspect":
            for scope in [
                ctx.identity.kv_scope,
                candidate_scope(ctx.identity),
                "memory:" + json.dumps([state["org"], args.team], ensure_ascii=False),
            ]:
                for key, value in ctx.store.kv_list_prefix(scope, ""):
                    print(
                        json.dumps(
                            {"scope": scope, "key": key, "value": json.loads(value)},
                            ensure_ascii=False,
                            indent=2,
                        )
                    )
        elif args.command == "approve":
            raw = ctx.store.kv_get(
                candidate_scope(ctx.identity), "candidate:" + args.candidate
            )
            if raw is None:
                raise ValueError("Candidate not found")
            candidate = json.loads(raw)
            print(json.dumps(candidate, ensure_ascii=False, indent=2))
            if (
                candidate["term"] != "매출"
                or candidate["layer"] != "team"
                or args.team != "sales"
            ):
                raise ValueError(
                    "This fixture approval supports sales/team/매출 only; use operator CLI for other terms"
                )
            print(
                "Reviewed view: CO.L2S_SALES; SUM(AMOUNT); ORDER_DAY; ORDER_STATUS=COMPLETE"
            )
            if candidate["definition"] != SALES_DEFINITION:
                raise ValueError(
                    "Definition differs from the fixture. Review manually; no automatic semantic equivalence assumed."
                )
            if (
                input("Approve this exact definition and view? Type APPROVE: ")
                != "APPROVE"
            ):
                raise ValueError("Not approved")
            await decide_candidate(ctx, args.candidate, approve=True)
            await bind_view(
                ctx,
                "매출",
                "team",
                candidate["definition"],
                contract("매출", args.team),
            )
            print("Approved and bound; query from Codex now.")
    finally:
        ctx.store.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--team", choices=["sales", "finance"], default="sales")
    subs = parser.add_subparsers(dest="command", required=True)
    for name in ["new", "prepare", "inspect"]:
        subs.add_parser(name)
    approve = subs.add_parser("approve")
    approve.add_argument("candidate")
    asyncio.run(main(parser.parse_args()))
