"""Verify Oracle metadata, candidate approval and team-specific view results."""

import argparse
import asyncio
import json
import os
import uuid
from pathlib import Path

from lang2sql.adapters.db.sqlalchemy_explorer import SqlAlchemyExplorer
from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context
from lang2sql.tools.bound_metric import bind_view
from lang2sql.tools.context_candidates import decide_candidate
from lang2sql.tools.resolve_business_terms import (
    DefinitionCheckedSQL,
    ResolveBusinessTerms,
)


async def main(args):
    metadata_url = os.environ["LANG2SQL_METADATA_URL"]
    package_url = os.environ[args.package_url_env] if args.package_url_env else None
    scope = "meeting-" + uuid.uuid4().hex
    report = {"scope": scope, "cases": []}
    for team, view, definition, expected in [
        (
            "marketing",
            "V_MARKETING_ACTIVE",
            "조회 월 로그인 기록이 있는 고객. ACTIVITY_DAY는 최종 로그인일, CUSTOMER_COUNT는 고객당 1이며 고객별 한 행이므로 SUM으로 중복 없이 인원수를 계산",
            3,
        ),
        (
            "finance",
            "V_FINANCE_ACTIVE",
            "2025년 10월 스냅샷에서 결제 중인 고객. SNAPSHOT_DAY는 집계 월, CUSTOMER_COUNT는 고객당 1이며 고객별 한 행이므로 SUM으로 인원수를 계산",
            2,
        ),
        (
            "package",
            "V_PACKAGE_NET",
            "POSTED 주문에서 패키지의 할인·반품 계산을 적용",
            118,
        ),
    ]:
        explorer = (
            SqlAlchemyExplorer(package_url)
            if team == "package" and package_url
            else None
        )
        term = "순매출" if team == "package" else "활성고객"
        ident = Identity("operator", scope, team)
        ctx = build_tool_context(
            ident,
            metadata_url=metadata_url,
            explorer=explorer,
            initialize_metadata=True,
        )
        proposed = await ctx.tools.dispatch(
            "context_candidates",
            {"term": term, "definition": definition, "evidence": definition},
            ctx,
            "propose",
        )
        candidate = json.loads(proposed.content)
        resolver = ResolveBusinessTerms()
        assert (await resolver.run({"terms": [term]}, ctx)).is_error
        await decide_candidate(ctx, candidate["id"], approve=True)
        contract = {
            "owner": "L2S_VIEW_OWNER",
            "view": view,
            "amount": "NET_AMOUNT" if team == "package" else "CUSTOMER_COUNT",
            "period": {
                "package": "BOOKED_ON",
                "marketing": "ACTIVITY_DAY",
                "finance": "SNAPSHOT_DAY",
            }[team],
            "aggregation": "SUM",
            "grain": "one customer" if team != "package" else "one order",
            "filters": {"STATUS": "POSTED"} if team == "package" else {},
        }
        await bind_view(ctx, term, "team", definition, contract)
        await ctx.tools.dispatch(
            "remember", {"text": f"{team} confirmed definition"}, ctx, "remember"
        )
        ctx.store.close()
        ctx = build_tool_context(ident, metadata_url=metadata_url, explorer=explorer)
        notes = await ctx.tools.dispatch(
            "recall_memory", {"query": term}, ctx, "recall"
        )
        assert team + " confirmed definition" in notes.content
        resolver = ResolveBusinessTerms()
        checked = await resolver.run({"terms": [term]}, ctx)
        token = json.loads(checked.content)["definition_check_id"]
        result = await DefinitionCheckedSQL(resolver).run(
            {
                "definition_check_id": token,
                "period_start": "2025-10-01",
                "period_end": "2025-11-01",
            },
            ctx,
        )
        assert not result.is_error, result.content
        payload = json.loads(result.content)
        assert next(iter(payload["rows"][0].values())) == expected, payload
        report["cases"].append(
            {
                "team": team,
                "expected": expected,
                "result": payload,
                "candidate": candidate["id"],
                "restart_verified": True,
            }
        )
        ctx.store.close()
    report["passed"] = True
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(json.dumps({"passed": True, "teams": [c["team"] for c in report["cases"]]}))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--package-url-env",
        help="Optional account with package-body metadata visibility, for the package fixture only",
    )
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new evidence path")
    asyncio.run(main(args))
