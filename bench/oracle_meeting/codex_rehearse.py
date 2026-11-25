"""Actual Codex extraction/reuse and team queries against the meeting fixture."""

import argparse
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "oracle_view"))
from codex_demo import READ_TOOLS, mcp_settings
from long_context import run_turn

from lang2sql.core.identity import Identity
from lang2sql.tenancy.tool_runtime import build_tool_context
from lang2sql.tools.context_candidates import decide_candidate


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rehearsal", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--metadata-url-env", default="LANG2SQL_METADATA_URL")
    parser.add_argument("--model")
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new output directory")
    url = os.environ[args.metadata_url_env]
    scope = json.loads(args.rehearsal.read_text())["scope"]
    args.output.mkdir(parents=True)
    summary = []

    def ask(label, team, user, question, tools=READ_TOOLS):
        settings = mcp_settings(
            [
                "--metadata-url-env",
                args.metadata_url_env,
                "--org-id",
                scope,
                "--team-id",
                team,
                "--user-id",
                user,
            ],
            tools=tools,
            extra_env=[args.metadata_url_env],
        )
        with tempfile.TemporaryDirectory(prefix="l2s-meeting-codex-") as cwd:
            (Path(cwd) / "AGENTS.md").write_text(
                (Path(__file__).parent / "codex" / "AGENTS.md").read_text()
            )
            _, report = run_turn(
                settings,
                cwd,
                "lang2sql MCP만 사용하고 셸은 사용하지 마. " + question,
                None,
                args.timeout,
                args.model,
            )
        (args.output / f"{label}.json").write_text(
            json.dumps(report, ensure_ascii=False, indent=2)
        )
        return report

    definition = "확정 주문의 할인·반품 차감액 합계"
    term = "데모확정매출"
    proposal = ask(
        "candidate",
        "sales",
        "speaker",
        f'우리 팀 {term}의 정의는 "{definition}"야. 앞으로 이 뜻으로 이야기할게.',
        tools=["context_candidates"],
    )
    ctx = build_tool_context(Identity("speaker", scope, "sales"), metadata_url=url)

    async def approve_fixture():
        result = await ctx.tools.dispatch("context_candidates", {}, ctx, "inspect")
        candidates = [
            c
            for c in json.loads(result.content)
            if c["term"] == term
            and c["definition"] == definition
            and c["status"] == "pending"
            and definition in c["evidence"]
        ]
        if not candidates:
            return False
        await decide_candidate(ctx, candidates[-1]["id"], approve=True)
        return True

    try:
        approved = asyncio.run(approve_fixture())
    finally:
        ctx.store.close()
    summary.append(
        {
            "case": "candidate",
            "passed": approved
            and proposal["exit_code"] == 0
            and not proposal["shell_used"],
        }
    )
    reuse = ask(
        "reuse",
        "sales",
        "speaker",
        f"저장된 {term}의 정확한 정의와 적용 범위를 확인해줘.",
        tools=["resolve_business_terms"],
    )
    definition_found = False
    for event in reuse["events"]:
        item = event.get("item", {})
        if item.get("tool") == "resolve_business_terms":
            for block in (item.get("result") or {}).get("content", []):
                try:
                    payload = json.loads(block.get("text", ""))
                except json.JSONDecodeError:
                    continue
                if isinstance(payload, dict):
                    definition_found = any(
                        d.get("definition") == definition and d.get("entity") == "sales"
                        for d in payload.get("definitions", [])
                    )
    summary.append(
        {
            "case": "fresh_session_reuse",
            "passed": definition_found
            and reuse["exit_code"] == 0
            and not reuse["shell_used"],
        }
    )
    for team, expected in [("marketing", 3), ("finance", 2), ("unknown", None)]:
        report = ask(
            team,
            team,
            "colleague",
            "2025년 10월 활성고객은 몇 명이야? 저장된 팀 기준을 확인하고 조회해줘. 정의가 없다면 추측하지 말고 질문해줘.",
        )
        if expected is None:
            passed = not report["evidence"] and "DEFINITION_REQUIRED" in json.dumps(
                report["events"]
            )
        else:
            passed = (
                bool(report["evidence"])
                and next(iter(report["evidence"][-1]["rows"][0].values())) == expected
            )
        summary.append(
            {
                "case": team,
                "passed": passed
                and report["exit_code"] == 0
                and not report["shell_used"],
            }
        )
        print(json.dumps(summary[-1]), flush=True)
    (args.output / "summary.json").write_text(
        json.dumps(
            {"cases": summary, "requested_model": args.model},
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0 if all(c["passed"] for c in summary) else 1


if __name__ == "__main__":
    raise SystemExit(main())
