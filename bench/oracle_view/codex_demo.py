"""Verify a natural-language Oracle query in Codex CLI using an isolated MCP config."""

import argparse
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path

READ_TOOLS = ["resolve_business_terms", "explore_schema", "run_sql"]


def mcp_settings(server_args, *, tools=READ_TOOLS, extra_env=()):
    """Invocation-local tools and approvals shared by the Codex demo runners."""
    settings = {
        "approval_policy": "never",
        "sandbox_mode": "read-only",
        "mcp_servers.lang2sql.command": sys.executable,
        "mcp_servers.lang2sql.required": True,
        "mcp_servers.lang2sql.args": [
            "-m",
            "lang2sql.frontends.mcp.server",
            *server_args,
        ],
        "mcp_servers.lang2sql.env_vars": [
            "LANG2SQL_DB_URL",
            "LANG2SQL_DB_SCHEMA",
            *extra_env,
        ],
        "mcp_servers.lang2sql.enabled_tools": tools,
    }
    for tool in tools:
        settings[f"mcp_servers.lang2sql.tools.{tool}.approval_mode"] = "approve"
    return settings


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--store", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--team", default="sales")
    parser.add_argument("--user", default="codex-colleague")
    parser.add_argument("--model", help="Omit to use the Codex CLI default model")
    parser.add_argument("--timeout", type=int, default=180)
    parser.add_argument(
        "--question",
        default="2025년 10월 순매출은 얼마야? 적용한 기준과 조회 근거도 알려줘.",
    )
    expected = parser.add_mutually_exclusive_group(required=True)
    expected.add_argument(
        "--expect-value",
        help="Expected JSON number or null; never included in the model prompt",
    )
    expected.add_argument("--expect-clarification", action="store_true")
    args = parser.parse_args()
    if not args.store.is_file() or args.output.exists():
        parser.error("Use an existing metadata store and a new output path")
    if not os.environ.get("LANG2SQL_DB_URL"):
        parser.error("Set LANG2SQL_DB_URL for the dedicated read-only fixture account")
    expected_value = json.loads(args.expect_value) if args.expect_value else None
    settings = mcp_settings(
        [
            "--store",
            str(args.store.resolve()),
            "--org-id",
            "demo",
            "--user-id",
            args.user,
            "--team-id",
            args.team,
        ]
    )
    command = [
        "codex",
        "exec",
        "--ignore-user-config",
        "--ephemeral",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--json",
    ]
    if args.model:
        command += ["--model", args.model]
    for key, value in settings.items():
        command += ["-c", key + "=" + json.dumps(value, ensure_ascii=False)]
    command += [
        "연결된 lang2sql MCP만 사용해서 조회해줘. 셸이나 파일은 사용하지 마. "
        + args.question
    ]
    with sqlite3.connect(args.store) as conn:
        before = conn.execute(
            "SELECT count(*) FROM audit WHERE action IN ('run_sql','oracle_metric_query') AND actor=?",
            (args.user,),
        ).fetchone()[0]
    with tempfile.TemporaryDirectory(prefix="lang2sql-codex-") as cwd:
        result = subprocess.run(
            command,
            check=False,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            timeout=args.timeout,
        )
    events = [
        json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")
    ]
    items = [e["item"] for e in events if e.get("type") == "item.completed"]
    calls = [i for i in items if i.get("type") == "mcp_tool_call"]
    answers = [i["text"] for i in items if i.get("type") == "agent_message"]
    errors = []
    missing_definition = False
    for item in calls:
        expected_missing = (
            args.expect_clarification
            and item.get("tool") == "resolve_business_terms"
            and item.get("error") is None
            and any(
                c.get("text", "").startswith("DEFINITION_REQUIRED:")
                for c in (item.get("result") or {}).get("content", [])
            )
        )
        missing_definition = missing_definition or expected_missing
        if not expected_missing and (
            item.get("error") or item.get("status") != "completed"
        ):
            errors.append(item)
    values = []
    for item in calls:
        if item.get("tool") != "run_sql":
            continue
        for content in (item.get("result") or {}).get("content", []):
            try:
                evidence = json.loads(content.get("text", ""))
            except json.JSONDecodeError:
                continue
            if (
                isinstance(evidence, dict)
                and evidence.get("object") == "L2S_VIEW_OWNER.V_NET_SALES"
            ):
                values.extend(
                    next(iter(row.values())) for row in evidence.get("rows", [])
                )
    with sqlite3.connect(args.store) as conn:
        after = conn.execute(
            "SELECT count(*) FROM audit WHERE action IN ('run_sql','oracle_metric_query') AND actor=?",
            (args.user,),
        ).fetchone()[0]
    observed = [c.get("tool") for c in calls]
    passed = (
        result.returncode == 0
        and not errors
        and bool(answers)
        and "resolve_business_terms" in observed
        and not any(i.get("type") == "command_execution" for i in items)
        and (
            missing_definition and after == before and "run_sql" not in observed
            if args.expect_clarification
            else values == [expected_value] and after == before + 1
        )
    )
    report = {
        "passed": passed,
        "codex_version": subprocess.check_output(
            ["codex", "--version"], text=True
        ).strip(),
        "requested_model": args.model,
        "question": args.question,
        "team": args.team,
        "sql_execution_count": after - before,
        "expected_value": expected_value,
        "expect_clarification": args.expect_clarification,
        "values": values,
        "answers": answers,
        "events": events,
        "stderr": result.stderr,
    }
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(
        json.dumps(
            {k: report[k] for k in ("passed", "team", "sql_execution_count", "values")},
            ensure_ascii=False,
        )
    )
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
