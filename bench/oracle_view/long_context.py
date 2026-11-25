"""Real Codex multi-turn evaluation; no expected answers are sent to the model."""

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import time
from pathlib import Path

from codex_demo import mcp_settings


def run_turn(settings, cwd, question, thread, timeout, model):
    command = ["codex", "exec"] + (["resume"] if thread else [])
    command += ["--ignore-user-config", "--skip-git-repo-check", "--json"]
    if model:
        command += ["--model", model]
    for key, value in settings.items():
        command += ["-c", key + "=" + json.dumps(value)]
    command += ([thread] if thread else []) + [question]
    start = time.monotonic()
    result = subprocess.run(
        command, check=False, cwd=cwd, capture_output=True, text=True, timeout=timeout
    )
    events = [
        json.loads(line) for line in result.stdout.splitlines() if line.startswith("{")
    ]
    thread = next(
        (e["thread_id"] for e in events if e.get("type") == "thread.started"), thread
    )
    items = [e["item"] for e in events if e.get("type") == "item.completed"]
    evidence = []
    for item in items:
        if item.get("type") != "mcp_tool_call" or item.get("tool") != "run_sql":
            continue
        for block in (item.get("result") or {}).get("content", []):
            try:
                payload = json.loads(block.get("text", ""))
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict) and "rows" in payload:
                evidence.append(payload)
    return thread, {
        "exit_code": result.returncode,
        "elapsed_seconds": time.monotonic() - start,
        "events": events,
        "evidence": evidence,
        "stderr": result.stderr,
        "shell_used": any(i.get("type") == "command_execution" for i in items),
    }


def main():
    p = argparse.ArgumentParser(description=__doc__)
    backend = p.add_mutually_exclusive_group(required=True)
    backend.add_argument("--store", type=Path)
    backend.add_argument("--metadata-url-env")
    p.add_argument("--org-id", default="demo")
    p.add_argument("--team-id", default="sales")
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--turns", type=int, default=100)
    p.add_argument("--timeout", type=int, default=240)
    p.add_argument("--model")
    args = p.parse_args()
    if (
        args.output.exists()
        or (args.store and not args.store.is_file())
        or args.turns < 1
    ):
        p.error("Use existing store, new output directory, positive turn count")
    args.output.mkdir(parents=True)
    if args.metadata_url_env and not os.environ.get(args.metadata_url_env):
        p.error("Metadata environment variable missing")
    storage_args = (
        ["--store", str(args.store.resolve())]
        if args.store
        else ["--metadata-url-env", args.metadata_url_env]
    )
    settings = mcp_settings(
        [
            *storage_args,
            "--org-id",
            args.org_id,
            "--team-id",
            args.team_id,
            "--user-id",
            "long-context-evaluator",
        ],
        extra_env=[args.metadata_url_env] if args.metadata_url_env else [],
    )
    cases = []
    for i in range(args.turns):
        month = 9 + i % 3
        question = f"2025년 {month}월 순매출을 조회하고 적용 기간과 기준을 알려줘."
        if i % 3:
            question = "같은 기준으로 그다음 달 순매출은 얼마야?"
        cases.append(
            {
                "question": question,
                "explicit": f"2025년 {month}월 순매출은 얼마야?",
                "expected": {9: 25, 10: 118, 11: 10}[month],
                "start": f"2025-{month:02d}-01",
            }
        )
    summary = {
        "codex_version": subprocess.check_output(
            ["codex", "--version"], text=True
        ).strip(),
        "requested_model": args.model,
        "metadata_backend": "oracle" if args.metadata_url_env else "sqlite",
        "dataset_sha256": hashlib.sha256(
            json.dumps(cases, sort_keys=True).encode()
        ).hexdigest(),
        "turns": [],
        "baselines": [],
        "limitations": [
            "One run is not evidence of improvement; no monetary pricing assumed. Usage is retained in raw turn.completed events."
        ],
    }

    def score(report, case):
        e = report["evidence"]
        return (
            report["exit_code"] == 0
            and not report["shell_used"]
            and bool(e)
            and all(
                next(iter(row.values())) == case["expected"] for row in e[-1]["rows"]
            )
            and bool(e[-1]["rows"])
            and e[-1].get("params", {}).get("period_start") == case["start"]
        )

    with tempfile.TemporaryDirectory(prefix="lang2sql-long-context-") as cwd:
        thread = None
        for i, case in enumerate(cases, 1):
            question = (
                "Use only lang2sql MCP. Never use shell or files. " if i == 1 else ""
            ) + case["question"]
            thread, report = run_turn(
                settings, cwd, question, thread, args.timeout, args.model
            )
            if not thread:
                raise RuntimeError("Codex did not return a resumable thread ID")
            report.update(turn=i, question=question, passed=score(report, case))
            (args.output / f"turn-{i:03d}.json").write_text(
                json.dumps(report, ensure_ascii=False, indent=2)
            )
            summary["turns"].append(
                {k: report[k] for k in ("turn", "passed", "elapsed_seconds")}
            )
            if i in {1, 10, 50, args.turns}:
                _, base = run_turn(
                    settings,
                    cwd,
                    "Use only lang2sql MCP. Never use shell or files. "
                    + case["explicit"],
                    None,
                    args.timeout,
                    args.model,
                )
                base["passed"] = score(base, case)
                (args.output / f"baseline-{i:03d}.json").write_text(
                    json.dumps(base, ensure_ascii=False, indent=2)
                )
                summary["baselines"].append({"turn": i, "passed": base["passed"]})
            (args.output / "summary.json").write_text(
                json.dumps(summary, ensure_ascii=False, indent=2)
            )
            print(json.dumps(summary["turns"][-1]), flush=True)
    return 0 if all(x["passed"] for x in summary["turns"] + summary["baselines"]) else 1


if __name__ == "__main__":
    raise SystemExit(main())
