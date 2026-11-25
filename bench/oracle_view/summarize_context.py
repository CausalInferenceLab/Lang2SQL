"""Independently check saved Codex turns, date bounds, session continuity and usage."""

import argparse
import json
import statistics
from pathlib import Path


def check_result(report, turn):
    month = 9 + (turn - 1) % 3
    expected = {9: 25, 10: 118, 11: 10}[month]
    evidence = report["evidence"]
    passed = report["exit_code"] == 0 and not report["shell_used"] and bool(evidence)
    for result in evidence:
        passed = (
            passed
            and result.get("object") == "L2S_VIEW_OWNER.V_NET_SALES"
            and result.get("params", {}).get("period_start") == f"2025-{month:02d}-01"
            and result.get("params", {}).get("period_end") == f"2025-{month+1:02d}-01"
            and len(result["rows"]) == 1
            and next(iter(result["rows"][0].values())) == expected
        )
    return {"turn": turn, "passed": bool(passed), "sql_results": len(evidence)}


def summarize(directory, expected_turns):
    reports = [
        json.loads((directory / f"turn-{i:03d}.json").read_text())
        for i in range(1, expected_turns + 1)
    ]
    thread_ids = set()
    checks = []
    usage = {}
    usage_deltas = []
    baseline_usage = {}
    for i, report in enumerate(reports, 1):
        events = report["events"]
        thread_ids.update(
            e["thread_id"] for e in events if e.get("type") == "thread.started"
        )
        checks.append(check_result(report, i))
        for event in events:
            if event.get("type") == "turn.completed":
                current = event.get("usage", {})
                delta = {
                    key: value - usage.get(key, 0) for key, value in current.items()
                }
                if any(value < 0 for value in delta.values()):
                    raise ValueError(
                        "Usage counter reset; do not treat it as cumulative"
                    )
                usage_deltas.append({"turn": i, **delta})
                usage = current
    baseline_ids = set()
    baseline_checks = []
    for i in sorted({1, 10, 50, expected_turns} & set(range(1, expected_turns + 1))):
        report = json.loads((directory / f"baseline-{i:03d}.json").read_text())
        baseline_ids.update(
            e["thread_id"]
            for e in report["events"]
            if e.get("type") == "thread.started"
        )
        baseline_checks.append(check_result(report, i))
        for event in report["events"]:
            if event.get("type") == "turn.completed":
                for key, value in event.get("usage", {}).items():
                    baseline_usage[key] = baseline_usage.get(key, 0) + value
    durations = [r["elapsed_seconds"] for r in reports]
    continuity = (
        len(thread_ids) == 1
        and not (thread_ids & baseline_ids)
        and len(baseline_ids) == len(baseline_checks)
    )
    original = json.loads((directory / "summary.json").read_text())
    return {
        "passed": continuity and all(c["passed"] for c in checks + baseline_checks),
        "turns": checks,
        "baselines": baseline_checks,
        "same_session_verified": continuity,
        "dataset_sha256": original["dataset_sha256"],
        "codex_version": original["codex_version"],
        "requested_model": original["requested_model"],
        "metadata_backend": original.get("metadata_backend", "sqlite"),
        "session_usage": usage,
        "per_turn_usage_deltas": usage_deltas,
        "baseline_usage_total": baseline_usage,
        "usage_interpretation": "Codex 0.170 resume turn.completed usage is cumulative; use final session counter, not sum of counters",
        "elapsed_seconds_sum": sum(durations),
        "median_turn_seconds": statistics.median(durations),
        "limitations": [
            "Synthetic fixture, one run, no claim of accuracy improvement or universal long-term memory. Token usage is not a monetary cost estimate."
        ],
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--expected-turns", type=int, default=100)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("Use a new output path")
    report = summarize(args.run, args.expected_turns)
    args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
    print(
        json.dumps(
            {
                k: report[k]
                for k in ["passed", "same_session_verified", "median_turn_seconds"]
            }
        )
    )
    raise SystemExit(0 if report["passed"] else 1)
