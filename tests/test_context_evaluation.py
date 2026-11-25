"""The result auditor must reject wrong date bounds and reused baseline sessions."""

import json
import runpy
from pathlib import Path

import pytest

summarize = runpy.run_path(
    str(Path(__file__).parents[1] / "bench/oracle_view/summarize_context.py")
)["summarize"]


def result_fixture(directory):
    for i, month, value in [(1, 9, 25), (2, 10, 118)]:
        report = {
            "exit_code": 0,
            "shell_used": False,
            "elapsed_seconds": 1,
            "events": [
                {"type": "thread.started", "thread_id": "conversation"},
                {"type": "turn.completed", "usage": {"input_tokens": 10}},
            ],
            "evidence": [
                {
                    "object": "L2S_VIEW_OWNER.V_NET_SALES",
                    "params": {
                        "period_start": f"2025-{month:02d}-01",
                        "period_end": f"2025-{month+1:02d}-01",
                    },
                    "rows": [{"metric_value": value}],
                }
            ],
            "passed": True,
        }
        (directory / f"turn-{i:03d}.json").write_text(json.dumps(report))
        report["events"][0]["thread_id"] = f"baseline-{i}"
        (directory / f"baseline-{i:03d}.json").write_text(json.dumps(report))
    (directory / "summary.json").write_text(
        json.dumps(
            {
                "dataset_sha256": "fixture",
                "codex_version": "fixture",
                "requested_model": "fixture",
            }
        )
    )


def test_auditor_counts_usage_and_continuity(tmp_path):
    result_fixture(tmp_path)
    report = summarize(tmp_path, 2)
    assert report["passed"]
    assert report["usage_sum"]["input_tokens"] == 20


@pytest.mark.parametrize("corruption", ["end_date", "baseline_session"])
def test_auditor_rejects_corrupted_evidence(tmp_path, corruption):
    result_fixture(tmp_path)
    path = tmp_path / (
        "turn-001.json" if corruption == "end_date" else "baseline-001.json"
    )
    report = json.loads(path.read_text())
    if corruption == "end_date":
        report["evidence"][0]["params"]["period_end"] = "2026-01-01"
    else:
        report["events"][0]["thread_id"] = "conversation"
    path.write_text(json.dumps(report))
    assert not summarize(tmp_path, 2)["passed"]
