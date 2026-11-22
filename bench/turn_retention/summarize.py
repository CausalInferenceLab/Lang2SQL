"""Check all pairs, then summarize the per-turn records. No model calls."""

import argparse
import gzip
import json
from pathlib import Path


def read_records(folder, config):
    records = []
    for mode in ("fresh", "long"):
        for seed in config["seeds"]:
            path = folder / f"{mode}-{seed}.jsonl"
            if path.exists():
                data = path.read_text()
            else:
                data = gzip.decompress(
                    path.with_suffix(".jsonl.gz").read_bytes()
                ).decode()
            lane = [json.loads(line) for line in data.splitlines()]
            if [r["turn"] for r in lane] != list(range(1, 101)):
                raise ValueError(f"Missing or duplicate turns in {path}")
            for r in lane:
                if (r["mode"], r["seed"]) != (mode, seed):
                    raise ValueError("Mislabeled condition or repetition")
                if mode == "fresh" and r["history_before"] != 0:
                    raise ValueError("Fresh-session history was not reset")
                if (
                    mode == "long"
                    and r["turn"] > 1
                    and r["history_before"] < r["turn"] - 1
                ):
                    raise ValueError("Long-session history did not persist")
            records.extend(lane)
    return records


def totals(rows):
    queries = [
        r for r in rows if r["probe_applied"] is not None or r["turn"] in (30, 60, 90)
    ]
    return {
        "n": len(rows),
        "correct": sum(r["correct"] for r in rows),
        "executed": sum(r["executed"] for r in rows),
        "correct_sql": sum(r["correct_sql"] for r in rows),
        "clarifications": sum(r["clarified"] for r in rows),
        "api_errors": sum(r["error"] is not None for r in rows),
        "calls": sum(len(r["calls"]) for r in rows),
        "missing_usage": sum(c["usage"] is None for r in rows for c in r["calls"]),
        "input_tokens": sum(r["input"] for r in rows),
        "cached_tokens": sum(r["cached"] for r in rows),
        "output_tokens": sum(r["output"] for r in rows),
        "estimated_usd": round(sum(r["estimated_usd"] for r in rows), 6),
        "definition_probes": len(queries),
        "applied": sum(r["probe_applied"] is True for r in queries),
    }


def summary(records, config):
    by_mode = {}
    for mode in ("fresh", "long"):
        lane = [r for r in records if r["mode"] == mode]
        by_mode[mode] = {
            "overall": totals(lane),
            "repeats": {
                str(seed): totals([r for r in lane if r["seed"] == seed])
                for seed in config["seeds"]
            },
            "blocks": {
                f"{n+1}–{n+20}": totals([r for r in lane if n < r["turn"] <= n + 20])
                for n in range(0, 100, 20)
            },
            "probes": {
                str(n): totals([r for r in lane if r["turn"] == n])
                for n in (30, 60, 90)
            },
        }
    key = {(r["mode"], r["seed"], r["turn"]): r for r in records}
    gains = losses = 0
    for r in records:
        if r["mode"] == "long":
            fresh = key["fresh", r["seed"], r["turn"]]
            gains += int(r["correct"] and not fresh["correct"])
            losses += int(not r["correct"] and fresh["correct"])
    return {"by_mode": by_mode, "paired": {"long_only": gains, "fresh_only": losses}}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("records", type=Path)
    args = parser.parse_args()
    config = json.loads((args.records / "config.json").read_text())
    print(
        json.dumps(
            summary(read_records(args.records, config), config),
            ensure_ascii=False,
            indent=2,
        )
    )
