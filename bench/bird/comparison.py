"""Join the two product revisions by profile, evidence condition and question."""

from __future__ import annotations

import json
from pathlib import Path

from .dataset import load_manifest
from .io import ProtocolError, digest, json_digest, read_json, write_json, write_text
from .protocol import CONDITIONS, PROFILES, check_settings


def load_runs(root: Path, commit: str, *, smoke: bool) -> dict:
    runs = {}
    for profile in PROFILES:
        for condition in CONDITIONS:
            path = root / f"{condition}-{profile}"
            manifest = load_manifest(path)
            settings = read_json(path / "settings.json")
            check_settings(manifest, settings, commit)
            if (manifest["smoke_cases"] is not None) != smoke:
                raise ProtocolError("Comparison requires runs of the selected mode")
            if (
                settings["condition"] != condition
                or settings["profile"]["name"] != profile
            ):
                raise ProtocolError(f"Run has the wrong condition or profile: {path}")

            summary = read_json(path / "summary.json")
            scores_path = path / "scores.json"
            identity = {
                "status": "completed",
                "condition": condition,
                "profile": profile,
                "model": settings["profile"]["model"],
                "benchmark_commit": commit,
                "product_commit": settings["product_commit"],
                "mode": settings["mode"],
                "manifest_digest": settings["manifest_digest"],
                "settings_digest": json_digest(settings),
                "scores_sha256": digest(scores_path),
                "total": len(manifest["cases"]),
            }
            if any(summary.get(key) != value for key, value in identity.items()):
                raise ProtocolError(
                    f"Run has incomplete or mismatched score artifacts: {path}"
                )
            if set(summary["response_models"]) != {PROFILES[profile]["response_model"]}:
                raise ProtocolError(
                    f"Run has unexpected provider response models: {path}"
                )

            rows = read_json(scores_path)
            expected = [
                (case["question_id"], case["db_id"]) for case in manifest["cases"]
            ]
            if [(row["question_id"], row["db_id"]) for row in rows] != expected or any(
                row["condition"] != condition
                or row["profile"] != profile
                or row["ex"] not in (0, 1)
                for row in rows
            ):
                raise ProtocolError(
                    f"Scores do not cover the exact frozen cohort: {path}"
                )
            if summary["correct"] != sum(row["ex"] for row in rows):
                raise ProtocolError(
                    f"Summary correct count differs from scores: {path}"
                )
            runs[condition, profile] = settings, summary, rows
    return runs


def paired_metrics(before: list[dict], after: list[dict]) -> dict:
    pairs = list(zip(before, after, strict=True))
    if any(left["question_id"] != right["question_id"] for left, right in pairs):
        raise ProtocolError("Scores must be paired by question ID")
    a_correct = sum(row["ex"] for row in before)
    b_correct = sum(row["ex"] for row in after)
    return {
        "questions": len(pairs),
        "before_correct": a_correct,
        "after_correct": b_correct,
        "before_ex_pct": 100 * a_correct / len(pairs),
        "after_ex_pct": 100 * b_correct / len(pairs),
        "delta_ex_pp": 100 * (b_correct - a_correct) / len(pairs),
        "wrong_to_right": sum(left["ex"] < right["ex"] for left, right in pairs),
        "right_to_wrong": sum(left["ex"] > right["ex"] for left, right in pairs),
    }


def compare(
    baseline: Path,
    candidate: Path,
    output: Path,
    commit: str,
    *,
    smoke: bool,
) -> dict:
    versions = {
        "baseline": load_runs(baseline, commit, smoke=smoke),
        "candidate": load_runs(candidate, commit, smoke=smoke),
    }
    manifests = {
        settings["manifest_digest"]
        for runs in versions.values()
        for settings, _, _ in runs.values()
    }
    if len(manifests) != 1:
        raise ProtocolError("All runs must use exactly the same prepared manifest")
    products = {}
    for label, runs in versions.items():
        sources = {
            (settings["product_commit"], settings["product_lock_sha256"])
            for settings, _, _ in runs.values()
        }
        if len(sources) != 1:
            raise ProtocolError(f"{label} mixes different product checkouts")
        products[label] = sources.pop()[0]

    comparisons, evidence = {}, {label: {} for label in versions}
    paired = []
    for profile in PROFILES:
        runtimes = [
            runs[condition, profile][0]["runtime"]
            for runs in versions.values()
            for condition in CONDITIONS
        ]
        if any(runtime != runtimes[0] for runtime in runtimes[1:]):
            raise ProtocolError(f"Comparisons must use the same runtime for {profile}")

        for label, runs in versions.items():
            without = runs["D", profile][2]
            supplied = runs["B", profile][2]
            for left, right in zip(without, supplied, strict=True):
                for field in ("system_digest", "tools_digest"):
                    if left[field] and right[field] and left[field] != right[field]:
                        raise ProtocolError(
                            f"Evidence conditions differ in {field} for {label}/"
                            f"{profile}/{left['question_id']}"
                        )
            evidence[label][profile] = paired_metrics(without, supplied)

        for condition in CONDITIONS:
            _, baseline_summary, before = versions["baseline"][condition, profile]
            _, candidate_summary, after = versions["candidate"][condition, profile]
            comparisons[f"{condition}-{profile}"] = {
                **paired_metrics(before, after),
                "before_model_calls": baseline_summary["model_calls"],
                "after_model_calls": candidate_summary["model_calls"],
                "before_case_seconds": baseline_summary["case_seconds"],
                "after_case_seconds": candidate_summary["case_seconds"],
                "before_system_fingerprints": baseline_summary["system_fingerprints"],
                "after_system_fingerprints": candidate_summary["system_fingerprints"],
            }
            paired.extend(
                {
                    "condition": condition,
                    "profile": profile,
                    "question_id": left["question_id"],
                    "db_id": left["db_id"],
                    "baseline_ex": left["ex"],
                    "candidate_ex": right["ex"],
                }
                for left, right in zip(before, after, strict=True)
            )
    report = {
        "mode": "scripted-smoke" if smoke else "benchmark",
        "benchmark_commit": commit,
        "product_commits": products,
        "manifest_digest": manifests.pop(),
        "matched_product_pairs": len(paired),
        "product_comparisons": comparisons,
        "evidence_comparisons_d_to_b": evidence,
    }
    output.mkdir(parents=True, exist_ok=False)
    write_text(
        output / "paired-scores.jsonl",
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in paired),
    )
    write_json(output / "comparison.json", report)
    return report
