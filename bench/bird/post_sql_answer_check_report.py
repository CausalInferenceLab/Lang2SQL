"""Pair issue #313 BIRD scores with #280 and audit post-run SQL revisions."""

from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import re

BASELINE = "fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7"
TREATMENT = "f4635ea567698f32d788688d327d2af33b59de9b"
PROFILES = ("openai-gpt41-mini", "openai-gpt41", "openai-gpt6-luna")
CONDITIONS = ("A", "B", "D")
PROMPT_RULE = (
    "- After `run_sql` succeeds, compare the original question, the executed SQL,\n"
    "  and the returned rows before answering. Check that the filters, date field,\n"
    "  and aggregation match the request. Ground the answer's values and units in\n"
    "  the returned data, distinguishing a zero aggregate from no returned rows.\n"
    "  If you find a concrete mismatch in the SQL, correct it and run the query again.\n"
)
SQL_TOKENS = re.compile(
    r"(?P<comment>--[^\r\n]*|/\*[\s\S]*?\*/)"
    r"|(?P<quoted>'(?:''|[^'])*'|\"(?:\"\"|[^\"])*\"|`(?:``|[^`])*`|\[[^\]]*\])"
    r"|(?P<word>[^\W\d][\w$]*|\d+)"
    r"|(?P<symbol>\S)"
)


def json_digest(value: dict) -> str:
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8"))


def load_records(directory: Path) -> dict[int, dict]:
    archive = directory / "records.jsonl.gz"
    if archive.exists():
        with gzip.open(archive, "rt", encoding="utf-8") as stream:
            records = [json.loads(line) for line in stream]
    elif (directory / "records.jsonl").exists():
        with (directory / "records.jsonl").open(encoding="utf-8") as stream:
            records = [json.loads(line) for line in stream]
    else:
        records = [load_json(path) for path in (directory / "records").glob("*.json")]
    indexed = {record["question_id"]: record for record in records}
    if len(indexed) != len(records):
        raise ValueError(f"Duplicate record in {directory}")
    return indexed


def sql_tokens(sql: str) -> tuple[str, ...]:
    tokens = [
        (
            match[0].lower()
            if match.lastgroup == "word" and match[0].isascii()
            else match[0]
        )
        for match in SQL_TOKENS.finditer(sql)
        if match.lastgroup != "comment"
    ]
    if tokens and tokens[-1] == ";":
        tokens.pop()
    return tuple(tokens)


def run_sql_activity(record: dict) -> tuple[int, list[dict]]:
    executed = {
        event["call_id"]: event["sql"] for event in record["recovery"]["db_executions"]
    }
    outputs = {event["call_id"]: event for event in record["tools"]}
    messages = {
        message["tool_call_id"]: message["content"]
        for message in record["transcript"]
        if message["role"] == "tool"
    }
    previous_success = None
    success_count = 0
    revised_runs = []
    for turn, call in enumerate(record["calls"], 1):
        next_success = previous_success
        for tool_call in call.get("tool_calls", []):
            call_id = tool_call["id"]
            if tool_call["name"] != "run_sql" or call_id not in executed:
                continue
            sql = executed[call_id]
            tokens = sql_tokens(sql)
            output = outputs.get(call_id)
            content = output["content"] if output else ""
            succeeded = bool(
                output is not None
                and not output["is_error"]
                and messages.get(call_id) == content
                and content.endswith(f"\nSQL: {sql}")
            )
            if previous_success is not None and tokens != previous_success[1]:
                revised_runs.append(
                    {
                        "after_call_id": previous_success[0],
                        "call_id": call_id,
                        "model_call": turn,
                        "succeeded": succeeded,
                    }
                )
            if succeeded:
                next_success = (call_id, tokens)
                success_count += 1
        previous_success = next_success
    return success_count, revised_runs


def load_lane(directory: Path, cases: dict, product: str) -> tuple[dict, dict, dict]:
    status = load_json(directory / "status.json")
    summary = load_json(directory / "summary.json")
    settings = load_json(directory / "settings.json")
    scores_file = directory / "scores.json"
    scores = load_json(scores_file)
    indexed = {score["question_id"]: score for score in scores}
    records = load_records(directory)
    if (
        status["status"] != "completed"
        or summary["status"] != "completed"
        or status["completed"] != len(cases)
        or summary["total"] != len(cases)
        or summary["correct"] != sum(score["ex"] for score in scores)
        or summary["scores_sha256"]
        != hashlib.sha256(scores_file.read_bytes()).hexdigest()
        or settings["product_commit"] != product
        or summary["settings_digest"] != json_digest(settings)
        or len(scores) != len(cases)
        or indexed.keys() != records.keys()
        or records.keys() != cases.keys()
    ):
        raise ValueError(f"Incomplete or inconsistent scores in {directory}")

    for question_id, score in indexed.items():
        record = records[question_id]
        if (
            score["ex"] not in (0, 1)
            or score["db_id"] != cases[question_id]
            or score["record_digest"] != json_digest(record)
            or record["settings_digest"] != summary["settings_digest"]
            or record["profile"] != settings["profile"]["name"]
            or score["profile"] != settings["profile"]["name"]
            or record["condition"] != settings["condition"]
            or score["condition"] != settings["condition"]
        ):
            raise ValueError(
                f"Invalid record or score in {directory}, question {question_id}"
            )
    return settings, indexed, records


def case_metrics(score: dict, record: dict) -> dict:
    successful_runs, revised_runs = run_sql_activity(record)
    usage = [call.get("usage") or {} for call in record["calls"]]
    return {
        "ex": score["ex"],
        "record_digest": score["record_digest"],
        "submitted": record["status"] == "predicted",
        "failure": score["failure"],
        "model_calls": len(record["calls"]),
        "api_errors": sum(call["status"] == "error" for call in record["calls"]),
        "known_input_tokens": sum(item.get("prompt_tokens") or 0 for item in usage),
        "known_output_tokens": sum(
            item.get("completion_tokens") or 0 for item in usage
        ),
        "successful_run_sql": successful_runs,
        "revised_run_sql": revised_runs,
    }


def reason(failure: dict | None) -> str | None:
    return failure["reason"] if failure else None


def pair_lane(
    base: Path, treatment: Path, cases: dict, manifest_digest: str
) -> list[dict]:
    old_settings, old_scores, old_records = load_lane(base, cases, BASELINE)
    new_settings, new_scores, new_records = load_lane(treatment, cases, TREATMENT)
    differences = {
        key
        for key in old_settings.keys() | new_settings.keys()
        if old_settings.get(key) != new_settings.get(key)
    }
    if (
        differences != {"product_commit", "git_head"}
        or new_settings["git_head"] != TREATMENT
        or new_settings["manifest_digest"] != manifest_digest
    ):
        raise ValueError(
            f"Unexpected configuration differences in {treatment}: {differences}"
        )

    rows = []
    for question_id, db_id in sorted(cases.items()):
        old_record, new_record = old_records[question_id], new_records[question_id]
        old_request, new_request = (
            old_record["first_request"],
            new_record["first_request"],
        )
        if (
            new_request["system"].count(PROMPT_RULE) != 1
            or new_request["system"].replace(PROMPT_RULE, "", 1)
            != old_request["system"]
            or any(
                new_request[key] != old_request[key]
                for key in (
                    "user",
                    "evidence_role",
                    "evidence_digest",
                    "tool_specs_digest",
                    "knowledge_entries",
                )
            )
        ):
            raise ValueError(
                f"Unexpected first-request changes in {treatment}, question {question_id}"
            )
        old = case_metrics(old_scores[question_id], old_record)
        new = case_metrics(new_scores[question_id], new_record)
        mismatch_after_success = (
            reason(old["failure"]) == "result_mismatch"
            and old["successful_run_sql"] > 0
        )
        rows.append(
            {
                "question_id": question_id,
                "db_id": db_id,
                "baseline": old,
                "treatment": new,
                "baseline_mismatch_after_run_sql": mismatch_after_success,
            }
        )
    return rows


def summarize_side(rows: list[dict], side: str) -> dict:
    results = [row[side] for row in rows]
    return {
        "correct": sum(row["ex"] for row in results),
        "no_submission": sum(not row["submitted"] for row in results),
        "model_calls": sum(row["model_calls"] for row in results),
        "api_errors": sum(row["api_errors"] for row in results),
        "known_input_tokens": sum(row["known_input_tokens"] for row in results),
        "known_output_tokens": sum(row["known_output_tokens"] for row in results),
        "successful_run_sql_cases": sum(
            row["successful_run_sql"] > 0 for row in results
        ),
        "revised_run_sql_cases": sum(bool(row["revised_run_sql"]) for row in results),
        "revised_run_sql_calls": sum(len(row["revised_run_sql"]) for row in results),
        "revised_run_sql_successes": sum(
            run["succeeded"] for row in results for run in row["revised_run_sql"]
        ),
        "failures": dict(
            Counter(reason(row["failure"]) for row in results if row["failure"])
        ),
    }


def summarize(rows: list[dict]) -> dict:
    before = summarize_side(rows, "baseline")
    after = summarize_side(rows, "treatment")
    mismatches = [row for row in rows if row["baseline_mismatch_after_run_sql"]]
    return {
        "pairs": len(rows),
        "baseline": before,
        "treatment": after,
        "delta_ex_pp": 100 * (after["correct"] - before["correct"]) / len(rows),
        "correct_to_incorrect": sum(
            row["baseline"]["ex"] > row["treatment"]["ex"] for row in rows
        ),
        "incorrect_to_correct": sum(
            row["baseline"]["ex"] < row["treatment"]["ex"] for row in rows
        ),
        "baseline_mismatch_after_run_sql": len(mismatches),
        "recovered_baseline_mismatches": sum(
            row["treatment"]["ex"] for row in mismatches
        ),
    }


def compare(baseline: Path, treatment: Path, output: Path) -> dict:
    base_manifest = (baseline / "manifest.json").read_bytes()
    treatment_manifest = treatment / "manifest.json"
    if not treatment_manifest.exists():
        treatment_manifest = treatment / "prepared/manifest.json"
    if treatment_manifest.read_bytes() != base_manifest:
        raise ValueError("The baseline and treatment manifests differ")
    manifest = json.loads(base_manifest)
    cases = {case["question_id"]: case["db_id"] for case in manifest["cases"]}
    manifest_digest = json_digest(manifest)
    lanes = {}
    summary = {}
    for condition in CONDITIONS:
        for profile in PROFILES:
            lane = f"{condition}-{profile}"
            rows = pair_lane(
                baseline / lane / "treatment", treatment / lane, cases, manifest_digest
            )
            lanes[lane] = rows
            summary[lane] = summarize(rows)
            directory = output / lane
            directory.mkdir(parents=True, exist_ok=True)
            (directory / "paired-scores.jsonl").write_text(
                "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
                encoding="utf-8",
            )
    return {
        "baseline_product_commit": BASELINE,
        "treatment_product_commit": TREATMENT,
        "manifest_sha256": hashlib.sha256(base_manifest).hexdigest(),
        "conditions": summary,
        "models": {
            profile: summarize(
                [
                    row
                    for condition in CONDITIONS
                    for row in lanes[f"{condition}-{profile}"]
                ]
            )
            for profile in PROFILES
        },
        "overall": summarize([row for rows in lanes.values() for row in rows]),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("baseline", "treatment", "output"):
        parser.add_argument(f"--{name}", type=Path, required=True)
    args = parser.parse_args()
    summary = compare(args.baseline, args.treatment, args.output)
    (args.output / "comparison.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    main()
