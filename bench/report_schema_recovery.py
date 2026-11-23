"""Verify, pair and publish raw records from the #275 and #278 SQLite runs."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import sqlite3
import time
from collections import Counter
from contextlib import closing
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

BASE = "9274ff1f7fca3834701e1927f8c412407216f0b2"
PRODUCT = "fb1f36e9b0980c5e593d2fddb5ecc6607cd101d7"
MODELS = ("openai-gpt41-mini", "openai-gpt41", "openai-gpt6-luna")
STATES = {
    "correct": "정답",
    "result_mismatch": "결과 불일치",
    "no_such_column": "컬럼 오류",
    "other_sql_error": "기타 SQL 오류",
    "sql_timeout": "SQL 시간 초과",
    "unclassified_sql_failure": "실행 실패 원인 미분류",
    "scorer_error": "채점기 오류",
    "invalid_format": "형식 오류",
    "missing_sql": "SQL 없음",
    "call_budget_exhausted": "호출 예산 소진",
    "case_timeout": "문항 시간 초과",
    "provider_error": "API 오류",
    "generation_error": "생성 오류",
}


def load(path):
    return json.loads(path.read_text(encoding="utf-8"))


def read_lines(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def write_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )


def write_lines(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        for row in rows:
            file.write(
                json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n"
            )


def sha(path):
    with path.open("rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def canonical_sha(value):
    data = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def index(rows, key):
    result = {}
    for row in rows:
        ident = row[key]
        if ident in result:
            raise ValueError(f"Duplicate {key}: {ident}")
        result[ident] = row
    return result


def require(condition, message):
    if not condition:
        raise ValueError(message)


def db_path(root, name):
    flat = root / f"{name}.sqlite"
    return flat if flat.is_file() else root / name / f"{name}.sqlite"


def plan_error(db, sql):
    """Prepare the submitted query against the same read-only snapshot as scoring."""
    deadline = time.monotonic() + 30
    with closing(sqlite3.connect(db.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
        conn.execute("PRAGMA query_only=ON")
        conn.set_authorizer(
            lambda action, *_: (
                sqlite3.SQLITE_DENY
                if action in (sqlite3.SQLITE_ATTACH, sqlite3.SQLITE_DETACH)
                else sqlite3.SQLITE_OK
            )
        )
        conn.set_progress_handler(lambda: int(time.monotonic() > deadline), 1000)
        try:
            conn.execute("EXPLAIN QUERY PLAN " + sql).fetchall()
        except sqlite3.Error as error:
            return str(error)
    return None


def submission_failure(record, score_failure):
    reason = score_failure.get("reason")
    if reason in {"call_budget_exhausted", "case_timeout", "generation_error"}:
        return reason
    if reason in {"provider_error", "provider_timeout"}:
        return "provider_error"
    if reason in {"multiple_sql_blocks", "invalid_final_sql"}:
        return "invalid_format"
    answer = record.get("answer") or ""
    if re.search(r"(?im)(?:```sql\b|^\s*(?:select|with)\s+)", answer):
        return "invalid_format"
    return "missing_sql"


def case_metrics(benchmark, record, score, sql, db, *, baseline):
    calls = record["calls"]
    valid = score.get("score", score.get("ex"))
    scored = score.get("failure") or record.get("failure") or {}
    if sql is None and benchmark == "spider2" and not scored:
        if len(calls) >= 8:
            scored = {"reason": "call_budget_exhausted"}
        elif any(call["status"] == "error" for call in calls):
            scored = {"reason": "provider_error"}
    if (
        sql is None
        and scored.get("reason") == "provider_error"
        and not any(call["status"] == "error" for call in calls)
    ):
        # The Spider2 guard raises RuntimeError before appending a ninth call.
        hit_guard = (
            benchmark == "spider2"
            and record.get("error_type") == "RuntimeError"
            and len(calls) == 8
            and all(call["status"] == "ok" for call in calls)
            and any(call.get("harness") is False for call in calls)
        )
        scored = {
            "reason": "call_budget_exhausted" if hit_guard else "generation_error"
        }
    prep_error = None
    if sql is None:
        state = submission_failure(record, scored)
    elif valid:
        state = "correct"
    elif benchmark == "bird" and scored["reason"] == "result_mismatch":
        state = "result_mismatch"
    elif benchmark == "spider2" and record["status"] == "predicted":
        state = (
            "result_mismatch"
            if score["error_info"] == "Result Error"
            else "scorer_error"
        )
    else:
        prep_error = plan_error(db, sql)
        if prep_error and prep_error.casefold().startswith("no such column"):
            state = "no_such_column"
        elif scored.get("reason") == "sql_timeout":
            state = "sql_timeout"
        elif baseline and benchmark == "spider2" and prep_error is None:
            state = "unclassified_sql_failure"
        else:
            state = "other_sql_error"

    assistant_requests = [
        request
        for message in record.get("transcript", [])
        if message["role"] == "assistant"
        for request in message["tool_calls"]
    ]
    if baseline:
        requests = assistant_requests
        dispatches = record.get("tools")
        if dispatches is None:
            received = {
                message["tool_call_id"]
                for message in record.get("transcript", [])
                if message["role"] == "tool"
                and not message["content"].startswith("unknown tool: ")
            }
            dispatches = [request for request in requests if request["id"] in received]
        recovery = None
    else:
        requests = [request for call in calls for request in call.get("tool_calls", [])]
        dispatches = record["tools"]
        events = record["recovery"]["turn_events"]
        withheld_runs = sum(len(event["withheld_run_sql"]) for event in events)
        recovery = {
            "withheld_run_sql": withheld_runs,
            "withheld_final_sql": sum(event["withheld_final_sql"] for event in events),
            "same_response_columns": sum(
                event["same_response_columns"] for event in events
            ),
            "forced_schema_requests": sum(
                event["forced_schema_request"] for event in events
            ),
            "column_responses": sum(
                bool(event["tables_with_columns"]) for event in events
            ),
            "forced_column_responses": sum(
                bool(event["tables_with_columns"]) and event["forced_schema_request"]
                for event in events
            ),
            "rewritten_submissions": int(record["recovery"]["rewritten_submission"]),
            "rewritten_correct": int(
                record["recovery"]["rewritten_submission"] and valid
            ),
        }

    usages = [call.get("usage") or {} for call in calls]
    response_models = Counter(
        c["response_model"] for c in calls if c.get("response_model")
    )
    require(
        valid in {0, 1} and (not valid or sql is not None),
        "Invalid official score or submission",
    )
    return {
        "correct": valid,
        "state": state,
        "submitted": sql is not None,
        "query_plan_error": prep_error,
        "sql": sql,
        "model_calls": len(calls),
        "tool_requests": len(requests),
        "run_sql_requests": sum(r["name"] == "run_sql" for r in requests),
        "tool_dispatches": len(dispatches),
        "run_sql_dispatches": sum(r["name"] == "run_sql" for r in dispatches),
        "db_executions": None if baseline else len(record["recovery"]["db_executions"]),
        "final_db_executions": record.get("final_db_executions", int(sql is not None)),
        "input_tokens": sum(u.get("prompt_tokens") or 0 for u in usages),
        "output_tokens": sum(u.get("completion_tokens") or 0 for u in usages),
        "missing_token_calls": sum(
            u.get("prompt_tokens") is None or u.get("completion_tokens") is None
            for u in usages
        ),
        "seconds": record.get("seconds"),
        "reached_8": len(calls) == 8,
        "budget_without_sql": state == "call_budget_exhausted",
        "api_errors": sum(call["status"] == "error" for call in calls),
        "api_error_types": dict(
            Counter(
                str(call.get("error_type", "unknown"))
                for call in calls
                if call["status"] == "error"
            )
        ),
        "api_call_statuses": dict(Counter(call["status"] for call in calls)),
        "response_models": dict(response_models),
        "recovery": recovery,
    }


def percentile(values, fraction):
    location = (len(values) - 1) * fraction
    lower, upper = math.floor(location), math.ceil(location)
    return values[lower] + (values[upper] - values[lower]) * (location - lower)


def summarize(rows, records, interruptions):
    states = Counter(row["state"] for row in rows)
    totals = {}
    for field in (
        "correct",
        "submitted",
        "model_calls",
        "tool_requests",
        "run_sql_requests",
        "tool_dispatches",
        "run_sql_dispatches",
        "db_executions",
        "final_db_executions",
        "input_tokens",
        "output_tokens",
        "missing_token_calls",
        "reached_8",
        "budget_without_sql",
        "api_errors",
    ):
        values = [row[field] for row in rows]
        totals[field] = sum(values) if None not in values else None
    durations = sorted(row["seconds"] for row in rows if row["seconds"] is not None)
    response_models = Counter()
    api_call_statuses = Counter()
    api_error_types = Counter()
    for row in rows:
        response_models.update(row["response_models"])
        api_call_statuses.update(row["api_call_statuses"])
        api_error_types.update(row["api_error_types"])
    recovery = [row["recovery"] for row in rows if row["recovery"] is not None]
    start = [
        datetime.fromisoformat(record["started_at"])
        for record in records
        if record.get("started_at")
    ]
    finish = [
        datetime.fromisoformat(record["started_at"])
        + timedelta(seconds=record["seconds"])
        for record in records
        if record.get("started_at") and record.get("seconds") is not None
    ]
    return {
        **totals,
        "planned": len(rows),
        "attempted": len(rows),
        "completed": len(records),
        "scored": len(rows),
        "missing": 0,
        "total_attempts": len(rows) + len(interruptions),
        "interrupted_attempts": len(interruptions),
        "rerun_questions": len(
            {row.get("question_id", row.get("instance_id")) for row in interruptions}
        ),
        "not_submitted": len(rows) - totals["submitted"],
        "invalid_format": states["invalid_format"],
        "sql_execution_success": sum(
            states[s] for s in ("correct", "result_mismatch", "scorer_error")
        ),
        "only_known_sql_timeouts": any(
            row["state"] == "unclassified_sql_failure" for row in rows
        ),
        "states": {state: states[state] for state in STATES},
        "latency_p50_seconds": percentile(durations, 0.50) if durations else None,
        "latency_p95_seconds": percentile(durations, 0.95) if durations else None,
        "measured_durations": len(durations),
        "response_models": dict(response_models),
        "missing_response_model_calls": totals["model_calls"]
        - sum(response_models.values()),
        "api_call_statuses": dict(api_call_statuses),
        "api_error_types": dict(api_error_types),
        "recovery": (
            {metric: sum(row[metric] for row in recovery) for metric in recovery[0]}
            if recovery
            else None
        ),
        "started_at": min(start).isoformat() if start else None,
        "last_case_finished_at": max(finish).isoformat() if finish else None,
    }


@dataclass
class GroupInput:
    manifest: dict
    settings: dict
    scores: list
    records: dict
    interrupted: list
    submitted_sql: dict
    reference: dict


def read_group(args, group, profile, condition, baseline):
    arm = "baseline" if baseline else "treatment"
    root = args.baseline if baseline else args.treatment
    archive = root / group / arm
    archived = (archive / "records.jsonl").is_file()
    interrupted = []
    id_field = "question_id" if condition else "instance_id"
    run = root / group
    data = run if args.benchmark == "bird" else run / profile
    summary_dir = data if args.benchmark == "bird" else run
    if archived:
        run = data = summary_dir = archive
        manifest_path = root / "manifest.json"
        records = {
            str(key): row
            for key, row in index(
                read_lines(archive / "records.jsonl"), id_field
            ).items()
        }
        interrupted = read_lines(archive / "interruptions.jsonl")
    else:
        manifest_path = run / "manifest.json"
        if baseline and group == "D-openai-gpt6-luna":
            data = summary_dir = root / "recovered" / group
            interrupted = [
                load(run / "records" / f"{ident}.json") for ident in (29, 69, 146, 287)
            ]
            require(
                all(row["failure"]["reason"] == "interrupted" for row in interrupted),
                "Invalid Luna D recovery archive",
            )
        if not baseline:
            interrupted = [
                load(path) for path in sorted(data.glob("interruptions/*/*.json"))
            ]
        records = {path.stem: load(path) for path in (data / "records").glob("*.json")}

    manifest = load(manifest_path)
    snapshot = load(run / "settings.json")
    status = load(data / "status.json")
    require(status["status"] == "completed", f"Run is incomplete: {data}")
    scores = load(data / "scores.json")
    summary_path = summary_dir / "summary.json"
    summary = load(summary_path)
    require(
        len(records) == len(manifest["cases"]) == len(scores),
        f"Wrong record count: {data}",
    )
    if not baseline:
        require(
            {r["status"] for r in records.values()} <= {"predicted", "failed"},
            f"Unfinished records: {data}",
        )
    expected_product = BASE if baseline else PRODUCT
    if condition:
        require(
            snapshot["product_commit"] == expected_product, f"Product mismatch: {data}"
        )
        require(
            snapshot["condition"] == condition
            and snapshot["profile"]["name"] == profile,
            f"Profile mismatch: {data}",
        )
        require(
            snapshot["manifest_digest"] == canonical_sha(manifest),
            f"Manifest digest mismatch: {data}",
        )
        require(
            sha(data / "scores.json") == summary["scores_sha256"],
            f"Score hash mismatch: {data}",
        )
        require(
            status["completed"] == status["expected"] == len(records),
            f"Status count mismatch: {data}",
        )
        submitted_sql = {
            ident: r["sql"] for ident, r in records.items() if r.get("sql")
        }
    else:
        require(
            snapshot.get("product_commit", snapshot["git_commit"]) == expected_product,
            f"Product mismatch: {data}",
        )
        require(
            [p["name"] for p in snapshot["profiles"]] == [profile],
            f"Profile mismatch: {data}",
        )
        if archived:
            submitted_sql = {
                row["instance_id"]: row["sql"]
                for row in read_lines(archive / "submitted-sql.jsonl")
            }
        else:
            submitted_sql = {
                path.stem: path.read_text(encoding="utf-8").strip()
                for path in (data / "sql").glob("*.sql")
            }
        require(
            set(submitted_sql) <= set(records), f"Submitted SQL ID mismatch: {data}"
        )
    raw_records = []
    for score in scores:
        record = records[str(score[id_field])]
        require(
            score[id_field] == record[id_field] and record["profile"] == profile,
            f"Record identity mismatch: {data}",
        )
        require(
            len(record["calls"]) <= 8, f"Call budget exceeded: {data}/{score[id_field]}"
        )
        if condition:
            require(
                record["condition"] == score["condition"] == condition,
                f"Evidence condition mismatch: {data}",
            )
            require(
                score["record_digest"] == canonical_sha(record),
                f"Score record digest mismatch: {data}/{score[id_field]}",
            )
        raw_records.append(record)

    reference = (
        load(archive / "provenance.json")
        if archived
        else {
            "evaluator_git_head": snapshot.get("git_head", snapshot.get("git_commit")),
            "settings_sha256": sha(run / "settings.json"),
            "scores_sha256": sha(data / "scores.json"),
            "source_manifest_sha256": sha(manifest_path),
            "settings_saved_utc": datetime.fromtimestamp(
                (run / "settings.json").stat().st_mtime, timezone.utc
            ).isoformat(),
            "scores_saved_utc": datetime.fromtimestamp(
                (data / "scores.json").stat().st_mtime, timezone.utc
            ).isoformat(),
        }
    )
    require(
        reference["settings_sha256"] == sha(run / "settings.json"),
        f"Settings hash mismatch: {data}",
    )
    require(
        reference["scores_sha256"] == sha(data / "scores.json"),
        f"Scores hash mismatch: {data}",
    )
    export = args.output / group / arm
    write_lines(export / "records.jsonl", raw_records)
    write_lines(export / "interruptions.jsonl", interrupted)
    if not condition:
        write_lines(
            export / "submitted-sql.jsonl",
            (
                {"instance_id": ident, "sql": sql}
                for ident, sql in sorted(submitted_sql.items())
            ),
        )
    for name, source in (
        ("settings.json", run / "settings.json"),
        ("scores.json", data / "scores.json"),
        ("status.json", data / "status.json"),
        ("summary.json", summary_path),
    ):
        (export / name).write_bytes(source.read_bytes())
    write_json(export / "provenance.json", reference)
    return GroupInput(
        manifest, snapshot, scores, records, interrupted, submitted_sql, reference
    )


def compare_group(args, profile, condition, published):
    group = f"{condition}-{profile}" if condition else profile
    base = read_group(args, group, profile, condition, True)
    treatment = read_group(args, group, profile, condition, False)
    old_settings, settings = base.settings, treatment.settings
    manifest = treatment.manifest
    require(base.manifest == manifest, f"Manifest mismatch: {group}")
    if condition:
        require(
            old_settings["budgets"] == settings["budgets"], f"Budget mismatch: {group}"
        )
        require(
            old_settings["profile"] == settings["profile"], f"Profile changed: {group}"
        )
        require(
            old_settings["scoring"] == settings["scoring"],
            f"Scorer configuration mismatch: {group}",
        )
        require(
            old_settings["question_order"] == settings["question_order"],
            f"Question order mismatch: {group}",
        )
        key, db_field, expected_count = "question_id", "db_id", 365
    else:
        for name in (
            "profiles",
            "max_calls",
            "max_output_tokens",
            "concurrency",
            "call_timeout",
            "case_timeout",
            "sql_timeout",
            "max_rows",
            "instruction",
        ):
            require(old_settings[name] == settings[name], f"{name} mismatch: {group}")
        key, db_field, expected_count = "instance_id", "db", 135
    by_id = index(manifest["cases"], key)
    old_score, new_score = index(base.scores, key), index(treatment.scores, key)
    require(
        set(by_id) == set(old_score) == set(new_score) and len(by_id) == expected_count,
        f"Pairing mismatch: {group}",
    )
    rows = []
    for ident, case in by_id.items():
        db = db_path(args.db_dir, case[db_field])
        if condition:
            require(
                old_score[ident]["db_id"]
                == new_score[ident]["db_id"]
                == case[db_field],
                f"Scored database mismatch: {group}/{ident}",
            )
        before = case_metrics(
            args.benchmark,
            base.records[str(ident)],
            old_score[ident],
            base.submitted_sql.get(str(ident)),
            db,
            baseline=True,
        )
        after = case_metrics(
            args.benchmark,
            treatment.records[str(ident)],
            new_score[ident],
            treatment.submitted_sql.get(str(ident)),
            db,
            baseline=False,
        )
        prior = published[(profile, condition, ident)]
        if condition:
            require(
                before["correct"] == prior["pr275_ex"]
                and old_score[ident]["failure"] == prior["pr275_failure"],
                f"#276 score mismatch: {group}/{ident}",
            )
        else:
            old = prior["treatment"]
            require(
                before["correct"] == old["correct"]
                and (before["state"] == "no_such_column") == old["no_such_column"],
                f"#277 score or column classification mismatch: {group}/{ident}",
            )
        rows.append(
            {
                key: ident,
                "profile": profile,
                "condition": condition,
                "db_id": case[db_field],
                "baseline": before,
                "treatment": after,
            }
        )
    out = args.output / group
    write_lines(out / "paired-scores.jsonl", rows)
    matrices = {state: {target: 0 for target in STATES} for state in STATES}
    for row in rows:
        matrices[row["baseline"]["state"]][row["treatment"]["state"]] += 1
    left = summarize(
        [row["baseline"] for row in rows], list(base.records.values()), base.interrupted
    )
    right = summarize(
        [row["treatment"] for row in rows],
        list(treatment.records.values()),
        treatment.interrupted,
    )
    correct_pairs = Counter(
        (row["baseline"]["correct"], row["treatment"]["correct"]) for row in rows
    )
    model_config = settings["profile"] if condition else settings["profiles"][0]
    result = {
        "group": group,
        "profile": profile,
        "condition": condition,
        "requested_model": model_config["model"],
        "baseline": left,
        "treatment": right,
        "pairs": len(rows),
        "baseline_only": correct_pairs[1, 0],
        "treatment_only": correct_pairs[0, 1],
        "both_correct": correct_pairs[1, 1],
        "both_wrong": correct_pairs[0, 0],
        "delta_percentage_points": (right["correct"] - left["correct"])
        * 100
        / len(rows),
        "state_transitions": matrices,
        "sources": {"baseline": base.reference, "treatment": treatment.reference},
    }
    for arm in (left, right):
        arm["accuracy_pct"] = arm["correct"] * 100 / len(rows)
    return result, manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmark", choices=("spider2", "bird"))
    for argument in ("baseline", "treatment", "db-dir", "published", "output"):
        parser.add_argument("--" + argument, type=Path, required=True)
    args = parser.parse_args()
    prior = read_lines(args.published)
    key = "question_id" if args.benchmark == "bird" else "instance_id"
    published = {(row["profile"], row.get("condition"), row[key]): row for row in prior}
    groups = {}
    manifests = []
    for condition in ("A", "B", "D") if args.benchmark == "bird" else (None,):
        for profile in MODELS if condition else MODELS[:2]:
            group, manifest = compare_group(args, profile, condition, published)
            groups[group["group"]] = group
            manifests.append(manifest)
    planned = 3285 if args.benchmark == "bird" else 270
    require(
        all(manifest == manifests[0] for manifest in manifests),
        "Group manifest mismatch",
    )
    require(
        sum(group["pairs"] for group in groups.values())
        == planned
        == len(published)
        == len(prior),
        "Benchmark pair count does not match",
    )
    manifest = manifests[0]
    hashes = manifest.get("database_sha256", manifest.get("db_sha256"))
    for name, digest in hashes.items():
        require(
            sha(db_path(args.db_dir, name)) == digest, f"Database hash mismatch: {name}"
        )
    manifest_source = args.treatment / "manifest.json"
    if not manifest_source.is_file():
        subdir = "prepared" if args.benchmark == "bird" else MODELS[0]
        manifest_source = args.treatment / subdir / "manifest.json"
    (args.output / "manifest.json").write_bytes(manifest_source.read_bytes())
    (args.output / "published-baseline-scores.jsonl").write_bytes(
        args.published.read_bytes()
    )
    if (args.treatment / "checks").is_dir():
        for path in (args.treatment / "checks").glob("*.json"):
            write_json(args.output / "checks" / path.name, load(path))
    else:
        smoke = args.treatment.glob("smoke-*.json")
        for path in (
            args.treatment / "mock.json",
            *smoke,
            *args.treatment.glob("runtime-*.json"),
        ):
            if path.is_file():
                write_json(args.output / "checks" / path.name, load(path))
        if args.benchmark == "bird":
            for path in (
                args.treatment / "prepared/gold_check.json",
                *args.treatment.glob("dry-*/status.json"),
            ):
                filename = (
                    "gold_check.json"
                    if path.name.startswith("gold")
                    else path.parent.name + ".json"
                )
                write_json(args.output / "checks" / filename, load(path))
    write_json(
        args.output / "comparison.json",
        {
            "benchmark": args.benchmark,
            "baseline_product": BASE,
            "treatment_product": PRODUCT,
            "planned_pairs": planned,
            "paired_cases": sum(group["pairs"] for group in groups.values()),
            "published_baseline_sha256": sha(args.published),
            "manifest_artifact_sha256": sha(args.output / "manifest.json"),
            "groups": groups,
        },
    )
    print(f"{args.benchmark}: verified {planned} pairs, {len(hashes)} databases")


if __name__ == "__main__":
    main()
