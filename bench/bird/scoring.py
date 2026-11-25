"""Score raw SQLite row tuples with the pinned BIRD calculate_ex rule."""

from __future__ import annotations

import sqlite3
import time
from collections import Counter
from contextlib import closing
from pathlib import Path

from .dataset import gold_rows, load_manifest, verify_databases
from .io import (
    ProtocolError,
    digest,
    json_digest,
    lock_output,
    read_json,
    write_json,
    write_text,
)
from .protocol import BUDGETS, Budgets, check_settings, scoring_settings
from .records import CaseFiles
from .sqlite import read_connection


class QueryFailure(ValueError):
    pass


def read_rows(db: Path, sql: str, budgets: Budgets) -> list[tuple]:
    deadline = time.monotonic() + budgets.sql_timeout
    try:
        with closing(read_connection(db, deadline)) as connection:
            cursor = connection.execute(sql)
            if cursor.description is None:
                raise QueryFailure("sql_no_result")
            rows = []
            for row in cursor:
                if time.monotonic() > deadline:
                    raise QueryFailure("sql_timeout")
                if len(rows) == budgets.max_rows:
                    raise QueryFailure("row_limit_exceeded")
                rows.append(row)
            if time.monotonic() > deadline:
                raise QueryFailure("sql_timeout")
            return rows
    except sqlite3.Error as error:
        if (
            getattr(error, "sqlite_errorname", None) == "SQLITE_INTERRUPT"
            or time.monotonic() > deadline
        ):
            raise QueryFailure("sql_timeout") from None
        raise QueryFailure("sql_error") from None


def calculate_ex(predicted: list[tuple], gold: list[tuple]) -> int:
    return int(set(predicted) == set(gold))


def read_gold(row: dict, databases: dict[str, Path], budgets: Budgets) -> list[tuple]:
    sql = row.get("SQL")
    if not isinstance(sql, str) or not sql.strip():
        raise ProtocolError(f"Gold SQL is missing for question {row['question_id']}")
    try:
        return read_rows(databases[row["db_id"]], sql, budgets)
    except QueryFailure as error:
        raise ProtocolError(
            f"Gold query failed for question {row['question_id']} ({error})"
        ) from None


def expected_gold_check(manifest: dict, budgets: Budgets) -> dict:
    return {
        "status": "passed",
        "manifest_digest": json_digest(manifest),
        "scoring": scoring_settings(budgets),
        "checked": len(manifest["cases"]),
    }


def check_gold(
    prepared: Path, source: Path, db_dir: Path, budgets: Budgets = BUDGETS
) -> dict:
    with lock_output(prepared):
        manifest = load_manifest(prepared)
        databases = verify_databases(db_dir, manifest)
        stamp = prepared / "gold_check.json"
        write_json(stamp, {"status": "checking"})
        try:
            for row in gold_rows(source, manifest):
                read_gold(row, databases, budgets)
        except (OSError, ValueError) as error:
            write_json(stamp, {"status": "blocked", "error": str(error)})
            raise
        receipt = expected_gold_check(manifest, budgets)
        write_json(stamp, receipt)
        return receipt


def require_gold_check(prepared: Path, manifest: dict, budgets: Budgets) -> None:
    receipt = prepared / "gold_check.json"
    if not receipt.is_file() or read_json(receipt) != expected_gold_check(
        manifest, budgets
    ):
        raise ProtocolError(
            "Run score --gold-only for this prepared snapshot before generation"
        )


def score_record(
    record: dict, expected: list[tuple], db: Path, budgets: Budgets
) -> dict:
    first = record["first_request"]
    result = {
        "question_id": record["question_id"],
        "condition": record["condition"],
        "profile": record["profile"],
        "record_digest": json_digest(record),
        "system_digest": json_digest(first["system"]) if first else None,
        "tools_digest": json_digest(first["tool_specs"]) if first else None,
        "ex": 0,
    }
    if record["status"] == "failed":
        return {**result, "failure": record["failure"]}
    try:
        actual = read_rows(db, record["sql"], budgets)
    except QueryFailure as error:
        return {**result, "failure": {"stage": "scoring", "reason": str(error)}}
    ex = calculate_ex(actual, expected)
    return {
        **result,
        "ex": ex,
        "failure": None if ex else {"stage": "scoring", "reason": "result_mismatch"},
        "predicted_rows": len(actual),
        "gold_rows": len(expected),
    }


def summarize(scores: list[dict], records: list[dict]) -> dict:
    calls = [call for record in records for call in record["calls"]]
    correct = sum(score["ex"] for score in scores)
    failures = Counter(
        score["failure"]["reason"] for score in scores if score["failure"]
    )
    usage = [call["usage"] or {} for call in calls]
    return {
        "correct": correct,
        "total": len(scores),
        "ex_pct": 100 * correct / len(scores),
        "failures": dict(failures),
        "model_calls": len(calls),
        "response_models": dict(
            Counter(
                call["response_model"] for call in calls if call.get("response_model")
            )
        ),
        "system_fingerprints": dict(
            Counter(
                call["system_fingerprint"]
                for call in calls
                if call.get("system_fingerprint")
            )
        ),
        "usage_missing_calls": sum(
            item.get("prompt_tokens") is None or item.get("completion_tokens") is None
            for item in usage
        ),
        "known_input_tokens": sum(item.get("prompt_tokens") or 0 for item in usage),
        "known_output_tokens": sum(
            item.get("completion_tokens") or 0 for item in usage
        ),
        "case_seconds": sum(record.get("seconds") or 0 for record in records),
        "duration_missing_cases": sum(
            record.get("seconds") is None for record in records
        ),
    }


def score(output: Path, source: Path, db_dir: Path, commit: str) -> dict:
    with lock_output(output):
        manifest = load_manifest(output)
        snapshot = read_json(output / "settings.json")
        check_settings(manifest, snapshot, commit)
        status = read_json(output / "status.json")
        expected_records = len(manifest["cases"])
        if status["status"] != "completed" or status["completed"] != expected_records:
            raise ProtocolError("Score requires a complete generation run")
        budgets = Budgets(**snapshot["budgets"])
        if snapshot["scoring"] != scoring_settings(budgets):
            raise ProtocolError(
                "Scorer source, SQLite version, or scoring budgets changed"
            )
        databases = verify_databases(db_dir, manifest)
        settings_digest = json_digest(snapshot)
        condition, profile = snapshot["condition"], snapshot["profile"]
        identity = {
            "benchmark_commit": commit,
            "product_commit": snapshot["product_commit"],
            "mode": snapshot["mode"],
            "condition": condition,
            "profile": profile["name"],
            "model": profile["model"],
            "manifest_digest": snapshot["manifest_digest"],
            "settings_digest": settings_digest,
        }
        scores, records = [], []
        summary_path = output / "summary.json"
        write_json(summary_path, {**identity, "status": "scoring"})
        try:
            for row in gold_rows(source, manifest):
                ident, db_id = row["question_id"], row["db_id"]
                expected = read_gold(row, databases, budgets)
                record = CaseFiles(
                    output, ident, condition, profile["name"], settings_digest
                ).read_result()
                records.append(record)
                if record["status"] == "predicted":
                    write_text(output / "sql" / f"{ident}.sql", record["sql"] + "\n")
                row_score = score_record(record, expected, databases[db_id], budgets)
                if (
                    manifest["smoke_cases"] is not None
                    and "predicted_rows" not in row_score
                ):
                    raise ProtocolError(
                        f"Smoke prediction did not execute for question {ident}"
                    )
                scores.append({**row_score, "db_id": db_id})
        except (OSError, ValueError) as error:
            write_json(
                summary_path, {**identity, "status": "blocked", "error": str(error)}
            )
            raise
        score_path = output / "scores.json"
        write_json(score_path, scores)
        summary = {
            **identity,
            **summarize(scores, records),
            "status": "completed",
            "scores_sha256": digest(score_path),
        }
        write_json(summary_path, summary)
        return summary
