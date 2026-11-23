"""Benchmark protocol regressions using local fixtures and a scripted transport."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import replace
from types import SimpleNamespace

import pytest

from bench.bird import dataset, evaluate
from bench.bird.dataset import load_cases, prepare, verify_databases
from bench.bird.protocol import (
    BUDGETS,
    DEFAULT_PROFILES,
    git,
    load_profile,
    question_order,
    settings,
)
from bench.bird.records import CaseFiles
from bench.bird.runner import run_case, run_evaluation
from bench.bird.scoring import QueryFailure, calculate_ex, check_gold, read_rows, score
from bench.common.io import ProtocolError, digest, json_digest, read_json
from bench.common.submissions import SubmissionError, final_sql


@pytest.fixture
def prepared(tmp_path, monkeypatch):
    source = tmp_path / "source.jsonl"
    rows = []
    for ident, question, evidence in (
        (11, "What is the first set?", "Use the displayed point value."),
        (24, "What is the second set?", "Keep the original measurement."),
        (32, "What is the third set?", " "),
    ):
        rows.append(
            {
                "question_id": ident,
                "db_id": "test",
                "difficulty": "challenging",
                "question": question,
                "evidence": evidence,
                "SQL": "SELECT x FROM fixture",
            }
        )
    source.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
    cohort = tmp_path / "cohort.json"
    cohort.write_text(
        json.dumps(
            {
                "source_count": 3,
                "source_sha256": digest(source),
                "question_ids": [11, 24],
                "db_ids": ["test"],
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(dataset, "COHORT_PATH", cohort)
    db = tmp_path / "test.sqlite"
    with sqlite3.connect(db) as connection:
        connection.executescript(
            "CREATE TABLE fixture (x INTEGER, y TEXT);"
            "INSERT INTO fixture VALUES (1,'a'),(2,'b'),(2,'b');"
        )
    path = tmp_path / "prepared"
    manifest = prepare(source, tmp_path, "fixture", path)
    budgets = replace(BUDGETS, case_timeout=5, pilot_questions=2)
    return SimpleNamespace(
        source=source,
        db=db,
        path=path,
        manifest=manifest,
        budgets=budgets,
        output=tmp_path / "case-run",
    )


def test_frozen_question_ids_and_deterministic_order():
    cohort = dataset.frozen_cohort()
    ids = cohort["question_ids"]
    assert len(ids) == len(set(ids)) == 365
    assert len(cohort["db_ids"]) == 11
    ordered = question_order(ids)
    assert ordered == question_order(list(reversed(ids)))
    assert sorted(ordered) == ids


def test_prepare_projects_gold_out_and_checks_input_hashes(prepared):
    assert [
        case.question_id for case in load_cases(prepared.path, prepared.manifest)
    ] == [11, 24]
    assert "SQL" not in (prepared.path / "cases.jsonl").read_text(encoding="utf-8")
    assert (
        check_gold(
            prepared.path, prepared.source, prepared.db.parent, prepared.budgets
        )["checked"]
        == 2
    )
    with sqlite3.connect(prepared.db) as connection:
        connection.execute("INSERT INTO fixture VALUES (5, 'changed')")
    with pytest.raises(ProtocolError, match="snapshot hash changed"):
        verify_databases(prepared.db.parent, prepared.manifest)


def test_dry_run_cli_uses_scripted_transport_and_cannot_be_scored(
    prepared, monkeypatch
):
    monkeypatch.delenv("GEMINI_API_KEY", raising=False)
    monkeypatch.setattr(evaluate, "BUDGETS", prepared.budgets)
    assert (
        evaluate.main(
            [
                "run",
                "--dry-run",
                "--product-commit",
                "HEAD",
                "--condition",
                "B",
                "--profile",
                "google-gemini25-flash",
                "--prepared",
                str(prepared.path),
                "--db-dir",
                str(prepared.db.parent),
                "--output",
                str(prepared.output),
            ]
        )
        == 0
    )
    snapshot = read_json(prepared.output / "settings.json")
    assert snapshot["mode"] == "dry-run"
    assert snapshot["product_commit"] == git("rev-parse", "HEAD")
    assert snapshot["profile"]["model"] == "gemini/gemini-2.5-flash"
    assert read_json(prepared.output / "status.json")["completed"] == 2
    with pytest.raises(ProtocolError, match="cannot be scored"):
        score(prepared.output, prepared.source, prepared.db.parent)


def test_scoring_uses_row_sets_and_read_only_sqlite(prepared):
    budgets = prepared.budgets
    assert calculate_ex([(None,), (2,), (2,)], [(2,), (None,)]) == 1
    assert calculate_ex([(1, "a")], [("a", 1)]) == 0
    assert calculate_ex([(1,)], [("1",)]) == 0
    with pytest.raises(QueryFailure, match="row_limit_exceeded"):
        read_rows(prepared.db, "SELECT x FROM fixture", replace(budgets, max_rows=2))
    for sql in (
        "DELETE FROM fixture",
        "ATTACH DATABASE ':memory:' AS other",
        "SELECT 1; SELECT 2",
    ):
        with pytest.raises(QueryFailure, match="sql_error"):
            read_rows(prepared.db, sql, budgets)
    with pytest.raises(QueryFailure, match="sql_timeout"):
        read_rows(
            prepared.db,
            "WITH RECURSIVE s(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM s) SELECT MAX(i) FROM s",
            replace(budgets, sql_timeout=0.02),
        )


@pytest.mark.parametrize(
    "answer, reason",
    [
        ("What do you mean?", "missing_sql"),
        ("```sql\nDELETE FROM fixture\n```", "invalid_final_sql"),
        ("```sql\nSELECT 1\n```\n```sql\nSELECT 2\n```", "multiple_sql_blocks"),
    ],
)
def test_only_one_fenced_read_only_sql_is_a_submission(answer, reason):
    with pytest.raises(SubmissionError, match=reason):
        final_sql(answer)


def response(content=None, *, tool_name=None, arguments=None, model="fixture-model"):
    message = {"role": "assistant", "content": content}
    if tool_name:
        message["tool_calls"] = [
            {
                "id": tool_name + "-1",
                "type": "function",
                "function": {"name": tool_name, "arguments": json.dumps(arguments)},
            }
        ]
    raw = {
        "id": "fixture-response",
        "model": model,
        "usage": {"prompt_tokens": 10, "completion_tokens": 2},
        "choices": [
            {"message": message, "finish_reason": "tool_calls" if tool_name else "stop"}
        ],
    }
    return SimpleNamespace(model_dump=lambda: raw)


def config(
    prepared,
    transport,
    stop,
    *,
    condition="A",
    profile_name="openai-gpt41-mini",
    resume=False,
    snapshot=None,
):
    snapshot = snapshot or {
        **settings(
            prepared.manifest,
            prepared.budgets,
            "test",
            condition=condition,
            profile=load_profile(DEFAULT_PROFILES, profile_name),
        ),
        "run_id": prepared.output.name,
    }
    return run_evaluation(
        prepared.output,
        prepared.manifest,
        snapshot,
        load_cases(prepared.path, prepared.manifest),
        {"test": prepared.db},
        transport,
        "mock",
        stop,
        resume=resume,
    )


@pytest.mark.asyncio
async def test_call_budget_and_writable_tool_isolation(prepared):
    case = load_cases(prepared.path, prepared.manifest)[0]
    budgets = replace(prepared.budgets, max_calls=2)
    profile = load_profile(DEFAULT_PROFILES, "openai-gpt41-mini")
    files = CaseFiles(prepared.output, case.question_id, "A", profile["name"], "test")

    async def transport(**kwargs):
        assert [f["function"]["name"] for f in kwargs["tools"]] == [
            "run_sql",
            "explore_schema",
        ]
        return response(
            tool_name="term_custom", arguments={"term": "new", "definition": "x"}
        )

    record = await run_case(
        case, prepared.db, files, budgets, profile, transport, "mock"
    )
    assert record["first_request"]["knowledge_entries"] == 1
    assert len(record["calls"]) == 2
    assert all(tool["is_error"] for tool in record["tools"])
    assert record["failure"]["reason"] == "call_budget_exhausted"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "condition,profile_name,role,bad_submit,failure,reasoning_effort",
    [
        (
            "A",
            "openai-gpt41-mini",
            "system",
            "```sql\nSELECT y FROM fixture\n```",
            "result_mismatch",
            None,
        ),
        ("D", "openai-gpt41", None, "Which value?", "missing_sql", None),
        (
            "B",
            "anthropic-sonnet45",
            "user",
            "```sql\nSELECT missing FROM fixture\n```",
            "sql_error",
            None,
        ),
        (
            "A",
            "openai-gpt6-luna",
            "system",
            "```sql\nSELECT y FROM fixture\n```",
            "result_mismatch",
            "none",
        ),
    ],
)
async def test_scores_one_condition_and_model_in_isolation(
    prepared, condition, profile_name, role, bad_submit, failure, reasoning_effort
):
    requests = []
    profile = load_profile(DEFAULT_PROFILES, profile_name)

    async def transport(**kwargs):
        requests.append(kwargs)
        messages = kwargs["messages"]
        if len(messages) == 2:
            return response(tool_name="explore_schema", arguments={"table": "fixture"})
        if len(messages) == 4:
            return response(
                tool_name="run_sql", arguments={"sql": "SELECT x FROM fixture"}
            )
        user = messages[1]["content"]
        return response(
            (
                "```sql\nSELECT DISTINCT x FROM fixture ORDER BY x DESC\n```"
                if "first" in user
                else bad_submit
            ),
            model="provider-returned-id",
        )

    complete = await config(
        prepared,
        transport,
        asyncio.Event(),
        condition=condition,
        profile_name=profile_name,
    )
    assert complete["completed"] == 2
    assert len(requests) == 6
    assert all(
        request["model"] == profile["model"]
        and "api_base" not in request
        and request.get("reasoning_effort") == reasoning_effort
        and ("reasoning_effort" in request) == (reasoning_effort is not None)
        and request["num_retries"] == 0
        and request["max_tokens"] == 4096
        for request in requests
    )
    record = read_json(prepared.output / "records/11.json")
    assert record["first_request"]["evidence_role"] == role
    assert [tool["name"] for tool in record["tools"]] == ["explore_schema", "run_sql"]
    assert all(not tool["is_error"] for tool in record["tools"])
    assert record["calls"][-1]["response_model"] == "provider-returned-id"
    summary = score(prepared.output, prepared.source, prepared.db.parent)
    assert summary == read_json(prepared.output / "summary.json")
    assert summary["condition"] == condition and summary["profile"] == profile_name
    assert summary["total"] == 2 and summary["ex_pct"] == 50
    assert summary["failures"] == {failure: 1}
    scores = read_json(prepared.output / "scores.json")
    assert [(row["question_id"], row["ex"]) for row in scores] == [(11, 1), (24, 0)]
    assert all(
        row["condition"] == condition and row["profile"] == profile_name
        for row in scores
    )


@pytest.mark.asyncio
async def test_resume_preserves_completed_and_interrupted_requests(prepared):
    prepared.budgets = replace(prepared.budgets, concurrency=1)
    stop = asyncio.Event()
    calls = 0

    async def pause(**kwargs):
        stop.set()
        return await answer(**kwargs)

    async def answer(**kwargs):
        nonlocal calls
        calls += 1
        return response("```sql\nSELECT 1\n```")

    first = await config(prepared, pause, stop)
    assert first["status"] == "interrupted" and first["completed"] == calls == 1
    snapshot = read_json(prepared.output / "settings.json")
    first_id, second_id = snapshot["question_order"]
    existing = CaseFiles(
        prepared.output,
        first_id,
        "A",
        snapshot["profile"]["name"],
        json_digest(snapshot),
    )
    original = existing.record_path.read_bytes()
    orphan = replace(existing, question_id=second_id)
    orphan.save_attempt({}, [{"status": "started", "usage": None}])
    for change in (
        {"condition": "B"},
        {"profile": load_profile(DEFAULT_PROFILES, "openai-gpt41")},
    ):
        with pytest.raises(ProtocolError, match="original manifest and settings"):
            await config(
                prepared,
                answer,
                asyncio.Event(),
                resume=True,
                snapshot={**snapshot, **change},
            )
    completed = await config(prepared, answer, asyncio.Event(), resume=True)
    assert completed["completed"] == 2 and calls == 2
    assert existing.record_path.read_bytes() == original
    assert orphan.read_result()["status"] == "predicted"
    interrupted = read_json(
        prepared.output / "interruptions" / str(second_id) / "1.json"
    )
    assert interrupted["calls"] == [{"status": "started", "usage": None}]
    assert not orphan.attempt_path.exists()
    await config(prepared, answer, asyncio.Event(), resume=True)
    assert calls == 2
