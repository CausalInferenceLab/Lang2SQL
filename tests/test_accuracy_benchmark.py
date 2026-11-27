"""Evaluation integrity: no gold input, no missing-prediction denominator tricks."""

import json
import sqlite3
from argparse import Namespace

import pytest

from bench.accuracy.evaluate import (
    digest,
    load_bird_comparator,
    query_rows,
    score,
    split_manifest,
    validate_inputs,
    write_json,
)


def test_split_is_fixed_and_disjoint():
    cases = [{"instance_id": f"bird{i:03d}"} for i in range(500)]
    a = split_manifest({"cases": cases}, 20)
    b = split_manifest({"cases": list(reversed(cases))}, 20)
    assert a["development_ids"] == b["development_ids"]
    assert not set(a["development_ids"]) & set(a["holdout_ids"])
    assert len(a["holdout_ids"]) == 480


def test_gold_field_rejected_before_inference(tmp_path):
    with pytest.raises(ValueError, match="gold"):
        validate_inputs(
            {"cases": [{"instance_id": "bird001", "SQL": "SELECT 1"}]}, tmp_path
        )


def test_queries_read_only_and_bounded(tmp_path):
    db = tmp_path / "test.sqlite"
    with sqlite3.connect(db) as c:
        c.executescript("CREATE TABLE t(x); INSERT INTO t VALUES(1),(2);")
    assert query_rows(db, "SELECT x FROM t", 1, 2) == [(1,), (2,)]
    with pytest.raises(sqlite3.DatabaseError):
        query_rows(db, "DELETE FROM t", 1, 10)
    with pytest.raises(ValueError, match="budget"):
        query_rows(db, "SELECT x FROM t", 1, 1)


def test_official_function_only_and_hash_guard(tmp_path):
    source = tmp_path / "official.py"
    source.write_text(
        "raise RuntimeError('do not run module')\ndef calculate_ex(a,b):\n return int(set(a)==set(b))\n"
    )
    comparator = load_bird_comparator(source, digest(source))
    assert comparator([(1,), (1,), (2,)], [(2,), (1,)]) == 1
    assert comparator([(1,)], [(2,)]) == 0
    with pytest.raises(ValueError, match="changed"):
        load_bird_comparator(source, "wrong")


def test_failed_predictions_stay_in_denominator(tmp_path):
    db = tmp_path / "db.sqlite"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t(x)")
    questions = tmp_path / "gold.jsonl"
    questions.write_text(
        "\n".join(json.dumps({"question_id": i, "SQL": "SELECT 1"}) for i in range(2))
    )
    evaluator = tmp_path / "official.py"
    evaluator.write_text("def calculate_ex(a,b):\n return int(set(a)==set(b))\n")
    out = tmp_path / "run"
    cases = [
        {"instance_id": f"bird{i:03d}", "db": "db", "question": "one"} for i in range(2)
    ]
    write_json(
        out / "manifest.json",
        {
            "suite": "bird",
            "cases": cases,
            "db_sha256": {"db": digest(db)},
            "question_file_sha256": digest(questions),
            "evaluator_sha256": digest(evaluator),
        },
    )
    write_json(out / "settings.json", {"profiles": [{"name": "model"}]})
    write_json(out / "model/status.json", {"status": "completed"})
    write_json(
        out / "model/records/bird000.json", {"status": "predicted", "sql": "SELECT 1"}
    )
    write_json(out / "model/records/bird001.json", {"status": "failed"})
    score(
        Namespace(
            output=out,
            db_dir=tmp_path,
            questions=questions,
            evaluator=evaluator,
            sql_timeout=1,
            max_rows=10,
        )
    )
    result = json.loads((out / "summary.json").read_text())[0]
    assert (result["correct"], result["total"], result["accuracy"]) == (1, 2, 0.5)
    (out / "model/records/bird001.json").unlink()
    with pytest.raises(FileNotFoundError):
        score(
            Namespace(
                output=out,
                db_dir=tmp_path,
                questions=questions,
                evaluator=evaluator,
                sql_timeout=1,
                max_rows=10,
            )
        )


@pytest.mark.asyncio
async def test_transport_retry_is_logged_bounded_and_does_not_retry_answers(
    monkeypatch,
):
    litellm = pytest.importorskip("litellm")

    from bench.spider2.providers import MeasuredLLM
    from lang2sql.core.types import Message, Role

    attempts = []

    async def api(**kwargs):
        attempts.append(kwargs)
        if len(attempts) == 1:
            raise litellm.Timeout(
                message="redacted", model="test", llm_provider="openai"
            )

        class Response:
            def model_dump(self):
                return {
                    "model": "test",
                    "choices": [{"message": {"content": "answer"}}],
                    "usage": {},
                }

        return Response()

    monkeypatch.setattr(litellm, "acompletion", api)
    profile = {
        "name": "test",
        "model": "openai/test",
        "transport_retries": 1,
        "retry_delay_seconds": 0,
    }
    llm = MeasuredLLM(profile, "not-a-key", 1, 2)
    answer = await llm.complete([Message(role=Role.USER, content="question")])
    assert answer.content == "answer"
    assert [c["status"] for c in llm.calls] == ["error", "ok"]
    assert attempts[0]["messages"] == attempts[1]["messages"]
    with pytest.raises(RuntimeError, match="budget"):
        await llm.complete([])
    assert len(attempts) == 2
