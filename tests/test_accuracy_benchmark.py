"""Evaluation integrity: no gold input, no missing-prediction denominator tricks."""

import json
import sqlite3
from argparse import Namespace
from contextlib import nullcontext

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


@pytest.mark.asyncio
@pytest.mark.parametrize("clarify", [False, True, "quota"])
async def test_review_is_measured_and_does_not_override_clarification(
    tmp_path, monkeypatch, clarify
):
    from bench.spider2 import evaluate

    db = tmp_path / "test.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE items(value INTEGER)")
        conn.execute("INSERT INTO items VALUES (7)")
    directory = tmp_path / "out"
    for sub in ("sql", "csv", "records"):
        (directory / sub).mkdir(parents=True)
    prompts = []

    async def loop(ctx, prompt):
        prompts.append(prompt)
        if clarify == "quota":
            raise evaluate.ProviderQuotaExhausted("quota")
        if clarify:
            return "Which business definition should apply?"
        return "```sql\nSELECT value FROM items\n```"

    monkeypatch.setattr(evaluate, "agent_loop", loop)
    args = Namespace(
        call_timeout=1,
        max_calls=8,
        sql_timeout=1,
        case_timeout=10,
        max_rows=10,
        review_rounds=1,
    )
    with (
        pytest.raises(evaluate.ProviderQuotaExhausted)
        if clarify == "quota"
        else nullcontext()
    ):
        await evaluate.run_case(
            {"instance_id": "case", "db": "test", "question": "List item values"},
            tmp_path,
            tmp_path,
            directory,
            {"name": "test", "model": "fake"},
            "not-a-key",
            args,
        )
    record = json.loads((directory / "records/case.json").read_text())
    assert len(prompts) == (1 if clarify else 2)
    assert record["status"] == ("failed" if clarify else "predicted")
    if clarify == "quota":
        assert record["error_type"] == "ProviderQuotaExhausted"
    else:
        assert record["initial_answer"]
    if not clarify:
        assert "List item values" in prompts[1]
        assert len(record["reviews"]) == 1
        assert record["rows"] == 1


@pytest.mark.parametrize(
    "sql",
    [
        "-- explanation\nSELECT 1",
        "/* note */ WITH x AS (SELECT 1) SELECT * FROM x",
        "SELECT '--literal' AS value",
    ],
)
def test_final_sql_accepts_comments_without_rewriting_query(sql):
    from bench.spider2.evaluate import final_sql

    assert final_sql("```sql\n" + sql + "\n```") == sql


def test_final_sql_still_rejects_commented_write():
    from bench.spider2.evaluate import final_sql

    with pytest.raises(ValueError, match="SELECT/WITH"):
        final_sql("```sql\n-- note\nDELETE FROM items\n```")


def test_schema_input_is_pinned_and_separate_from_gold(tmp_path):
    from pathlib import Path

    from bench.accuracy.evaluate import load_schema_documents

    docs = load_schema_documents(Path("bench/accuracy/bird-schema.json"), "bird")
    assert len(docs) == 11 and "CREATE TABLE" in docs["financial"]
    spider = load_schema_documents(
        Path("bench/accuracy/spider2-schema.json"), "spider2"
    )
    assert "sample_rows" in spider["Baseball"]
    assert load_schema_documents(None, "bird") == {}
    annotated = tmp_path / "gold.json"
    annotated.write_text('{"SQL": "SELECT 42"}')
    with pytest.raises(ValueError, match="pinned"):
        load_schema_documents(annotated, "bird")
    with pytest.raises(ValueError, match="pinned"):
        load_schema_documents(Path("bench/accuracy/bird-schema.json"), "spider2")


def test_official_value_descriptions_are_reproducible_and_pinned(tmp_path):
    import json
    from pathlib import Path

    from bench.accuracy.build_bird_descriptions import build_documents
    from bench.accuracy.evaluate import load_schema_documents

    root = Path("bench/accuracy")
    baseline = load_schema_documents(root / "bird-schema.json", "bird")
    enriched = load_schema_documents(root / "bird-schema-with-values.json", "bird")
    assert enriched == build_documents(root / "bird-value-descriptions", baseline)
    assert all(enriched[db].startswith(text) for db, text in baseline.items())
    assert "0: negative (no thrombosis)" in enriched["thrombosis_prediction"]
    assert "If the weight_kg is NULL or 0" in enriched["superhero"]
    enriched["superhero"] += "\nSELECT 42 -- injected answer"
    modified = tmp_path / "modified.json"
    modified.write_text(json.dumps(enriched))
    with pytest.raises(ValueError, match="pinned"):
        load_schema_documents(modified, "bird")


def test_description_builder_rejects_changed_source(tmp_path):
    import json

    from bench.accuracy.build_bird_descriptions import build_documents

    (tmp_path / "table.csv").write_text("modified")
    (tmp_path / "provenance.json").write_text(
        json.dumps({"files": {"table.csv": "not-the-file-hash"}})
    )
    with pytest.raises(ValueError, match="Official description changed"):
        build_documents(tmp_path, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("quota", [True, False])
async def test_account_quota_stops_without_retry_but_rate_limit_can_retry(
    monkeypatch, quota
):
    import httpx

    litellm = pytest.importorskip("litellm")
    from bench.spider2.providers import MeasuredLLM, ProviderQuotaExhausted

    attempts = []

    async def api(**kwargs):
        attempts.append(kwargs)
        exc = litellm.RateLimitError(
            message="redacted", model="test", llm_provider="openai"
        )
        exc.body = None
        exc.response = httpx.Response(
            429,
            json={
                "error": {
                    "code": "credit_balance_exhausted"
                    if quota
                    else "rate_limit_exceeded",
                    "type": "insufficient_quota" if quota else "requests",
                }
            },
        )
        raise exc

    monkeypatch.setattr(litellm, "acompletion", api)
    llm = MeasuredLLM(
        {
            "name": "test",
            "model": "openai/test",
            "transport_retries": 1,
            "retry_delay_seconds": 0,
        },
        "not-a-key",
        1,
        3,
    )
    with pytest.raises(ProviderQuotaExhausted if quota else litellm.RateLimitError):
        await llm.complete([])
    assert len(attempts) == (1 if quota else 2)
    if quota:
        assert llm.calls[0]["error_category"] == "quota_exhausted"
        assert not llm.calls[0].get("retry_scheduled")


@pytest.mark.asyncio
async def test_quota_aborts_batch_before_next_case(tmp_path, monkeypatch):
    from bench.accuracy import evaluate
    from bench.spider2.providers import ProviderQuotaExhausted

    db = tmp_path / "db.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t(x)")
    manifest = tmp_path / "manifest.json"
    write_json(
        manifest,
        {
            "suite": "bird",
            "db_sha256": {"db": digest(db)},
            "cases": [
                {"instance_id": str(i), "db": "db", "question": "Count rows"}
                for i in range(3)
            ],
        },
    )
    profiles = tmp_path / "profiles.json"
    write_json(
        profiles, [{"name": "test", "model": "fake", "api_key_env": "BENCH_TEST_KEY"}]
    )
    monkeypatch.setenv("BENCH_TEST_KEY", "not-a-key")
    monkeypatch.setattr(evaluate.subprocess, "run", lambda *a, **k: None)
    started = []

    async def case(case, *args):
        started.append(case["instance_id"])
        raise ProviderQuotaExhausted("quota")

    monkeypatch.setattr(evaluate, "run_case", case)
    out = tmp_path / "out"
    args = Namespace(
        manifest=manifest,
        db_dir=tmp_path,
        root=tmp_path,
        schema_docs=None,
        split="all",
        product_commit="test",
        output=out,
        profiles=profiles,
        env_file=None,
        max_calls=2,
        call_timeout=1,
        case_timeout=2,
        sql_timeout=1,
        max_rows=10,
        concurrency=1,
        review_rounds=0,
    )
    with pytest.raises(ProviderQuotaExhausted):
        await evaluate.run(args)
    assert started == ["0"]
    status = json.loads((out / "test/status.json").read_text())
    assert status["status"] == "aborted" and status["reason"] == "quota_exhausted"
    assert not (out / "summary.json").exists()
