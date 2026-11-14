"""Evaluator regressions: no gold leakage, read-only execution, fixed denominator."""

import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
import sqlite3
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from bench.spider2.evaluate import final_sql, export_result, digest, score
from bench.spider2.providers import MeasuredLLM
from lang2sql.core.types import Message, Role, ToolCall


@pytest.mark.parametrize(
    "answer",
    [
        "no SQL",
        "```sql\nSELECT 1\n```\n```sql\nSELECT 2\n```",
        "```sql\nDELETE FROM t\n```",
    ],
)
def test_no_guessing_final_submission(answer):
    with pytest.raises(ValueError):
        final_sql(answer)


@pytest.fixture
def db(tmp_path):
    p = tmp_path / "test.sqlite"
    with sqlite3.connect(p) as c:
        c.execute("CREATE TABLE t(x INTEGER)")
        c.execute("INSERT INTO t VALUES(1),(2)")
    return p


def test_export_preserves_empty_columns_and_blocks_writes(db, tmp_path):
    output = tmp_path / "out.csv"
    assert export_result(db, "SELECT x FROM t WHERE 0", output, 1, 10) == 0
    assert output.read_text().strip() == "x"
    with pytest.raises(sqlite3.OperationalError):
        export_result(db, "DELETE FROM t", output, 1, 10)
    assert not output.exists()
    with sqlite3.connect(db) as c:
        assert c.execute("SELECT count(*) FROM t").fetchone()[0] == 2


def test_export_fails_instead_of_truncating(db, tmp_path):
    output = tmp_path / "out.csv"
    with pytest.raises(ValueError):
        export_result(db, "SELECT * FROM t", output, 1, 1)
    assert not output.exists()


def test_query_timeout_interrupts_recursive_sql(db, tmp_path):
    with pytest.raises(sqlite3.OperationalError):
        export_result(
            db,
            "WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n) SELECT SUM(x) FROM n",
            tmp_path / "out.csv",
            0.001,
            10,
        )


def test_official_scoring_counts_failed_predictions(tmp_path):
    # Exact upstream call boundary; only the external evaluator is replaced.
    root = tmp_path / "upstream"
    suite = root / "spider2-lite/evaluation_suite"
    suite.mkdir(parents=True)
    evaluator = suite / "evaluate.py"
    evaluator.write_text("""import sys
class Tee:
 def close(self): pass
sys.stdout=Tee()
def load_jsonl_to_dict(path): return {'local1': {}, 'local2': {}}
def resolve_gold_paths(ident,path): return ([ident], True)
def evaluate_single_exec_result_instance(ident,*args): return {'instance_id': ident, 'score': 1}
""")
    output = tmp_path / "run"
    directory = output / "m"
    (directory / "records").mkdir(parents=True)
    (directory / "status.json").write_text('{"status":"completed"}')
    for ident, status in [("local1", "predicted"), ("local2", "failed")]:
        (directory / "records" / f"{ident}.json").write_text(
            json.dumps(dict(status=status, calls=[], seconds=1))
        )
    (output / "manifest.json").write_text(
        json.dumps(
            dict(
                cases=[{"instance_id": "local1"}, {"instance_id": "local2"}],
                evaluator_sha256=digest(evaluator),
            )
        )
    )
    (output / "settings.json").write_text('{"profiles":[{"name":"m"}]}')
    score(root, output)
    summary = json.loads((output / "summary.json").read_text())[0]
    assert (
        summary["total"] == 2 and summary["correct"] == 1 and summary["accuracy"] == 0.5
    )


def test_provider_metadata_roundtrip_and_call_budget(monkeypatch):
    litellm = pytest.importorskip("litellm")
    seen = []

    async def complete(**kwargs):
        seen.append(kwargs)
        raw = {
            "model": "test",
            "usage": {"prompt_tokens": 3, "completion_tokens": 2},
            "choices": [
                {
                    "finish_reason": "tool_calls",
                    "message": {
                        "role": "assistant",
                        "content": None,
                        "tool_calls": [
                            {
                                "id": "c1",
                                "type": "function",
                                "function": {
                                    "name": "run_sql",
                                    "arguments": '{"sql":"SELECT 1"}',
                                },
                                "provider_specific_fields": {
                                    "thought_signature": "opaque"
                                },
                            }
                        ],
                    },
                }
            ],
        }
        return SimpleNamespace(model_dump=lambda: raw)

    monkeypatch.setattr(litellm, "acompletion", complete)
    llm = MeasuredLLM({"name": "m", "model": "gemini/test"}, "not-real", 1, 2)

    async def check():
        await llm.complete([Message(Role.USER, "q")])
        await llm.complete(
            [
                Message(
                    Role.ASSISTANT,
                    tool_calls=[ToolCall("c1", "run_sql", {"sql": "SELECT 1"})],
                )
            ]
        )
        with pytest.raises(RuntimeError, match="budget"):
            await llm.complete([])

    asyncio.run(check())
    assert (
        seen[1]["messages"][0]["tool_calls"][0]["provider_specific_fields"][
            "thought_signature"
        ]
        == "opaque"
    )
    assert seen[0]["num_retries"] == 0 and llm.calls[0]["usage"]["prompt_tokens"] == 3


def test_database_attachment_is_denied(db, tmp_path):
    with pytest.raises(sqlite3.DatabaseError):
        export_result(
            db, "ATTACH DATABASE ':memory:' AS other", tmp_path / "out.csv", 1, 10
        )


def test_actual_harness_readonly_adapter_writes_final_csv(db, tmp_path, monkeypatch):
    from bench.spider2.evaluate import run_case
    from lang2sql.core.types import Completion

    async def complete(self, *args, **kwargs):
        return Completion(content="```sql\nSELECT SUM(x) AS total FROM t\n```")

    monkeypatch.setattr(MeasuredLLM, "complete", complete)
    output = tmp_path / "output"
    for sub in ("sql", "csv", "records"):
        (output / sub).mkdir(parents=True)
    args = SimpleNamespace(
        call_timeout=1, max_calls=2, sql_timeout=1, case_timeout=5, max_rows=100
    )
    case = {
        "instance_id": "local-test",
        "db": "test",
        "question": "sum x",
        "external_knowledge": None,
    }
    asyncio.run(
        run_case(
            case,
            tmp_path,
            tmp_path,
            output,
            {"name": "test", "model": "test"},
            "unused",
            args,
        )
    )
    record = json.loads((output / "records/local-test.json").read_text())
    assert record["status"] == "predicted"
    assert (output / "csv/local-test.csv").read_text().splitlines() == ["total", "3"]


def test_run_bounds_concurrency_and_marks_missing_keys(tmp_path, monkeypatch):
    from bench.spider2 import evaluate

    cases = [{"instance_id": f"local{i}"} for i in range(5)]
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"cases": cases}))
    profiles = tmp_path / "profiles.json"
    profiles.write_text(
        json.dumps(
            [
                {"name": "available", "model": "test", "api_key_env": "TEST_KEY"},
                {"name": "unavailable", "model": "test", "api_key_env": "MISSING"},
            ]
        )
    )
    monkeypatch.setattr(evaluate, "validate_manifest", lambda *a: None)
    monkeypatch.setattr(
        evaluate.subprocess,
        "check_output",
        lambda a, **kw: "head" if kw.get("text") else b"",
    )
    active = peak = 0
    seen = []

    async def run_case(case, *args):
        nonlocal active, peak
        active += 1
        peak = max(peak, active)
        await asyncio.sleep(0.001)
        seen.append(case["instance_id"])
        active -= 1

    monkeypatch.setattr(evaluate, "run_case", run_case)
    args = SimpleNamespace(
        manifest=manifest,
        profiles=profiles,
        root=tmp_path,
        db_dir=tmp_path,
        output=tmp_path / "out",
        max_calls=8,
        call_timeout=120,
        case_timeout=480,
        sql_timeout=30,
        max_rows=100000,
        concurrency=2,
    )
    asyncio.run(evaluate.run(args, {"TEST_KEY": "fake"}))
    assert peak == 2 and len(seen) == 5
    status = json.loads((args.output / "unavailable/status.json").read_text())
    assert status["status"] == "blocked_missing_key"
    with pytest.raises(ValueError, match="already exists"):
        asyncio.run(evaluate.run(args, {"TEST_KEY": "fake"}))
