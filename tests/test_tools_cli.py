"""Use the actual direct CLI process, independently of the MCP transport."""

import json
import os
import sqlite3
import subprocess
import sys

import pytest

pytest.importorskip("jsonschema")


def invoke(tmp_path, *args):
    db = tmp_path / "analytics.db"
    with sqlite3.connect(db) as connection:
        connection.execute("CREATE TABLE IF NOT EXISTS orders (amount INTEGER)")
    env = {**os.environ, "LANG2SQL_DB_URL": f"sqlite:///{db}"}
    env.pop("LANG2SQL_DB_SCHEMA", None)
    env.pop("OPENAI_API_KEY", None)
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "lang2sql.frontends.cli.tools",
            "--user-id",
            "analyst",
            "--org-id",
            "demo",
            "--store",
            str(tmp_path / "metadata.db"),
            *args,
        ],
        check=False,
        env=env,
        capture_output=True,
        text=True,
        timeout=20,
    )
    return result.returncode, json.loads(result.stdout)


def test_catalog_and_real_sql(tmp_path):
    status, result = invoke(tmp_path, "list")
    assert status == 0
    assert "run_sql" in {tool["name"] for tool in result["tools"]}
    status, result = invoke(
        tmp_path,
        "call",
        "run_sql",
        "--args",
        '{"sql":"SELECT COUNT(*) AS answer FROM orders"}',
    )
    assert status == 0 and "answer\n---\n0" in result["content"]


def test_sqlite_error_reaches_cli(tmp_path):
    status, result = invoke(
        tmp_path, "call", "run_sql", "--args", '{"sql":"SELECT missing FROM orders"}'
    )
    assert status == 1 and result["is_error"]
    assert "no such column: missing" in result["content"]


def test_explore_schema_preserves_its_public_result_shape(tmp_path):
    status, result = invoke(
        tmp_path, "call", "explore_schema", "--args", '{"tables":["orders"]}'
    )
    assert status == 0
    assert set(result) == {"call_id", "content", "is_error"}
    assert not result["is_error"]
    assert result["content"] == "orders\n- amount: INTEGER"


@pytest.mark.parametrize(
    "args,status",
    [
        (("call", "run_sql", "--args", '{"sql":"DROP TABLE orders"}'), 1),
        (("call", "run_sql", "--args", '{"sql":123}'), 2),
        (("call", "org_setup"), 2),
        (("call", "term_custom", "--args", '{"list":true,"is_admin":true}'), 2),
        (("call", "run_sql", "--args", "not json"), 2),
    ],
)
def test_failures_have_json_and_nonzero_exit(tmp_path, args, status):
    actual, result = invoke(tmp_path, *args)
    assert actual == status and result["is_error"]


def test_batch_retains_memory_and_stops_on_failure(tmp_path):
    calls = [
        {"name": "remember", "arguments": {"text": "exclude refunds"}},
        {"name": "recall_memory", "arguments": {"query": "refund"}},
        {"name": "run_sql", "arguments": {"sql": "DROP TABLE orders"}},
        {"name": "remember", "arguments": {"text": "must not execute"}},
    ]
    source = tmp_path / "calls.json"
    source.write_text(json.dumps(calls))
    status, result = invoke(tmp_path, "batch", "--args-file", str(source))
    assert status == 1 and result["completed"] == 3 and result["requested"] == 4
    assert "exclude refunds" in result["results"][1]["content"]
    status, result = invoke(
        tmp_path, "call", "recall_memory", "--args", '{"query":"refund"}'
    )
    assert status == 0 and result["content"] == "No remembered notes."


def test_batch_validates_before_writes(tmp_path):
    status, result = invoke(
        tmp_path,
        "batch",
        "--args",
        json.dumps(
            [
                {
                    "name": "term_custom",
                    "arguments": {
                        "term": "revenue",
                        "definition": "gross",
                        "layer": "user",
                    },
                },
                {"name": "run_sql", "arguments": {"sql": 123}},
            ]
        ),
    )
    assert status == 2
    status, result = invoke(tmp_path, "call", "term_custom", "--args", '{"list":true}')
    assert "gross" not in result["content"]
