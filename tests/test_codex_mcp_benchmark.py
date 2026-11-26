"""Final SQL must actually cross the production MCP boundary."""

import pytest

from bench.accuracy.codex_mcp import executed_sql


def event(tool="run_sql", status="completed", server="lang2sql", sql="SELECT 1"):
    return {
        "type": "item.completed",
        "item": {
            "type": "mcp_tool_call",
            "server": server,
            "tool": tool,
            "status": status,
            "error": None,
            "arguments": {"sql": sql},
            "result": {"content": [{"type": "text", "text": "1 row(s):\n1\nSQL: " + sql}]},
        },
    }


def test_mcp_executed_sql_required():
    answer = "```sql\nSELECT 1\n```"
    assert executed_sql(answer, [event()]) == "SELECT 1"
    for events in [
        [],
        [event(status="failed")],
        [event(sql="SELECT 2")],
        [event(server="other")],
        [event(tool="remember")],
        [event(), {"type": "item.completed", "item": {"type": "command_execution"}}],
    ]:
        with pytest.raises(ValueError):
            executed_sql(answer, events)


def test_clarification_never_falls_back_to_diagnostic_sql():
    with pytest.raises(ValueError):
        executed_sql("Which business definition should I use?", [event()])


def test_optional_statement_terminator_does_not_invalidate_execution():
    assert executed_sql("```sql\nSELECT 1;\n```", [event()]) == "SELECT 1;"
    with pytest.raises(ValueError):
        executed_sql("```sql\nSELECT 'a b';\n```", [event(sql="SELECT 'a;b'")])


def test_sqlite_bytecode_accepts_comment_formatting_but_not_different_values(tmp_path):
    import sqlite3

    db = tmp_path / "db.sqlite"
    with sqlite3.connect(db) as c:
        c.execute("CREATE TABLE t(x)")
    sql = "SELECT x, 'a--b' FROM t WHERE x > 1"
    answer = "```sql\nselect x, 'a--b' from t -- explanation\nwhere x > 1;\n```"
    assert executed_sql(answer, [event(sql=sql)], db=db).startswith("select")
    with pytest.raises(ValueError):
        executed_sql(answer.replace("a--b", "a--c"), [event(sql=sql)], db=db)
    with pytest.raises(ValueError):
        executed_sql(answer.replace("> 1", "> 2"), [event(sql=sql)], db=db)


def test_result_display_keeps_numeric_and_text_distinguishable():
    from lang2sql.tools.run_sql import _render_rows

    output = _render_rows(
        "SELECT ...", [{"numeric": 0.9833, "formatted": "0.9833", "missing": None}]
    )
    assert "numeric=float" in output and "formatted=str" in output
    assert "missing=NoneType" in output


@pytest.mark.parametrize(
    "response", ["BLOCKED: permission denied", "NEEDS CONFIRMATION: proceed?"]
)
def test_completed_tool_transport_is_not_proof_sql_executed(response):
    call = event()
    call["item"]["result"]["content"][0]["text"] = response
    with pytest.raises(ValueError):
        executed_sql("```sql\nSELECT 1\n```", [call])


@pytest.mark.parametrize("prefix", ["1 row(s):\n1", "(0 rows)"])
def test_server_rewritten_sql_is_execution_evidence(prefix):
    call = event(sql="SELECT 1")
    call["item"]["result"]["content"][0]["text"] = prefix + "\nSQL: SELECT 1 LIMIT 1000"
    assert executed_sql("```sql\nSELECT 1 LIMIT 1000\n```", [call]) == "SELECT 1 LIMIT 1000"
    # A submitted alias binds to the actual rewrite, not an unexecuted unlimited query.
    assert executed_sql("```sql\nSELECT 1\n```", [call]) == "SELECT 1 LIMIT 1000"
    for sql in ["SELECT 1 LIMIT 2000"]:
        with pytest.raises(ValueError):
            executed_sql("```sql\n" + sql + "\n```", [call])


def test_review_evidence_excludes_evaluation_feedback():
    import json

    from bench.accuracy.codex_mcp import review_evidence

    record = {"answer": "previous SQL", "score": 0, "gold_sql": "SECRET_GOLD",
              "status": "failed", "error_message": "GRADER_FEEDBACK",
              "mcp_calls": [event()["item"]]}
    payload = review_evidence(record)
    assert "SECRET_GOLD" not in payload and "GRADER_FEEDBACK" not in payload
    data = json.loads(payload)
    assert set(data) == {"previous_answer", "tool_observations"}
    assert data["tool_observations"][0]["arguments"]["sql"] == "SELECT 1"


@pytest.mark.parametrize("invalid", [None, "incomplete", "missing_score", "duplicate_score", "changed_question", "unknown_case", "nonbinary"])
def test_failed_case_selection_preserves_inputs_and_rejects_incomplete_grades(tmp_path, invalid):
    from bench.accuracy.codex_mcp import failed_cases
    from bench.spider2.evaluate import write_json

    cases = [{"instance_id": name, "question": "question " + name, "db": "demo"}
             for name in ("correct", "wrong", "execution_failed")]
    scores = [{"instance_id": case["instance_id"], "score": int(i == 0)}
              for i, case in enumerate(cases)]
    write_json(tmp_path / "manifest.json", {"cases": cases})
    write_json(tmp_path / "codex-mcp/status.json", {"status": "running" if invalid == "incomplete" else "completed"})
    if invalid == "missing_score":
        scores.pop()
    elif invalid == "duplicate_score":
        scores.append(scores[0])
    elif invalid == "unknown_case":
        scores[-1]["instance_id"] = "other"
    elif invalid == "nonbinary":
        scores[-1]["score"] = 0.5
    elif invalid == "changed_question":
        cases[-1]["question"] = "different question"
    write_json(tmp_path / "codex-mcp/scores.json", scores)
    if invalid:
        with pytest.raises(ValueError):
            failed_cases(cases, tmp_path)
    else:
        selected = failed_cases(cases, tmp_path)
        assert selected == cases[1:]
        assert all(set(case) == {"instance_id", "question", "db"} for case in selected)
        write_json(tmp_path / "codex-mcp/scores.json", [{**row, "score": 1} for row in scores])
        assert failed_cases(cases, tmp_path) == []

@pytest.mark.parametrize('invalid', [None, 'running', 'missing', 'changed', 'duplicate_case', 'database'])
def test_exclude_completed_passes_from_failed_backlog(tmp_path, invalid):
    from bench.accuracy.codex_mcp import failed_cases
    from bench.spider2.evaluate import write_json

    cases = [{'instance_id': name, 'question': 'question ' + name, 'db': 'demo'}
             for name in ['repaired', 'still_wrong']]
    base, repair = tmp_path / 'base', tmp_path / 'repair'
    for directory, selected, scores in [
        (base, cases, [0, 0]), (repair, cases[:1], [1]),
    ]:
        write_json(directory / 'manifest.json', {'cases': selected, 'db_sha256': {'demo': 'same-db'}})
        write_json(directory / 'codex-mcp/status.json', {'status': 'completed'})
        write_json(directory / 'codex-mcp/scores.json', [
            {'instance_id': c['instance_id'], 'score': score}
            for c, score in zip(selected, scores)
        ])
    if invalid == 'running':
        write_json(repair / 'codex-mcp/status.json', {'status': 'running'})
    elif invalid == 'missing':
        write_json(repair / 'codex-mcp/scores.json', [])
    elif invalid in {'changed', 'duplicate_case', 'database'}:
        import json
        path = repair / 'manifest.json'
        data = json.loads(path.read_text())
        if invalid == 'changed': data['cases'][0]['question'] = 'different question'
        elif invalid == 'duplicate_case': data['cases'].append(data['cases'][0])
        else: data['db_sha256']['demo'] = 'different-db'
        write_json(path, data)
    if invalid:
        with pytest.raises(ValueError):
            failed_cases(cases, base, exclude_passed_sources=[repair], db_sha256={'demo': 'same-db'})
    else:
        selected = failed_cases(cases, base, exclude_passed_sources=[repair], db_sha256={'demo': 'same-db'})
        assert selected == cases[1:]
        assert set(selected[0]) == {'instance_id', 'question', 'db'}
        # A failed repair is retained, and an explicit pass exclusion is idempotent.
        write_json(repair / 'codex-mcp/scores.json', [{'instance_id': 'repaired', 'score': 0}])
        assert failed_cases(cases, base, exclude_passed_sources=[repair]) == cases
        write_json(repair / 'codex-mcp/scores.json', [{'instance_id': 'repaired', 'score': 1}])
        assert failed_cases(cases, base, exclude_passed_sources=[repair, repair]) == cases[1:]

@pytest.mark.asyncio
async def test_pass_exclusion_cannot_filter_full_accuracy():
    from types import SimpleNamespace

    from bench.accuracy.codex_mcp import run

    with pytest.raises(ValueError, match='requires --failures-from'):
        await run(SimpleNamespace(exclude_passed_from=['completed'], failures_from=None))


@pytest.mark.asyncio
@pytest.mark.parametrize("split,failed_only,expected,raises", [
    ("development", False, 2, True),
    ("all", True, 1, True),
    ("all", True, 0, False),
    ("all", False, -1, True),
])
async def test_selection_contract_precedes_output_and_inference(
    tmp_path, monkeypatch, split, failed_only, expected, raises
):
    import json
    from types import SimpleNamespace

    from bench.accuracy import codex_mcp as runner

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "suite": "bird", "development_ids": ["first"], "db_sha256": {},
        "cases": [{"instance_id": "first"}, {"instance_id": "second"}],
    }))
    monkeypatch.setattr(runner, "validate_inputs", lambda *a: None)
    monkeypatch.setattr(runner, "load_schema_documents", lambda *a: {})
    monkeypatch.setattr(runner, "failed_cases", lambda *a, **k: [])
    args = SimpleNamespace(
        exclude_passed_from=[], failures_from=tmp_path if failed_only else None,
        manifest=manifest, db_dir=tmp_path, schema_docs=None, split=split,
        expected_cases=expected, output=tmp_path / "run",
    )
    if raises:
        with pytest.raises(ValueError, match="check --split"):
            await runner.run(args)
    else:
        await runner.run(args)
    # No output directory, subprocess metadata lookup or model process is created.
    assert not args.output.exists()


@pytest.mark.asyncio
@pytest.mark.parametrize("timeout", [False, True])
@pytest.mark.parametrize("cache_columns", [False, True])
async def test_runner_preserves_submitted_input_and_partial_trace(tmp_path, monkeypatch, timeout, cache_columns):
    import asyncio
    import hashlib
    import json
    from types import SimpleNamespace

    from bench.accuracy import codex_mcp as runner

    case = {"instance_id": "case1", "db": "fixture", "question": "Return one"}
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"suite": "bird", "cases": [case], "db_sha256": {}}))
    schema = tmp_path / "schema.json"
    schema.write_text("{}")
    monkeypatch.setattr(runner, "validate_inputs", lambda *a: None)
    document = "fixture schema" + runner.DESCRIPTION_MARKER + json.dumps({
        "fixture": [{"original_column_name": "value", "column_description": "Exact source description"}],
    })
    monkeypatch.setattr(runner, "load_schema_documents", lambda *a: {"fixture": document})
    monkeypatch.setattr(runner.subprocess, "check_output", lambda *a, **k: "test-version")
    monkeypatch.setattr(runner, "export_result", lambda *a: 1)
    raw_event = json.dumps({"type": "item.completed", "item": event()["item"]}) + "\n"
    submitted = []

    class Process:
        returncode = None
        killed = False

        async def communicate(self, data):
            submitted.append(data)
            if timeout:
                await asyncio.sleep(60)
            self.returncode = 0

        def kill(self):
            self.killed = True
            self.returncode = -9

        async def wait(self):
            return self.returncode

    process = Process()

    async def spawn(*command, **kwargs):
        if cache_columns:
            from lang2sql.adapters.storage.sqlite_store import SqliteStore
            configured = next(v for v in command if v.startswith("mcp_servers.lang2sql.args="))
            server_args = json.loads(configured.split("=", 1)[1])
            cache = SqliteStore(server_args[server_args.index("--store") + 1])
            try:
                assert "Exact source description" in cache.kv_get("benchmark", "enriched_desc:fixture:value")
            finally:
                cache.close()
        kwargs["stdout"].write(raw_event.encode())
        kwargs["stdout"].flush()
        Path(command[command.index("--output-last-message") + 1]).write_text("```sql\nSELECT 1\n```")
        return process

    from pathlib import Path
    monkeypatch.setattr(runner.asyncio, "create_subprocess_exec", spawn)
    args = SimpleNamespace(
        exclude_passed_from=[], failures_from=None, manifest=manifest, db_dir=tmp_path,
        root=tmp_path, schema_docs=schema, split="all", output=tmp_path / "run",
        model="test-model", reasoning_effort="high", concurrency=1,
        case_timeout=0.01 if timeout else 1, sql_timeout=1, max_rows=100,
        review_source=None, expected_cases=1, cache_official_columns=cache_columns,
    )
    await runner.run(args)
    root = args.output / "codex-mcp"
    assert json.loads((args.output / "settings.json").read_text())["case_selection"] == {
        "split": "all", "input_cases": 1, "selected_cases": 1, "expected_cases": 1,
    }
    record = json.loads((root / "records/case1.json").read_text())
    assert record["status"] == ("failed" if timeout else "predicted")
    assert len(record["mcp_calls"]) == 1
    assert record["events_sha256"] == hashlib.sha256(raw_event.encode()).hexdigest()
    assert record["answer"] == "```sql\nSELECT 1\n```"
    assert record["cli_exit_code"] == (-9 if timeout else 0)
    assert process.killed == timeout
    assert (root / "events/case1.prompt.txt").read_bytes() == submitted[0]
    assert record["prompt_sha256"] == hashlib.sha256(submitted[0]).hexdigest()
    if timeout:
        assert record["error_type"] == "TimeoutError"
        assert "sql" not in record


def test_interrupted_event_tail_preserves_complete_calls(tmp_path, caplog):
    import hashlib
    import json
    from types import SimpleNamespace

    from bench.accuracy.codex_mcp import capture_trace

    events = tmp_path / "events.jsonl"
    payload = json.dumps({"type": "item.completed", "item": event()["item"]}) + '\n{"type":'
    events.write_text(payload)
    record = {"instance_id": "interrupted"}
    complete = capture_trace(record, events, tmp_path / "missing-answer", SimpleNamespace(returncode=-9))
    assert len(complete) == 1
    assert len(record["mcp_calls"]) == 1
    assert record["event_parse_errors"][0]["line"] == 2
    assert record["events_sha256"] == hashlib.sha256(payload.encode()).hexdigest()
    assert record["answer"] == ""
    assert "interrupted" in caplog.text
    assert "JSONDecodeError" in caplog.text
    assert events.read_text() == payload


def test_spawn_failure_has_explicit_absent_trace(tmp_path):
    from bench.accuracy.codex_mcp import capture_trace

    record = {"instance_id": "not-started"}
    assert capture_trace(record, tmp_path / "missing-events", tmp_path / "missing-answer", None) == []
    assert record["cli_exit_code"] is None
    assert record["mcp_calls"] == []
    assert "events_sha256" not in record


def test_export_binds_to_executed_rewrite_not_unbounded_request(tmp_path):
    import sqlite3

    from bench.accuracy.evaluate import query_rows

    db = tmp_path / "data.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t(x)")
        conn.executemany("INSERT INTO t VALUES (?)", [(1,), (2,)])
    call = event(sql="SELECT x FROM t ORDER BY x")
    call["item"]["result"]["content"][0]["text"] = "1 row(s):\nx\n1\nSQL: SELECT x FROM t ORDER BY x LIMIT 1"
    sql = executed_sql("```sql\nSELECT x FROM t ORDER BY x\n```", [call], db=db)
    assert query_rows(db, sql, 1, 100) == [(1,)]
    assert sql.endswith("LIMIT 1")


def test_missing_execution_footer_is_not_execution_proof():
    call = event()
    call["item"]["result"]["content"][0]["text"] = "1 row(s):\n1"
    with pytest.raises(ValueError, match="not executed"):
        executed_sql("```sql\nSELECT 1\n```", [call])


def test_reformatted_request_binds_to_rewrite_without_accepting_other_literals(tmp_path):
    import sqlite3

    db = tmp_path / "data.sqlite"
    with sqlite3.connect(db) as conn:
        conn.execute("CREATE TABLE t(x)")
    call = event(sql="SELECT x FROM t WHERE x > 1")
    call["item"]["result"]["content"][0]["text"] = "(0 rows)\nSQL: SELECT x FROM t WHERE x > 1 LIMIT 2"
    answer = "```sql\nselect x from t -- display formatting\nwhere x > 1;\n```"
    assert executed_sql(answer, [call], db=db) == "SELECT x FROM t WHERE x > 1 LIMIT 2"
    with pytest.raises(ValueError):
        executed_sql(answer.replace("> 1", "> 2"), [call], db=db)


def test_repeated_request_alias_binds_to_latest_successful_execution():
    calls = []
    for limit in [2, 5]:
        call = event(sql="SELECT x FROM t")
        call["item"]["result"]["content"][0]["text"] = f"{limit} row(s):\nSQL: SELECT x FROM t LIMIT {limit}"
        calls.append(call)
    assert executed_sql("```sql\nSELECT x FROM t\n```", calls) == "SELECT x FROM t LIMIT 5"
    # An earlier executed SQL is still valid when the final answer names it explicitly.
    assert executed_sql("```sql\nSELECT x FROM t LIMIT 2\n```", calls) == "SELECT x FROM t LIMIT 2"


@pytest.mark.asyncio
async def test_official_cache_rejects_baseline_metadata_before_model_calls(tmp_path, monkeypatch):
    import json
    from pathlib import Path
    from types import SimpleNamespace

    from bench.accuracy import codex_mcp as runner

    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"suite": "bird", "cases": [], "db_sha256": {}}))
    monkeypatch.setattr(runner, "validate_inputs", lambda *a: None)
    args = SimpleNamespace(
        exclude_passed_from=[], failures_from=None, manifest=manifest, db_dir=tmp_path,
        schema_docs=Path("bench/accuracy/bird-schema.json"), cache_official_columns=True,
        output=tmp_path / "run",
    )
    with pytest.raises(ValueError, match="requires the pinned BIRD"):
        await runner.run(args)
    assert not args.output.exists()
