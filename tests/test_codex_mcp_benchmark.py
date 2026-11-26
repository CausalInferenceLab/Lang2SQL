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
            "result": {"content": [{"type": "text", "text": "1 row(s):\n1"}]},
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
    # Preserve the existing input-query contract, but reject invented limits.
    assert executed_sql("```sql\nSELECT 1\n```", [call]) == "SELECT 1"
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
