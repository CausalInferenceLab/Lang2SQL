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
