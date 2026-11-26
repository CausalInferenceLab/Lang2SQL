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
