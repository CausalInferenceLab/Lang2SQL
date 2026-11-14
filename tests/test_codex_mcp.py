"""MCP boundary, terminal latch and real stdio handshake regressions."""

from __future__ import annotations

import asyncio
import json
import os
import sys

import pytest
from sqlalchemy import create_engine, text

pytest.importorskip("mcp")

from lang2sql.frontends.codex.app import command, run
from lang2sql.frontends.mcp.server import McpSettings
from lang2sql.tenancy.mcp_runtime import build_gateway


@pytest.fixture
def settings(tmp_path):
    url = f"sqlite:///{tmp_path / 'data.sqlite'}"
    engine = create_engine(url)
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (amount INTEGER)"))
        conn.execute(text("INSERT INTO orders VALUES (200)"))
    engine.dispose()
    return McpSettings(
        url, None, str(tmp_path / "state.sqlite"), "reader", "org", "sales"
    )


def test_missing_identity_and_db_fail_closed(monkeypatch):
    for key in (
        "LANG2SQL_DB_URL",
        "LANG2SQL_USER_ID",
        "LANG2SQL_ORG_ID",
        "LANG2SQL_TEAM_ID",
    ):
        monkeypatch.delenv(key, raising=False)
    with pytest.raises(ValueError, match="Missing MCP settings"):
        McpSettings.from_env()


def test_context_first_readonly_catalog_and_sql_gate(settings):
    async def check():
        gateway = await build_gateway(settings)

        async def no_inner_llm(*args, **kwargs):
            raise AssertionError("MCP must never invoke an inner LLM")

        gateway.ctx.llm.complete = no_inner_llm
        assert {s.name for s in gateway.specs()} == {
            "get_context",
            "explore_schema",
            "run_sql",
            "ask_user",
        }
        result = await gateway.call("run_sql", {"sql": "SELECT * FROM orders"})
        assert result.is_error and "get_context" in result.content
        result = await gateway.call("get_context", {})
        assert "Team: sales" in result.content and settings.db_url not in result.content
        assert (await gateway.call("run_sql", {"sql": "DROP TABLE orders"})).is_error
        assert (
            "200"
            in (await gateway.call("run_sql", {"sql": "SELECT * FROM orders"})).content
        )
        assert (
            await gateway.call("term_custom", {"term": "x", "definition": "bad"})
        ).is_error
        gateway.ctx.store.close()

    asyncio.run(check())


def test_clarification_latches_all_tools_including_context(settings):
    async def check():
        gateway = await build_gateway(settings)
        await gateway.call("get_context", {})
        results = await asyncio.gather(
            gateway.call("ask_user", {"question": "충성고객 기준?"}),
            gateway.call("run_sql", {"sql": "SELECT * FROM orders"}),
        )
        assert all(r.stop_turn for r in results)
        assert (await gateway.call("get_context", {})).stop_turn
        assert (
            await gateway.call("run_sql", {"sql": "SELECT * FROM orders"})
        ).stop_turn
        events = await gateway.ctx.store.query(settings.user_id)
        assert not any(e.action == "run_sql" for e in events)
        gateway.ctx.store.close()

    asyncio.run(check())


def test_access_denial_latches_server(settings):
    from lang2sql.core.ports.explorer import DataAccessError

    async def check():
        gateway = await build_gateway(settings)
        await gateway.call("get_context", {})

        async def denied(*args, **kwargs):
            raise DataAccessError("ORA-00942")

        gateway.ctx.explorer.execute = denied
        result = await gateway.call("run_sql", {"sql": "SELECT * FROM salaries"})
        assert result.stop_turn and result.is_error
        assert "권한이 없거나" in result.content
        assert (await gateway.call("explore_schema", {})).stop_turn
        gateway.ctx.store.close()

    asyncio.run(check())


def test_real_stdio_initialize_list_and_query(settings):
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    async def check():
        env = dict(
            os.environ,
            LANG2SQL_DB_URL=settings.db_url,
            LANG2SQL_DB_SCHEMA="",
            LANG2SQL_DATA_PATH=settings.data_path,
            LANG2SQL_USER_ID=settings.user_id,
            LANG2SQL_ORG_ID=settings.org_id,
            LANG2SQL_TEAM_ID=settings.team_id,
        )
        params = StdioServerParameters(
            command=sys.executable,
            args=["-m", "lang2sql.frontends.mcp.server"],
            env=env,
        )
        async with stdio_client(params) as (read, write):
            async with ClientSession(read, write) as client:
                await client.initialize()
                catalog = await client.list_tools()
                assert len(catalog.tools) == 4
                assert all(t.annotations.readOnlyHint for t in catalog.tools)
                await client.call_tool("get_context", {})
                result = await client.call_tool(
                    "run_sql", {"sql": "SELECT SUM(amount) FROM orders"}
                )
                assert not result.isError
                assert "200" in result.structuredContent["text"]
                result = await client.call_tool(
                    "ask_user", {"question": "Which metric?"}
                )
                assert result.structuredContent["stop_turn"]
                result = await client.call_tool(
                    "run_sql", {"sql": "SELECT * FROM orders"}
                )
                assert result.isError and result.structuredContent["stop_turn"]

    asyncio.run(check())


def test_codex_command_disables_other_execution_paths():
    cmd = command()
    assert "--ignore-user-config" in cmd and "--ephemeral" in cmd
    assert "read-only" in cmd and "features.shell_tool=false" in cmd
    assert "features.multi_agent=false" in cmd
    assert "mcp_servers.lang2sql.required=true" in cmd
    assert not any("oracle+oracledb://" in value for value in cmd)


@pytest.mark.parametrize(
    "events,exit_code,raises",
    [
        (
            [
                {
                    "type": "item.completed",
                    "item": {
                        "type": "mcp_tool_call",
                        "server": "lang2sql",
                        "tool": "get_context",
                        "status": "completed",
                    },
                },
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "answer"},
                },
                {"type": "turn.completed"},
            ],
            0,
            False,
        ),
        (
            [
                {
                    "type": "item.completed",
                    "item": {"type": "agent_message", "text": "unverified"},
                },
                {"type": "turn.completed"},
            ],
            0,
            True,
        ),
        ([{"type": "turn.failed"}], 0, True),
        ([], 0, True),
        ([], 1, True),
    ],
)
def test_codex_process_completion_checks(monkeypatch, events, exit_code, raises):
    class Process:
        pid = 123
        returncode = exit_code

        async def communicate(self, prompt):
            assert b"get_context" in prompt
            return "\n".join(json.dumps(e) for e in events).encode(), b"test error"

    async def spawn(*args, **kwargs):
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    if raises:
        with pytest.raises(RuntimeError):
            asyncio.run(run("매출?", env={}))
    else:
        assert asyncio.run(run("매출?", env={}))["answer"] == "answer"


def test_timeout_kills_child_process_group(monkeypatch):
    from lang2sql.frontends.codex import app

    killed = []

    class Process:
        pid = 123

        async def communicate(self, prompt):
            await asyncio.sleep(10)

        async def wait(self):
            killed.append("waited")

        def kill(self):
            killed.append("killed")

    async def spawn(*args, **kwargs):
        return Process()

    monkeypatch.setattr(asyncio, "create_subprocess_exec", spawn)
    monkeypatch.setattr(app.os, "killpg", lambda pid, sig: killed.append(pid))
    with pytest.raises(asyncio.TimeoutError):
        asyncio.run(run("매출?", env={}, timeout=0.01))
    assert killed == ([123, "waited"] if os.name == "posix" else ["killed", "waited"])
