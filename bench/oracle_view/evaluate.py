"""Compare text and bound definitions over real stdio MCP sessions and the same Oracle data."""

import asyncio
import json
import os
import sys
import tempfile
import time
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

from lang2sql.adapters.llm.openai_ import OpenAILLM
from lang2sql.core.types import Message, Role, ToolSpec

TERM = "순매출"
DEFINITION = "확정(POSTED) 주문의 총액에서 할인과 반품을 차감한 원화 합계. NULL 할인·반품은 0. 주문일 기준."
MODEL = "gpt-4.1-mini-2025-04-14"
CASES = [
    ("2025년 9월", 25),
    ("2025년 10월", 118),
    ("2025년 11월", 10),
    ("2025년 12월", None),
]
SYSTEM = """Use the tools to query Oracle and return the numeric answer (or NULL). Resolve all business terms first.
Use the returned contract for run_sql. A period_end is exclusive. Ask rather than invent definitions.
The reader has SELECT on L2S_VIEW_OWNER.ORDERS(DAY,GROSS,DISCOUNT,RETURNED,STATUS)
and L2S_VIEW_OWNER.V_NET_SALES(BOOKED_ON,NET_AMOUNT,STATUS)."""


async def question(path, month, expected, llm):
    spec = StdioServerParameters(
        command=sys.executable,
        args=[
            "-m",
            "lang2sql.frontends.mcp.server",
            "--store",
            str(path),
            "--user-id",
            "colleague",
            "--org-id",
            "demo",
            "--team-id",
            "sales",
        ],
        env={
            "LANG2SQL_DB_URL": os.environ["LANG2SQL_DB_URL"],
            "LANG2SQL_DB_SCHEMA": "l2s_view_owner",
        },
    )
    messages = [Message(Role.SYSTEM, SYSTEM), Message(Role.USER, f"{month} 순매출은?")]
    outcome = {
        "question": messages[-1].content,
        "expected": expected,
        "calls": 0,
        "executed": False,
        "correct_result": False,
        "used_view": False,
        "raw_table": False,
        "tool_calls": [],
    }
    async with stdio_client(spec) as transport, ClientSession(*transport) as client:
        initialized = await client.initialize()
        messages[0].content += "\n" + (initialized.instructions or "")
        specs = [
            ToolSpec(t.name, t.description or "", t.inputSchema)
            for t in (await client.list_tools()).tools
            if t.name in ("resolve_business_terms", "run_sql", "explore_schema")
        ]
        for _ in range(8):
            outcome["calls"] += 1
            turn = await llm.complete(messages, specs)
            messages.append(
                Message(Role.ASSISTANT, turn.content, tool_calls=turn.tool_calls)
            )
            if not turn.tool_calls:
                outcome["answer"] = turn.content

                break
            for call in turn.tool_calls:
                outcome["tool_calls"].append(call.name)
                result = await client.call_tool(call.name, call.arguments)
                content = result.content[0].text
                messages.append(
                    Message(Role.TOOL, content, name=call.name, tool_call_id=call.id)
                )
                if call.name == "run_sql" and not result.isError:
                    if content.startswith("{"):
                        evidence = json.loads(content)
                        sql = evidence["sql"]
                        values = list(evidence["rows"][0].values())
                    else:
                        rows, _, sql = content.partition("\nSQL: ")
                        lines = rows.splitlines()
                        values = [lines[3].strip()] if len(lines) == 4 else []
                    outcome.update(
                        executed=True,
                        sql=sql,
                        values=values,
                        used_view="V_NET_SALES" in sql.upper(),
                        raw_table="L2S_VIEW_OWNER.ORDERS" in sql.upper(),
                        correct_result=len(values) == 1
                        and (
                            str(values[0]) in ("None", "NULL")
                            if expected is None
                            else str(values[0]) == str(expected)
                        ),
                    )
    return outcome


async def main():
    contract = Path(__file__).with_name("contract.json")
    report = {"model": MODEL, "max_calls": 8, "conditions": {}}
    with tempfile.TemporaryDirectory() as tmp:
        for mode in ("text", "bound"):
            path = Path(tmp) / (mode + ".sqlite")
            common = [
                "--store",
                str(path),
                "--user-id",
                "registrar",
                "--org-id",
                "demo",
                "--team-id",
                "sales",
            ]
            started = time.monotonic()
            proc = await asyncio.create_subprocess_exec(
                sys.executable,
                "-m",
                "lang2sql.frontends.cli.tools",
                *common,
                "call",
                "term_custom",
                "--args",
                json.dumps({"term": TERM, "definition": DEFINITION, "kind": "metric"}),
                stdout=asyncio.subprocess.DEVNULL,
            )
            if await proc.wait():
                raise RuntimeError("definition save failed")
            if mode == "bound":
                proc = await asyncio.create_subprocess_exec(
                    sys.executable,
                    "-m",
                    "lang2sql.frontends.cli.tools",
                    *common,
                    "bind-oracle-view",
                    "--term",
                    TERM,
                    "--definition",
                    DEFINITION,
                    "--layer",
                    "team",
                    "--contract",
                    str(contract),
                    stdout=asyncio.subprocess.DEVNULL,
                )
                if await proc.wait():
                    raise RuntimeError("view confirmation failed")
            setup = time.monotonic() - started
            records = await asyncio.gather(
                *(question(path, m, e, OpenAILLM(model=MODEL)) for m, e in CASES)
            )
            report["conditions"][mode] = {"setup_seconds": setup, "records": records}
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
