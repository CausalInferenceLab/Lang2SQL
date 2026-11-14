"""Run one natural-language question with Codex as the agent and Lang2SQL as MCP."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
from pathlib import Path

from ..mcp.server import ENV_KEYS, McpSettings
from ...adapters.llm import codex_process

logger = logging.getLogger(__name__)


def _mcp_config() -> dict[str, str]:
    return {
        "mcp_servers.lang2sql.command": json.dumps(sys.executable),
        "mcp_servers.lang2sql.args": '["-m", "lang2sql.frontends.mcp.server"]',
        "mcp_servers.lang2sql.env_vars": json.dumps(list(ENV_KEYS)),
        "mcp_servers.lang2sql.required": "true",
        "mcp_servers.lang2sql.default_tools_approval_mode": '"approve"',
    }


def command(model: str | None = None) -> list[str]:
    return codex_process.command(model, overrides=_mcp_config())


async def run(
    question: str,
    *,
    env: dict[str, str],
    model: str | None = None,
    timeout: float = 180,
) -> dict:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if not question.strip():
        raise ValueError("question must not be empty")
    prompt = (
        "You are answering an analytics question through the lang2sql MCP server. "
        "Call get_context first and follow its definitions and SQL dialect. "
        "Use the MCP tools for all data; do not invent SQL results. "
        "For undefined ambiguous business terms call ask_user before running SQL. "
        "A tool result with stop_turn=true ends this request: relay the message and call no more tools. "
        "Answer in Korean, explain the applied business definition and show the successful SQL. "
        "Never use shell, files, web, other agents, or other data connections.\n\nQuestion:\n"
        + question
    )
    result = await codex_process.execute(
        prompt, env=env, model=model, timeout=timeout, overrides=_mcp_config()
    )
    context_calls = [
        e["item"]
        for e in result["events"]
        if e.get("type") == "item.completed"
        and e.get("item", {}).get("type") == "mcp_tool_call"
        and e["item"].get("server") == "lang2sql"
        and e["item"].get("tool") == "get_context"
        and e["item"].get("status") == "completed"
        and not e["item"].get("error")
    ]
    if not context_calls:
        raise RuntimeError("Codex completed without reading Lang2SQL context")
    return {"runtime": "codex-mcp", **result}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("question")
    parser.add_argument("--model")
    parser.add_argument("--timeout", type=float, default=180)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    McpSettings.from_env()  # Fail before invoking Codex if identity/DB are missing.
    logging.basicConfig(level=logging.INFO)
    result = asyncio.run(
        run(args.question, env=dict(os.environ), model=args.model, timeout=args.timeout)
    )
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n")
    print(result["answer"])


if __name__ == "__main__":
    main()
