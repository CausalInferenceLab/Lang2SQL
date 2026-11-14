"""Run one natural-language question with Codex as the agent and Lang2SQL as MCP."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import sys
import tempfile
from pathlib import Path

from ..mcp.server import ENV_KEYS, McpSettings

logger = logging.getLogger(__name__)


def command(model: str | None = None) -> list[str]:
    """A dedicated, read-only Codex session with only this MCP connection."""
    overrides = {
        "approval_policy": '"never"',
        "web_search": '"disabled"',
        "features.shell_tool": "false",
        "features.apps": "false",
        "features.multi_agent": "false",
        "mcp_servers.lang2sql.command": json.dumps(sys.executable),
        "mcp_servers.lang2sql.args": '["-m", "lang2sql.frontends.mcp.server"]',
        "mcp_servers.lang2sql.env_vars": json.dumps(list(ENV_KEYS)),
        "mcp_servers.lang2sql.required": "true",
        "mcp_servers.lang2sql.default_tools_approval_mode": '"approve"',
    }
    cmd = [
        "codex",
        "exec",
        "--ignore-user-config",
        "--ephemeral",
        "--skip-git-repo-check",
        "--sandbox",
        "read-only",
        "--json",
        "--color",
        "never",
    ]
    for key, value in overrides.items():
        cmd += ["-c", f"{key}={value}"]
    if model:
        cmd += ["--model", model]
    return cmd + ["-"]


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
    # Empty cwd prevents a user's repository instructions/files becoming analytics context.
    with tempfile.TemporaryDirectory(prefix="lang2sql-codex-") as cwd:
        proc = await asyncio.create_subprocess_exec(
            *command(model),
            cwd=cwd,
            start_new_session=(os.name == "posix"),
            env=env,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        logger.info("codex started pid=%s", proc.pid)
        try:
            stdout, stderr = await asyncio.wait_for(
                proc.communicate(prompt.encode()), timeout
            )
        except (asyncio.TimeoutError, asyncio.CancelledError):
            logger.error("codex interrupted or timed out pid=%s", proc.pid)
            if os.name == "posix":
                os.killpg(proc.pid, signal.SIGKILL)
            else:
                proc.kill()
            await proc.wait()
            raise
        if proc.returncode:
            logger.error("codex failed exit=%s", proc.returncode)
            raise RuntimeError(
                f"Codex exited {proc.returncode}: {stderr.decode()[-2000:]}"
            )
        events = [
            json.loads(line) for line in stdout.decode().splitlines() if line.strip()
        ]
        if any(e.get("type") in {"error", "turn.failed"} for e in events):
            raise RuntimeError("Codex reported a failed turn")
        messages = [
            e["item"]["text"]
            for e in events
            if e.get("type") == "item.completed"
            and e.get("item", {}).get("type") == "agent_message"
        ]
        if not messages or not any(e.get("type") == "turn.completed" for e in events):
            raise RuntimeError("Codex did not complete an answer")
        context_calls = [
            e["item"]
            for e in events
            if e.get("type") == "item.completed"
            and e.get("item", {}).get("type") == "mcp_tool_call"
            and e["item"].get("server") == "lang2sql"
            and e["item"].get("tool") == "get_context"
            and e["item"].get("status") == "completed"
            and not e["item"].get("error")
        ]
        if not context_calls:
            raise RuntimeError("Codex completed without reading Lang2SQL context")
        logger.info("codex completed events=%d", len(events))
        return {
            "runtime": "codex-mcp",
            "model": model or "Codex CLI default",
            "answer": messages[-1],
            "events": events,
        }


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
