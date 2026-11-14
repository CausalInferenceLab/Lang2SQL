"""One asynchronous Codex subprocess implementation for all product paths."""

from __future__ import annotations
import asyncio
import json
import logging
import os
import signal
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


def command(
    model: str | None = None, *, overrides: dict[str, str] | None = None
) -> list[str]:
    config = {
        "approval_policy": '"never"',
        "web_search": '"disabled"',
        "features.shell_tool": "false",
        "features.apps": "false",
        "features.multi_agent": "false",
    }
    config.update(overrides or {})
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
    for key, value in config.items():
        cmd += ["-c", f"{key}={value}"]
    if model:
        cmd += ["--model", model]
    return cmd + ["-"]


async def execute(
    prompt: str,
    *,
    env: dict[str, str],
    model: str | None = None,
    timeout: float = 180,
    overrides: dict[str, str] | None = None,
    schema: dict | None = None,
) -> dict:
    if timeout <= 0:
        raise ValueError("timeout must be positive")
    if not prompt.strip():
        raise ValueError("prompt must not be empty")
    # Empty cwd prevents a user's repository instructions/files becoming analytics context.
    with tempfile.TemporaryDirectory(prefix="lang2sql-codex-") as cwd:
        cmd = command(model, overrides=overrides)
        if schema is not None:
            schema_path = Path(cwd) / "response.schema.json"
            schema_path.write_text(json.dumps(schema))
            cmd[-1:-1] = ["--output-schema", str(schema_path)]
        proc = await asyncio.create_subprocess_exec(
            *cmd,
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
        logger.info("codex completed events=%d", len(events))
        return {
            "model": model or "Codex CLI default",
            "answer": messages[-1],
            "events": events,
        }
