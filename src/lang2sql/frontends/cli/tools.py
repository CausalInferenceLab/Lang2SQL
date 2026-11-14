"""Invoke existing tools directly, without an LLM or MCP transport."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any

from jsonschema import ValidationError, validate

from ...core.identity import Identity
from ...tenancy.tool_runtime import build_tool_context

logger = logging.getLogger(__name__)


async def execute(ctx, calls):
    results = []
    for call in calls:
        call_id = uuid.uuid4().hex
        logger.info("cli_tool_start tool=%s call_id=%s", call["name"], call_id)
        result = await ctx.tools.dispatch(call["name"], call["arguments"], ctx, call_id)
        results.append(asdict(result))
        if result.is_error:
            logger.warning("cli_tool_failed tool=%s call_id=%s", call["name"], call_id)
            break
        logger.info("cli_tool_done tool=%s call_id=%s", call["name"], call_id)
    return results


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--user-id", required=True)
    parser.add_argument("--org-id", required=True)
    parser.add_argument("--team-id")
    parser.add_argument("--admin", action="store_true")
    parser.add_argument("--store", default=":memory:")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="Print tool names and JSON argument schemas")
    call_parser = commands.add_parser("call", help="Invoke one tool")
    call_parser.add_argument("name")
    batch = commands.add_parser(
        "batch", help="Run a JSON array of calls in one process"
    )
    for command in (call_parser, batch):
        source = command.add_mutually_exclusive_group()
        source.add_argument(
            "--args", default=None, help="JSON object, or array for batch"
        )
        source.add_argument(
            "--args-file", help="Read JSON from a file; '-' reads stdin"
        )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO)
    ctx = None
    output: dict[str, Any]
    try:
        if not args.user_id.strip() or not args.org_id.strip():
            raise ValueError("user and org IDs must not be blank")
        ctx = build_tool_context(
            Identity(
                user_id=args.user_id,
                guild_id=args.org_id,
                channel_id=args.team_id,
                is_admin=args.admin,
            ),
            store_path=args.store,
        )
        specs = {spec.name: spec for spec in ctx.tools.specs()}
        if args.command == "list":
            output = {
                "tools": [
                    {
                        **asdict(spec),
                        "parameters": {
                            **spec.parameters,
                            "additionalProperties": False,
                        },
                    }
                    for spec in specs.values()
                ]
            }
            status = 0
        else:
            raw = (
                args.args
                if args.args is not None
                else ("[]" if args.command == "batch" else "{}")
            )
            if args.args_file:
                raw = (
                    sys.stdin.read()
                    if args.args_file == "-"
                    else Path(args.args_file).read_text()
                )
            value = json.loads(raw)
            calls = (
                value
                if args.command == "batch"
                else [{"name": args.name, "arguments": value}]
            )
            validate(
                calls,
                {
                    "type": "array",
                    "minItems": 1,
                    "items": {
                        "type": "object",
                        "required": ["name", "arguments"],
                        "properties": {
                            "name": {"type": "string"},
                            "arguments": {"type": "object"},
                        },
                        "additionalProperties": False,
                    },
                },
            )
            # Validate the whole batch before any tool performs a metadata write.
            for call in calls:
                if call["name"] not in specs:
                    raise ValueError(f"unknown tool: {call['name']}")
                validate(
                    call["arguments"],
                    {**specs[call["name"]].parameters, "additionalProperties": False},
                )
            results = asyncio.run(execute(ctx, calls))
            output = (
                results[0]
                if args.command == "call"
                else {
                    "results": results,
                    "completed": len(results),
                    "requested": len(calls),
                }
            )
            status = 1 if any(result["is_error"] for result in results) else 0
    except (ValueError, OSError, ValidationError) as exc:
        logger.warning("cli_input_failed error_type=%s", type(exc).__name__)
        output = {"is_error": True, "content": str(exc)}
        status = 2
    finally:
        if ctx is not None and ctx.store is not None:
            ctx.store.close()
    print(json.dumps(output, ensure_ascii=False))
    return status


if __name__ == "__main__":
    raise SystemExit(main())
