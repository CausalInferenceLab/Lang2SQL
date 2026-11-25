"""Open real Codex with one registered demo MCP, or record an actual model turn."""

import argparse
import json
import os
import sys
from pathlib import Path

import tomllib

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "bench" / "oracle_view"))
from long_context import run_turn

TOOLS = [
    "resolve_business_terms",
    "explore_schema",
    "run_sql",
    "context_candidates",
    "remember",
    "recall_memory",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--thread")
    parser.add_argument("--question")
    parser.add_argument("--timeout", type=int, default=240)
    args = parser.parse_args()
    config = tomllib.loads((Path.home() / ".codex/config.toml").read_text())
    servers = config.get("mcp_servers", {})
    server = servers[args.server]
    settings = {
        "sandbox_mode": "read-only",
        "approval_policy": "never",
        "check_for_update_on_startup": False,
        **(
            {
                f"mcp_servers.{name}.enabled": False
                for name in servers
                if name != args.server
            }
            if not args.question
            else {}
        ),
        **{
            f"mcp_servers.{args.server}.{key}": value
            for key, value in {
                **server,
                "enabled": True,
                "enabled_tools": TOOLS,
            }.items()
        },
    }
    # Per-invocation read/candidate/note tools; approval is NOT exposed to the model.
    for tool in TOOLS:
        settings[f"mcp_servers.{args.server}.tools.{tool}.approval_mode"] = "approve"
    if args.question:
        if args.output is None or args.output.exists():
            parser.error("Use a new --output file for evidence")
        thread, report = run_turn(
            settings,
            str(Path(__file__).parent),
            args.question,
            args.thread,
            args.timeout,
            None,
        )
        report["thread_id"] = thread
        report["question"] = args.question
        if report["exit_code"]:
            print(report["stderr"], file=sys.stderr)
        args.output.write_text(json.dumps(report, ensure_ascii=False, indent=2))
        for event in report["events"]:
            item = event.get("item", {})
            if event.get("type") == "item.completed":
                if item.get("type") == "mcp_tool_call":
                    print(
                        "\nMCP >",
                        item["tool"],
                        json.dumps(item["arguments"], ensure_ascii=False),
                    )
                    for block in (item.get("result") or {}).get("content", []):
                        if "text" in block:
                            print(block["text"])
                elif item.get("type") == "agent_message":
                    print("\nCodex >", item["text"])
        print(f"\nElapsed: {report['elapsed_seconds']:.1f}s; thread: {thread}")
        raise SystemExit(report["exit_code"])
    command = ["codex"] + (["resume", args.thread] if args.thread else [])
    command += ["--no-alt-screen", "-C", str(Path(__file__).parent)]
    for key, value in settings.items():
        command += ["-c", key + "=" + json.dumps(value)]
    os.execvp(command[0], command)


if __name__ == "__main__":
    main()
