"""Prepare, run and score independent BIRD SQLite evaluations."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from types import SimpleNamespace

from bench.common.io import ProtocolError
from bench.common.providers import Transport

from .dataset import load_cases, load_manifest, prepare, verify_databases
from .protocol import (
    BUDGETS,
    CONDITIONS,
    DEFAULT_PROFILES,
    load_profile,
    settings,
    verify_product,
)
from .runner import run_evaluation
from .scoring import check_gold, require_gold_check, score


async def live_transport(**request):
    from litellm import acompletion

    return await acompletion(**request)


async def dry_run_transport(**request):
    raw = {
        "model": request["model"],
        "choices": [
            {
                "message": {"content": "```sql\nSELECT 1\n```"},
                "finish_reason": "stop",
            }
        ],
    }
    return SimpleNamespace(model_dump=lambda: raw)


async def generate(
    args: argparse.Namespace,
    profile: dict[str, str],
    transport: Transport,
    api_key: str,
    provider_version: str,
) -> dict:
    verify_product()
    manifest = load_manifest(args.prepared)
    cases = load_cases(args.prepared, manifest)
    if not args.dry_run:
        require_gold_check(args.prepared, manifest, BUDGETS)
    databases = verify_databases(args.db_dir, manifest)
    snapshot = {
        **settings(
            manifest,
            BUDGETS,
            provider_version,
            condition=args.condition,
            profile=profile,
            mode="dry-run" if args.dry_run else "live",
        ),
        "run_id": args.output.name,
    }
    stop = asyncio.Event()

    def stop_after_active_cases(_signum, _frame) -> None:
        stop.set()

    handlers = {
        num: signal.signal(num, stop_after_active_cases)
        for num in (signal.SIGINT, signal.SIGTERM)
    }
    try:
        return await run_evaluation(
            args.output,
            manifest,
            snapshot,
            cases,
            databases,
            transport,
            api_key,
            stop,
            resume=args.resume,
        )
    finally:
        for num, handler in handlers.items():
            signal.signal(num, handler)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    subcommands = root.add_subparsers(dest="command", required=True)
    prepare_cmd = subcommands.add_parser(
        "prepare", help="Freeze inputs and DB checksums"
    )
    run_cmd = subcommands.add_parser(
        "run", help="Generate SQL for one condition and model profile"
    )
    score_cmd = subcommands.add_parser("score", help="Score an independent run offline")

    for command in (prepare_cmd, run_cmd, score_cmd):
        command.add_argument("--output", type=Path, required=True)
        command.add_argument("--db-dir", type=Path, required=True)
    for command in (prepare_cmd, score_cmd):
        command.add_argument(
            "--source", type=Path, required=True, help="Pinned SQLite JSONL annotations"
        )
    prepare_cmd.add_argument(
        "--db-revision", required=True, help="SQLite archive hash or image digest"
    )
    run_cmd.add_argument("--prepared", type=Path, required=True)
    run_cmd.add_argument("--condition", choices=CONDITIONS, required=True)
    run_cmd.add_argument("--profile", required=True, help="Model profile name")
    run_cmd.add_argument(
        "--profiles", type=Path, default=DEFAULT_PROFILES, help="Model profiles JSON"
    )
    run_cmd.add_argument(
        "--dry-run",
        action="store_true",
        help="Check first-request prompts with a scripted transport",
    )
    run_cmd.add_argument(
        "--resume",
        action="store_true",
        help="Resume an interrupted run with the same settings",
    )
    score_cmd.add_argument(
        "--gold-only", action="store_true", help="Check gold SQL in the prepared output"
    )
    return root


def main(argv: list[str] | None = None) -> int:
    cli = parser()
    args = cli.parse_args(argv)
    try:
        if args.command == "prepare":
            manifest = prepare(args.source, args.db_dir, args.db_revision, args.output)
            result = {"status": "prepared", "questions": len(manifest["cases"])}
        elif args.command == "run":
            profile = load_profile(args.profiles, args.profile)
            if args.dry_run:
                result = asyncio.run(
                    generate(args, profile, dry_run_transport, "", "scripted")
                )
            else:
                key_env = profile["api_key_env"]
                key = os.environ.get(key_env, "").strip()
                if not key:
                    raise ProtocolError(f"Profile {profile['name']} requires {key_env}")
                try:
                    provider_version = version("litellm")
                except PackageNotFoundError:
                    raise ProtocolError(
                        "Install the benchmark extra before starting a run"
                    ) from None
                result = asyncio.run(
                    generate(args, profile, live_transport, key, provider_version)
                )
        else:
            function = check_gold if args.gold_only else score
            result = function(args.output, args.source, args.db_dir)
    except (OSError, ValueError) as error:
        cli.exit(1, f"{error}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 130 if result.get("status") == "interrupted" else 0


if __name__ == "__main__":
    raise SystemExit(main())
