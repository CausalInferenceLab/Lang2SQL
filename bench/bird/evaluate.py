"""Prepare, generate, score and compare independent BIRD SQLite runs."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
from importlib import import_module
from pathlib import Path

from .comparison import compare
from .dataset import SMOKE_LIMIT, load_cases, load_manifest, prepare, verify_databases
from .io import ProtocolError
from .protocol import (
    BUDGETS,
    CONDITIONS,
    PROFILES,
    TRANSPORT,
    benchmark_commit,
    product_source,
    settings,
)
from .scoring import check_gold, require_gold_check, score


async def generate(args: argparse.Namespace, commit: str) -> dict:
    manifest = load_manifest(args.prepared)
    cases = load_cases(args.prepared, manifest)
    require_gold_check(args.prepared, manifest, BUDGETS)
    databases = verify_databases(args.db_dir, manifest)
    product_tree = args.product_tree.resolve()
    source = product_source(product_tree, args.product_commit)
    snapshot = settings(
        manifest,
        condition=args.condition,
        profile_name=args.profile,
        product_commit=args.product_commit,
        product_tree=product_tree,
        commit=commit,
    )
    transport = None
    if manifest["smoke_cases"] is None:
        if args.transport != TRANSPORT:
            raise ProtocolError(f"Benchmark runs require --transport {TRANSPORT}")
        os.environ["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
        name, function = args.transport.split(":")
        transport = getattr(import_module(name), function)
    elif args.transport is not None:
        raise ProtocolError("A smoke cohort uses the built-in scripted transport")

    sys.path.insert(0, str(source))
    product = import_module("lang2sql")
    if Path(product.__file__).resolve() != source / "lang2sql" / "__init__.py":
        raise ProtocolError(
            "Imported Lang2SQL does not come from the specified checkout"
        )
    from .runner import run_evaluation

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
            stop,
            resume=args.resume,
        )
    finally:
        for num, handler in handlers.items():
            signal.signal(num, handler)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest="command", required=True)
    prepare_cmd = commands.add_parser(
        "prepare", help="Verify and project the frozen cohort"
    )
    gold_cmd = commands.add_parser("check-gold", help="Verify reference SQL execution")
    run_cmd = commands.add_parser(
        "run", help="Generate one condition and model profile"
    )
    score_cmd = commands.add_parser("score", help="Score one completed run on SQLite")
    compare_cmd = commands.add_parser(
        "compare", help="Compare two complete product matrices"
    )

    for command in (prepare_cmd, gold_cmd, score_cmd):
        command.add_argument("--source", required=True, type=Path)
    for command in (prepare_cmd, gold_cmd, run_cmd, score_cmd):
        command.add_argument("--db-dir", required=True, type=Path)
    for command in (prepare_cmd, run_cmd, score_cmd, compare_cmd):
        command.add_argument("--output", required=True, type=Path)
    for command in (gold_cmd, run_cmd):
        command.add_argument("--prepared", required=True, type=Path)
    prepare_cmd.add_argument(
        "--smoke-cases",
        type=int,
        choices=range(1, SMOKE_LIMIT + 1),
        help="Prepare a separate scripted smoke cohort",
    )
    run_cmd.add_argument("--condition", choices=CONDITIONS, required=True)
    run_cmd.add_argument("--profile", choices=PROFILES, required=True)
    run_cmd.add_argument("--product-tree", type=Path, required=True)
    run_cmd.add_argument(
        "--product-commit", required=True, help="Full product commit SHA"
    )
    run_cmd.add_argument("--transport", choices=(TRANSPORT,))
    run_cmd.add_argument("--resume", action="store_true")
    compare_cmd.add_argument("--baseline", type=Path, required=True)
    compare_cmd.add_argument("--candidate", type=Path, required=True)
    compare_cmd.add_argument("--smoke", action="store_true")
    return root


def main(argv: list[str] | None = None) -> int:
    cli = parser()
    args = cli.parse_args(argv)
    try:
        commit = benchmark_commit()
        if args.command == "prepare":
            manifest = prepare(args.source, args.db_dir, args.output, args.smoke_cases)
            result = {"status": "prepared", "questions": len(manifest["cases"])}
        elif args.command == "check-gold":
            result = check_gold(args.prepared, args.source, args.db_dir)
        elif args.command == "run":
            result = asyncio.run(generate(args, commit))
        elif args.command == "score":
            result = score(args.output, args.source, args.db_dir, commit)
        else:
            result = compare(
                args.baseline, args.candidate, args.output, commit, smoke=args.smoke
            )
    except (OSError, ValueError, ImportError, KeyError) as error:
        cli.exit(1, f"{error}\n")
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 130 if result.get("status") == "interrupted" else 0


if __name__ == "__main__":
    raise SystemExit(main())
