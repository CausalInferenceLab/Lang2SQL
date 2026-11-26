"""Pinned Spider2/BIRD execution accuracy over the real Lang2SQL harness.

Gold SQL is loaded only by score; inference uses an allowlisted input manifest.
The existing Spider runner owns provider calls, read-only execution and records.
"""

from __future__ import annotations

import argparse
import ast
import asyncio
import json
import logging
import os
import random
import subprocess
import time
from pathlib import Path

from bench.spider2.evaluate import (
    digest,
    read_connection,
    run_case,
    validate_manifest,
    write_json,
)
from bench.spider2.evaluate import (
    score as score_spider,
)
from bench.spider2.providers import ProviderQuotaExhausted

logger = logging.getLogger(__name__)
BIRD_REVISION = "5b256b5a9c17478f06ed6912612e9a14d17065c8"
BIRD_QUESTIONS_SHA = "926af2a0d1b47e5d0d15a7aa8a34d4721007f4550a881555b6a89496b54a2606"
SCHEMA_SHA = {
    "bird": "5176e324a72e80a8a37a12285596d123e0ba6c310dceaa9f13a1e413ee88ff5b",
    "spider2": "195a0e722bb4e670fda45308e583203fccd7a4979b412a05215ca525e7aec5bf",
}
SEED = 20261126
INPUT_KEYS = {
    "instance_id",
    "db",
    "question",
    "evidence",
    "external_knowledge",
    "document_sha256",
}


def split_manifest(manifest, pilot_size):
    """Freeze development IDs before model generation or inspection of errors."""
    ids = sorted(c["instance_id"] for c in manifest["cases"])
    random.Random(SEED).shuffle(ids)
    manifest["development_ids"] = ids[:pilot_size]
    manifest["holdout_ids"] = ids[pilot_size:]
    manifest["split_seed"] = SEED
    return manifest


def prepare(args):
    if args.benchmark == "spider2":
        manifest = json.loads(Path("bench/spider2/manifest-sqlite135.json").read_text())
        validate_manifest(manifest, args.root, args.db_dir)
    else:
        if digest(args.questions) != BIRD_QUESTIONS_SHA:
            raise ValueError("BIRD annotations differ from pinned release")
        rows = [json.loads(line) for line in args.questions.read_text().splitlines()]
        # Do not retain gold SQL or any other annotation in inference input.
        cases = [
            {
                "instance_id": f"bird{r['question_id']:03d}",
                "db": r["db_id"],
                "question": r["question"],
                "evidence": r["evidence"],
            }
            for r in rows
        ]
        manifest = {
            "benchmark": "BIRD Mini-Dev SQLite 500 (2026-10-25)",
            "upstream_commit": BIRD_REVISION,
            "question_file_sha256": digest(args.questions),
            "evaluator_sha256": digest(args.evaluator),
            "cases": cases,
            "db_sha256": {
                db: digest(args.db_dir / (db + ".sqlite"))
                for db in sorted({c["db"] for c in cases})
            },
        }
    manifest["suite"] = args.benchmark
    write_json(args.output, split_manifest(manifest, args.pilot_size))


def validate_inputs(manifest, db_dir):
    ids = [c["instance_id"] for c in manifest["cases"]]
    if not ids or len(ids) != len(set(ids)):
        raise ValueError("Empty or duplicate cases")
    for case in manifest["cases"]:
        if set(case) - INPUT_KEYS:
            raise ValueError("Unexpected inference fields; gold must never be supplied")
    for db, sha in manifest["db_sha256"].items():
        if digest(db_dir / (db + ".sqlite")) != sha:
            raise ValueError("Database changed: " + db)


def load_schema_documents(path, suite):
    """Only the pinned official schema map is accepted, never annotated SQL rows."""
    if path is None:
        return {}
    if digest(path) != SCHEMA_SHA.get(suite):
        raise ValueError("Schema documentation must match the pinned benchmark release")
    documents = json.loads(path.read_text())
    if not isinstance(documents, dict) or not all(
        isinstance(db, str) and isinstance(doc, str) for db, doc in documents.items()
    ):
        raise ValueError("Schema documentation must be a database-to-text map")
    return documents


async def run(args):
    from dotenv import dotenv_values

    manifest = json.loads(args.manifest.read_text())
    validate_inputs(manifest, args.db_dir)
    args.schema_documents = load_schema_documents(args.schema_docs, manifest["suite"])
    if args.schema_documents and any(
        c["db"] not in args.schema_documents for c in manifest["cases"]
    ):
        raise ValueError("Missing database schema documentation")
    if manifest["suite"] == "spider2":
        validate_manifest(manifest, args.root, args.db_dir)
    if args.split != "all":
        selected = set(manifest[args.split + "_ids"])
        manifest["cases"] = [
            c for c in manifest["cases"] if c["instance_id"] in selected
        ]
    # A fixed product revision is required; benchmark source is hashed separately.
    await asyncio.to_thread(
        subprocess.run,
        ["git", "diff", "--exit-code", args.product_commit, "--", "src"],
        check=True,
    )
    if args.output.exists():
        raise ValueError("Output exists; never overwrite or cherry-pick attempts")
    profiles = json.loads(args.profiles.read_text())
    env = {**os.environ, **(dotenv_values(args.env_file) if args.env_file else {})}
    if not profiles or any(not env.get(p["api_key_env"]) for p in profiles):
        raise ValueError("All requested profiles need credentials before starting")
    args.output.mkdir(parents=True)
    settings = {
        "profiles": profiles,
        "split": args.split,
        "product_commit": args.product_commit,
        "manifest_sha256": digest(args.manifest),
        "schema_document_sha256": digest(args.schema_docs)
        if args.schema_docs
        else None,
        "sources": {str(p): digest(p) for p in Path("bench").rglob("*.py")},
        "limits": {
            k: getattr(args, k)
            for k in (
                "max_calls",
                "call_timeout",
                "case_timeout",
                "sql_timeout",
                "max_rows",
                "concurrency",
                "review_rounds",
            )
        },
    }
    write_json(args.output / "manifest.json", manifest)
    write_json(args.output / "settings.json", settings)
    for profile in profiles:
        directory = args.output / profile["name"]
        for sub in ("sql", "csv", "records"):
            (directory / sub).mkdir(parents=True)
        semaphore = asyncio.Semaphore(args.concurrency)
        exhausted = asyncio.Event()

        async def bounded(
            case,
            semaphore=semaphore,
            directory=directory,
            profile=profile,
            exhausted=exhausted,
        ):
            async with semaphore:
                if exhausted.is_set():
                    raise ProviderQuotaExhausted("Provider account quota exhausted")
                try:
                    await run_case(
                        case,
                        args.root,
                        args.db_dir,
                        directory,
                        profile,
                        env[profile["api_key_env"]],
                        args,
                    )
                except ProviderQuotaExhausted:
                    exhausted.set()
                    raise

        started = time.monotonic()
        tasks = [asyncio.create_task(bounded(case)) for case in manifest["cases"]]
        try:
            await asyncio.gather(*tasks)
        except ProviderQuotaExhausted:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            write_json(
                directory / "status.json",
                {
                    "status": "aborted",
                    "reason": "quota_exhausted",
                    "wall_seconds": time.monotonic() - started,
                },
            )
            logger.error(
                "Benchmark aborted profile=%s reason=quota_exhausted", profile["name"]
            )
            raise
        write_json(
            directory / "status.json",
            {"status": "completed", "wall_seconds": time.monotonic() - started},
        )


def load_bird_comparator(path, expected_sha):
    """Execute only the pinned upstream pure calculate_ex function, unchanged."""
    if digest(path) != expected_sha:
        raise ValueError("BIRD evaluator changed")
    module = ast.parse(path.read_text())
    functions = [
        n
        for n in module.body
        if isinstance(n, ast.FunctionDef) and n.name == "calculate_ex"
    ]
    if len(functions) != 1:
        raise ValueError("Missing official calculate_ex")
    namespace = {}
    exec(  # noqa: S102 — only the hash-pinned upstream comparator
        compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"),
        namespace,
    )
    return namespace["calculate_ex"]


def query_rows(db, sql, timeout, max_rows):
    conn = read_connection(db, time.monotonic() + timeout)
    try:
        rows = conn.execute(sql).fetchmany(max_rows + 1)
        if len(rows) > max_rows:
            raise ValueError("Row budget exceeded")
        return rows
    finally:
        conn.close()


def score(args):
    manifest = json.loads((args.output / "manifest.json").read_text())
    validate_inputs(manifest, args.db_dir)
    if manifest["suite"] == "spider2":
        return score_spider(args.root, args.output)
    if digest(args.questions) != manifest["question_file_sha256"]:
        raise ValueError("Gold annotations changed")
    comparator = load_bird_comparator(args.evaluator, manifest["evaluator_sha256"])
    gold = {
        f"bird{r['question_id']:03d}": r
        for r in map(json.loads, args.questions.read_text().splitlines())
    }
    settings = json.loads((args.output / "settings.json").read_text())
    summaries = []
    for profile in settings["profiles"]:
        directory = args.output / profile["name"]
        if json.loads((directory / "status.json").read_text())["status"] != "completed":
            raise ValueError("Cannot publish partial-run accuracy")
        results = []
        for case in manifest["cases"]:
            ident = case["instance_id"]
            record = json.loads((directory / "records" / (ident + ".json")).read_text())
            db = args.db_dir / (case["db"] + ".sqlite")
            # A broken gold/database pairing invalidates scoring, never drops a case.
            reference = query_rows(
                db, gold[ident]["SQL"], args.sql_timeout, args.max_rows
            )
            result = {"instance_id": ident, "score": 0, "status": record["status"]}
            if record["status"] == "predicted":
                try:
                    predicted = query_rows(
                        db, record["sql"], args.sql_timeout, args.max_rows
                    )
                    result["score"] = comparator(predicted, reference)
                except Exception as exc:  # noqa: BLE001 — prediction errors remain in denominator
                    result["error_type"] = type(exc).__name__
                    logger.warning(
                        "Scoring prediction failed id=%s error=%s",
                        ident,
                        type(exc).__name__,
                    )
            results.append(result)
        write_json(directory / "scores.json", results)
        summary = {
            "profile": profile["name"],
            "correct": sum(r["score"] for r in results),
            "total": len(results),
            "accuracy": sum(r["score"] for r in results) / len(results),
        }
        summaries.append(summary)
    write_json(args.output / "summary.json", summaries)
    print(json.dumps(summaries, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "run", "score"])
    parser.add_argument("--benchmark", choices=["spider2", "bird"])
    for name in (
        "root",
        "db-dir",
        "output",
        "manifest",
        "profiles",
        "env-file",
        "questions",
        "evaluator",
        "schema-docs",
    ):
        parser.add_argument("--" + name, type=Path)
    parser.add_argument(
        "--split", choices=["development", "holdout", "all"], default="development"
    )
    parser.add_argument(
        "--product-commit", default="e7f0b0644883b6e3fa760e706fd396cc6730e846"
    )
    parser.add_argument("--pilot-size", type=int, default=20)
    parser.add_argument("--review-rounds", type=int, choices=[0, 1, 2], default=0)
    parser.add_argument("--max-calls", type=int, default=16)
    parser.add_argument("--call-timeout", type=float, default=180)
    parser.add_argument("--case-timeout", type=float, default=900)
    parser.add_argument("--sql-timeout", type=float, default=30)
    parser.add_argument("--max-rows", type=int, default=100000)
    parser.add_argument("--concurrency", type=int, default=4)
    args = parser.parse_args()
    if args.root:
        args.root = args.root.resolve()
    if args.output:
        args.output = args.output.resolve()
    logging.basicConfig(level=logging.INFO)
    if args.command == "run":
        asyncio.run(run(args))
    elif args.command == "prepare":
        prepare(args)
    else:
        score(args)


if __name__ == "__main__":
    main()
